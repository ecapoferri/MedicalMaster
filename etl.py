"""
Used to extrack data from ATT and AnswerFirst reporting files in
    the lake repos and load to data sink DB for production.
"""
import configparser
import logging
import traceback
from datetime import datetime, timedelta
from os import chdir
from os import environ as os_environ
from os import system as os_system
from pathlib import Path
from sys import stdout
from threading import Thread
from time import perf_counter

import dotenv
from dotenv import load_dotenv

# For verifying DB updates.
from db_engines import RPRT_DB
from db_engines import WH_DB as DB
from db_engines import MySQL_OpErr, check_connection
from etl_af_repos import main as af
# Individual ETL functions.
from etl_att_repos import main as att
from etl_client_key import main as client
from etl_f_in_house_leads import main as inhouse
from logging_setup import HDLR_FILE, HDLR_STRM
# Configuration dictionaries for ETL of certain downstream tables.
from table_config import AF_CFGS, ATT_FILE_CFG

PERF_START = perf_counter()
CALLING_DIR = Path().cwd()
# Must be set in env on host/container.
ROOT_PATH = Path(os_environ['APPS_ROOT'])
APP_PATH = ROOT_PATH / 'PM_MedMaster'
chdir(APP_PATH)

load_dotenv(ROOT_PATH / '.env')

conf = configparser.ConfigParser()
conf.read('.conf')
conf.read(ROOT_PATH / 'app.conf')
conf.read(ROOT_PATH / 'conn.conf')

LOGGER = logging.getLogger(conf['DEFAULT']['LOGGER_NAME'])

########## CONFIG CONSTANTS ##########
# Date string formats for the data vintage timestamp and SQL query dates.
TMSTMP_FMT: str = r'%Y-%m-%d %H:%M:%S'
QUERY_DATE_FMT: str = r'%Y-%m-%d'

REPOS_PATH = Path(os_environ['LOCAL_STORAGE'])

TODAY: str = datetime.now().strftime(QUERY_DATE_FMT)
# Command to restore views after the table truncate inserts.
XTRA_SQL = Path('billables_views.sql').read_text()

AF_GLOB: str = AF_CFGS['src_label']
ATT_GLOB: str = ATT_FILE_CFG['src_label']
# Number of recent files to check for mtimes.
REPO_CHECK_RNG = 5

LOGGER = logging.getLogger(conf['DEFAULT']['LOGGER_NAME'])


def db_connection_check():
    """Checks for active database connection. If the query is
        unsuccessful, we bail on executing
        the module.

    Raises:
        Exception: If the query is unsuccessful.
    """
    for d in DB, RPRT_DB:
        try:
            check_connection(d)
        except MySQL_OpErr:
            logging.error(f"Bad Connection\n{traceback.format_exc}")
            raise Exception(f"SEE BELOW/ABOVE")
        else:
            pass
    return


def check_repo_vintages():
    """Checks vintages of <RNG> most recent files in the lake repository.
        Prints log messages which are color coded based on recency so
        the user can quickly check whether daily reports subscriptions
        are flowing."""
    ansi = '\x1b[{clr}m'
    good_ansi = '93'
    bad_ansi = '1;91'
    good_ansi = ansi.format(clr=good_ansi)
    ansi_rst = '\x1b[0m'
    bad_ansi = ansi.format(clr=bad_ansi)
    rpo_chk_prstr = (
        "❇️{an}{nm}{anr}, source or top of glob for ({ds}) "
        + "Repos Vintage: {an}{ts}{anr}"
    )


    # get recent mtimes
    af_files: list[Path]
    att_files: list[Path]
    af_files, att_files = (
        list(REPOS_PATH.rglob(glob))
        for glob in (AF_GLOB, ATT_GLOB)
    )

    for list_, mtime_ in ((af_files, 'af_message_data'), (att_files, 'att_data')):
        list_.sort(reverse=True, key=lambda path_: path_.stat().st_mtime)

        for i in range(REPO_CHECK_RNG):
            path_ = list_[i]
            mtime_ = datetime.fromtimestamp(path_.stat().st_mtime)
            tmst = mtime_.strftime(TMSTMP_FMT)
            name_ = list_[i].name

            now, dlt = datetime.now(), timedelta(hours=(16))
            clr = good_ansi if (now - mtime_ < dlt) else bad_ansi
            del now, dlt

            LOGGER.info(rpo_chk_prstr.format(
                an=clr, anr=ansi_rst, nm=name_, ds=mtime_, ts=tmst))
    return


def main():
    db_connection_check()
    check_repo_vintages()

    att_thr = Thread(target=att)
    af_thr = Thread(target=af)
    client_thr = Thread(target=client)
    inhouse_thr = Thread(target=inhouse)
    
    threads: tuple[Thread,...] = (
        af_thr,
        att_thr,
        client_thr,
        inhouse_thr,
    )

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    # Re-instate views after truncate-inserts.
    #   (which involve DROP ... <table|view> CASCADE queries).
    LOGGER.info(
        f"Re-instating SQL views (replacing after 'DROP ... CASCADE' preparation queries)."
    )
    with DB.connect() as conn:
        conn.execute(XTRA_SQL)

    return


if __name__ == "__main__":
    try:
        LOGGER.addHandler(HDLR_STRM)
        LOGGER.addHandler(HDLR_FILE)
        LOGGER.setLevel(logging.INFO)
        main()
    finally:
        LOGGER.debug(f"Run duration: {perf_counter() - PERF_START :.4f}")
        for h in HDLR_STRM, HDLR_STRM:
            h.close()

chdir(CALLING_DIR)