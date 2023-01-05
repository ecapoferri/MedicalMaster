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
from logging_setup import HDLR
# Configuration dictionaries for ETL of certain downstream tables.
from table_config import AF_CFGS, ATT_FILE_CFG

START = perf_counter()

load_dotenv('./.env')
load_dotenv('../.env')

CWD = Path().cwd()
chdir(os_environ['APP_PATH'])

config = configparser.ConfigParser()
config.read('.conf')
config.read('../app.conf')
config.read('../conn.conf')

LOGGER = logging.getLogger(config['DEFAULT']['LOGGER_NAME'])

########## CONFIG CONSTANTS ##########
# Date string formats for the data vintage timestamp and SQL query dates.
TMSTMP_FMT: str = r'%Y-%m-%d %H:%M:%S'
QUERY_DATE_FMT: str = r'%Y-%m-%d'

REPOS_PATH = Path(config['PM']['LOCAL_STORAGE'])

TODAY: str = datetime.now().strftime(QUERY_DATE_FMT)
# Command to restore views after the table truncate inserts.
XTRA_SQL = Path('billables_views.sql').read_text()

AF_GLOB: str = AF_CFGS['src_label']
ATT_GLOB: str = ATT_FILE_CFG['src_label']
# Number of recent files to check for mtimes.
REPO_CHECK_RNG = 5

LOGGER = logging.getLogger(os_environ['PRMDIA_MM_LOGNAME'])


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

    for l, t in ((af_files, 'af_message_data'), (att_files, 'att_data')):
        l.sort(reverse=True, key=lambda p: p.stat().st_mtime)

        for i in range(REPO_CHECK_RNG):
            p = l[i]
            t = datetime.fromtimestamp(p.stat().st_mtime)
            ts = t.strftime(TMSTMP_FMT)
            nm = l[i].name

            now, dlt = datetime.now(), timedelta(hours=(16))
            clr = good_ansi if (now - t < dlt) else bad_ansi
            del now, dlt

            LOGGER.info(rpo_chk_prstr.format(
                an=clr, anr=ansi_rst, nm=nm, ds=t, ts=ts))
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
        LOGGER.addHandler(HDLR)
        LOGGER.setLevel(logging.DEBUG)
        main()
    finally:
        LOGGER.debug(f"Run duration: {perf_counter() - START:.4f}")
        HDLR.close()

chdir(CWD)