"""
Used to extrack data from ATT and AnswerFirst reporting files in
    the lake repos and load to data sink DB for production.
"""
# IMPORTS
import logging
import traceback
from os import environ as os_environ, system as os_system
from sys import stdout

from datetime import datetime, timedelta

from pathlib import Path
from threading import Thread

# For verifying DB updates.
from db_engines import RPRT_DB, WH_CONN_STR, WH_DB as DB,\
    MySQL_OpErr, check_connection
# Configuration dictionaries for ETL of certain downstream tables.
from table_config import AF_CFGS, ATT_FILE_CFG
# Individual ETL functions.
from etl_att_repos import main as att
from etl_af_repos import main as af
from etl_client_key import main as client
from etl_f_in_house_leads import main as inhouse

import dotenv

dotenv.load_dotenv()

########## CONFIG CONSTANTS ##########
# Date string formats for the data vintage timestamp and SQL query dates.
TMSTMP_FMT: str = r'%Y-%m-%d %H:%M:%S'
QUERY_DATE_FMT: str = r'%Y-%m-%d'

REPOS_PATH = Path(os_environ['PRMDIA_EVAN_LOCAL_LAKEPATH'])

TODAY: str = datetime.now().strftime(QUERY_DATE_FMT)
# Command to restore views after the table truncate inserts.
XTRA_SQL_FILE = 'billables_views.sql'
PSQL_CMD: str = f"psql --file={XTRA_SQL_FILE} {WH_CONN_STR}"
# Glob strings to locate the relevant files in the lake repository.
AF_GLOB: str = AF_CFGS['src_label']
ATT_GLOB: str = ATT_FILE_CFG['src_label']
# Number of recent files to check for mtimes.
REPO_CHECK_RNG = 5

########## LOGGING SETUP ##########
LOG_FMT_DATE_STRM = r'%y%m%d|%H%M'
LOG_FMT_DATE_FILE = r'%Y-%m-%d %H:%M:%S'
LOG_FMT_FILE =\
    '%(asctime)s [%(name)s,%(funcName)s,%(module)s::%(levelname)s]>>%(message)s'
LOG_FMT_STRM =\
    '\x1b[32m%(asctime)s[%(name)s %(levelname)s]\x1b[0m >> %(message)s'

LOGGER = logging.getLogger(os_environ['PRMDIA_MM_LOGNAME'])
hdlr = logging.StreamHandler(stdout)
hdlr.setFormatter(
    logging.Formatter(
        fmt=LOG_FMT_STRM, datefmt=LOG_FMT_DATE_STRM))
# hdlr.setLevel(logging.DEBUG)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


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
    os_system(PSQL_CMD)
    return


if __name__ == "__main__":
    main()
