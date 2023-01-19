"""TODO: DOCSTRING"""
import configparser
import logging
from json import loads as json_loads
from os import chdir
from os import environ as os_environ
from pathlib import Path
from time import perf_counter

from dotenv import load_dotenv
from pandas import DataFrame as Df

from db_engines import WH_DB as DB
from logging_setup import HDLR_STRM

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

# Unpack Config
TBL_CONFIG: dict[str, str|int] = json_loads(
    Path(conf['INTERNAL_RESOURCES']['MM_CLIENT_MAP_PTH'])\
    .read_text(encoding='utf-8'))

TBLNM: str = TBL_CONFIG['tblnm']
ASTYPE: dict[str, str] = TBL_CONFIG['astype']
PRE_SQL: list[str] = TBL_CONFIG['pre_sql']
DF_KEY = 'map'

def main():
    """TODO: DOCSTRING"""
    the_df = (
        Df.from_dict(TBL_CONFIG[DF_KEY])
        .convert_dtypes()
        .astype(ASTYPE)
    )

    with DB.connect() as conn:
        for query_ in PRE_SQL:
            conn.execute(query_)
        the_df.to_sql(
            name=TBLNM,
            con=conn,
            index=False,
            if_exists='replace'
        )


    LOGGER.info('%sSuccessfully loaded %s to %s%s',
                '\x1b[36;1m', TBLNM, DB.engine, '\x1b[0m',)

    return

if __name__ == "__main__":
    try:
        LOGGER.addHandler(HDLR_STRM)
        LOGGER.setLevel(logging.DEBUG)
        main()
    finally:
        LOGGER.debug('Run duration: %s', f"{perf_counter() - PERF_START :.4f}")
        HDLR_STRM.close()

chdir(CALLING_DIR)
