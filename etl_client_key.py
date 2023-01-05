import configparser
import logging
import traceback
from json import loads as json_loads
from os import chdir
from os import environ as os_environ
from pathlib import Path
from time import perf_counter

from dotenv import load_dotenv
from pandas import DataFrame as Df

from db_engines import WH_DB as DB
from logging_setup import HDLR

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

# Unpack Config
TBL_CONFIG: dict[str, str|int] = json_loads(
    Path(os_environ['PRMDIA_MM_CLIENT_MAP_PTH']).read_text())

TBLNM: str = TBL_CONFIG['tblnm']
ASTYPE: dict[str, str] = TBL_CONFIG['astype']
PRE_SQL: list[str] = TBL_CONFIG['pre_sql']
DF_KEY = 'map'

def main():
    df = (
        Df.from_dict(TBL_CONFIG[DF_KEY])
        .convert_dtypes()
        .astype(ASTYPE)
    )

    with DB.connect() as conn:
        for s in PRE_SQL:
            conn.execute(s)
        df.to_sql(
            name=TBLNM,
            con=conn,
            index=False,
            if_exists='replace'
        )

    LOGGER.info(
        f"\x1b[36;1mSuccessfully loaded {TBLNM} to {DB.engine}\x1b[0m")

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