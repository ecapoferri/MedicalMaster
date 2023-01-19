import configparser
import json
import logging
import traceback
from os import chdir
from os import environ as os_environ
from pathlib import Path
from time import perf_counter

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from pandas import DataFrame as Df
from pandas import Series as Ser

from db_engines import WH_DB as DB
from logging_setup import HDLR_STRM
from table_config import CALLERID_FLDNM
from table_config import INHOUSE_LEAD_DATE_FLDNM as DATE_FLDNM
from table_config import INHOUSE_LEADS_CFGS as CFGS
from table_config import INHOUSE_LEADS_TIMESTAMP_FLDNM as TIMESTAMP_FLDNM

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
CLIENT_KEY: dict = json.loads(
    Path(conf['INTERNAL_RESOURCES']['MM_CLIENT_MAP_PTH']).read_text())['map']
CLIENT_KEY_USECOLS: list[str] = ['practice_id', 'lead_email_form']

BQ_SQL = CFGS['src_label']
ASTYPE = CFGS['astype']
DTYPE = CFGS['dtype']
TBLNM = CFGS['tblnm']
PRE_SQL = CFGS['pre_sql']

LOGGER = logging.getLogger(conf['DEFAULT']['LOGGER_NAME'])


def extract_bq(query: str) -> Df:
    return (
        bigquery.Client()
        .query(query)
        .to_dataframe()
        .convert_dtypes()
    )

def transform(df_: Df) -> Df:
    df_c = df_.copy()
    # Fix inconsistent date format to datetime
    df_c[TIMESTAMP_FLDNM] = pd.to_datetime(df_c[TIMESTAMP_FLDNM])
    # Create delivery date field.
    df_c[DATE_FLDNM] = pd.to_datetime(df_c[TIMESTAMP_FLDNM].dt.date)

    # Phone numbers to int's.
    df_c[CALLERID_FLDNM] = (
        df_c[CALLERID_FLDNM]
        .str.replace(regex=True, pat=r'\D', repl='')
        .astype('Int64')
    )

    df_c = df_c.astype(ASTYPE)

    return df_c


def map_to_internal_keys(df__: Df) -> Df:
    # Collapse the client key json into a dictionary of
    #   {{practice: practice},...}
    ids = Df.from_dict(CLIENT_KEY)[CLIENT_KEY_USECOLS]
    ids = ids.loc[ids[CLIENT_KEY_USECOLS[1]].notna()]
    ids.index = ids.index.astype(int)
    ids = ids.set_index(CLIENT_KEY_USECOLS[0])
    # reverse keys and values
    id_map = {v: k for k, v in ids[CLIENT_KEY_USECOLS[1]].to_dict().items()}
    df_c_ = df__.copy()
    df_c_['practice_id'] = df_c_['practice'].map(id_map)
    df_c_ = df_c_.drop(columns=['practice'])
    return df_c_


def load(df_load: Df, presql: list[str]) -> None:
    with DB.connect() as conn:
        for s in presql:
            conn.execute(s)
        df_load.to_sql(
            name=TBLNM, con=conn, index=False, if_exists='replace', dtype=DTYPE)
    return


def main():
    df = (
        extract_bq(BQ_SQL)
        .pipe(transform)
        .pipe(map_to_internal_keys)
    )
    load(df_load=df, presql=PRE_SQL)

    # this will broadcast to the outer LOGGER via name hierarchy
    LOGGER.info(f"\x1b[36;1mSuccessfully loaded {TBLNM} to {DB.engine}\x1b[0m")

    return


if __name__ == "__main__":
    try:
        LOGGER.addHandler(HDLR_STRM)
        LOGGER.setLevel(logging.DEBUG)
        main()
    finally:
        LOGGER.debug(f"Run duration: {perf_counter() - PERF_START :.4f}")
        HDLR_STRM.close()

chdir(CALLING_DIR)