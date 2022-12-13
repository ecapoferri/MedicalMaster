from google.cloud import bigquery

import json
from pandas import DataFrame as Df, Series as Ser
import pandas as pd

from pathlib import Path
from os import environ as os_environ
from dotenv import load_dotenv
import logging
import traceback

from table_config import INHOUSE_LEADS_TIMESTAMP_FLDNM as TIMESTAMP_FLDNM,\
    CALLERID_FLDNM, INHOUSE_LEADS_CFGS as CFGS,\
    INHOUSE_LEAD_DATE_FLDNM as DATE_FLDNM

from db_engines import WH_DB as DB

load_dotenv()
CLIENT_KEY: dict = json.loads(
    Path(os_environ['PRMDIA_MM_CLIENT_MAP_PTH']).read_text())['map']
CLIENT_KEY_USECOLS: list[str] = ['practice_id', 'lead_email_form']

BQ_SQL = CFGS['src_label']
ASTYPE = CFGS['astype']
DTYPE = CFGS['dtype']
TBLNM = CFGS['tblnm']
PRE_SQL = CFGS['pre_sql']

LOGGER = logging.getLogger(f"{os_environ['PRMDIA_MM_LOGNAME']}")


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
    main()
