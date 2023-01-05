# HEADERS
import configparser
import logging
import re
import traceback
from datetime import datetime
from os import chdir
from os import environ as os_environ
from pathlib import Path
from time import perf_counter

import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame as Df
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.types import TypeEngine

from db_engines import WH_DB as DB
from db_engines import db_load
from logging_setup import HDLR
from table_config import (AF_CFGS, DATE_OUT_FLDNM, ENUM_SQL, VNTGE_FMT,
                          VNTGE_VW_SQL)

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

VNTGE_FMT = config['PM']['VNTGE_FMT']

REPOS_PATH = Path(config['PM']['LOCAL_STORAGE'])

SKIPHEAD: int = AF_CFGS['skiphead']
RENAME: dict[str, str] = AF_CFGS['rename']
FILTER_FLD: str = AF_CFGS['other_']['filter_fld']
REMAP: dict[str, dict[str, str | bool]] = AF_CFGS['other_']['remap']
ASTYPE: dict[str, TypeEngine] = AF_CFGS['astype']

# string for globbing paths
AF_GLOB: str = AF_CFGS['src_label']
# table name for db, also used as a general label
TBLNM: str = AF_CFGS['tblnm']
# dtype map
DTYPE: dict[str, TypeEngine] = AF_CFGS['dtype']
# string to format with data vintage timestamp
VNTGE_VW: str = AF_CFGS['vintage_view_nm']
# enums that are already defined
ENUMS: dict[str, str] = AF_CFGS['other_']['enums']
PRE_SQL: list[str] = AF_CFGS['pre_sql']

LOGGER = logging.getLogger(f"{config['DEFAULT']['LOGGER_NAME']}")

def data_vintage_timestamp(paths: list[Path]) -> str:
    timestamp_lst: list[float] = [
            f.stat().st_mtime for f in paths
        ]
    timestamp_lst.sort(reverse=True)
    return datetime.fromtimestamp(timestamp_lst[0]).strftime(VNTGE_FMT)


def load_xls_sheet(f: Path, skiphead) -> Df:
    xl = pd.ExcelFile(f)
    load_df = Df()
    for sh in xl.sheet_names:
        load_df = pd.concat([
            load_df,
            xl.parse(sh, skiprows=skiphead)
        ])
    return load_df


def et_(paths: list[Path]) -> Df:

    # initilize a df to add parsed tables
    accum_df = Df()

    for f in paths:

        # get acct number from filename
        acct_num: int = int(
                re.sub(
                    r'_|\.', '',
                    re.findall(
                        r'_\d*\.',
                        f.name
                    )[0]
                )
            )
        
        xl = pd.ExcelFile(f)
        
        accum_df = pd.concat([
            accum_df,
            *[
                xl.parse(sh, skiprows=SKIPHEAD)
                # add column with acct number, based on filename
                .assign(acct=acct_num)
                for sh in xl.sheet_names
            ]
        ])

    # Remove junk columns to avoid column bloat
    #   from random junk columns across the source files.
    use_cols: list[str] = [
        c for c in list(accum_df.columns)
        if not re.findall(r'Unnamed: \d', c)
    ]

    # Select down to intended columns and rename.
    accum_df = (
        accum_df[use_cols]
        .convert_dtypes()
        .rename(columns=RENAME)
    )

    # Removes junk rows/records.
    accum_df = accum_df.loc[accum_df[FILTER_FLD].notna()]

    # Remap to better values, such as Yes/No to booleans
    #   structure in input config is <column_name>: {<old_value>: <new_value>}
    #   i.e.: {'call_for_ad': {'Yes': True, 'No': False}
    for c, m in REMAP.items():
        accum_df[c] = accum_df[c].map(m, na_action='ignore')

    # Tidy up dtypes.
    accum_df = accum_df.astype(ASTYPE)

    accum_df[DATE_OUT_FLDNM] = accum_df['connected'].dt.date

    return accum_df

def main():
    # enums that will need to capture their values from the DF categories
    enums_to_update: dict[str, str] = {
        k: AF_CFGS['fields'][k]['enum_name'] for k in
        AF_CFGS['fields'].keys()
        if
            (AF_CFGS['fields'][k]['enum_name'] != None)
            &
            (not AF_CFGS['fields'][k]['dtype'] != None)
    }

    # PARSE PATHS
    af_files: list[Path] = list(REPOS_PATH.rglob(AF_GLOB))

    # get data vintage
    tmstmp = data_vintage_timestamp(paths=af_files)

    # update post load sql query with a view holding data vintage
    xtrasql: list[str] = [
        VNTGE_VW_SQL.format(nm=VNTGE_VW, ts=tmstmp)
    ]

    df: Df = et_(paths=af_files)

    # capture enum values to update enum dtype in db
    enums: dict[str, str] = ENUMS
    for e, n in enums_to_update.items():
        # create string of enum values
        enums.update({
            e: ENUM(*list(df[e].cat.categories), name=n)
        })

    # SQL queries to prep db.
    #   In this case dropping and replacing enums and dropping table to replace
    presql: list[str] = PRE_SQL + [
        ENUM_SQL.format(e)
        for e in enums.keys()
    ]

    # add captured enum types with new values to dtype map
    dtype: dict[str, TypeEngine] =\
        DTYPE.update({k: v for k, v in enums.items()})

    db_load(
        db=DB,
        df=df,
        tblnm=TBLNM,
        dtype=dtype,
        presql=presql,
        xtrasql=xtrasql
    )
    LOGGER.info(f"\x1b[36;1mSuccessfully loaded {TBLNM} to {DB.engine}\x1b[0m")


if __name__ == "__main__":
    try:
        LOGGER.addHandler(HDLR)
        LOGGER.setLevel(logging.DEBUG)
        main()
    finally:
        LOGGER.debug(f"Run duration: {perf_counter() - START:.4f}")
        HDLR.close()

chdir(CWD)