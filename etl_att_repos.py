# HEADERS
from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame as Df

from pathlib import Path
from io import BytesIO
import gzip
import re
import json

from os import environ as os_environ
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy.types import TypeEngine

from table_config import ATT_FILE_CFG, VNTGE_VW_SQL, VNTGE_FMT, DATE_OUT_FLDNM
from db_engines import db_load, WH_DB as DB

from logging import getLogger

REPOS_PATH = Path(os_environ['PRMDIA_EVAN_LOCAL_LAKEPATH'])

PHONE_PATH = os_environ['PRMDIA_MM_PHONE_MAP_PTH']

USE_COLS: list[str | int] = ATT_FILE_CFG['use_cols']
RENAME: dict[str, str] = ATT_FILE_CFG['rename']
SKIPHEAD: int = ATT_FILE_CFG['skiphead']

# column by which to remove bad records with a pd.dataframe.dropna
# and with values listed in the BAD_FILT_VALS list
FILT_COL: str = ATT_FILE_CFG['other_']['non_null_filt']
BAD_FILT_VALS: list = ATT_FILE_CFG['other_']['bad_filt_vals']

# regex pattern used to extract af acct num from source file name
# (itself derived from the email subject)
RE_PTN: str = ATT_FILE_CFG['other_']['acct_reptn']

ASTYPE: dict[str, str] = ATT_FILE_CFG['astype']

# columns subset to eval duplicate records
DUP_SUBSET: list[str] = ATT_FILE_CFG['other_']['dup_subset']

# list in config with column names
DTPARTS: dict[str, tuple[str]] = ATT_FILE_CFG['other_']['dtparts']
# name of timestamp column
DATETIME_COL: str = list(DTPARTS.keys())[0]
# name of column with date part of datetime
DATETIME_DATE: str = list(DTPARTS.values())[0][0]
# name of column with time part of datetime
DATETIME_TIME: str = list(DTPARTS.values())[0][1]

# name of column with af account
ACCT_COL: str = ATT_FILE_CFG['other_']['acct_col']
# name of column with toll number dialed
TOLL_COL: str = 'number_dial'

# string addressing source file paths
FILE_RGLOB: str = ATT_FILE_CFG['src_label']
TBLNM: str = ATT_FILE_CFG['tblnm']

DTYPE: dict[str, TypeEngine] = ATT_FILE_CFG['dtype']
PRESQL: list[str] = ATT_FILE_CFG['pre_sql']
XTRASQL: list[str] = ATT_FILE_CFG['xtra_sql']
VNTGE_VW: str = ATT_FILE_CFG['vintage_view_nm']


def get_toll_map() -> dict:
    phone_path = Path(PHONE_PATH)

    return {
        int(k): int(v) for k, v in
        json.loads(phone_path.read_text()).items()
    }


def get_latest_vntge(paths_: list[Path]) -> datetime:
    mts: list[float] = [
            p.stat().st_mtime for p in paths_
        ]
    mx: float = max(mts)

    return datetime.fromtimestamp(mx)


def read_append(path_list: list[Path]) -> Df:
    df_ = Df()

    # TODO: #1 THREAD THIS OUT: DO A THREAD POOL OR MULTIPROCESSING POOL OR
    #  WHATEVER AND PD.CONCAT THE RESULTING DFS,
    #  THERE'S A WAY TO GET A LIST OF RESULTS FROM A LIST OF THREADS/PROCESSES
    for file_pth in path_list:
        # get acct number from filename
        st = re.search(RE_PTN, file_pth.name).start()
        lmt = re.search(RE_PTN, file_pth.name).end() - 3
        acct = int(file_pth.name[st:lmt])

        tab_bytes = BytesIO(gzip.decompress(file_pth.read_bytes()))
        df_read: Df = pd.read_csv(
            tab_bytes, header=None, sep='\t'
        )
        # empty table files will throw an error with pd.read_csv
        # this string shows up in the first cell of the file
        if re.findall('No records found for report', df_read.iat[0,0]):
            continue

        tab_bytes = BytesIO(gzip.decompress(file_pth.read_bytes()))
        df_part: Df = (
                pd.read_csv(
                    tab_bytes,
                    header=None,
                    skiprows=SKIPHEAD,
                    usecols=USE_COLS,
                    sep='\t'
                )
                .rename(columns=RENAME)
            )
        df_part = df_part.loc[
                pd.notna(df_part[FILT_COL])
                &
                ~df_part[FILT_COL].isin(BAD_FILT_VALS)
            ]
        # assign acct number
        df_part = df_part.assign(acct_af=acct)

        df_ = (
            pd.concat([df_, df_part])
            .reset_index(drop=True)
        )

    return df_


def clean(df__: Df):

    toll_map = get_toll_map()

    df__ = df__.drop_duplicates(subset=DUP_SUBSET).reset_index(drop=True)

    # date and time are separate and ... (central time in the source...there's...weirdness).
    # need to convert, combine, localize, and convert
    df__[DATETIME_COL] = (
        # this makes a pd.Series
        (
            pd.to_datetime(df__[DATETIME_DATE])
            +
            pd.to_timedelta(df__[DATETIME_TIME])
        )
        # .dt.tz_localize(tz='UTC')
        .dt.tz_localize(tz='US/Central')
    )
    df__ = df__.drop(columns=[DATETIME_DATE, DATETIME_TIME])

    df__[DATE_OUT_FLDNM] = df__[DATETIME_COL].dt.date

    df__ = df__.astype(ASTYPE)

    # map af_acct via toll list to avoid lag in human updates...
    df__ = df__.drop(columns=[ACCT_COL])
    df__[ACCT_COL] = df__[TOLL_COL].map(toll_map)

    return df__


def main():
    path_list: list[Path] = list(REPOS_PATH.rglob(FILE_RGLOB))

    LOGGER = getLogger(f"{os_environ['PRMDIA_MM_LOGNAME']}")

    # get data vintage and append sql query to create view
    vntge: datetime = get_latest_vntge(path_list)
    xtrasql: list[str] = XTRASQL + [
        VNTGE_VW_SQL.format(
            nm=VNTGE_VW,
            ts=vntge.strftime(VNTGE_FMT)
        )
    ]

    # df = read_append(path_list=path_list).pipe(clean)
    # read in from source files
    df = read_append(path_list=path_list)

    # cleanup:
    #   dates/times
    #   assign AF acct by toll num
    df = clean(df)

    db_load(
        tblnm=TBLNM,
        db=DB,
        df=df,
        dtype=DTYPE,
        presql=PRESQL,
        xtrasql=xtrasql,
    )

    # this will broadcast to the outer LOGGER via name hierarchy
    LOGGER.info(f"\x1b[36;1mSuccessfully loaded {TBLNM} to {DB.engine}\x1b[0m")

    return


if __name__ == "__main__":
    main()
