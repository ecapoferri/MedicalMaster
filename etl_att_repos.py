# %%
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

from table_config import att_file_cfg, vntge_vw_sql, vntge_fmt
from db_engines import db_load, wh_db as db

from logging import getLogger, Logger
import traceback

REPOS_PTH = Path(os_environ['PRMDIA_EVAN_LOCAL_LAKEPATH'])


# %%
def get_toll_map() -> dict:
    PHONE_PATH = Path(os_environ['PRMDIA_MM_PHONE_MAP_PTH'])

    return {
        int(k): int(v) for k, v in
        json.loads(PHONE_PATH.read_text()).items()
    }

def get_latest_vntge(paths_: list[Path]) -> datetime:
    mts: list[float] = [
            p.stat().st_mtime for p in paths_
        ]
    mx: float = max(mts)

    return datetime.fromtimestamp(mx)


def read_append(path_list: list[Path]) -> Df:
    USE_COLS: list[str | int] = att_file_cfg['use_cols']
    RENAME: dict[str, str] = att_file_cfg['rename']
    SKIPHEAD: int = att_file_cfg['skiphead']
    FILT_COL: str = att_file_cfg['other_']['non_null_filt']
    BAD_FILT_VALS: list = att_file_cfg['other_']['bad_filt_vals']
    RE_PTN: str = att_file_cfg['other_']['acct_reptn']


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
        else:
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
    ASTYPE: dict[str, str] = att_file_cfg['astype']
    DTPARTS: dict[str, tuple[str]] = att_file_cfg['other_']['dtparts']

    # columns subset to eval duplicate records
    DUP_SUBSET: list[str] = att_file_cfg['other_']['dup_subset']

    DATETIME_COL: str = list(DTPARTS.keys())[0]
    DATETIME_DATE: str = list(DTPARTS.values())[0][0]
    DATETIME_TIME: str = list(DTPARTS.values())[0][1]

    ACCT_COL: str = att_file_cfg['other_']['acct_col']

    toll_map = get_toll_map()

    df__ = df__.drop_duplicates(subset=DUP_SUBSET).reset_index(drop=True)

    # date and time are separate and utc.
    # need to convert, combine, localize, and convert
    df__[DATETIME_COL] = (
        # this makes a pd.Series
        (
            pd.to_datetime(df__[DATETIME_DATE])
            +
            pd.to_timedelta(df__[DATETIME_TIME])
        )
        .dt.tz_localize(tz='UTC')
        # .dt.tz_localize(tz='US/Central')
    )
    df__ = df__.drop(columns=[DATETIME_DATE, DATETIME_TIME])

    df__ = df__.astype(ASTYPE)

    # map af_acct via toll list to avoid lag in human updates...
    df__ = df__.drop(columns=[ACCT_COL])
    df__[ACCT_COL] = df__['number_dial'].map(toll_map)

    return df__


def main():
    # strings and things for addressing source files
    SRC_PARTS: list[str] = att_file_cfg['src_label'].split('||')
    file_glob: str = SRC_PARTS[1]
    REPOS_PTH_STR: str = SRC_PARTS[0]

    repos_pth_att: Path = REPOS_PTH / REPOS_PTH_STR
    path_list: list[Path] = list(repos_pth_att.glob(file_glob))

    TBLNM: str = att_file_cfg['tblnm']

    logger = getLogger(f"{os_environ['PRMDIA_MM_LOGNAME']}")

    dtype: dict[str, TypeEngine] = att_file_cfg['dtype']
    presql: list[str] = att_file_cfg['pre_sql']
    xtrasql: list[str] = att_file_cfg['xtra_sql']
    vntge_vw: str = att_file_cfg['vintage_view_nm']

    # get data vintage and add sql query to create view
    vntge: datetime = get_latest_vntge(path_list)
    xtrasql.append(
        vntge_vw_sql.format(
            nm=vntge_vw,
            ts=vntge.strftime(vntge_fmt)
        )
    )

    # read in from source files
    df = read_append(
        path_list=path_list
    )

    # cleanup:
    #   dates/times
    #   assign AF acct by toll num
    df = clean(df)

    db_load(
        tblnm=TBLNM,
        db=db,
        df=df,
        dtype=dtype,
        presql=presql,
        xtrasql=xtrasql
    )

    logger.info(f"\x1b[36;1mSuccessfully loaded {TBLNM} to {db.engine}\x1b[0m")

    return


if __name__ == "__main__":
    main()
