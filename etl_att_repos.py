# %%
# HEADERS
from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame as Df

from pathlib import Path
from io import BytesIO
import gzip
import re

from os import environ as os_environ
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy.types import TypeEngine

from table_config import att_file_cfg, vntge_vw_sql, vntge_fmt
from db_engines import db_load, wh_db as db

from logging import getLogger, Logger
import traceback


repos_pth = Path(os_environ['PRMDIA_EVAN_LOCAL_LAKEPATH'])

logger = getLogger(f"{os_environ['PRMDIA_MM_LOGNAME']}")

tblnm: str = att_file_cfg['tblnm']
use_cols: list[str|int] = att_file_cfg['use_cols']
dtype: dict[str, TypeEngine] = att_file_cfg['dtype']
astype: dict[str, str] = att_file_cfg['astype']
rename: dict[str, str] = att_file_cfg['rename']
dtparts: dict[str, tuple[str]] = att_file_cfg['other_']['dtparts']
datetime_col: str = list(dtparts.keys())[0]
datetime_date: str = list(dtparts.values())[0][0]
datetime_time: str = list(dtparts.values())[0][1]
out_cols: list[str] = att_file_cfg['out_cols']
skiphead: int = att_file_cfg['skiphead']
src_parts: list[str] = att_file_cfg['src_label'].split('||')
repos_dir_pth: str = src_parts[0]
file_glob: str = src_parts[1]
filt_col: str = att_file_cfg['other_']['non_null_filt']
bad_filt_vals: list = att_file_cfg['other_']['bad_filt_vals']
acct_col: str = att_file_cfg['other_']['acct_col']
re_ptn: str = att_file_cfg['other_']['acct_reptn']
dup_subset: list[str] = att_file_cfg['other_']['dup_subset']
fix_intervals: list[str] = att_file_cfg['other_']['fix_intervals']
presql: list[str] = att_file_cfg['pre_sql']
xtrasql: list[str] = att_file_cfg['xtra_sql']
vntge_vw: str = att_file_cfg['vintage_view_nm']

repos_dir: Path = repos_pth / repos_dir_pth
path_list: list[Path] = list(repos_dir.glob(file_glob))

# %%
def get_latest_vntge(paths_: list[Path]) -> datetime:
    mts: list[float] = [
            p.stat().st_mtime for p in paths_
        ]
    mx: float = max(mts)

    return datetime.fromtimestamp(mx)


def read_append(paths: list[Path]) -> Df:
    df_ = Df()

    # TODO: #1 THREAD THIS OUT: DO A THREAD POOL OR MULTIPROCESSING POOL OR
    #  WHATEVER AND PD.CONCAT THE RESULTING DFS,
    #  THERE'S A WAY TO GET A LIST OF RESULTS FROM A LIST OF THREADS/PROCESSES
    for file_pth in paths:
        # get acct number from filename
        st = re.search(re_ptn, file_pth.name).start()
        lmt = re.search(re_ptn, file_pth.name).end() - 3
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
                        skiprows=skiphead,
                        usecols=use_cols,
                        sep='\t'
                    )
                    .rename(columns=rename)
                )
            df_part = df_part.loc[
                    pd.notna(df_part[filt_col])
                    &
                    ~df_part[filt_col].isin(bad_filt_vals)
                ]
            # assign acct number
            df_part = df_part.assign(acct_af=acct)

            df_ = (
                pd.concat([df_, df_part])
                .reset_index(drop=True)
            )

    return df_


def clean(df__: Df):
    df__ = df__.drop_duplicates(subset=dup_subset).reset_index(drop=True)

    # date and time are separate and utc. need to convert, combine, localize, and convert
    df__[datetime_col] = (
        # this makes a pd.Series
        (
            pd.to_datetime(df__[datetime_date])
            +
            pd.to_timedelta(df__[datetime_time])
        )
        .dt.tz_localize(tz='UTC')
        # .dt.tz_localize(tz='US/Central')
    )
    df__ = df__.drop(columns=[datetime_date, datetime_time])

    # for c in fix_intervals:
    #     df__[c] = pd.to_timedelta(df__[c])

    df__ = df__.astype(astype)

    return df__


def main():
    # get data vintage and add sql query to create view
    vntge: datetime = get_latest_vntge(path_list)
    xtrasql.append(
        vntge_vw_sql.format(
            nm=vntge_vw,
            ts=vntge.strftime(vntge_fmt)
        )
    )


    df = read_append(path_list)

    db_load(
        tblnm=tblnm,
        db=db,
        df=clean(df),
        dtype=dtype,
        presql=presql,
        xtrasql=xtrasql
    )

    logger.info(f"\x1b[36;1mSuccessfully loaded {tblnm} to {db.engine}\x1b[0m")

    return


if __name__ == "__main__":
    main()
