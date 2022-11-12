# %%
# HEADERS
from datetime import datetime
from os import environ as os_environ
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
from pandas import DataFrame as Df

from table_config import af_cfgs, vntge_vw_sql, vntge_fmt, enum_sql
from db_engines import wh_db as db, db_load
from sqlalchemy.types import TypeEngine
from sqlalchemy.dialects.postgresql import ENUM

import re

from logging import getLogger, Logger
import traceback


logger = getLogger(f"{os_environ['PRMDIA_MM_LOGNAME']}")



# %% BASE CFGS
load_dotenv()
repos_path = Path(os_environ['PRMDIA_EVAN_LOCAL_LAKEPATH'])

# %%
# UNPAC VALS FROM CFGS DICT
tblnm: str = af_cfgs['tblnm']
rename: dict[str, str] = af_cfgs['rename']
filter_fld: str = af_cfgs['other_']['filter_fld']
skiphead: int = af_cfgs['skiphead']
remap: dict[str, dict[str, str | bool]] = af_cfgs['other_']['remap']
dtype: dict[str, TypeEngine] = af_cfgs['dtype']
astype: dict[str, TypeEngine] = af_cfgs['astype']
src_label: str = af_cfgs['src_label']
vntge_vw: str = af_cfgs['vintage_view_nm']
# enums that are already defined
enums: dict[str, str] = af_cfgs['other_']['enums']

# enums that will need to capture their values from the DF categories
enums_to_update: dict[str, str] = {
        k: af_cfgs['fields'][k]['enum_name'] for k in
        af_cfgs['fields'].keys()
        if
            (af_cfgs['fields'][k]['enum_name'] != None)
            &
            (not af_cfgs['fields'][k]['dtype'] != None)
    }

# %%
# PARSE PATHS
af_glob: str = src_label.replace('||', '*')
af_files: list[Path] = list(repos_path.rglob(af_glob))

# get timestamp for data vintage
timestamp_lst: list[float] = [
        f.stat().st_mtime for f in af_files
    ]
timestamp_lst.sort(reverse=True)
tmstmp: str = datetime.fromtimestamp(timestamp_lst[0]).strftime(vntge_fmt)

xtrasql: list[str] = [
    vntge_vw_sql.format(nm=vntge_vw, ts=tmstmp)
]
del timestamp_lst, tmstmp

def load_xls_sheet(f: Path) -> Df:
    xl = pd.ExcelFile(f)
    load_df: Df
    for sh in xl.sheet_names:
        load_df = pd.concat([
            load_df,
            xl.parse(sh, skiprows=skiphead)
        ])
    return load_df


def et_() -> Df:
    df_ = Df()
    for f in af_files:
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
        
        df_ = pd.concat([
            df_,
            *[
                xl.parse(sh, skiprows=skiphead)
                .assign(acct=acct_num)
                for sh in xl.sheet_names
            ]
        ])

    # thread_list = [Thread(target=load_xls_sheet, args=[f]) for f in af_files]

    # remove junk columns
    use_cols: list[str] = [
        c for c in list(df_.columns)
        if not re.findall(r'Unnamed: \d', c)
    ]

    df_ = (
        df_[use_cols]
        .convert_dtypes()
        .rename(columns=rename)
    )

    # removes junk rows/records
    df_ = df_.loc[df_[filter_fld].notna()]

    for c, m in remap.items():
        df_[c] = df_[c].map(m, na_action='ignore')

    df_['connected'] = df_['connected'].dt.tz_localize(tz='US/Central')

    df_ = df_.astype(astype)
    return df_

def main():

    df: Df = et_()

    # capture enum values
    for e, n in enums_to_update.items():
        # create string of enum values
        enums.update({
            e: ENUM(*list(df[e].cat.categories), name=n)
        })

    # sql to drop enum types
    presql: list[str] = [
        enum_sql.format(e)
        for e in enums.keys()
    ]

    dtype.update({
        k: v for k, v in enums.items()
    })

    db_load(
        db=db,
        df=df,
        tblnm=tblnm,
        dtype=dtype,
        presql=presql,
        xtrasql=xtrasql
    )
    logger.info(f"\x1b[36;1mSuccessfully loaded {tblnm} to {db.engine}\x1b[0m")


if __name__ == "__main__":
    main()
