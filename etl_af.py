# %%
# HEADERS
from datetime import datetime
from os import environ
from pathlib import Path
from threading import Thread
from time import perf_counter
from xmlrpc.client import DateTime
from dotenv import load_dotenv

import pandas as pd
from pandas import Series as Ser, DataFrame as Df

from table_config import FldCfg, TblCfg, af_cfgs, vntge_vw_sql, vntge_fmt
from db_engines import wh_db as db, db_load
from sqlalchemy.types import TypeEngine

from threading import Thread

import re
# %% BASE CFGS
load_dotenv()
repos_path = Path(environ['PRMDIA_EVAN_LOCAL_LAKEPATH'])

# %%
# UNPAC VALS FROM CFGS DICT
tblnm: str = af_cfgs['tblnm']
rename: dict[str, str] = af_cfgs['rename']
filter_fld: str = af_cfgs['other_']['filter_fld']
skiphead: int = af_cfgs['skiphead']
remap: dict[str, dict[str, str | bool]] = af_cfgs['other_']['remap']
dtype: dict[str, TypeEngine] = af_cfgs['dtype']
src_label: str = af_cfgs['src_label']
vntge_vw: str = af_cfgs['vintage_view_nm']
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
    df_ = df_.loc[df_[filter_fld].notna()]

    for c, m in remap.items():
        df_[c] = df_[c].map(m, na_action='ignore')

    return df_

def main():
    db_load(
        db=db,
        df=et_(),
        tblnm=tblnm,
        dtype=dtype,
        xtrasql=xtrasql
    )
    print(f"\x1b[36;1mSuccessfully loaded {tblnm} to {db.engine}")


if __name__ == "__main__":
    main()
