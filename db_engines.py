from sqlite3 import Row
from typing import Iterable
from sqlalchemy import create_engine
from sqlalchemy.types import TypeEngine
from sqlalchemy.engine.base import Engine  # for type hints
from MySQLdb._exceptions import OperationalError as MySQL_OpErr

import traceback

from os import environ
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

from pandas import DataFrame as Df

from table_config import vntge_fmt

load_dotenv()

#Destination WH DB Connection config/s================================>
wh_port = environ['PRMDIA_POSTGRES_CONT_PORT']
wh_un = environ['PRMDIA_POSTGRES_CONT_UN']
wh_pw = environ['PRMDIA_POSTGRES_CONT_PW']
wh_host = environ['PRMDIA_POSTGRES_CONT_HOST']
wh_db_name = environ['PRMDIA_POSTGRES_CONT_MM_DB']
#<==Destination WH DB Connection config/s=========================<

#MMS/WO=MySQL config/s================================>
mmswo_un = environ['PRMDIA_SRVR_UN']
mmswo_pw = environ['PRMDIA_SRVR_PW']
mmswo_port = environ['PRMDIA_SRVR_DB_PORT']
mmswo_host = environ['PRMDIA_SRVR_DB_HOST']
mms_db_name = environ['PRMDIA_SRVR_MMS_DB']
#<==MMS/WO=MySQL config/s=========================<

#==Connection Engines=============================>
# connection to wh db
wh_conn_str: str = f"postgresql://{wh_un}:{wh_pw}@{wh_host}:{wh_port}/{wh_db_name}"
wh_db: Engine = create_engine(wh_conn_str)
# connections to primedia dbs
mms_conn_str = f"mysql+mysqldb://{mmswo_un}:{mmswo_pw}@{mmswo_host}:{mmswo_port}/{mms_db_name}"
mms_db = create_engine(mms_conn_str)
#<==Connection Engines===========================<

def db_load(
    db: Engine,
    df: Df,
    tblnm: str,
    dtype: dict[str, TypeEngine],
    presql: Iterable[str|None]|bool=False,
    xtrasql: Iterable[str|None]|bool=False,
    ifexists: str='replace',
    index: bool=False
) -> None:
    with db.connect() as conn:
        if presql:
            [conn.execute(q) for q in presql]
        df.to_sql(
            tblnm,
            conn,
            index=index,
            dtype=dtype,
            if_exists=ifexists
        )
        if xtrasql:
            [conn.execute(q) for q in xtrasql]
    return


def fs_tmstmp(path_: Path) -> str:
    """
    Args:
        path_ (pathlib.Path): _description_

    Returns:
        str: zone naive timestamp string with fmt: <yyyy-mm-dd hh:mm:ss>
    """
    tmstmp: str = (
        datetime
        .fromtimestamp(
            path_.stat()
            .st_mtime
        )
        .strftime(vntge_fmt))
    return tmstmp


def check_connection(db_: Engine):
    """Checks that connection exists. Uses a basic query, not dependend on data in the DB.

    Args:
        db_ (Engine): sqlalchemy connection engine to check

    Raises:
        Exception: Try/Except excepts MySQLdb._exceptions.OperationalError (asliased as 'MySQL_OpErr'). Raises basic exception to stop execution if that's the case.
    """
    with db_.connect() as conn:
        try:
            print(f"Checking {db_.engine} -->")
            rows: list[Row] = conn.execute(
                "SELECT 'Hello There' AS greeting;").all()
            rows_str: list[str] = [f"\t{r}" for r in rows]
            print(
                *rows_str,
                sep='\n',
                end='\n'
            )
            print(f"--> \x1b[32m{db_.engine} \x1b[1m✔️\x1b[0m\n")
        except MySQL_OpErr:
            raise Exception(
                f"\x1b[91mLooks Like a problem with your connection to {db_.name};\x1b[0m\nSee below:\n\n{traceback.format_exc()}\n"
            )
