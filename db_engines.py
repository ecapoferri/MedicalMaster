from sqlite3 import Row
from typing import Iterable
from sqlalchemy import create_engine
from sqlalchemy.types import TypeEngine
from sqlalchemy.engine.base import Engine  # for type hints
from MySQLdb._exceptions import OperationalError as MySQL_OpErr

import traceback
from logging import getLogger, Logger, info, debug, error

from os import environ
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

from pandas import DataFrame as Df

from table_config import VNTGE_FMT

load_dotenv()

#Destination WH DB Connection config/s================================>
WH_PORT = environ['PRMDIA_POSTGRES_CONT_PORT']
WH_UN = environ['PRMDIA_POSTGRES_CONT_UN']
WH_PW = environ['PRMDIA_POSTGRES_CONT_PW']
WH_HOST = environ['PRMDIA_POSTGRES_CONT_HOST']
WH_DB_NAME = environ['PRMDIA_POSTGRES_CONT_MM_DB']
WH_DB_NAME_RPRT = environ['PRMDIA_POSTGRES_CONT_RPRT_DB']
#<==Destination WH DB Connection config/s=========================<

#MMS/WO=MySQL config/s================================>
MMSWO_UN = environ['PRMDIA_SRVR_UN']
MMSWO_PW = environ['PRMDIA_SRVR_PW']
MMSWO_PORT = environ['PRMDIA_SRVR_DB_PORT']
MMSWO_HOST = environ['PRMDIA_SRVR_DB_HOST']
MMS_DB_NAME = environ['PRMDIA_SRVR_MMS_DB']
#<==MMS/WO=MySQL config/s=========================<

LOGGER = getLogger(environ['PRMDIA_MM_LOGNAME'])

#==Connection Engines=============================>
# connection to wh db
WH_CONN_STR, WH_CONN_STR_RPRT = (
        f"postgresql://{WH_UN}:{WH_PW}@{WH_HOST}:{WH_PORT}/{n}"
        for n in (WH_DB_NAME, WH_DB_NAME_RPRT)
    )
WH_DB: Engine
RPRT_DB: Engine
WH_DB, RPRT_DB = (
        create_engine(s) for s in
        (WH_CONN_STR, WH_CONN_STR_RPRT)
    )
# connections to primedia dbs
MMS_CONN_STR =(
    f"mysql+mysqldb://{MMSWO_UN}:{MMSWO_PW}@{MMSWO_HOST}:"
    + f"{MMSWO_PORT}/{MMS_DB_NAME}"
)
MMS_DB = create_engine(MMS_CONN_STR)
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
        .strftime(VNTGE_FMT))
    return tmstmp


def check_connection(db_: Engine):
    """Checks that connection exists.
        Uses a basic query, not dependend on data in the DB.

    Args:
        db_ (Engine): sqlalchemy connection engine to check

    Raises:
        Exception: Try/Except excepts MySQLdb._exceptions.OperationalError
            (asliased as 'MySQL_OpErr').
            Raises basic exception to stop execution if that's the case.
    """
    with db_.connect() as conn:
        try:
            LOGGER.info(f"Checking {db_.engine} -->")
            rows: list[Row] = conn.execute(
                "SELECT 'Hello There' AS greeting;").all()
            rows_str: list[str] = [f"\t{r}" for r in rows]
            LOGGER.info('\n'.join(rows_str))
            LOGGER.info(f"--> \x1b[32m{db_.engine} \x1b[1m✔️\x1b[0m\n")
        except MySQL_OpErr:
            raise Exception(
                f"\x1b[91mLooks Like a problem with your connection to {db_.name};\x1b[0m\nSee below:\n\n{traceback.format_exc()}\n"
            )


def vintage_check(path_: str | Path) -> tuple[datetime | str]:
    """
    Args:
        path_ (str | Path): string or path to file

    Raises:
        ValueError: if the path_ arg is the wrong type

    Returns:
        tuple[datetime|str]:
            Actual datetime object (tz agnostic)
            AND a formatted string as '%Y-%m-%d %H:%M:%S',
                aka yyyy-mm-dd hh:mm:ss
    """
    if not (type(path_) in (type(Path('/')), type(str('')))):
        raise ValueError(f"'path_ arg must be of type pathlib.Path or str.")
    pth = path_ if type(path_) == type(Path()) else Path(path_)
    dt = datetime.fromtimestamp(pth.stat().st_mtime)

    return (dt, dt.strftime(VNTGE_FMT))
