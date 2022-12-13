#Headers
from db_engines import WH_DB as DB
import logging, traceback
from os import environ as os_environ
from dotenv import load_dotenv
from json import loads as json_loads
from pathlib import Path
from pandas import DataFrame as Df

load_dotenv()

# Unpack Config
LOGGER = logging.getLogger(os_environ['PRMDIA_MM_LOGNAME'])
CONFIG: dict[str, str|int] = json_loads(
    Path(os_environ['PRMDIA_MM_CLIENT_MAP_PTH']).read_text())

TBLNM: str = CONFIG['tblnm']
ASTYPE: dict[str, str] = CONFIG['astype']
PRE_SQL: list[str] = CONFIG['pre_sql']
DF_KEY = 'map'

def main():
    df = (
        Df.from_dict(CONFIG[DF_KEY])
        .convert_dtypes()
        .astype(ASTYPE)
    )

    with DB.connect() as conn:
        for s in PRE_SQL:
            conn.execute(s)
        df.to_sql(
            name=TBLNM,
            con=conn,
            index=False,
            if_exists='replace'
        )

    LOGGER.info(
        f"\x1b[36;1mSuccessfully loaded {TBLNM} to {DB.engine}\x1b[0m")

    return

# %%
if __name__ == "__main__":
    main()
