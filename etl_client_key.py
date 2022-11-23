# %% Headers
from db_engines import wh_db as db
import logging, traceback
from os import environ as os_environ
from dotenv import load_dotenv
from json import loads as json_loads
from pathlib import Path
from pandas import DataFrame as Df

load_dotenv()

# %% Config
# Unpack Config
logger = logging.getLogger(os_environ['PRMDIA_MM_LOGNAME'])

config: dict[str, str|int] = json_loads(Path('client_map.json').read_text())

tblnm: str = config['tblnm']

astype: dict[str, str] = config['astype']

df_key = 'map'

# %%
def main():
    df = (
        Df.from_dict(config[df_key])
        .convert_dtypes()
        .astype(astype)
    )

    with db.connect() as conn:
        df.to_sql(
            name=tblnm,
            con=conn,
            index=False,
            if_exists='replace'
        )

    logger.info(f"\x1b[36;1mSuccessfully loaded {tblnm} to {db.engine}\x1b[0m")

    return

# %%
if __name__ == "__main__":
    main()
