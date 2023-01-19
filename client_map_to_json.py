"""
Translates the spreadsheet with client identifying values (name in AF,
    name in lead email form, AF practice id, etc) used to map identifiers
    to practice id.
"""
import configparser
import json
from os import chdir
from os import environ as os_environ
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame as Df

CALLING_DIR = Path.cwd()
# Must be set in env on host/container.
ROOT_PATH = Path(os_environ['APPS_ROOT'])
APP_PATH = ROOT_PATH / 'PM_MedMaster'
chdir(APP_PATH)

load_dotenv(ROOT_PATH / '.env')

conf = configparser.ConfigParser()
conf.read('.conf')
conf.read(ROOT_PATH / 'app.conf')

VNTGE_FMT = conf['PM']['VNTGE_FMT']

AS_TYPE = {'astype': {
    'af_practice': 'string',
    'practice_id': 'Int16',
    'af_acct': 'Int32',
    'client_id': 'Int64',
    'lead_ref_id': 'Int64',
    'lead_company': 'string',
    'status': 'string',
    'lead_email_form': 'string',
}}

SRC_DIR = Path(os_environ['LOCAL_STORAGE'])
SRC_FN = conf['HOST_RESOURCES']['MM_CLIENT_MAP_FN']
src_pth = SRC_DIR / SRC_FN

USECOLS = [
    'af_practice',
    'af_prac_id',
    'af_acct',
    'lead_ref_id',
    'client_id',
    'lead_company',
    'status',
    'lead_email_form'
]

RENAME = {
    'af_prac_id': 'practice_id'
}
CLIENT_MASTER_SHEET_NAME = 'master'

PHONE_MAP_SHEET_NAME = 'phone_map'

TBLNM = conf['OUTPUT']['CLIENT_MAP_TBLNM']
TBLNM_D: dict[str, str] = {'tblnm': TBLNM}

PRE_SQL = {
    'pre_sql': [
        f"--sql DROP TABLE IF EXISTS {TBLNM} CASCADE".replace('--sql ', '')]}

JSON_OUT = Path(conf['INTERNAL_RESOURCES']['MM_CLIENT_MAP_PTH'])
PHONE_OUT = Path(conf['INTERNAL_RESOURCES']['PHONE_MAP_FILE'])


def main():
    """TODO: DOCSTRING"""
    xl_ = pd.ExcelFile(src_pth)

    client_map: Df = (
        xl_.parse(
            sheet_name=CLIENT_MASTER_SHEET_NAME,
            usecols=USECOLS
        )
        .convert_dtypes()
        .rename(columns=RENAME)
    )

    # Remove pairs with NA values and load to dict.
    map_ = {col_key: {idx: val for idx, val in col.items()
                      if pd.notna(val)}
            for col_key, col in client_map.to_dict().items()}

    out_dict = TBLNM_D | AS_TYPE | PRE_SQL | {"map": map_}

    JSON_OUT.write_text(json.dumps(obj=out_dict), encoding='utf-8')

    # Parse toll number map and store to json.
    # TODO: May need to load to the db.
    phone = xl_.parse(sheet_name=PHONE_MAP_SHEET_NAME)
    phone_dict = {k: v for v, k in phone.to_dict(orient='tight')['data']}
    PHONE_OUT.write_text(json.dumps(obj=phone_dict), encoding='utf-8')


if __name__ == "__main__":
    main()
chdir(CALLING_DIR)
