import pandas as pd
from pandas import Series as Ser, DataFrame as Df

from pathlib import Path

from os import environ as os_environ
from dotenv import load_dotenv

import json

load_dotenv()

SRC_DIR = Path(os_environ['PRMDIA_MM_CLIENT_MAP_SRCPTH'])

AS_TYPE = {'astype': {
    'af_practice': 'string',
    'practice_id': 'Int16',
    'af_acct': 'Int32',
    'client_id': 'Int64',
    'lead_ref_id': 'Int64',
    'lead_company': 'string',
    'status': 'string'
}}

SRC_FN = 'AFDir.xlsx'
src_pth = SRC_DIR / SRC_FN

USECOLS = [
    'af_practice',
    'af_prac_id',
    'af_acct',
    'lead_ref_id',
    'client_id',
    'lead_company',
    'status'
]

RENAME = {
    'af_prac_id': 'practice_id'
}
CLIENT_MASTER_SHEET_NAME = 'master'

PHONE_MAP_SHEET_NAME = 'phone_map'

TBLNM: dict[str, str] =  {"tblnm": os_environ['PRMDIA_MM_CLIENT_MAP_TBLNM']}

json_out = Path(os_environ['PRMDIA_MM_CLIENT_MAP_PTH'])
phone_out = Path(os_environ['PRMDIA_MM_PHONE_MAP_PTH'])


def main():
    xl = pd.ExcelFile(src_pth)

    client_map: Df = (
        xl.parse(
            sheet_name=CLIENT_MASTER_SHEET_NAME,
            usecols=USECOLS
        )
        .convert_dtypes()
        .rename(columns=RENAME)
    )

    map_ = {
        col_key: {
            idx: val for idx, val in col.items()
            if pd.notna(val)
        } for col_key, col in client_map.to_dict().items()
    }

    out_dict = TBLNM | {"map": map_} | AS_TYPE

    json_out.write_text(json.dumps(obj=out_dict))
    
    phone = xl.parse(sheet_name=PHONE_MAP_SHEET_NAME)
    phone_dict = {k: v for v, k in phone.to_dict(orient='tight')['data']}
    phone_out.write_text(json.dumps(obj=phone_dict))

    return 


if __name__ == "__main__":
    main()
