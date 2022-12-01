import pandas as pd
from pandas import Series as Ser, DataFrame as Df

from pathlib import Path

from os import environ as os_environ
from dotenv import load_dotenv

import json

load_dotenv()

src_dir = os_environ['PRMDIA_MM_CLIENT_MAP_SRCPTH']
# src_dir = Path('/mnt/c/Users/ecapo/OneDrive - Primedia Network, Inc/Reports/MedicalMaster/AFDir')
src_fn = 'AFDir.xlsx'
src_pth = src_dir / src_fn

usecols = [
    'af_practice',
    'af_rec_id',
    'af_acct',
    'lead_ref_id',
    'lead_company'
]
sn_nm = 'Sheet1'

tblnm: dict[str, str] =  {"tblnm": os_environ['PRMDIA_MM_CLIENT_MAP_TBLNM']}

json_out = Path(os_environ['PRMDIA_MM_CLIENT_MAP_PTH'])


def main():
    client_map: Df = (
        pd.ExcelFile(src_pth)
        .parse(
            sheet_name=sn_nm,
            usecols=usecols
        )
        .convert_dtypes())

    out_dict = tblnm | {"map": client_map.to_dict()}

    json_out.write_text(json.dumps(obj=out_dict))

    return 


if __name__ == "__main__":
    main()
