import logging
import traceback

import configparser
from os import chdir
from os import environ as os_environ
from pathlib import Path
from time import perf_counter
from typing import NewType, Optional, TypedDict

from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import (BIGINT, BOOLEAN, DATE, ENUM,
                                            INTEGER, INTERVAL, TEXT, TIMESTAMP,
                                            VARCHAR)
from sqlalchemy.types import TypeEngine

PERF_START = perf_counter()
CALLING_DIR = Path().cwd()
# Must be set in env on host/container.
ROOT_PATH = Path(os_environ['APPS_ROOT'])
APP_PATH = ROOT_PATH / 'PM_MedMaster'
chdir(APP_PATH)

load_dotenv(ROOT_PATH / '.env')

conf = configparser.ConfigParser()
conf.read('.conf')
conf.read(ROOT_PATH / 'app.conf')

LOGGER = logging.getLogger(conf['DEFAULT']['LOGGER_NAME'])
# custom types aliased for easy for name hints
AsType = NewType('PandasAsType | StypeStr', [type | str])
DType = NewType('SQLAlchemyDType', TypeEngine)
SQLqTypeStr = NewType('SQLQueryTypeString', str)
FldNmOrig = NewType('FieldNameOrigString', str)
FldNmOut = NewType('FieldNameString', str)
SrcStr = NewType('SourceString', str)
SQLQueryStr = NewType('SQLQueryString', str)

XSQLStrsList = NewType('XtraSQLStrings', list[str])
KeyColsList = NewType('KeyCols', list[str])


DTypeMapDict = NewType('DTypeMap', dict[str, DType])
AsTypeMapDict = NewType('DTypeMap', dict[str, AsType])

# DTypeKeyDict = NewType('DataTypesKey', dict[str, AsType | TypeEngine | SQLqTypeStr])
# # Just a key from string to type objects to be used to translate json data to a FldCfg TypedDict


class FldCfg(TypedDict):
    """
    see TblCfg
    """
    astype: Optional[AsType]  # Values For Pandas astype
    dtype: Optional[DType]  # Values for SQLAlchemy dtype
    # Values for additional SQL queries after sqlalchemy inserts
    qdtype: Optional[SQLqTypeStr]
    # Column Name from source input for pandas rename
    orig: Optional[FldNmOrig]
    use_col: Optional[bool] # whether to use the field
    enum_name: Optional[str]


class TblCfg(TypedDict):
    """
    output table name for Pandas/SQLAlchemy .to_sql and add'l SQLAlchemy queries
    """
    tblnm: Optional[str]
    vintage_view_nm: Optional[str]  # name of view holding timestamp
    src_label: Optional[SrcStr]  # string for source path
    # list of col names for uncooperative source tables
    src_cols_in: Optional[list[str | int]]
    use_cols: Optional[list[str | int]]
    rename: Optional[dict[str | int, str | int]]
    astype: Optional[dict[str, type | str]]
    dtype: Optional[dict[str, TypeEngine]]
    pre_sql: Optional[XSQLStrsList]  # additional SQLAlchemy queries
    xtra_sql: Optional[XSQLStrsList]  # additional SQLAlchemy queries
    # list of column names to be added and or used for joins
    key_cols: Optional[KeyColsList]
    # dict of values for per-field operations
    fields: Optional[dict[str, FldCfg | list[str|int]]]
    # number of rows to skip at top in ingestion
    skiphead: Optional[int]
    # number of rows to skip at bottom in ingestion
    skipfoot: Optional[int]
    other_: Optional[dict]  #extra cfgs
    out_cols: Optional[list[str]]


#== TABLE CONFIGS ==================>
trailing_days: int = 10

# Name of the field which
DATE_OUT_FLDNM = 'call_date'
# Name of field with phone number.
#   It will need to have non-digit characters removed.
CALLERID_FLDNM = 'af_msg_id'


VNTGE_VW_SQL = """--sql
        CREATE OR REPLACE VIEW {nm} AS
        SELECT CAST('{ts}' AS TIMESTAMP) AS {nm}
        ;
    """.replace('--sql\n', '')

ENUM_SQL = """
        DROP TYPE IF EXISTS {} CASCADE;
    """

VNTGE_FMT: str = conf['PM']['VNTGE_FMT']


# ATT - for a simple query to mms
ATT_CFGS: dict[str, dict|str] = {
    'tblnm': 'att_data',
    'vintage_view_nm': 'att_vntge',
    'field_stmts': [
        'id',
        'connected',
        'duration',
        'number_orig',
        'number_dial',
        'number_term',
        'disconnect',
        'zip',
        'state',
        'created',
    ]

}

STATE_LIST: list[str] = [
    'AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
    'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
    'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
    'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX',
    'UM', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY',
]
# ANSWER FIRST
AF_FIELDS = {
    'connected': FldCfg(
        orig='Date/Time',
        dtype=TIMESTAMP(timezone=False),
        astype='datetime64[ns]',
        enum_name=None
    ),
    'recording_id': FldCfg(
        orig='Recording#',
        dtype=INTEGER,
        astype=None,
        enum_name=None
    ),
    'callerid': FldCfg(
        orig='Caller ID',
        dtype=BIGINT,
        astype=None,
        enum_name=None
    ),
    'call_for_ad': FldCfg(
        orig='CallType',
        dtype=BOOLEAN,
        astype=None,
        enum_name=None
    ),
    'caller_name': FldCfg(
        orig='Caller',
        dtype=VARCHAR,
        astype=None,
        enum_name=None
    ),
    'phone': FldCfg(
        orig='Phone',
        dtype=VARCHAR,
        astype=None,
        enum_name=None
    ),
    'ext': FldCfg(
        orig='Extension',
        dtype=VARCHAR,
        astype='string',
        enum_name=None
    ),
    'addr_state': FldCfg(
        orig='State',
        dtype=VARCHAR,
        astype=None,
        enum_name=None
    ),
    'addr_city': FldCfg(
        orig='City',
        dtype=VARCHAR,
        astype=None,
        enum_name=None
    ),
    'practice_id': FldCfg(
        orig='PracticeID',
        dtype=INTEGER,
        astype=None,
        enum_name=None
    ),
    'besttime': FldCfg(
        orig='BestTime',
        dtype=VARCHAR,
        astype=None,
        enum_name=None
    ),
    'sent_emails_to': FldCfg(
        orig='SentEmailsTo',
        dtype=TEXT,
        astype=None,
        enum_name=None
    ),
    'reference': FldCfg(
        orig='Reference',
        dtype=TEXT,
        astype=None,
        enum_name=None
    ),
    'city_id': FldCfg(
        orig='CityID',
        dtype=VARCHAR,
        astype=None,
        enum_name=None
    ),
    'email': FldCfg(
        orig='Email',
        dtype=VARCHAR,
        astype=None,
        enum_name=None
    ),
    'statecheck': FldCfg(
        orig='StateCheck',
        dtype=ENUM(*STATE_LIST, name='af_lead_state_enum'),
        astype=None,
        enum_name='enum_statecheck_enum'
    ),
    'zipcode': FldCfg(
        orig='PostalCode',
        dtype=INTEGER,
        astype=None,
        enum_name=None
    ),
    'majorcity': FldCfg(
        orig='MajorCity',
        dtype=VARCHAR,
        astype=None,
        enum_name=None
    ),
    'acct': FldCfg(
        dtype=INTEGER,
        orig=None,
        enum_name=None,
        astype=None
    ),
    'dispo': FldCfg(
        dtype=TEXT,
        orig='_DISPOSITION',
        astype=None,
        enum_name=None
    ),
    'history': FldCfg(
        dtype=TEXT,
        orig='History',
        astype=None,
        enum_name=None
    ),
    'client': FldCfg(
        orig='PracticeName',
        dtype=None,
        astype='category',
        enum_name='af_clients_enum'
    )
}
AF_TBLNM = 'af_message_data'
AF_CFGS = TblCfg(
    # for glob or regex
    src_label=(
        conf['ANSWER_FIRST']['AF_FILE_LABEL']
        + '*'
        + conf['ANSWER_FIRST']['AF_FILE_EXT']
    ),
    tblnm=AF_TBLNM,
    fields=AF_FIELDS,
    vintage_view_nm='af_message_vntge',
    rename={d['orig']: k for k, d in AF_FIELDS.items() if d['orig']},
    other_={
        'filter_fld': 'connected',
        'remap': {
            'call_for_ad': {
                'Yes': True,
                'No': False
            },
        },
        'enums': {
            d['enum_name']: d['dtype']
            for d in AF_FIELDS.values()
            if d['enum_name']
        },
    },
    skiphead=1,
    dtype={
        k: d['dtype'] for k, d in AF_FIELDS.items()
        if
            (type(d['dtype']) != None)
            &
            (type(d['dtype']) != type(ENUM('x', name='x')))
    } | {DATE_OUT_FLDNM: DATE},
    astype={
        k: d['astype']
        for k, d in AF_FIELDS.items()
        if d['astype']
    },
    pre_sql=[
        f"--sql DROP TABLE IF EXISTS {AF_TBLNM} CASCADE;"\
            .replace('--sql ', '').replace('  ', '')
    ],
)


ATT_FILE_FIELDS: dict[str|int, FldCfg] = {
        'connected_date': FldCfg(
            orig=2,
            dtype=None,
            astype=None
        ),
        'connected_time': FldCfg(
            orig=3,
            dtype=None,
            astype=None
        ),
        'connected': FldCfg(
            orig=None,
            astype='datetime64[ns, US/Central]',
            dtype=TIMESTAMP(timezone=True)
        ),
        'number_orig': FldCfg(
            orig=4,
            dtype=BIGINT,
            astype='int64'
        ),
        'number_dial': FldCfg(
            orig=5,
            dtype=BIGINT,
            astype='int64'
        ),
        'number_term': FldCfg(
            orig=6,
            dtype=BIGINT,
            astype='int64'
        ),
        'duration': FldCfg(
            orig=7,
            dtype=INTERVAL,
            astype='string'
        ),
        'state': FldCfg(
            orig=10,
            # dtype=ENUM(*STATE_LIST, name='att_stat_enum'),
            dtype=VARCHAR,
            astype='category'
        ),
        'dispo_code': FldCfg(
            orig=9,
            dtype=INTEGER,
            astype='Int64'
        ),
        'acct_af': FldCfg(
            orig=None,
            astype='UInt32',
            dtype=INTEGER
        )
    }
ATT_FILE_CFG = TblCfg(
    src_label='*DAILY DEBT - ANSWER FIRST*.tab.gz',
        fields=ATT_FILE_FIELDS,
        dtype={
            k: d['dtype']
            for k, d in ATT_FILE_FIELDS.items()
            if d['dtype'] != None
        } | {DATE_OUT_FLDNM: DATE},
        astype={
            k: d['astype']
            for k, d in ATT_FILE_FIELDS.items()
            if d['astype'] != None
        },
        rename={
            d['orig']: k
            for k, d in ATT_FILE_FIELDS.items()
            
        },
        use_cols=[
            d['orig'] for d in ATT_FILE_FIELDS.values()
            if d['orig'] != None
        ],
        other_={
            'dtparts': {
                'connected': ('connected_date', 'connected_time')
            },
            'fix_intervals': ['duration'],
            'non_null_filt': 'connected_date',
            'bad_filt_vals': ['TOTAL', '', None, ' '],
            'acct_col': 'acct_af',
            'acct_reptn': r'\d{4,5} - ',
            'dup_subset': [
                'connected_date',
                'connected_time',
                'number_orig',
                'number_dial'
            ]
        },
        out_cols=[
            k for k in ATT_FILE_FIELDS.keys()
            if ATT_FILE_FIELDS[k]['dtype'] != None
        ],
        skiphead=3,
        tblnm='att_data',
        xtra_sql=[],
        pre_sql=[],
        vintage_view_nm='att_vntge'
    )
[
    ATT_FILE_CFG['pre_sql'].append(s) for s in (
        f"""--sql
                DROP TABLE IF EXISTS {ATT_FILE_CFG['tblnm']} CASCADE;
        """.replace('--sql\n', ''),
        f"""--sql
                DROP VIEW IF EXISTS {ATT_FILE_CFG['vintage_view_nm']};
        """.replace('--sql\n', '')
    )
]

# Name of the timestamp field. Will be used to derive a delivery date.
INHOUSE_LEADS_TIMESTAMP_FLDNM = 'submitted'
# Statement to get all records from BQ.
BQ_SQL = f"""--sql
SELECT
    date_submitted {INHOUSE_LEADS_TIMESTAMP_FLDNM},
    af_acct,
    lead_name,
    lead_phone,
    practice,
    city,
    af_msg_id,
    send_to_emails
FROM `med_mstr.leads_email_form`
;
""".replace('--sql\n', '')
# Incoming fields:
INHOUSE_LEADS_SRC_FLDS: dict[str, FldCfg] = {
    'submitted': FldCfg(
        dtype=TIMESTAMP(timezone=False),
        astype='datetime64[ns]',
        enum_name=None,
        use_col=True,
    ),
    'af_acct': FldCfg(
        dtype=INTEGER,
        astype='Int32',
        enum_name=None,
        use_col=True,
    ),
    'lead_name': FldCfg(
        dtype=VARCHAR,
        astype='string',
        enum_name=None,
        use_col=True,
    ),
    'lead_phone': FldCfg(
        dtype=TEXT,
        astype='string',
        enum_name=None,
        use_col=True,
    ),
    'practice': FldCfg(
        dtype=None,
        astype='string',
        enum_name=None,
        use_col=True,
    ),
    'city': FldCfg(
        dtype=TEXT,
        astype='string',
        enum_name=None,
        use_col=True,
    ),
    CALLERID_FLDNM: FldCfg(
        dtype=BIGINT,
        astype='string',
        enum_name=None,
        use_col=True,
    ),
    'send_to_emails': FldCfg(
        dtype=TEXT,
        astype='string',
        enum_name=None,
        use_col=True,
    ),
}
# Extra fields:
INHOUSE_LEAD_DATE_FLDNM = 'lead_delivery_date'
INHOUSE_LEADS_OUT_FLDS: dict[str, FldCfg] = {
    INHOUSE_LEAD_DATE_FLDNM: FldCfg(
        astype='datetime64[ns]',
        dtype=DATE,
    ),
    'practice_id': FldCfg(
        dtype=INTEGER,
        astype=None
    ),
}
INHOUSE_LEADS_FLDS = INHOUSE_LEADS_SRC_FLDS | INHOUSE_LEADS_OUT_FLDS
INHOUSE_TBLNM = 'f_lead_email_form'
INHOUSE_LEADS_CFGS = TblCfg(
    tblnm=INHOUSE_TBLNM,
    src_label=BQ_SQL,
    dtype={k: v['dtype'] for k, v in INHOUSE_LEADS_FLDS.items() if v['dtype']},
    astype={
        k: v['astype'] for k, v in INHOUSE_LEADS_FLDS.items() if v['astype']},
    use_cols=[k for k in INHOUSE_LEADS_SRC_FLDS.keys()],
    pre_sql=[
        f"--sql DROP TABLE IF EXISTS {INHOUSE_TBLNM} CASCADE"
        .replace('--sql ', '')
    ],
)
chdir(CALLING_DIR)
LOGGER.debug(f"{__name__} execution time: {perf_counter() - PERF_START:.4f}")