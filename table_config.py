from typing import\
    NewType,\
    TypedDict,\
    Optional

from sqlalchemy.types import TypeEngine
from sqlalchemy.dialects.postgresql import TIMESTAMP, INTEGER, BIGINT, BOOLEAN, VARCHAR, TEXT

from os import environ
from dotenv import load_dotenv
load_dotenv()

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


#== TABLE CONFIGS ==================>
trailing_days: int = 10

vntge_vw_sql = """
        CREATE OR REPLACE VIEW {nm} AS
        SELECT CAST('{ts}' AS TIMESTAMP) AS {nm}
        ;
    """

vntge_fmt: str = r'%Y-%m-%d %H:%M:%S'


# ATT - for a simple query to mms
att_cfgs: dict[str, dict|str] = {
    'tblnm': 'att_data',
    'vintage_view_nm': 'att_vntge',
    'field_stmts': [
        'id',
        "CONVERT_TZ(connected, 'UTC', 'US/Central') AS connected",
        'duration',
        'number_orig',
        'number_dial',
        'number_term',
        "CONVERT_TZ(disconnect, 'UTC', 'US/Central') AS disconnect",
        'zip',
        'state',
        "CONVERT_TZ(created, 'UTC', 'US/Central') AS created",
    ]

}


# ANSWER FIRST
af_fields = {
    'connected': FldCfg(
        orig='Date/Time',
        dtype=TIMESTAMP
    ),
    'recording_id': FldCfg(
        orig='Recording#',
        dtype=INTEGER
    ),
    'callerid': FldCfg(
        orig='Caller ID',
        dtype=BIGINT
    ),
    'call_for_ad': FldCfg(
        orig='CallType',
        dtype=BOOLEAN
    ),
    'caller_name': FldCfg(
        orig='Caller',
        dtype=VARCHAR
    ),
    'phone': FldCfg(
        orig='Phone',
        dtype=VARCHAR
    ),
    'ext': FldCfg(
        orig='Extension',
        dtype=INTEGER
    ),
    'addr_state': FldCfg(
        orig='State',
        dtype=VARCHAR
    ),
    'addr_city': FldCfg(
        orig='City',
        dtype=VARCHAR
    ),
    'practice_id': FldCfg(
        orig='PracticeID',
        dtype=INTEGER
    ),
    'besttime': FldCfg(
        orig='BestTime',
        dtype=VARCHAR
    ),
    'sent_emails_to': FldCfg(
        orig='SentEmailsTo',
        dtype=TEXT
    ),
    'reference': FldCfg(
        orig='Reference',
        dtype=TEXT
    ),
    'city_id': FldCfg(
        orig='CityID',
        dtype=VARCHAR
    ),
    'email': FldCfg(
        orig='Email',
        dtype=VARCHAR
    ),
    'statecheck': FldCfg(
        orig='StateCheck',
        dtype=VARCHAR(2)
    ),
    'zipcode': FldCfg(
        orig='PostalCode',
        dtype=INTEGER
    ),
    'majorcity': FldCfg(
        orig='MajorCity',
        dtype=VARCHAR
    ),
    'acct': FldCfg(
        dtype=INTEGER,
        orig=None
    ),
    'dispo': FldCfg(
        dtype=TEXT,
        orig='_DISPOSITION'
    ),
    'history': FldCfg(
        dtype=TEXT,
        orig='History'
    )
}
af_cfgs = TblCfg(
    # for glob or regex
    src_label=environ['PRMDIA_REPOS_AF_FILE_LABEL']+'||'+environ['PRMDIA_REPOS_AF_FILE_EXT'],
    tblnm='af_message_data',
    fields=af_fields,
    vintage_view_nm='af_message_vntge',
    rename={d['orig']: k for k, d in af_fields.items() if d['orig']},
    other_={
        'filter_fld': 'connected',
        'remap': {
            'call_for_ad': {
                'Yes': True,
                'No': False
            }
        }
    },
    skiphead=1,
    dtype={k: d['dtype'] for k, d in af_fields.items()}
)
#<== TABLE CONFIGS =================<
