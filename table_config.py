from typing import\
    NewType,\
    TypedDict,\
    Optional

from sqlalchemy.types import TypeEngine
from sqlalchemy.dialects.postgresql import INTEGER, BIGINT, BOOLEAN, VARCHAR, TEXT, ENUM, INTERVAL, TIMESTAMP

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
    out_cols: Optional[list[str]]


#== TABLE CONFIGS ==================>
trailing_days: int = 10

vntge_vw_sql = """--sql
        CREATE OR REPLACE VIEW {nm} AS
        SELECT CAST('{ts}' AS TIMESTAMP) AS {nm}
        ;
    """.replace('--sql\n', '')

enum_sql = """
        DROP TYPE IF EXISTS {} CASCADE;
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

state_list: list[str] = [
    'AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UM', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY'
]
# ANSWER FIRST
af_fields = {
    'connected': FldCfg(
        orig='Date/Time',
        dtype=TIMESTAMP(timezone=True),
        astype='datetime64[ns, US/Central]',
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
        dtype=ENUM(*state_list, name='af_lead_state_enum'),
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
            },
        },
        'enums': {
            d['enum_name']: d['dtype']
            for d in af_fields.values()
            if d['enum_name']
        },
    },
    skiphead=1,
    dtype={
        k: d['dtype'] for k, d in af_fields.items()
        if
            (type(d['dtype']) != None)
            &
            (type(d['dtype']) != type(ENUM('x', name='x')))
    },
    astype={
        k: d['astype']
        for k, d in af_fields.items()
        if d['astype']
    }
)


att_file_fields: dict[str|int, FldCfg] = {
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
            # dtype=TIMESTAMP(timezone=True)
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
            # dtype=ENUM(*state_list, name='att_stat_enum'),
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
att_file_cfg = TblCfg(
        src_label='RPRT.ATT||*.tab.gz',
        fields=att_file_fields,
        dtype={
            k: d['dtype']
            for k, d in att_file_fields.items()
            if d['dtype'] != None
        },
        astype={
            k: d['astype']
            for k, d in att_file_fields.items()
            if d['astype'] != None
        },
        rename={
            d['orig']: k
            for k, d in att_file_fields.items()
            
        },
        use_cols=[
            d['orig'] for d in att_file_fields.values()
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
            k for k in att_file_fields.keys()
            if att_file_fields[k]['dtype'] != None
        ],
        skiphead=3,
        tblnm='att_data',
        xtra_sql=[],
        pre_sql=[],
        vintage_view_nm='att_vntge'
    )
[
    att_file_cfg['pre_sql'].append(s) for s in (
        f"""--sql
                DROP TABLE IF EXISTS {att_file_cfg['tblnm']} CASCADE;
        """.replace('--sql\n', ''),
        f"""--sql
                DROP VIEW IF EXISTS {att_file_cfg['vintage_view_nm']};
        """.replace('--sql\n', '')
    )
]
