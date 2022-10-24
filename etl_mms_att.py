# %%
from db_engines import mms_db, wh_db

import pandas as pd
from pandas import DataFrame as Df

from datetime import datetime, date, timedelta

from table_config import vntge_fmt, vntge_vw_sql, trailing_days, att_cfgs

# %%
# CONFIGS
# %%
# VALUES
tblnm: str = att_cfgs['tblnm']
vntge_vw_nm: str = att_cfgs['vintage_view_nm']
fields: list[str] = att_cfgs['field_stmts']

df_query_fields: str = ',\n'.join(fields)

df_query: str = """
        SELECT {f}
        FROM att_inbound
        WHERE connected > '{d}'
        ORDER BY connected DESC
        ;
    """

# ==============================
q_date_fmt: str = r'%Y-%M-%d'
now: datetime = datetime.now()
min_date: date = now.date() - timedelta(days=float(trailing_days))
min_date_str: str = min_date.strftime(q_date_fmt)
vntge_ts: str = now.strftime(vntge_fmt)
# %%
def main():
    df: Df
    with mms_db.connect() as conn:
        df = (
                pd.read_sql_query(
                    df_query.format(
                        # plug in min date
                        d=min_date_str,
                        # plug in columns
                        f=df_query_fields
                    ),
                    conn
                )
                .convert_dtypes()
            )

    with wh_db.connect() as conn:
        # update table
        df.to_sql(tblnm, conn, if_exists='replace', index=False)
        # create view for data vintage
        conn.execute(vntge_vw_sql.format(nm=vntge_vw_nm, ts=vntge_ts))

        print(f"\x1b[36;1mSuccessfully loaded {tblnm} to {wh_db.engine}\x1b[0m")

if __name__ == "__main__":
    main()
