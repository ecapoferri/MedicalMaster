# SETUP
from db_engines import wh_db as db
from pandas import read_sql_query, DataFrame as Df

from os import environ as os_environ
from dotenv import load_dotenv
load_dotenv()

# CONFIG LITERALS
query = """--sql
    SELECT
        client_callerid,
        toll_dialed,
        campaign_id,
        practice_id,
        UNNEST(names) name,
        -- eliminates arrays for cleaner csv
        -- groupby other cols and agg_array
        -- (
        --  pd vers: df.groupby([client_callerid, toll_dialed, campaign_id, practice_id, lead_delivery_date, lead_delivery_code, id]).agg(lambda x: list(x))
        --  or
        --  df.groupby([c for c in df.columns if c!='name']).agg(lambda x: list(x))
        --  might have to rename the agg'ed col
        -- )
        -- to repopulate the db
        lead_delivery_date,
        lead_delivery_code,
        id
    FROM billables;
""".replace('--sql\n', '')

out_fn = 'billables-BAK.csv'



def main():
    df: Df
    with db.connect() as conn:
        df = read_sql_query(
            sql=query,
            con=conn
        )

    df.to_csv(out_fn, index=False, encoding='utf-8')
    # df.to_gbq
    return


if __name__ == "__main__":
    main()
