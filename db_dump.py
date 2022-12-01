from db_engines import wh_conn_str
from os import environ as os_environ, system as os_system
from dotenv import load_dotenv
load_dotenv()

dbnm = os_environ['PRMDIA_POSTGRES_CONT_MM_DB']

fn = f"db_{dbnm}.dump"

dump_cmd = f"pg_dump -f {fn} {wh_conn_str} && gzip {fn}"

def main():
    os_system(command=dump_cmd)

if __name__ == "__main__":
    main()
