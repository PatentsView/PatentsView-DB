from lib.configuration import get_connection_string, get_required_tables, get_current_config
from sqlalchemy import create_engine
import datetime

def reporting_db_creation(dbtype='granted_patent' , **kwargs):
    from lib.configuration import get_current_config
    config = get_current_config(dbtype, schedule="quarterly", **kwargs)
    ed = config['DATES']['END_DATE']
    connection_string = get_connection_string(config, database="PROD_DB")
    reporting_db = f'PatentsView_{ed}'
    engine = create_engine(connection_string)
    with engine.connect() as con:
        con.execute(f"""
DROP DATABASE if exists {reporting_db};
        """)
        con.execute(f"""
create database if not exists {reporting_db} default character set=utf8mb4
default collate=utf8mb4_unicode_ci
            """)


if __name__ == '__main__':
    reporting_db_creation(dbtype='granted_patent' , **{
        "execution_date": datetime.date(2024, 10, 1)
    })