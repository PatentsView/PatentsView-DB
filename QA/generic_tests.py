from lib.configuration import get_connection_string, get_current_config, get_version_indicator
import datetime

import pymysql
from sqlalchemy import create_engine
from time import time


def qa_test_table_updated(table, db, **kwargs):
    config = get_current_config(db, schedule="quarterly", **kwargs)
    cstr = get_connection_string(config, 'PROD_DB')
    sd = config['DATES']["start_date"]
    ed = config['DATES']["end_date"]
    engine = create_engine(cstr)
    patent_cq_query = f"""
    select count(*), count(distinct version_indicator)
    from {table}
    where version_indicator >= {sd} and version_indicator<= {ed}
            """
    print(patent_cq_query)
    query_start_time = time()
    # engine.execute(patent_cq_query)
    rows = engine.execute(patent_cq_query).fetchone()[0]
    distinct_vi = engine.execute(patent_cq_query).fetchone()[1]
    query_end_time = time()
    print("This query took:", query_end_time - query_start_time, "seconds")
    print(f"{table} HAS {distinct_vi} UNIQUE WEEKS OF DATA")
    if rows == 0:
        raise Exception(f"{db}.{table} WAS NOT UPDATED FOR THE CURRENT TIMEFRAME")


if __name__ == '__main__':
    qa_test_table_updated('cpc_current', 'granted_patent', **{
        "execution_date": datetime.date(2021, 9, 1)
    })
