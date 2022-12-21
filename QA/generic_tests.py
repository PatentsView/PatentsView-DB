from lib.configuration import get_connection_string, get_current_config

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
    where version_indicator >= DATE({sd}) and version_indicator <= DATE({ed})
            """
    print(patent_cq_query)
    query_start_time = time()
    rows, distinct_vi = engine.execute(patent_cq_query).fetchone()
    query_end_time = time()
    print("This query took:", query_end_time - query_start_time, "seconds")
    print(f"{table} HAS {distinct_vi} UNIQUE WEEKS OF DATA")
    if rows == 0:
        raise Exception(f"{db}.{table} WAS NOT UPDATED FOR THE CURRENT TIMEFRAME")



if __name__ == '__main__':
    raise NotImplementedError
