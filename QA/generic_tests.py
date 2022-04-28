from lib.configuration import get_connection_string, get_current_config, get_version_indicator
from lib.xml_helpers import process_date
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


def update_to_granular_version_indicator(table, db):
    from lib.configuration import get_current_config, get_connection_string
    config = get_current_config(type=db, **{"execution_date": datetime.date(2000, 1, 1)})
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    if db == 'granted_patent':
        id = 'id'
        fk = 'patent_id'
        fact_table = 'patent'
    else:
        id = 'document_number'
        fk = 'document_number'
        fact_table = 'publication'
    query = f"""
update {table} update_table 
	inner join {fact_table} p on update_table.{fk}=p.{id}
set update_table.version_indicator=p.version_indicator     
    """
    print(query)
    query_start_time = time()
    engine.execute(query)
    query_end_time = time()
    print("This query took:", query_end_time - query_start_time, "seconds")

def update_version_indicator(table, db, **kwargs):
    from lib.configuration import get_current_config, get_connection_string
    config = get_current_config(type=db, schedule="quarterly", **kwargs)
    ed = process_date(config['DATES']["end_date"], as_string=True)
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    query = f"""
update {table} update_table 
set update_table.version_indicator={ed} 
    """
    print(query)
    query_start_time = time()
    engine.execute(query)
    query_end_time = time()
    print("This query took:", query_end_time - query_start_time, "seconds")



if __name__ == '__main__':
    update_to_granular_version_indicator('cpc_current', 'pgpubs')
    # update_version_indicator('lawyer', 'granted_patent', **{
    #     "execution_date": datetime.date(2021, 10, 1)
    # })
    # update_version_indicator('persistent_assignee_disambig_long', 'granted_patent', **{
    #     "execution_date": datetime.date(2021, 10, 1)
    # })
