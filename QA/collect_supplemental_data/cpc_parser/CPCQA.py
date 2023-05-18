from lib.configuration import get_connection_string, get_current_config

from sqlalchemy import create_engine
from time import time
import datetime
from sqlalchemy import create_engine
import pymysql.cursors
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

def qa_cpc_current(**kwargs):
    db = kwargs['db']
    config = get_current_config(db, schedule="quarterly", **kwargs)
    sd = config['DATES']["start_date"]
    ed = config['DATES']["end_date"]
    df = pd.DataFrame(columns=['date'])
    df.loc[0] = [ed]
    df['quarter'] = pd.to_datetime(df.date).dt.to_period('Q')
    quarter = str(df['quarter'][0])

    connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                      user=config['DATABASE_SETUP']['USERNAME'],
                                      password=config['DATABASE_SETUP']['PASSWORD'],
                                      db=config['PATENTSVIEW_DATABASES']["PROD_DB"],
                                      charset='utf8mb4',
                                      cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    try:
        if not connection.open:
            connection.connect()
        qa_data = {
            'DataMonitor_cpc': []
        }
        patent_cq_query = f"""
        select version_indicator
            , count(distinct section_id) as unique_sections
            , count(distinct subsection_id) as unique_subsections
            , count(distinct group_id) as unique_groups
            , count(distinct subgroup_id) as unique_subgroups
            , count(*) as total_rows
        from cpc_current
        where version_indicator >= DATE({sd}) and version_indicator <= DATE({ed})
        group by 1
                """
        with connection.cursor() as generic_cursor:
            query_start_time = time()
            generic_cursor.execute(patent_cq_query)
            query_end_time = time()
            print("\t\tThis query took:", query_end_time - query_start_time, "seconds")
            return_records = generic_cursor.fetchall()
            rows = len(return_records)-1
            print(f"Cpc_current HAS {rows} UNIQUE WEEKS OF DATA")
            if rows < 1:
                raise Exception(f"{db}.Cpc_current WAS NOT UPDATED FOR THE CURRENT TIMEFRAME")
            for row in return_records:
                qa_data['DataMonitor_cpc'].append(
                    {
                        "database_type": db,
                        'update_version': row[0],
                        'unique_sections': row[1],
                        'unique_subsections': row[2],
                        'unique_groups': row[3],
                        'unique_subgroups': row[4],
                        'total_rows': row[5],
                        'quarter': quarter
                    })

    finally:
        if connection.open:
            connection.close()
    return qa_data


def qa_wipo(**kwargs):
    db = kwargs['db']
    config = get_current_config(type = db, schedule="quarterly", **kwargs)
    if db=='granted_patent':
        id = 'patent_id'
    else:
        id = "document_number"
    sd = config['DATES']["start_date"]
    ed = config['DATES']["end_date"]
    df = pd.DataFrame(columns=['date'])
    df.loc[0] = [ed]
    df['quarter'] = pd.to_datetime(df.date).dt.to_period('Q')
    quarter = str(df['quarter'][0])
    connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                      user=config['DATABASE_SETUP']['USERNAME'],
                                      password=config['DATABASE_SETUP']['PASSWORD'],
                                      db=config['PATENTSVIEW_DATABASES']["PROD_DB"],
                                      charset='utf8mb4',
                                      cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    try:
        if not connection.open:
            connection.connect()
        qa_data = {
            'DataMonitor_wipo': []
        }
        wipo_query = f"""
        select version_indicator
            , count(distinct field_id) as unique_fields
            , count(distinct {id}) as unique_ids
            , count(*) as total_rows
        from wipo
        where version_indicator >= DATE({sd}) and version_indicator <= DATE({ed})
        group by 1
                """
        print(wipo_query)
        with connection.cursor() as generic_cursor:
            query_start_time = time()
            generic_cursor.execute(wipo_query)
            query_end_time = time()
            print("\t\tThis query took:", query_end_time - query_start_time, "seconds")
            return_records = generic_cursor.fetchall()
            rows = len(return_records) - 1
            print(f"Wipo HAS {rows} UNIQUE WEEKS OF DATA")
            if rows < 1:
                raise Exception(f"{db}.WIPO WAS NOT UPDATED FOR THE CURRENT TIMEFRAME")
            for row in return_records:
                qa_data['DataMonitor_wipo'].append(
                    {
                        "database_type": db,
                        'update_version': row[0],
                        'unique_fields': row[1],
                        'unique_ids': row[2],
                        'total_rows': row[3],
                        'quarter': quarter
                    })
    finally:
        if connection.open:
            connection.close()
    return qa_data


def save_qa_data(qa_data, **kwargs):
    db = kwargs['db']
    config = get_current_config(db, schedule="quarterly", **kwargs)
    qa_connection_string = get_connection_string(config, database='QA_DATABASE', connection='APP_DATABASE_SETUP')
    qa_engine = create_engine(qa_connection_string)
    for qa_table in qa_data:
        qa_table_data = qa_data[qa_table]
        table_frame = pd.DataFrame(qa_table_data)
        try:
            table_frame.to_sql(name=qa_table, if_exists='append', con=qa_engine, index=False)
        except SQLAlchemyError as e:
            table_frame.to_csv("errored_qa_data" + qa_table, index=False)
            raise e

if __name__ == '__main__':
    run_list = [datetime.date(2022, 6, 30), datetime.date(2022, 3, 31), datetime.date(2021, 12, 3), datetime.date(2021, 9, 30)]
    db = 'granted_patent'
    for d in run_list:
        # qa_data = qa_cpc_current(db, **{"execution_date": d})
        qa_data = qa_wipo(db, **{"execution_date": d})
        save_qa_data(qa_data, db, **{"execution_date": d})
