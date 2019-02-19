from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime, timedelta
from Development.helpers import general_helpers
import configparser
import os
from Scripts.Website_Datbase_Generator import validate_query
import pprint
import configparser
import os

project_home = os.environ['PACKAGE_HOME']
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['smadhavan@air.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

reporting_db_dag = DAG("reporting_database_generation", default_args=default_args, start_date=datetime(2018, 12, 1),
                       schedule_interval=None)
prep_sql = PythonOperator(
    task_id='Preperation',
    provide_context=False,
    python_callable=validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs = {"filename":"00_Prep.sql"},
    templates_dict={'source_sql': '00_Prep.sql'},
    templates_exts=[".sql"],
    params={'raw_database': config['REPORTING_DATABASE_OPTIONS']['RAW_DATABASE_NAME'],
            'raw_database': config['REPORTING_DATABASE_OPTIONS']['REPORTING_DATABASE_NAME']},
    )
# start_json_downloads = PythonOperator(
#     task_id='start_json_downloads',
#     provide_context=False,
#     python_callable=start_download,
#     dag=health_data_dag)
# upload_to_google_bucket = PythonOperator(
#     task_id='upload_to_google_bucket',
#     provide_context=False,
#     python_callable=upload_to_storage_bucket,
#     dag=health_data_dag)
# load_bigquery_table = PythonOperator(
#     task_id='load_bigquery_table',
#     provide_context=False,
#     python_callable=load_to_bigquery_table,
#     dag=health_data_dag)
# start_json_downloads.set_upstream(collect_urls)
# upload_to_google_bucket.set_upstream(start_json_downloads)
# load_bigquery_table.set_upstream(upload_to_google_bucket)
