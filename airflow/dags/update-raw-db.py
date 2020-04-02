from airflow import DAG
from datetime import datetime, timedelta
import os
from airflow.operators.python_operator import PythonOperator
# Helper Imports
from lib.configuration import get_config
from updater.callbacks import airflow_task_success, airflow_task_failure

# Parser Imports
from updater.xml_to_csv.bulk_downloads import bulk_download
from updater.xml_to_csv.preprocess_xml import preprocess_xml

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['contact@patentsview.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 4
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'update_raw_database',
    description='Download and process main granted patent data and corresponding classifications data',
    start_date=datetime(2019, 8, 13, 0, 0, 0),
    catchup=True,
    schedule_interval=None)
project_home = os.environ['PACKAGE_HOME']
config = get_config()

download_xml_operator = PythonOperator(dag=dag, task_id='download_xml', python_callable=bulk_download,
                                       op_kwargs={'config': config},
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure)
process_xml_operator = PythonOperator(task_id='process_xml',
                                      python_callable=preprocess_xml,
                                      dag=dag, op_kwargs={'config': config},
                                      on_success_callback=airflow_task_success,
                                      on_failure_callback=airflow_task_failure)

process_xml_operator.set_upstream(download_xml_operator)
