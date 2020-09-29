import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.granted_patent_parser.operator_sources.disambiguation_post_processing import add_postprocessing_operators
from lib.configuration import get_config
from updater.callbacks import airflow_task_success, airflow_task_failure

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

disambig_post_processing = DAG(
    '04_disambiguation_postprocessing',
    description='Disambiguation Post Processing',
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    schedule_interval=None)
project_home = os.environ['PACKAGE_HOME']
config = get_config()

add_postprocessing_operators(disambig_post_processing, config, project_home, airflow_task_success,
                             airflow_task_failure)
