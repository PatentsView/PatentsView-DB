import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from lib.configuration import get_config, get_scp_copy_command
from updater.callbacks import airflow_task_success, airflow_task_failure
from updater.government_interest.post_manual import process_post_manual

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

disambig_support = DAG(
    '03_disambiguation_support',
    description='Disambiguation Support Step',
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    schedule_interval=None)
project_home = os.environ['PACKAGE_HOME']
config = get_config()

upload_disambig = BashOperator(task_id='upload_disambig_files', bash_command=get_scp_copy_command(config),
                               dag=disambig_support, on_success_callback=airflow_task_success,
                               on_failure_callback=airflow_task_failure)
