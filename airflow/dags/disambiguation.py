import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from lib.configuration import get_config
from updater.callbacks import airflow_task_success, airflow_task_failure
from updater.disambiguation_support.export_disambiguation_data import export_disambig_data

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

disambiguation_dag = DAG(
    'disambiguation',
    description='Upload CPC files to database',
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    schedule_interval=None)
project_home = os.environ['PACKAGE_HOME']
config = get_config()
withdrawn_operator = PythonOperator(task_id='export_disambig_data', python_callable=export_disambig_data,
                                    op_kwargs={'config': config}, dag=disambiguation_dag,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure)
