from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from lib.utilities import chain_operators

from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.create_databases.create_views_for_bulk_downloads import update_view_date_ranges

default_args = {
    'owner': 'smadhavan',
    'depends_on_past': False,
    'email': ['contact@patentsview.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 4,
    'queue': 'data_collector'
}

view_date_updater = DAG(
    dag_id='export_view_date_update',
    default_args=default_args,
    description='update the maximum version indicator for the download export views',
    start_date=datetime(2022, 6, 30),
    schedule_interval='@quarterly',
    catchup=False
)

operator_settings = {
    'dag': view_date_updater,
    'on_success_callback': airflow_task_success,
    'on_failure_callback': airflow_task_failure,
    'on_retry_callback': airflow_task_failure
}

update_max_vi = PythonOperator(task_id='update_max_vi', python_callable=update_view_date_ranges,
                        **operator_settings)

## TODO: update operator arguments to 