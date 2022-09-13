from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from lib.utilities import chain_operators
from lib.configuration import get_current_config, get_today_dict

from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.create_databases.create_views_for_bulk_downloads import update_view_date_ranges
from QA.post_processing.BulkDownloadsTesterGranted import run_bulk_downloads_qa
from QA.post_processing.BulkDownloadsTesterPgpubs import run_bulk_downloads_qa as run_pgpubs_bulk_downloads_qa

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

project_home = os.environ['PACKAGE_HOME']
templates_searchpath = "{home}/resources".format(home=project_home)
config = get_current_config(type='granted_patent', supplemental_configs=None, **get_today_dict())

view_date_updater = DAG(
    dag_id='regenerate_bulk_downloads',
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

update_max_vi = PythonOperator(task_id='update_bulk_downloads_views', python_callable=update_view_date_ranges,
                        **operator_settings)

qa_granted_bulk_downloads = PythonOperator(task_id='qa_granted_bulk_downloads', python_callable=run_bulk_downloads_qa,
                        **operator_settings)

qa_pgpubs_bulk_downloads = PythonOperator(task_id='qa_pgpubs_bulk_downloads', python_callable=run_pgpubs_bulk_downloads_qa,
                        **operator_settings)

qa_granted_bulk_downloads.set_upstream(update_max_vi)
qa_pgpubs_bulk_downloads.set_upstream(update_max_vi)