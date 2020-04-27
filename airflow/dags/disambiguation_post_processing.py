import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from lib.configuration import get_config, get_scp_copy_command, get_scp_download_command
from updater.callbacks import airflow_task_success, airflow_task_failure
from updater.post_processing.post_process_assignee import post_process_assignee
from updater.post_processing.post_process_inventor import post_process_inventor

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

disambiguation_post_processing = DAG(
    'disambiguation_post_processing',
    description='Upload CPC files to database',
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    schedule_interval=None)
project_home = os.environ['PACKAGE_HOME']
config = get_config()

download_disambig_operator = BashOperator(task_id='download_disambiguation',
                                          bash_command=get_scp_download_command(config),
                                          dag=disambiguation_post_processing,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure
                                          )
post_process_inventor_operator = PythonOperator(task_id='post_process_inventor', python_callable=post_process_inventor,
                                                op_kwargs={'config': config}, dag=disambiguation_post_processing,
                                                on_success_callback=airflow_task_success,
                                                on_failure_callback=airflow_task_failure)

post_process_assignee_operator = PythonOperator(task_id='post_process_assignee', python_callable=post_process_assignee,
                                                op_kwargs={'config': config}, dag=disambiguation_post_processing,
                                                on_success_callback=airflow_task_success,
                                                on_failure_callback=airflow_task_failure)
post_process_inventor_operator.set_upstream(download_disambig_operator)
post_process_assignee_operator.set_upstream(download_disambig_operator)
