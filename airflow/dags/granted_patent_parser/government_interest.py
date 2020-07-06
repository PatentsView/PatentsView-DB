import os
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from updater.government_interest.NER import begin_NER_processing
from lib.configuration import get_config
from updater.callbacks import airflow_task_success, airflow_task_failure
from updater.government_interest.NER_to_manual import process_ner_to_manual
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
project_home = os.environ['PACKAGE_HOME']
config = get_config()
gi_dag = DAG('government_interest', description='Process Government Interest',
             start_date=datetime(2020, 1, 1, 0, 0, 0), catchup=True, schedule_interval=None)

gi_NER = PythonOperator(task_id='gi_NER', python_callable=begin_NER_processing, dag=gi_dag,
                        op_kwargs={'config': config}, on_success_callback=airflow_task_success,
                        on_failure_callback=airflow_task_failure)

gi_postprocess_NER = PythonOperator(task_id='postprocess_NER', python_callable=process_ner_to_manual, dag=gi_dag,
                                    op_kwargs={'config': config}, on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure)

gi_post_manual = PythonOperator(task_id='gi_post_manual', python_callable=process_post_manual, dag=gi_dag,
                                op_kwargs={'config': config}, on_success_callback=airflow_task_success,
                                on_failure_callback=airflow_task_failure)
