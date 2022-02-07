from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from updater.create_databases.merge_in_new_data import post_merge_quarterly_granted, post_merge_quarterly_pgpubs
from updater.callbacks import airflow_task_failure, airflow_task_success



default_args = {
    'owner': 'cdipietro',
    'depends_on_past': False,
    'email': ['contact@patentsview.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 4,
    'queue': 'data_collector'
}


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


merge_quarterly = DAG(
    dag_id='merge_quarterly_updater',
    default_args=default_args,
    description='Download and process application patent data and corresponding classifications data',
    start_date=datetime(2021, 10, 1),
    schedule_interval='@quarterly',
    catchup=True
    # schedule_interval=None
)

qc_merge_quarterly_patent_operator = PythonOperator(task_id='qc_merge_quarterly_patents',
                                         python_callable=post_merge_quarterly_granted,
                                         provide_context=True,
                                         dag=merge_quarterly,
                                         on_success_callback=airflow_task_success,
                                         on_failure_callback=airflow_task_failure
                                         )

qc_merge_quarterly_pgpubs_operator = PythonOperator(task_id='qc_merge_quarterly_pgpubs',
                                         python_callable=post_merge_quarterly_pgpubs,
                                         provide_context=True,
                                         dag=merge_quarterly,
                                         on_success_callback=airflow_task_success,
                                         on_failure_callback=airflow_task_failure
                                         )


qc_merge_quarterly_pgpubs_operator.set_upstream(qc_merge_quarterly_patent_operator)

