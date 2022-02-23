from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from updater.create_databases.merge_in_new_data import post_merge_quarterly_granted, post_merge_quarterly_pgpubs
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.create_databases.rename_db import qc_database_quarterly_pgpubs, qc_database_quarterly_granted

default_args = {
    'owner': 'smadhavan',
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

qc_database_quarterly_granted_operator = PythonOperator(task_id='qc_database_quarterly_granted',
                                                        python_callable=qc_database_quarterly_granted,
                                                        dag=merge_quarterly,
                                                        on_success_callback=airflow_task_success,
                                                        on_failure_callback=airflow_task_failure
                                                        )

qc_database_quarterly_pgpubs_operator = PythonOperator(task_id='qc_database_quarterly_pgpubs',
                                                       python_callable=qc_database_quarterly_pgpubs,
                                                       dag=merge_quarterly,
                                                       on_success_callback=airflow_task_success,
                                                       on_failure_callback=airflow_task_failure
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

qc_merge_quarterly_pgpubs_operator.set_upstream(qc_database_quarterly_pgpubs_operator)
qc_merge_quarterly_patent_operator.set_upstream(qc_database_quarterly_granted_operator)
qc_database_quarterly_pgpubs_operator.set_upstream(qc_merge_quarterly_patent_operator)