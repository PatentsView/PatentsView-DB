from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from updater.create_databases.merge_in_new_data import post_merge_quarterly_granted, post_merge_quarterly_pgpubs
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.create_databases.rename_db import qc_database_quarterly_pgpubs, qc_database_quarterly_granted
from updater.text_data_processor.text_table_parsing import post_text_merge_quarterly_pgpubs, post_text_merge_quarterly_granted
from updater.post_processing.post_process_location import consolidate_location_disambiguation_patent, consolidate_location_disambiguation_pgpubs

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


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


merge_quarterly = DAG(
    dag_id='merge_quarterly_updater',
    default_args=default_args,
    description='Download and process application patent data and corresponding classifications data',
    start_date=datetime(2022, 6, 1),
    schedule_interval='@quarterly',
    catchup=True
    # schedule_interval=None
)

consolidation_task_granted = PythonOperator(task_id='consolidate_weekly_location_maps_granted',
                                            python_callable = consolidate_location_disambiguation_patent,
                                            dag=merge_quarterly,
                                            on_success_callback=airflow_task_success,
                                            on_failure_callback=airflow_task_failure
                                            )

consolidation_task_pregrant = PythonOperator(task_id='consolidate_weekly_location_maps_pgpubs',
                                            python_callable = consolidate_location_disambiguation_pgpubs,
                                            dag=merge_quarterly,
                                            on_success_callback=airflow_task_success,
                                            on_failure_callback=airflow_task_failure
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

qc_merge_text_quarterly_patent_operator = PythonOperator(task_id='qc_text_merge_quarterly_patents',
                                                    python_callable=post_text_merge_quarterly_granted,
                                                    provide_context=True,
                                                    dag=merge_quarterly,
                                                    on_success_callback=airflow_task_success,
                                                    on_failure_callback=airflow_task_failure
                                                    )

qc_merge_text_quarterly_pgpubs_operator = PythonOperator(task_id='qc_text_merge_quarterly_pgpubs',
                                                    python_callable=post_text_merge_quarterly_pgpubs,
                                                    provide_context=True,
                                                    dag=merge_quarterly,
                                                    on_success_callback=airflow_task_success,
                                                    on_failure_callback=airflow_task_failure
                                                    )

qc_database_quarterly_granted_operator.set_upstream(consolidation_task_granted)
qc_database_quarterly_granted_operator.set_upstream(consolidation_task_pregrant)
qc_merge_quarterly_patent_operator.set_upstream(qc_database_quarterly_granted_operator)
qc_database_quarterly_pgpubs_operator.set_upstream(qc_merge_quarterly_patent_operator)
qc_merge_quarterly_pgpubs_operator.set_upstream(qc_database_quarterly_pgpubs_operator)

qc_merge_text_quarterly_patent_operator.set_upstream(qc_merge_quarterly_pgpubs_operator)
qc_merge_text_quarterly_pgpubs_operator.set_upstream(qc_merge_text_quarterly_patent_operator)