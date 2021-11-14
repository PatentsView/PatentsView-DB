from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.create_databases.pregrant_database_setup import create_database, drop_database, merge_database
from updater.xml_to_sql.parser import begin_parsing
from updater.xml_to_sql.post_processing import begin_post_processing

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

app_xml_dag = DAG(
    dag_id='pregrant_publication_updater',
    default_args=default_args,
    description='Download and process application patent data and corresponding classifications data',
    start_date=datetime(2021, 7, 1),
    schedule_interval=timedelta(weeks=1),
    catchup=True
    # schedule_interval=None
)

create_database_operator = PythonOperator(task_id='create_pgpubs_database',
                                          python_callable=create_database,
                                          dag=app_xml_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure
                                          )

parse_xml_operator = PythonOperator(task_id='parse_pgpubs_xml',
                                    python_callable=begin_parsing,
                                    dag=app_xml_dag,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    pool='database_write_iops_contenders'
                                    )

post_processing_operator = PythonOperator(task_id='post_process',
                                          python_callable=begin_post_processing,
                                          dag=app_xml_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          pool='database_write_iops_contenders'
                                          )

merge_database_operator = PythonOperator(task_id='merge_database',
                                         python_callable=merge_database,
                                         provide_context=True,
                                         dag=app_xml_dag,
                                         on_success_callback=airflow_task_success,
                                         on_failure_callback=airflow_task_failure,
                                         pool='database_write_iops_contenders'
                                         )

parse_xml_operator.set_upstream(create_database_operator)
post_processing_operator.set_upstream(parse_xml_operator)
merge_database_operator.set_upstream(post_processing_operator)
