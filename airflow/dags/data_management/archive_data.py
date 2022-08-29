from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from lib.download_check_delete_databases import run_table_archive, run_database_archive
from updater.callbacks import airflow_task_failure, airflow_task_success

import pendulum

local_tz = pendulum.timezone("America/New_York")


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

project_home = os.environ['PACKAGE_HOME']
templates_searchpath = "{home}/resources".format(home=project_home)
# config = get_current_config(type='pgpubs', supplemental_configs=None, **get_today_dict())

archive_data_dag = DAG(
    dag_id='archive_data',
    default_args=default_args,
    description='Create data backup, upload and compare it to the original, and delete the original data in the production DB',
    start_date=datetime(2021, 1, 7, hour=5, minute=0, second=0, tzinfo=local_tz),
    catchup=False,
    template_searchpath=templates_searchpath,
    schedule_interval=timedelta(weeks=1)
)

# BACKUP TABLES

# TABLE LIST FORMAT EXAMPLE
# table_list = "assignee_20210330,assignee_20210629,assignee_archive,lawyer_20210330"
run_table_archive_patent = PythonOperator(task_id='run_table_archive_patent',
                                          python_callable=run_table_archive,
                                          dag=archive_data_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          op_kwargs={'config_db': 'granted_patent',
                                                     'tablelist': [],
                                                     'output_path': '/PatentDataVolume/DatabaseBackups/RawDatabase/patent_db_tables'}
                                          )

run_table_archive_pgpubs = PythonOperator(task_id='run_table_archive_pgpubs',
                                          python_callable=run_table_archive,
                                          dag=archive_data_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          op_kwargs={'config_db': 'pregrant_publications',
                                                     'tablelist': [],
                                                     'output_path': '/PatentDataVolume/DatabaseBackups/PregrantPublications/pgpubs_db_tables'}
                                          )

run_oldest_upload_db_archive = PythonOperator(task_id='run_oldest_upload_db',
                                          python_callable=run_database_archive,
                                          dag=archive_data_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          op_kwargs={'type': 'granted_patent',
                                                     'output_path': '/PatentDataVolume/DatabaseBackups/RawDatabase'}
                                          )

run_oldest_pgpubs_db_archive = PythonOperator(task_id='run_oldest_pgpubs_db',
                                          python_callable=run_database_archive,
                                          dag=archive_data_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          op_kwargs={'type': 'pgpubs',
                                                     'output_path': '/PatentDataVolume/DatabaseBackups/PregrantPublications'}
                                          )

# archive_data_dag.set_upstream(create_database_operator)

# BACKUP DATABASES


