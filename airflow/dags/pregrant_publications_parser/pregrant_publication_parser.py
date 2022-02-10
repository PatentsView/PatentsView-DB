from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from lib.configuration import get_section
from reporting_database_generator.database.validate_query import validate_and_execute
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.create_databases.pregrant_database_setup import create_database
from updater.xml_to_sql.parser import begin_parsing
from updater.xml_to_sql.post_processing import begin_post_processing

# QA STEPS
from updater.create_databases.upload_new import post_upload_pgpubs
from updater.create_databases.rename_db import qc_database_pgpubs
from updater.create_databases.merge_in_new_data import post_merge_weekly_pgpubs, post_merge_quarterly_pgpubs, begin_text_merging_pgpubs
from updater.text_data_processor.text_table_parsing import post_text_merge_pgpubs, post_text_parsing_pgpubs



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

project_home = os.environ['PACKAGE_HOME']
templates_searchpath = "{home}/resources".format(home=project_home)
# config = get_current_config(type='pgpubs', supplemental_configs=None, **get_today_dict())

app_xml_dag = DAG(
    dag_id='pregrant_publication_updater',
    default_args=default_args,
    description='Download and process application patent data and corresponding classifications data',
    start_date=datetime(2021, 1, 7),
    schedule_interval=timedelta(weeks=1),
    catchup=True,
    template_searchpath=templates_searchpath
    # schedule_interval=None
)

create_database_operator = PythonOperator(task_id='create_pgpubs_database',
                                          python_callable=create_database,
                                          dag=app_xml_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure
                                          )

qc_database_operator = PythonOperator(task_id='qc_database_setup',
                                      python_callable=qc_database_pgpubs,
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

qc_upload_operator = PythonOperator(task_id='qc_upload_new',
                                    python_callable=post_upload_pgpubs,
                                    dag=app_xml_dag,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure
                                    )

qc_text_upload_operator = PythonOperator(task_id='qc_text_upload_new',
                                    python_callable=post_text_parsing_pgpubs,
                                    dag=app_xml_dag,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure
                                    )

merge_database_operator = SQLTemplatedPythonOperator(
    task_id='merge_database',
    provide_context=True,
    python_callable=validate_and_execute,
    op_kwargs={
        'filename': 'merge_into_pgpubs',
        "schema_only": False,
        "section": get_section('pregrant_publications', 'merge_database'),

    },
    dag=app_xml_dag,
    templates_dict={
        'source_sql': 'merge_into_pgpubs.sql'
    },
    templates_exts=['.sql'],
    params={
        'database': 'pgpubs_',
        'add_suffix': True
    }
)

# merge_text_database_operator = PythonOperator(task_id='merge_text_database',
#                                          python_callable=begin_text_merging_pgpubs,
#                                          provide_context=True,
#                                          dag=app_xml_dag,
#                                          on_success_callback=airflow_task_success,
#                                          on_failure_callback=airflow_task_failure
#                                          )

qc_merge_weekly_operator = PythonOperator(task_id='qc_merge_weekly',
                                         python_callable=post_merge_weekly_pgpubs,
                                         provide_context=True,
                                         dag=app_xml_dag,
                                         on_success_callback=airflow_task_success,
                                         on_failure_callback=airflow_task_failure
                                         )

qc_merge_weekly_text_operator = PythonOperator(task_id='qc_text_merge_weekly',
                                         python_callable=post_text_merge_pgpubs,
                                         provide_context=True,
                                         dag=app_xml_dag,
                                         on_success_callback=airflow_task_success,
                                         on_failure_callback=airflow_task_failure
                                         )

qc_database_operator.set_upstream(create_database_operator)
parse_xml_operator.set_upstream(qc_database_operator)
post_processing_operator.set_upstream(parse_xml_operator)
qc_upload_operator.set_upstream(post_processing_operator)
qc_text_upload_operator.set_upstream(post_processing_operator)
merge_database_operator.set_upstream(qc_upload_operator)
merge_database_operator.set_upstream(qc_text_upload_operator)
qc_merge_weekly_operator.set_upstream(merge_database_operator)
qc_merge_weekly_text_operator.set_upstream(merge_database_operator)

