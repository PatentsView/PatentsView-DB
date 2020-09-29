from airflow import DAG
from datetime import datetime, timedelta
import os
from airflow.operators.python_operator import PythonOperator
import configparser
import sys
from updater.callbacks import airflow_task_success, airflow_task_failure

project_home = os.environ['PACKAGE_HOME']
sys.path.append(project_home + '/updater/text_parser/')

# Parser Imports
from updater.xml_to_sql.import_sql import create_database
from updater.xml_to_sql.import_sql import post_create_database
from updater.xml_to_sql.import_sql import merge_database
from updater.xml_to_sql.import_sql import post_merge_database
from updater.xml_to_sql.import_sql import drop_database
from updater.xml_to_sql.post_processing import begin_post_processing
from updater.xml_to_sql.post_processing import post_upload_database
from updater.xml_to_sql.parser import begin_parsing

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['contact@patentsview.org'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 13,

}

app_xml_dag = DAG(
    dag_id='parse_app_xml_10year',
    default_args=default_args,
    description='Download and process application patent data and corresponding classifications data',
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2018, 12, 31),
    schedule_interval=timedelta(weeks=1)
    # schedule_interval=None
)

create_database_operator = PythonOperator(task_id='create_database',
                                          provide_context=True,
                                          python_callable=create_database,
                                          dag=app_xml_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure
                                          )

# create_database_QA = PythonOperator(task_id='create_database_QA',
#                                           provide_context=True,
#                                           python_callable=post_create_database,
#                                           dag=app_xml_dag,
#                                           on_success_callback=airflow_task_success,
#                                           on_failure_callback=airflow_task_failure
#                                           )

parse_xml_operator = PythonOperator(task_id='parse_xml',
                                    python_callable=begin_parsing,
                                    provide_context=True,
                                    dag=app_xml_dag,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure
                                    )

post_processing_operator = PythonOperator(task_id='post_process',
                                          python_callable=begin_post_processing,
                                          provide_context=True,
                                          dag=app_xml_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure
                                          )

# upload_database_QA = PythonOperator(task_id='upload_database_QA',
#                                           provide_context=True,
#                                           python_callable=post_upload_database,
#                                           dag=app_xml_dag,
#                                           on_success_callback=airflow_task_success,
#                                           on_failure_callback=airflow_task_failure
#                                           )

merge_database_operator = PythonOperator(task_id='merge_database',
                                         python_callable=merge_database,
                                         provide_context=True,
                                         dag=app_xml_dag,
                                         on_success_callback=airflow_task_success,
                                         on_failure_callback=airflow_task_failure
                                         )

# merge_database_QA = PythonOperator(task_id='merge_database_QA',
#                                           provide_context=True,
#                                           python_callable=post_merge_database,
#                                           dag=app_xml_dag,
#                                           on_success_callback=airflow_task_success,
#                                           on_failure_callback=airflow_task_failure
#                                           )

drop_database_operator = PythonOperator(task_id='drop_database',
                                        python_callable=drop_database,
                                        provide_context=True,
                                        dag=app_xml_dag,
                                        on_success_callback=airflow_task_success,
                                        on_failure_callback=airflow_task_failure
                                        )

parse_xml_operator.set_upstream(create_database_operator)
post_processing_operator.set_upstream(parse_xml_operator)
merge_database_operator.set_upstream(post_processing_operator)
drop_database_operator.set_upstream(merge_database_operator)

# create_database_QA.set_upstream(create_database_operator)
# parse_xml_operator.set_upstream(create_database_QA)
# post_processing_operator.set_upstream(parse_xml_operator)
# upload_database_QA.set_upstream(post_processing_operator)
# merge_database_operator.set_upstream(upload_database_QA)
# merge_database_QA.set_upstream(merge_database_operator)
