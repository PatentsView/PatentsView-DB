import configparser
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from slack_sdk import WebClient
from updater.callbacks import airflow_task_failure, airflow_task_success
from reporting_database_generator.database import validate_query

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['bcard@air.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 4,
    'queue': 'data_collector'
}
class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)

go_live_dag = DAG("GO_LIVE"
                       , default_args=default_args
                       , start_date=datetime(2023, 10, 1)
                       , schedule_interval='@quarterly'
                       , template_searchpath="/project/go_live/")

web_tools = SQLTemplatedPythonOperator(
    task_id='Web_Tool',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=go_live_dag,
    op_kwargs={
        'filename': 'webtool_tables',
    },
    templates_dict={
        'source_sql': 'webtool_tables.sql'
    }
)

web_tools_2 = SQLTemplatedPythonOperator(
    task_id='Web_Tool_2',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=go_live_dag,
    op_kwargs={
        'filename': 'webtool2_tables',
        'host': 'APP_DATABASE_SETUP'
    },
    templates_dict={
        'source_sql': 'webtool2_tables.sql'
    }
)

web_tools_2.set_upstream(web_tools)