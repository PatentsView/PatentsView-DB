import configparser
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from slack_sdk import WebClient
from updater.callbacks import airflow_task_failure, airflow_task_success
from reporting_database_generator.database import validate_query
from QA.production.ProdDBTester import run_prod_db_qa
from go_live.comparison_flatfile import run_comparison_flatfile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['contact@patentsview.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 40,
    'queue': 'data_collector'
}
class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)

go_live_dag = DAG(dag_id="GO_LIVE"
                       , default_args=default_args
                       , start_date=datetime(2023, 4, 1)
                       , schedule_interval='@quarterly'
                       , template_searchpath="/project/go_live/")

web_tools = SQLTemplatedPythonOperator(
    task_id='Web_Tool',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=go_live_dag,
    op_kwargs={
        'filename': 'webtool_tables'
    },
    templates_dict={
        'source_sql': 'webtool_tables.sql'
    }
)

PVSupport = SQLTemplatedPythonOperator(
    task_id='PVSupport',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=go_live_dag,
    op_kwargs={
        'filename': 'PVSupport',
        'host':'PROD_DATABASE_SETUP'
    },
    templates_dict={
        'source_sql': 'PVSupport.sql'
    }
)

qa_production_data = PythonOperator(task_id='QA_PROD_DB'
                             , python_callable=run_prod_db_qa
                             , op_kwargs={'type': 'granted_patent'})

data_viz_comparison_ff = PythonOperator(task_id='data_viz_comparison_ff'
                             , python_callable=run_comparison_flatfile)

PVSupport.set_upstream(qa_production_data)
web_tools.set_upstream(qa_production_data)
data_viz_comparison_ff.set_upstream(qa_production_data)