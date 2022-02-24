import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from QA.collect_supplemental_data.cpc_parser.CPCCurrentTest import CPCTest
from lib.configuration import get_current_config, get_today_dict
# appending a path
from lib.utilities import chain_operators
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.disambiguation.lawyer_disambiguation.lawyer_disambiguation import start_lawyer_disambiguation, \
    load_clean_rawlawyer, clean_rawlawyer, rawlawyer_postprocesing, prepare_tables, post_process_qc


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


project_home = os.environ['PACKAGE_HOME']
templates_searchpath = "{home}/resources".format(home=project_home)
config = get_current_config(type='config.ini', supplemental_configs=None, **get_today_dict())

print(config)

default_args = {
    'owner': 'smadhavan',
    'depends_on_past': False,
    'email': ['contact@patentsview.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=120),
    'concurrency': 40,
    'queue': 'data_collector'
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

disambiguation = DAG(
    dag_id='lawyer_disambiguation',
    default_args=default_args,
    description='Perform lawyer disambiguation',
    start_date=datetime(2021, 10, 1),
    schedule_interval='@quarterly',
    template_searchpath=templates_searchpath,
    catchup=True,
)

quarterly_merge_completed = ExternalTaskSensor(
    task_id="quarterly_merge_completed",
    external_dag_id="merge_quarterly_updater",
    external_task_id="qc_merge_quarterly_pgpubs",
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode="reschedule",
)

prepare_rawlawyer_table = PythonOperator(task_id='prepare_rawlawyer_table',
                                         python_callable=prepare_tables,
                                         provide_context=True,
                                         dag=disambiguation,
                                         on_success_callback=airflow_task_success,
                                         on_failure_callback=airflow_task_failure,
                                         queue='disambiguator')
clean_rawlawyer_table = PythonOperator(task_id='clean_rawlawyer_table',
                                       python_callable=clean_rawlawyer,
                                       provide_context=True,
                                       dag=disambiguation,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure,
                                       queue='disambiguator')
load_clean_rawlawyer_table = PythonOperator(task_id='load_clean_rawlawyer_table',
                                            python_callable=load_clean_rawlawyer,
                                            provide_context=True,
                                            dag=disambiguation,
                                            on_success_callback=airflow_task_success,
                                            on_failure_callback=airflow_task_failure,
                                            queue='disambiguator')
lawyer_disambiguation = PythonOperator(task_id='lawyer_disambiguation',
                                       python_callable=start_lawyer_disambiguation,
                                       provide_context=True,
                                       dag=disambiguation,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure,
                                       queue='disambiguator', pool='high_memory_pool')
lawyer_post_disambiguation = PythonOperator(task_id='lawyer_post_disambiguation',
                                            python_callable=rawlawyer_postprocesing,
                                            provide_context=True,
                                            dag=disambiguation,
                                            on_success_callback=airflow_task_success,
                                            on_failure_callback=airflow_task_failure,
                                            queue='disambiguator')
lawyer_disambiguation_QC = PythonOperator(task_id='lawyer_disambiguation_QC',
                                          python_callable=post_process_qc,
                                          provide_context=True,
                                          dag=disambiguation,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          queue='disambiguator')
operator_sequence = {}
operator_sequence['lawyer_disambiguation'] = [prepare_rawlawyer_table, clean_rawlawyer_table,
                                              load_clean_rawlawyer_table, lawyer_disambiguation,
                                              lawyer_post_disambiguation, lawyer_disambiguation_QC]

for dependency_group in operator_sequence:
    dependency_sequence = operator_sequence[dependency_group]
    chain_operators(dependency_sequence)

prepare_rawlawyer_table.set_upstream(quarterly_merge_completed)