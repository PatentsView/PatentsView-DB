import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from QA.collect_supplemental_data.cpc_parser.CPCCurrentTest import CPCTest
from lib.configuration import get_config
from updater.callbacks import airflow_task_success, airflow_task_failure
from updater.collect_supplemental_data.cpc_parser.download_cpc import collect_cpc_data, post_download
from updater.collect_supplemental_data.cpc_parser.cpc_class_parser import process_cpc_class_parser, post_class_parser
from updater.collect_supplemental_data.cpc_parser.cpc_parser import start_cpc_parser, post_cpc_parser
from updater.collect_supplemental_data.cpc_parser.process_cpc_current import process_and_upload_cpc_current
from updater.collect_supplemental_data.cpc_parser.process_wipo import process_and_upload_wipo
from updater.collect_supplemental_data.cpc_parser.upload_cpc_classes import upload_cpc_classes
from updater.collect_supplemental_data.update_withdrawn import process_withdrawn, post_withdrawn

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['contact@patentsview.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 4
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

supplemental_dag = DAG(
    'supplemental_data_load',
    description='Upload CPC files to database',
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    schedule_interval=None)
project_home = os.environ['PACKAGE_HOME']
config = get_config()
withdrawn_operator = PythonOperator(task_id='withdrawn_processor', python_callable=process_withdrawn,
                                    op_kwargs={'config': config}, dag=supplemental_dag,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure)

qc_withdrawn_operator = PythonOperator(task_id='qc_withdrawn_processor', python_callable=post_withdrawn,
                                       op_kwargs={'config': config}, dag=supplemental_dag,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure)
download_cpc_operator = PythonOperator(task_id='download_cpc', python_callable=collect_cpc_data,
                                       op_kwargs={'config': config}, dag=supplemental_dag,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure)
qc_download_cpc_operator = PythonOperator(task_id='qc_download_cpc', python_callable=post_download,
                                          op_kwargs={'config': config}, dag=supplemental_dag,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure)
cpc_class_parser_operator = PythonOperator(task_id='cpc_class_parser', python_callable=process_cpc_class_parser,
                                           op_kwargs={'config': config}, dag=supplemental_dag,
                                           on_success_callback=airflow_task_success,
                                           on_failure_callback=airflow_task_failure)

qc_cpc_class_parser_operator = PythonOperator(task_id='qc_cpc_class_parser', python_callable=post_class_parser,
                                              op_kwargs={'config': config}, dag=supplemental_dag,
                                              on_success_callback=airflow_task_success,
                                              on_failure_callback=airflow_task_failure)

cpc_parser_operator = PythonOperator(task_id='cpc_parser', python_callable=start_cpc_parser,
                                     op_kwargs={'config': config}, dag=supplemental_dag,
                                     on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure)
qc_cpc_parser_operator = PythonOperator(task_id='qc_cpc_parser', python_callable=post_cpc_parser,
                                        op_kwargs={'config': config}, dag=supplemental_dag,
                                        on_success_callback=airflow_task_success,
                                        on_failure_callback=airflow_task_failure)

cpc_current_operator = PythonOperator(task_id='cpc_current_processor', python_callable=process_and_upload_cpc_current,
                                      op_kwargs={'config': config}, dag=supplemental_dag,
                                      on_success_callback=airflow_task_success,
                                      on_failure_callback=airflow_task_failure)
wipo_operator = PythonOperator(task_id='wipo_processor', python_callable=process_and_upload_wipo,
                               op_kwargs={'config': config}, dag=supplemental_dag,
                               on_success_callback=airflow_task_success,
                               on_failure_callback=airflow_task_failure)
cpc_class_operator = PythonOperator(task_id='cpc_class_uploader', python_callable=upload_cpc_classes,
                                    op_kwargs={'config': config}, dag=supplemental_dag,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure)


def post_cpc_wipo(config):
    qc = CPCTest(config)
    qc.runTests()


qc_cpc_current_wipo_operator = PythonOperator(task_id='qc_cpc_current_wipot', python_callable=post_cpc_wipo,
                                              op_kwargs={'config': config}, dag=supplemental_dag,
                                              on_success_callback=airflow_task_success,
                                              on_failure_callback=airflow_task_failure)

qc_download_cpc_operator.set_upstream(download_cpc_operator)

cpc_class_parser_operator.set_upstream(qc_download_cpc_operator)
qc_cpc_class_parser_operator.set_upstream(cpc_class_parser_operator)
cpc_class_operator.set_upstream(qc_cpc_class_parser_operator)
cpc_parser_operator.set_upstream(qc_download_cpc_operator)
qc_cpc_parser_operator.set_upstream(cpc_parser_operator)

cpc_current_operator.set_upstream(qc_cpc_parser_operator)
wipo_operator.set_upstream(cpc_current_operator)

qc_cpc_current_wipo_operator.set_upstream(wipo_operator)
qc_cpc_current_wipo_operator.set_upstream(cpc_current_operator)
qc_cpc_current_wipo_operator.set_upstream(cpc_class_operator)
qc_withdrawn_operator.set_upstream(withdrawn_operator)
