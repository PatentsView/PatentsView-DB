import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from QA.collect_supplemental_data.cpc_parser.CPCCurrentTest import CPCTest
from lib.configuration import get_current_config, get_today_dict
# appending a path
from lib.utilities import chain_operators
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.collect_supplemental_data.cpc_parser.cpc_class_parser import post_class_parser, process_cpc_class_parser
from updater.collect_supplemental_data.cpc_parser.download_cpc import collect_cpc_data, post_download
from updater.collect_supplemental_data.cpc_parser.process_cpc_current import process_and_upload_cpc_current
from updater.collect_supplemental_data.cpc_parser.process_wipo import process_and_upload_wipo
from updater.collect_supplemental_data.cpc_parser.upload_cpc_classes import upload_cpc_classes


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


def post_cpc_wipo(**kwargs):
    current_config = get_current_config(type='granted_patent', schedule='quarterly', **kwargs)
    qc = CPCTest(current_config)
    qc.runTests()


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

cpc_wipo_updater = DAG(
    dag_id='classifications_parser',
    default_args=default_args,
    description='Download and process CPC and WIPO classification data for each update',
    start_date=datetime(2021, 7, 1),
    schedule_interval='@quarterly',
    template_searchpath=templates_searchpath,
    catchup=True
)

download_cpc_operator = PythonOperator(task_id='download_cpc',
                                       python_callable=collect_cpc_data,
                                       provide_context=True,
                                       dag=cpc_wipo_updater,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure)

qc_download_cpc_operator = PythonOperator(task_id='qc_download_cpc',
                                          dag=cpc_wipo_updater,
                                          python_callable=post_download,
                                          provide_context=True,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure)

cpc_class_parser_operator = PythonOperator(task_id='cpc_class_parser',
                                           python_callable=process_cpc_class_parser,
                                           dag=cpc_wipo_updater,
                                           provide_context=True,
                                           on_success_callback=airflow_task_success,
                                           on_failure_callback=airflow_task_failure)
#
# # Good
qc_cpc_class_parser_operator = PythonOperator(task_id='qc_cpc_class_parser',
                                              python_callable=post_class_parser,
                                              dag=cpc_wipo_updater,
                                              provide_context=True,
                                              on_success_callback=airflow_task_success,
                                              on_failure_callback=airflow_task_failure)
#
# # consolidate_cpc_data changed {raw_db}.cpc_current to {raw_db}.temp_cpc_current
cpc_current_operator = PythonOperator(task_id='cpc_current_processor',
                                      python_callable=process_and_upload_cpc_current,
                                      dag=cpc_wipo_updater,
                                      provide_context=True,
                                      on_success_callback=airflow_task_success,
                                      on_failure_callback=airflow_task_failure,
                                      pool='database_write_iops_contenders')
#
# # consolidate_wipo changed INSERT INTO wipo to INSERT INTO temp_wipo
wipo_operator = PythonOperator(task_id='wipo_processor',
                               python_callable=process_and_upload_wipo,
                               dag=cpc_wipo_updater,
                               provide_context=True,
                               on_success_callback=airflow_task_success,
                               on_failure_callback=airflow_task_failure,
                               pool='database_write_iops_contenders')

# # Good
cpc_class_operator = PythonOperator(task_id='cpc_class_uploader',
                                    python_callable=upload_cpc_classes,
                                    dag=cpc_wipo_updater,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure)

qc_cpc_current_wipo_operator = PythonOperator(task_id='qc_cpc_current_wipo',
                                              python_callable=post_cpc_wipo,
                                              dag=cpc_wipo_updater,
                                              provide_context=True,
                                              on_success_callback=airflow_task_success,
                                              on_failure_callback=airflow_task_failure)
operator_sequence = {}
operator_sequence['cpc_lookup_sequence'] = [download_cpc_operator,
                                            qc_download_cpc_operator,
                                            cpc_class_parser_operator,
                                            qc_cpc_class_parser_operator, cpc_class_operator,
                                            qc_cpc_current_wipo_operator]

operator_sequence['cpc_current_sequence'] = [qc_download_cpc_operator, cpc_current_operator, wipo_operator,
                                             qc_cpc_current_wipo_operator]

for dependency_group in operator_sequence:
    dependency_sequence = operator_sequence[dependency_group]
    chain_operators(dependency_sequence)
