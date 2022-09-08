from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from lib.utilities import chain_operators

from lib.configuration import get_current_config, get_today_dict
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.government_interest.NER import begin_NER_processing
from updater.government_interest.NER_to_manual import process_ner_to_manual
from updater.government_interest.post_manual import process_post_manual, qc_gi
from updater.government_interest.simulate_manual import simulate_manual

project_home = os.environ['PACKAGE_HOME']
templates_searchpath = "{home}/resources".format(home=project_home)
config = get_current_config(type='granted_patent', supplemental_configs=None, **get_today_dict())

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

run_pgp_NER = DAG(
    dag_id='pgpubs_gov_int_NER',
    default_args=default_args,
    description='process government_interest statements for PGPubs',
    start_date=datetime(2022, 6, 30),
    schedule_interval='@quarterly',
    catchup=False
)

operator_settings = {
    'dag': run_pgp_NER,
    'on_success_callback': airflow_task_success,
    'on_failure_callback': airflow_task_failure,
    'on_retry_callback': airflow_task_failure
}

## TODO: update operator arguments 
db_args = {'doctype':'granted_patent','database':'PGPUBS_DATABASE'}

### GI Processing
gi_NER = PythonOperator(task_id='gi_NER', python_callable=begin_NER_processing, 
                        op_kwargs = {'doctype':'granted_patent','database':'PGPUBS_DATABASE'}, 
                        **operator_settings)
                        
gi_postprocess_NER = PythonOperator(task_id='postprocess_NER', python_callable=process_ner_to_manual,
                                    op_kwargs = {'doctype':'granted_patent','database':'PGPUBS_DATABASE'}, 
                                    **operator_settings)

manual_simulation_operator = PythonOperator(task_id='simulate_manual_task', python_callable=simulate_manual,
                                            op_kwargs = {'doctype':'granted_patent'}, 
                                            **operator_settings)

post_manual_operator = PythonOperator(task_id='post_manual', python_callable=process_post_manual,
                                      op_kwargs = {'doctype':'granted_patent','database':'PGPUBS_DATABASE'}, 
                                      **operator_settings)

gi_qc_operator = PythonOperator(task_id='GI_QC', python_callable=qc_gi,
                                op_kwargs = {'doctype':'granted_patent'}, 
                                **operator_settings)

chain_operators([gi_NER, gi_postprocess_NER, manual_simulation_operator, post_manual_operator, gi_qc_operator])