import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

from lib.configuration import get_current_config, get_today_dict
from lib.is_it_update_time import determine_granted_sensor_date, determine_pregranted_sensor_date, \
    determine_update_eligibility
from lib.utilities import chain_operators

default_args = {
        'owner':            'smadhavan',
        'depends_on_past':  False,
        'email':            ['contact@patentsview.org'],
        'email_on_failure': False,
        'email_on_retry':   False,
        'retries':          3,
        'retry_delay':      timedelta(minutes=120),
        'concurrency':      40
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        }

project_home = os.environ['PACKAGE_HOME']
templates_searchpath = "{home}/resources".format(home=project_home)
config = get_current_config(type='config.ini', supplemental_configs=None, **get_today_dict())
data_updater = DAG(
        dag_id='data_updater',
        default_args=default_args,
        description='Start update process after verifying data collection is complete',
        start_date=datetime(2021, 2, 1),
        schedule_interval='0 0 10 * 3,5',
        template_searchpath=templates_searchpath,
        catchup=True
        )
weekly_update_check_granted_db = ExternalTaskSensor(task_id='check_granted_data_collection', dag=data_updater,
                                                    external_dag_id='granted_patent_updater',
                                                    external_task_id='merge_db',
                                                    execution_date_fn=determine_granted_sensor_date,
                                                    mode='reschedule')
weekly_update_check_pregrant = ExternalTaskSensor(task_id='check_pregrant_data_collection', dag=data_updater,
                                                  external_dag_id='pregrant_publication_updater',
                                                  external_task_id='merge_database',
                                                  execution_date_fn=determine_pregranted_sensor_date, mode='reschedule')
weekly_update_check_granted_text_db = ExternalTaskSensor(task_id='check_text_data_collection',
                                                         dag=data_updater,
                                                         external_dag_id='granted_patent_updater',
                                                         external_task_id='merge_text_db',
                                                         execution_date_fn=determine_granted_sensor_date,
                                                         mode='reschedule')
update_eligibility_operator = ShortCircuitOperator(dag=data_updater, python_callable=determine_update_eligibility,
                                                   task_id='determine_update_eligibility')

# trigger_cpc_operator = PythonOperator(task_id='merge_database',
#                                       python_callable=lambda: 2,
#                                       provide_context=True,
#                                       dag=data_updater
#                                       )

trigger_cpc_operator = TriggerDagRunOperator(trigger_dag_id='classifications_parser', task_id='download_cpc',
                                             execution_date='{{ execution_date }}')

operator_sequences = {
        'granted_eligibility_sequence':  [weekly_update_check_granted_db, update_eligibility_operator],
        'text_eligibility_sequence':     [weekly_update_check_granted_text_db, update_eligibility_operator],
        'pregrant_eligibility_sequence': [weekly_update_check_pregrant, update_eligibility_operator],
        'update_sequence':               [update_eligibility_operator, trigger_cpc_operator]
        }

for dependency_group, dependency_sequence in operator_sequences.items():
    chain_operators(dependency_sequence)
