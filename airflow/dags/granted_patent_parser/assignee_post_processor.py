import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.configuration import get_current_config, get_today_dict
from lib.utilities import chain_operators
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.post_processing.post_process_assignee import post_process_assignee, post_process_qc
from updater.post_processing.post_process_persistent import prepare_wide_table, update_long_entity, write_wide_table

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

assignee_post_processor = DAG(
        dag_id='assignee_post_processor',
        default_args=default_args,
        description='Run assignee Post Processing',
        start_date=datetime(2021, 2, 1),
        schedule_interval=None,
        template_searchpath=templates_searchpath,
        )

post_process_assignee_operator = PythonOperator(task_id='post_process_assignee',
                                                python_callable=post_process_assignee,
                                                dag=assignee_post_processor,
                                                on_success_callback=airflow_task_success,
                                                on_failure_callback=airflow_task_failure)

update_granted_persistent_long_assignee = PythonOperator(
        task_id='update_persistent_long_assignee',
        python_callable=update_long_entity,
        op_kwargs={
                'entity': 'assignee',
                'database_type':'granted_patent'
                },
        dag=assignee_post_processor
        )

create_granted_persistent_wide_assignee = PythonOperator(
        task_id='create_persistent_wide_assignee',
        python_callable=write_wide_table,
        op_kwargs={
                'entity': 'assignee',
                'database_type':'granted_patent'
                },
        dag=assignee_post_processor
        )

prepare_granted_persistent_wide_assignee = PythonOperator(
        task_id='prepare_persistent_wide_assignee',
        python_callable=prepare_wide_table,
        op_kwargs={
                'entity': 'assignee',
                'database_type':'granted_patent'
                },
        dag=assignee_post_processor
        )

update_pregrant_persistent_long_assignee = PythonOperator(
        task_id='update_persistent_long_assignee',
        python_callable=update_long_entity,
        op_kwargs={
                'entity': 'assignee',
                'database_type':'pgpubs'
                },
        dag=assignee_post_processor
        )

create_pregrant_persistent_wide_assignee = PythonOperator(
        task_id='create_persistent_wide_assignee',
        python_callable=write_wide_table,
        op_kwargs={
                'entity': 'assignee',
                'database_type':'pgpubs'
                },
        dag=assignee_post_processor
        )

prepare_pregrant_persistent_wide_assignee = PythonOperator(
        task_id='prepare_persistent_wide_assignee',
        python_callable=prepare_wide_table,
        op_kwargs={
                'entity': 'assignee',
                'database_type':'pgpubs'
                },
        dag=assignee_post_processor
        )

qc_post_process_assignee_operator = PythonOperator(task_id='qc_post_process_assignee',
                                                   python_callable=post_process_qc,
                                                   dag=assignee_post_processor,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure)
dependency_list = {}
dependency_list['granted_persistent'] = [
        post_process_assignee_operator,
        prepare_granted_persistent_wide_assignee,
        update_granted_persistent_long_assignee,
        create_granted_persistent_wide_assignee,
        qc_post_process_assignee_operator]
dependency_list['pgpubs_persistent'] = [
        post_process_assignee_operator,
        prepare_pregrant_persistent_wide_assignee,
        update_pregrant_persistent_long_assignee,
        create_pregrant_persistent_wide_assignee,
        qc_post_process_assignee_operator]

for dependency_group, dependency_sequence in dependency_list.items():
    chain_operators(dependency_sequence)
