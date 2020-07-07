import os
from datetime import datetime, timedelta

from airflow import DAG

from dags.granted_patent_parser.operator_sources.database_load import add_database_load_operators
from dags.granted_patent_parser.operator_sources.disambiguation import add_disambiguation_operators
from dags.granted_patent_parser.operator_sources.disambiguation_post_processing import add_postprocessing_operators
from dags.granted_patent_parser.operator_sources.government_interest import add_government_interest_operators
from dags.granted_patent_parser.operator_sources.supplemental_data import add_supplemental_operators
from dags.granted_patent_parser.operator_sources.update_xml_processing import add_xml_operators
from lib.configuration import get_config
from updater.callbacks import airflow_task_success, airflow_task_failure

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

granted_patent_updater = DAG(
    '01_update_granted_patent',
    description='Update Granted Patent Database',
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    schedule_interval=None)
project_home = os.environ['PACKAGE_HOME']
config = get_config()

xml_end = add_xml_operators(granted_patent_updater, config, airflow_task_success, airflow_task_failure)
database_load_end = add_database_load_operators(granted_patent_updater, config, project_home, airflow_task_success,
                                                airflow_task_failure, prefixes=xml_end)
add_government_interest_operators(granted_patent_updater, config, project_home, airflow_task_success,
                                  airflow_task_failure, prefixes=database_load_end)
supplemental_end = add_supplemental_operators(granted_patent_updater, config, project_home, airflow_task_success,
                                              airflow_task_failure, prefixes=database_load_end)
add_disambiguation_operators(granted_patent_updater, config, project_home, airflow_task_success,
                             airflow_task_failure, prefixes=supplemental_end)
