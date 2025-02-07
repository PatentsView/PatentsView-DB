import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.configuration import get_current_config, get_section, get_today_dict
from lib.download_check_delete_databases import run_table_archive, run_database_archive
from lib.utilities import chain_operators
from reporting_database_generator.database.validate_query import validate_and_execute
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.collect_supplemental_data.update_withdrawn import post_withdrawn, process_withdrawn
from updater.create_databases.merge_in_new_data import begin_merging, begin_text_merging, post_merge_weekly_granted, \
    post_merge_quarterly_granted
from updater.create_databases.rename_db import check_patent_prod_integrity, qc_database_granted
from updater.create_databases.upload_new import begin_database_setup, post_upload_granted, upload_current_data
from updater.disambiguation.location_disambiguation.generate_locationID import run_location_disambiguation, \
    run_location_disambiguation_tests
from updater.disambiguation.location_disambiguation.osm_location_match import geocode_by_osm

from updater.government_interest.NER import begin_NER_processing
from updater.government_interest.NER_to_manual import process_ner_to_manual
from updater.government_interest.post_manual import process_post_manual, qc_gi
from updater.government_interest.simulate_manual import simulate_manual
from updater.text_data_processor.text_table_parsing import begin_text_parsing, post_text_parsing_granted, \
    post_text_merge_granted
from updater.xml_to_csv.bulk_downloads import bulk_download
from updater.xml_to_csv.parse_patents import patent_parser
from updater.xml_to_csv.preprocess_xml import preprocess_xml
from updater.xml_to_sql.patent_parser import patent_sql_parser

import pendulum

local_tz = pendulum.timezone("America/New_York")


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


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
    'queue': 'data_collector',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),

}
granted_patent_parser = DAG(
    dag_id='granted_patent_updater',
    default_args=default_args,
    description='Download and process granted patent data and corresponding classifications data',
    start_date=datetime(2022, 9, 27, hour=5, minute=0, second=0, tzinfo=local_tz),
    schedule_interval=timedelta(weeks=1),
    catchup=True,
    template_searchpath=templates_searchpath,
)



operator_settings = {
    'dag': granted_patent_parser,
    'on_success_callback': airflow_task_success,
    'on_failure_callback': airflow_task_failure,
    'on_retry_callback': airflow_task_failure
}

operator_sequence_groups = {}
# ##### Start Instance #####
# instance_starter = PythonOperator(task_id='start_data_collector', python_callable=start_data_collector_instance,
#                                   **operator_settings)
###### BACKUP OLDEST DATABASE #######
backup_data = PythonOperator(task_id='backup_oldest_database'
                             , python_callable=run_database_archive
                             , op_kwargs={'type': 'granted_patent'}
                             , queue= 'mydumper'
                             , on_success_callback = airflow_task_success
                             , on_failure_callback = airflow_task_failure
                             , on_retry_callback = airflow_task_failure)
###### Download & Parse #######
download_xml_operator = PythonOperator(task_id='download_xml', python_callable=bulk_download,
                                       dag=granted_patent_parser,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure,
                                       on_retry_callback=airflow_task_failure)
upload_setup_operator = PythonOperator(task_id='upload_database_setup', python_callable=begin_database_setup,
                                       dag=granted_patent_parser,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure,
                                       on_retry_callback=airflow_task_failure)

process_xml_operator = PythonOperator(task_id='process_xml',
                                      python_callable=preprocess_xml,
                                      dag=granted_patent_parser,
                                      on_success_callback=airflow_task_success,
                                      on_failure_callback=airflow_task_failure,
                                      on_retry_callback=airflow_task_failure
                                      , pool='high_memory_pool')

parse_xml_operator = PythonOperator(task_id='parse_xml',
                                    python_callable=patent_parser,
                                    dag=granted_patent_parser,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    on_retry_callback=airflow_task_failure,
                                    pool='high_memory_pool')

#### Database Load ####
qc_temp_database_operator = PythonOperator(task_id='qc_upload_database_setup',
                                           python_callable=qc_database_granted,
                                           dag=granted_patent_parser,
                                           on_success_callback=airflow_task_success,
                                           on_failure_callback=airflow_task_failure,
                                           on_retry_callback=airflow_task_failure)

upload_new_operator = PythonOperator(task_id='upload_current', python_callable=upload_current_data,
                                     dag=granted_patent_parser,
                                     on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure,
                                     on_retry_callback=airflow_task_failure
                                     )
upload_trigger_operator = SQLTemplatedPythonOperator(
    task_id='create_uuid_triggers',
    provide_context=True,
    python_callable=validate_and_execute,
    dag=granted_patent_parser,
    on_success_callback=airflow_task_success,
    on_failure_callback=airflow_task_failure,
    on_retry_callback=airflow_task_failure,
    op_kwargs={
        'filename': 'granted_patent_database',
        "schema_only": False,
        "section": get_section('granted_patent_updater', 'fix_patent_ids-upload'),

    },
    templates_dict={
        'source_sql': 'granted_patent_database.sql'
    },
    templates_exts=['.sql'],
    params={
        'database': 'upload_',
        'add_suffix': True
    }
)
patent_sql_operator = PythonOperator(task_id='parse_xml_to_sql', python_callable=patent_sql_parser,
                                     dag=granted_patent_parser,
                                     on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure,
                                     on_retry_callback=airflow_task_failure
                                     )
qc_upload_operator = PythonOperator(task_id='qc_upload_new', python_callable=post_upload_granted,
                                    dag=granted_patent_parser,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    on_retry_callback=airflow_task_failure
                                    )
### new OSM ElasticSearch geocoding
OSM_geocode_operator = PythonOperator(task_id='geocode_rawlocations', python_callable=geocode_by_osm,
                                      dag=granted_patent_parser,
                                      on_success_callback=airflow_task_success,
                                      on_failure_callback=airflow_task_failure,
                                      on_retry_callback=airflow_task_failure,
                                      pool = 'elastic_search_pool'
                                      )

### Location_ID generation
loc_disambiguation = PythonOperator(task_id='loc_disambiguation', python_callable=run_location_disambiguation, op_kwargs={'dbtype': 'granted_patent'},
                                    dag=granted_patent_parser,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    on_retry_callback=airflow_task_failure)

qc_loc_disambiguation = PythonOperator(task_id='qc_loc_disambiguation'
                                       , python_callable=run_location_disambiguation_tests
                                       , op_kwargs={'dbtype': 'granted_patent'}
                                       , dag=granted_patent_parser,
                                      on_success_callback=airflow_task_success,
                                      on_failure_callback=airflow_task_failure,
                                      on_retry_callback=airflow_task_failure)

### GI Processing
gi_NER = PythonOperator(task_id='gi_NER', python_callable=begin_NER_processing,
                        dag=granted_patent_parser,
                        on_success_callback=airflow_task_success,
                        on_failure_callback=airflow_task_failure,
                        on_retry_callback=airflow_task_failure)
gi_postprocess_NER = PythonOperator(task_id='postprocess_NER', python_callable=process_ner_to_manual,
                                    dag=granted_patent_parser,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    on_retry_callback=airflow_task_failure)
manual_simulation_operator = PythonOperator(task_id='simulate_manual_task', python_callable=simulate_manual,
                                            dag=granted_patent_parser,
                                            on_success_callback=airflow_task_success,
                                            on_failure_callback=airflow_task_failure,
                                            on_retry_callback=airflow_task_failure)

post_manual_operator = PythonOperator(task_id='post_manual', python_callable=process_post_manual,
                                      dag=granted_patent_parser,
                                      on_success_callback=airflow_task_success,
                                      on_failure_callback=airflow_task_failure,
                                      on_retry_callback=airflow_task_failure)
gi_qc_operator = PythonOperator(task_id='GI_QC', python_callable=qc_gi,
                                dag=granted_patent_parser,
                                on_success_callback=airflow_task_success,
                                on_failure_callback=airflow_task_failure,
                                on_retry_callback=airflow_task_failure)

### Long Text FIelds Parsing
table_creation_operator = SQLTemplatedPythonOperator(
    task_id='create_text_yearly_tables',
    provide_context=True,
    python_callable=validate_and_execute,
    dag=granted_patent_parser,
  on_success_callback=airflow_task_success,
  on_failure_callback=airflow_task_failure,
  on_retry_callback=airflow_task_failure,
    op_kwargs={
        'filename': 'text_tables',
        "schema_only": False,
        "section": get_section('granted_patent_updater', 'fix_patent_ids-upload')
    },
    templates_dict={
        'source_sql': 'text_tables.sql'
    },
    templates_exts=['.sql'],
    params={
        'database': 'patent_text',
        'add_suffix': False
    }
)
upload_table_creation_operator = SQLTemplatedPythonOperator(
    task_id='create_text_yearly_tables-upload',
    provide_context=True,
    python_callable=validate_and_execute,
    op_kwargs={
        'filename': 'text_tables',
        "schema_only": False,
        "section": get_section('granted_patent_updater', 'fix_patent_ids-upload'),

    },
    dag=granted_patent_parser,
    on_success_callback=airflow_task_success,
    on_failure_callback=airflow_task_failure,
    on_retry_callback=airflow_task_failure,
    templates_dict={
        'source_sql': 'text_tables.sql'
    },
    templates_exts=['.sql'],
    params={
        'database': 'upload_',
        'add_suffix': True
    }
)

# trigger_creation_operator = BashOperator(task_id='create_text_triggers',
#                                          bash_command=get_text_table_load_command( project_home),
#                                          on_success_callback=airflow_task_success,
#                                          on_failure_callback=airflow_task_failure)
# trigger_creation_operator.set_upstream(table_creation_operator)

parse_text_data_operator = PythonOperator(task_id='parse_text_data',
                                          python_callable=begin_text_parsing,
                                          dag=granted_patent_parser,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          on_retry_callback=airflow_task_failure)

patent_id_fix_operator = SQLTemplatedPythonOperator(
    task_id='fix_patent_ids-upload',
    provide_context=True,
    python_callable=validate_and_execute,
    dag=granted_patent_parser,
    on_success_callback=airflow_task_success,
    on_failure_callback=airflow_task_failure,
    on_retry_callback=airflow_task_failure,
    op_kwargs={
        'filename': 'patent_id_fix_text',
        "schema_only": False,
        "section": get_section('granted_patent_updater', 'fix_patent_ids-upload')
    },
    templates_dict={
        'source_sql': 'patent_id_fix_text.sql'
    },
    templates_exts=['.sql'],
    params={
        'database': 'upload_',
        'add_suffix': True
    }
)

qc_parse_text_operator = PythonOperator(task_id='qc_parse_text_data',
                                        python_callable=post_text_parsing_granted,
                                        dag=granted_patent_parser,
                                        on_success_callback=airflow_task_success,
                                        on_failure_callback=airflow_task_failure,
                                        on_retry_callback=airflow_task_failure)

#### merge in newly parsed data
integrity_check_operator = PythonOperator(task_id='check_prod_integrity',
                                          python_callable=check_patent_prod_integrity,
                                          dag=granted_patent_parser,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          on_retry_callback=airflow_task_failure,
                                          )

merge_new_operator = PythonOperator(task_id='merge_db',
                                    python_callable=begin_merging,
                                    dag=granted_patent_parser,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    on_retry_callback=airflow_task_failure
                                    )

qc_merge_operator = PythonOperator(task_id='qc_merge_db',
                                   python_callable=post_merge_weekly_granted,
                                   dag=granted_patent_parser,
                                   on_success_callback=airflow_task_success,
                                   on_failure_callback=airflow_task_failure,
                                   on_retry_callback=airflow_task_failure
                                   )

## Merge Text Data
merge_text_operator = PythonOperator(task_id='merge_text_db',
                                     python_callable=begin_text_merging,
                                     dag=granted_patent_parser,
                                     on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure,
                                     on_retry_callback=airflow_task_failure
                                     )

qc_text_merge_operator = PythonOperator(task_id='qc_merge_text_db',
                                        python_callable=post_text_merge_granted,
                                        depends_on_past=True,
                                        dag=granted_patent_parser,
                                        on_success_callback=airflow_task_success,
                                        on_failure_callback=airflow_task_failure,
                                        on_retry_callback=airflow_task_failure
                                        )

## Withdrawn Patents
withdrawn_operator = PythonOperator(task_id='withdrawn_processor', python_callable=process_withdrawn,
                                    dag=granted_patent_parser,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    on_retry_callback=airflow_task_failure)

qc_withdrawn_operator = PythonOperator(task_id='qc_withdrawn_processor', python_callable=post_withdrawn,
                                       dag=granted_patent_parser,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure,
                                       on_retry_callback=airflow_task_failure
                                       )

operator_sequence_groups['xml_sequence'] = [download_xml_operator, process_xml_operator,
                                            parse_xml_operator, upload_new_operator,
                                            upload_trigger_operator, patent_sql_operator,
                                            patent_id_fix_operator, qc_upload_operator, OSM_geocode_operator,
                                            loc_disambiguation, qc_loc_disambiguation, gi_NER,
                                            gi_postprocess_NER, manual_simulation_operator, post_manual_operator,
                                            gi_qc_operator, withdrawn_operator, qc_withdrawn_operator,
                                            merge_new_operator]

operator_sequence_groups['text_sequence'] = [upload_setup_operator, qc_temp_database_operator,
                                             upload_table_creation_operator, parse_text_data_operator,
                                             patent_id_fix_operator, qc_parse_text_operator,
                                             table_creation_operator, merge_text_operator]

operator_sequence_groups['xml_text_cross_dependency'] = [download_xml_operator, parse_text_data_operator]
operator_sequence_groups['xml_preprare_dependency'] = [qc_temp_database_operator, upload_new_operator]
operator_sequence_groups['merge_prepare_xml_dependency'] = [integrity_check_operator, merge_new_operator]
operator_sequence_groups['merge_prepare_text_dependency'] = [integrity_check_operator, merge_text_operator]
for dependency_group in operator_sequence_groups:
    dependency_sequence = operator_sequence_groups[dependency_group]
    chain_operators(dependency_sequence)

qc_merge_operator.set_upstream(merge_new_operator)
qc_text_merge_operator.set_upstream(merge_text_operator)
backup_data.set_upstream(download_xml_operator)
