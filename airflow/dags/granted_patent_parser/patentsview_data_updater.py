import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.configuration import get_current_config, get_section, get_today_dict
from reporting_database_generator.database.validate_query import validate_and_execute
from updater.callbacks import airflow_task_failure
from updater.collect_supplemental_data.update_withdrawn import post_withdrawn, process_withdrawn
from updater.create_databases.merge_in_new_data import begin_merging, begin_text_merging, post_merge, post_text_merge
from updater.create_databases.rename_db import qc_database
from updater.create_databases.upload_new import begin_database_setup, post_upload, upload_current_data
from updater.government_interest.NER import begin_NER_processing
from updater.government_interest.NER_to_manual import process_ner_to_manual
from updater.government_interest.post_manual import process_post_manual
from updater.government_interest.simulate_manual import simulate_manual
from updater.text_data_processor.text_table_parsing import begin_text_parsing, post_text_parsing
from updater.xml_to_csv.bulk_downloads import bulk_download
from updater.xml_to_csv.parse_patents import patent_parser
from updater.xml_to_csv.preprocess_xml import preprocess_xml
from updater.xml_to_sql.patent_parser import patent_sql_parser


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


project_home = os.environ['PACKAGE_HOME']
templates_searchpath = "{home}/resources".format(home=project_home)
config = get_current_config(type='granted_patent', supplemental_configs=None, **get_today_dict())

default_args = {
        'owner':            'smadhavan',
        'depends_on_past':  False,
        'email':            ['contact@patentsview.org'],
        'email_on_failure': False,
        'email_on_retry':   False,
        'retries':          3,
        'retry_delay':      timedelta(minutes=5),
        'concurrency':      4
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        }
granted_patent_parser = DAG(
        dag_id='granted_patent_updater',
        default_args=default_args,
        description='Download and process granted patent data and corresponding classifications data',
        start_date=datetime(2021, 1, 5),
        schedule_interval=timedelta(weeks=1), catchup=True,
        template_searchpath=templates_searchpath,
        )
operator_settings = {
        'dag':                 granted_patent_parser,
        'on_success_callback': airflow_task_failure,
        'on_failure_callback': airflow_task_failure
        }
operator_sequence_groups = {}
###### Download & Parse #######
download_xml_operator = PythonOperator(task_id='download_xml', python_callable=bulk_download,
                                       **operator_settings)
upload_setup_operator = PythonOperator(task_id='upload_database_setup', python_callable=begin_database_setup,
                                       **operator_settings)

process_xml_operator = PythonOperator(task_id='process_xml',
                                      python_callable=preprocess_xml,
                                      **operator_settings)

parse_xml_operator = PythonOperator(task_id='parse_xml',
                                    python_callable=patent_parser,
                                    **operator_settings)

#### Database Load ####
qc_database_operator = PythonOperator(task_id='qc_database_setup',
                                      python_callable=qc_database,
                                      **operator_settings)

upload_new_operator = PythonOperator(task_id='upload_current', python_callable=upload_current_data,
                                     **operator_settings
                                     )
upload_trigger_operator = SQLTemplatedPythonOperator(
        task_id='create_uuid_triggers',
        provide_context=True,
        python_callable=validate_and_execute,
        op_kwargs={
                'filename':    'granted_patent_database',
                "schema_only": False,
                "section":     get_section('granted_patent_updater', 'fix_patent_ids-upload'),

                },
        dag=granted_patent_parser,
        templates_dict={
                'source_sql': 'granted_patent_database.sql'
                },
        templates_exts=['.sql'],
        params={
                'database':   'upload_',
                'add_suffix': True
                }
        )
patent_sql_operator = PythonOperator(task_id='parse_xml_to_sql', python_callable=patent_sql_parser,
                                     **operator_settings
                                     )
qc_upload_operator = PythonOperator(task_id='qc_upload_new', python_callable=post_upload,
                                    **operator_settings
                                    )
### GI Processing
gi_NER = PythonOperator(task_id='gi_NER', python_callable=begin_NER_processing,
                        **operator_settings)
gi_postprocess_NER = PythonOperator(task_id='postprocess_NER', python_callable=process_ner_to_manual,
                                    **operator_settings)
manual_simulation_operator = PythonOperator(task_id='simulate_manual_task', python_callable=simulate_manual,
                                            **operator_settings)

post_manual_operator = PythonOperator(task_id='post_manual', python_callable=process_post_manual,
                                      **operator_settings)

### Long Text FIelds Parsing
table_creation_operator = SQLTemplatedPythonOperator(
        task_id='create_text_yearly_tables',
        provide_context=True,
        python_callable=validate_and_execute,
        dag=granted_patent_parser,
        op_kwargs={
                'filename':    'text_tables',
                "schema_only": False,
                "section":     get_section('granted_patent_updater', 'fix_patent_ids-upload')
                },
        templates_dict={
                'source_sql': 'text_tables.sql'
                },
        templates_exts=['.sql'],
        params={
                'database':   config['PATENTSVIEW_DATABASES']['TEXT_DATABASE'],
                'add_suffix': False
                }
        )
upload_table_creation_operator = SQLTemplatedPythonOperator(
        task_id='create_text_yearly_tables-upload',
        provide_context=True,
        python_callable=validate_and_execute,
        op_kwargs={
                'filename':    'text_tables',
                "schema_only": False,
                "section":     get_section('granted_patent_updater', 'fix_patent_ids-upload'),

                },
        dag=granted_patent_parser,
        templates_dict={
                'source_sql': 'text_tables.sql'
                },
        templates_exts=['.sql'],
        params={
                'database':   'upload_',
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
                                          **operator_settings)

patent_id_fix_operator = SQLTemplatedPythonOperator(
        task_id='fix_patent_ids-upload',
        provide_context=True,
        python_callable=validate_and_execute,
        dag=granted_patent_parser,
        op_kwargs={
                'filename':    'patent_id_fix_text',
                "schema_only": False,
                "section":     get_section('granted_patent_updater', 'fix_patent_ids-upload')
                },
        templates_dict={
                'source_sql': 'patent_id_fix_text.sql'
                },
        templates_exts=['.sql'],
        params={
                'database':   'upload_',
                'add_suffix': True
                }
        )

qc_parse_text_operator = PythonOperator(task_id='qc_parse_text_data',
                                        python_callable=post_text_parsing,
                                        **operator_settings)

#### merge in newly parsed data
merge_new_operator = PythonOperator(task_id='merge_db',
                                    python_callable=begin_merging,
                                    **operator_settings
                                    )

qc_merge_operator = PythonOperator(task_id='qc_merge_db',
                                   python_callable=post_merge,
                                   **operator_settings
                                   )

## Merge Text Data
merge_text_operator = PythonOperator(task_id='merge_text_db',
                                     python_callable=begin_text_merging,
                                     **operator_settings
                                     )

qc_text_merge_operator = PythonOperator(task_id='qc_merge_text_db',
                                        python_callable=post_text_merge,
                                        **operator_settings
                                        )

## Withdrawn Patents
withdrawn_operator = PythonOperator(task_id='withdrawn_processor', python_callable=process_withdrawn,
                                    **operator_settings)

qc_withdrawn_operator = PythonOperator(task_id='qc_withdrawn_processor', python_callable=post_withdrawn,
                                       **operator_settings)

operator_sequence_groups['xml_sequence'] = [download_xml_operator, process_xml_operator,
                                            parse_xml_operator, upload_new_operator,
                                            upload_trigger_operator, patent_sql_operator,patent_id_fix_operator,
                                            qc_upload_operator, gi_NER, gi_postprocess_NER,
                                            manual_simulation_operator, post_manual_operator,
                                            merge_new_operator, qc_merge_operator, withdrawn_operator,
                                            qc_withdrawn_operator]

operator_sequence_groups['text_sequence'] = [upload_setup_operator, table_creation_operator,
                                             upload_table_creation_operator, parse_text_data_operator,
                                             patent_id_fix_operator,
                                             qc_parse_text_operator, merge_text_operator,
                                             qc_text_merge_operator]
operator_sequence_groups['xml_text_cross_dependency'] = [download_xml_operator, parse_text_data_operator]
operator_sequence_groups['xml_preprare_dependency'] = [upload_setup_operator, upload_new_operator]
operator_sequence_groups['merge_prepare_xml_dependency'] = [qc_database_operator, merge_new_operator]
operator_sequence_groups['merge_prepare_text_dependency'] = [qc_database_operator, merge_text_operator]
operator_sequence_groups['qc_patent_dependency'] = [qc_upload_operator, qc_parse_text_operator]
for dependency_group in operator_sequence_groups:
    dependency_sequence = operator_sequence_groups[dependency_group]
    for upstream, downstream in zip(dependency_sequence[:-1], dependency_sequence[1:]):
        downstream.set_upstream(upstream)
