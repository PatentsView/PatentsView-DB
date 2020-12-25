import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


from lib.configuration import get_current_config, get_today_dict
from reporting_database_generator.database.validate_query import validate_and_execute
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.create_databases.merge_in_new_data import begin_merging, begin_text_merging, post_merge, post_text_merge
from updater.create_databases.rename_db import qc_database
from updater.create_databases.upload_new import begin_database_setup, post_upload, upload_current_data
from updater.text_data_processor.text_table_parsing import begin_text_parsing, post_text_parsing
from updater.xml_to_csv.bulk_downloads import bulk_download
from updater.xml_to_csv.parse_patents import patent_parser
from updater.xml_to_csv.preprocess_xml import preprocess_xml

project_home = os.environ['PACKAGE_HOME']
templates_searchpath = "{home}/resources".format(home=project_home)
config = get_current_config(**get_today_dict())

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
        start_date=datetime(2021, 1, 1),
        schedule_interval=timedelta(weeks=1), catchup=True,
        template_searchpath=templates_searchpath,
        )

###### Download & Parse #######
download_xml_operator = PythonOperator(dag=granted_patent_parser, task_id='download_xml',
                                       python_callable=bulk_download,
                                       provide_context=True,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure)
upload_setup_operator = PythonOperator(dag=granted_patent_parser, task_id='upload_database_setup',
                                       python_callable=begin_database_setup,
                                       provide_context=True,
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure)

process_xml_operator = PythonOperator(task_id='process_xml',
                                      python_callable=preprocess_xml,
                                      dag=granted_patent_parser,
                                      on_success_callback=airflow_task_success,
                                      on_failure_callback=airflow_task_failure)
process_xml_operator.set_upstream(download_xml_operator)

parse_xml_operator = PythonOperator(task_id='parse_xml',
                                    python_callable=patent_parser,
                                    dag=granted_patent_parser,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure)
parse_xml_operator.set_upstream(process_xml_operator)

#### Database Load ####
qc_database_operator = PythonOperator(task_id='qc_database_setup',
                                      python_callable=qc_database,
                                      dag=granted_patent_parser, on_success_callback=airflow_task_success,
                                      on_failure_callback=airflow_task_failure
                                      )

upload_new_operator = PythonOperator(task_id='upload_current', python_callable=upload_current_data,
                                     dag=granted_patent_parser, on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure
                                     )

upload_new_operator.set_upstream(upload_setup_operator)
upload_new_operator.set_upstream(parse_xml_operator)

qc_upload_operator = PythonOperator(task_id='qc_upload_new', python_callable=post_upload,
                                    dag=granted_patent_parser, on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure
                                    )
qc_upload_operator.set_upstream(upload_new_operator)

### Long Text FIelds Parsing
table_creation_operator = SQLTemplatedPythonOperator(
        task_id='create_text_yearly_tables',
        provide_context=True,
        python_callable=validate_and_execute,
        dag=granted_patent_parser,
        op_kwargs={
                'filename':      'text_tables.sql',
                'slack_client':  None,
                'slack_channel': None,
                "schema_only":   True,
                'drop_existing': False,
                'fk_check':      False
                },
        templates_dict={
                'source_sql': 'text_tables.sql'
                },
        templates_exts=['.sql'],
        params={
                'database':   config['DATABASE']['TEXT_DATABASE'],
                'add_suffix': False
                }
        )
table_creation_operator.set_upstream(upload_setup_operator)
upload_table_creation_operator = SQLTemplatedPythonOperator(
        task_id='create_text_yearly_tables-upload',
        provide_context=True,
        python_callable=validate_and_execute,
        dag=granted_patent_parser,
        op_kwargs={
                'filename':      'text_tables.sql',
                'slack_client':  None,
                'slack_channel': None,
                "schema_only":   True,
                'drop_existing': False,
                'fk_check':      False
                },
        templates_dict={
                'source_sql': 'text_tables.sql'
                },
        templates_exts=['.sql'],
        params={
                'database':   'upload_',
                'add_suffix': True
                }
        )
upload_table_creation_operator.set_upstream(table_creation_operator)

# trigger_creation_operator = BashOperator(dag=granted_patent_parser, task_id='create_text_triggers',
#                                          bash_command=get_text_table_load_command( project_home),
#                                          on_success_callback=airflow_task_success,
#                                          on_failure_callback=airflow_task_failure)
# trigger_creation_operator.set_upstream(table_creation_operator)

parse_text_data_operator = PythonOperator(task_id='parse_text_data',
                                          python_callable=begin_text_parsing,
                                          dag=granted_patent_parser,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure)
parse_text_data_operator.set_upstream(upload_table_creation_operator)
parse_text_data_operator.set_upstream(download_xml_operator)
patent_id_fix_operator = SQLTemplatedPythonOperator(
        task_id='fix_patent_ids-upload',
        provide_context=True,
        python_callable=validate_and_execute,
        dag=granted_patent_parser,
        op_kwargs={
                'filename':      'patent_id_fix_text.sql',
                'slack_client':  None,
                'slack_channel': None,
                "schema_only":   True,
                'drop_existing': False,
                'fk_check':      False
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
patent_id_fix_operator.set_upstream(parse_text_data_operator)
qc_parse_text_operator = PythonOperator(task_id='qc_parse_text_data',
                                        python_callable=post_text_parsing,
                                        dag=granted_patent_parser,
                                        on_success_callback=airflow_task_success,
                                        on_failure_callback=airflow_task_failure)
qc_parse_text_operator.set_upstream(patent_id_fix_operator)

#### merge in newly parsed data
merge_new_operator = PythonOperator(task_id='merge_db',
                                    python_callable=begin_merging,
                                    dag=granted_patent_parser,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure
                                    )
merge_new_operator.set_upstream(qc_upload_operator)
merge_new_operator.set_upstream(qc_database_operator)
qc_merge_operator = PythonOperator(task_id='qc_merge_db',
                                   python_callable=post_merge,
                                   dag=granted_patent_parser,
                                   on_success_callback=airflow_task_success,
                                   on_failure_callback=airflow_task_failure
                                   )
qc_merge_operator.set_upstream(merge_new_operator)

## Merge Text Data
merge_text_operator = PythonOperator(task_id='merge_text_db',
                                     python_callable=begin_text_merging,
                                     dag=granted_patent_parser,
                                     on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure
                                     )
merge_text_operator.set_upstream(qc_parse_text_operator)
merge_text_operator.set_upstream(qc_database_operator)
qc_text_merge_operator = PythonOperator(task_id='qc_merge_text_db',
                                        python_callable=post_text_merge,
                                        dag=granted_patent_parser,
                                        on_success_callback=airflow_task_success,
                                        on_failure_callback=airflow_task_failure
                                        )
qc_text_merge_operator.set_upstream(merge_text_operator)
