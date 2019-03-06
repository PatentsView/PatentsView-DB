from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import sys
import os
project_home = os.environ['PACKAGE_HOME']
import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')
from slackclient import SlackClient
from Development.helpers.general_helpers import send_slack_notification


#set up a few folders that are passed as arguments
disambig_input = '{}/disambig_inputs'.format(config['FOLDERS']['WORKING_FOLDER'])
disambig_output = '{}/disambig_output'.format(config['FOLDERS']['WORKING_FOLDER'])
keyfile = config['DISAMBIGUATION_CREDENTIALS']['KEY_FILE']
host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
slack_token = config["SLACK"]["API_TOKEN"]
slack_client = SlackClient(slack_token)
slack_channel = config["SLACK"]["CHANNEL"]




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['skelley@air.org'],
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

dag = DAG(
    'update_db',
    description='Download and process main data and classifications data',
    start_date=datetime.now(),
    catchup=True,
    schedule_interval=None)

def slack_success(context):
    send_slack_notification('%s succeeded'%(context['task'].task_id),slack_client, slack_channel,section="Data Prep", level='success')

def slack_failure(context):
    on_failure_callback  =send_slack_notification('%s failed'%(context['task'].task_id) ,slack_client, slack_channel,section="Data Prep", level='error')

download_xml_operator = BashOperator(task_id='download_xml',
                                     bash_command='python /project/Development/xml_to_csv/bulk_downloads.py 20180920',
                                     dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )


# process xml files
process_xml_operator = BashOperator(task_id='process_xml',
                                    bash_command='python /project/Development/xml_to_csv/preprocess_xml.py',
                                    dag=dag, 
                                    on_success_callback = slack_success,
                                     on_failure_callback = slack_failure)

# parse xml into csv
parse_xml_operator = BashOperator(task_id='parse_xml',
                                  bash_command='python /project/Development/xml_to_csv/parse_patents.py',
                                  dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

#rename old database for inplace modification
rename_old_operator = BashOperator(task_id='rename_db',
                                 bash_command='python /project/Development/create_databases/rename_db.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )
# upload newly parsed data
upload_new_operator = BashOperator(task_id='upload_new',
                                   bash_command='python /project/Development/create_databases/upload_new.py',
                                   dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )
qa_new = BashOperator(task_id='qa_new',
                      bash_command = 'python /project/Development/QA/03_temp_upload.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )
# merge in newly parsed data
merge_new_operator = BashOperator(task_id='merge_db',
                                  bash_command='python /project/Development/create_databases/merge_in_new_data.py',
                                  dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

#check that latest date is in merged file
qa_date = BashOperator(task_id='qa_date',
                      bash_command = 'python /project/Development/QA/02_date.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

#add withdrawn patent code
withdrawn_operator = BashOperator(task_id ='note_withdrawn',
                                  bash_command = 'python /project/Development/withdrawn_patents/update_withdrawn.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

#do government interest processing
gi_NER = BashOperator(task_id = 'gi_NER', 
                              bash_command = 'python /project/Development/government_interest/NER.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

gi_postprocess_NER = BashOperator(task_id = 'postprocess_NER', 
                              bash_command = 'python /project/Development/government_interest/NER_to_manual.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

gi_post_manual = BashOperator(task_id = 'gi_post_manual', 
                              bash_command = 'python /project/Development/government_interest/post_manual.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )


# download cpcs
cpc_download_operator = BashOperator(task_id='download_cpc',
                                     bash_command='python /project/Development/process_cpcs/download_cpc_ipc.py',
                                     dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

cpc_parser_operator = BashOperator(task_id='cpc_parser',
                                   bash_command='python /project/Development/process_cpcs/cpc_parser.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

cpc_class_parser_operator = BashOperator(task_id='cpc_class_parser',
                                         bash_command='python /project/Development/process_cpcs/cpc_class_parser.py',
                                         dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

cpc_current_operator = BashOperator(task_id='process_cpc_current',
                                    bash_command='python /project/Development/process_cpcs/process_cpc_current.py',
                                    dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

cpc_class_operator = BashOperator(task_id='upload_cpc_class',
                                  bash_command='python /project/Development/process_cpcs/upload_cpc_class_tables.py',
                                  dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

download_uspc_operator = BashOperator(task_id='download_uspc',
                                      bash_command='python /project/Development/process_uspc/download_and_parse_uspc.py',
                                      dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

upload_uspc_operator = BashOperator(task_id='upload_uspc',
                                    bash_command='python /project/Development/process_uspc/upload_uspc.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

process_wipo_operator = BashOperator(task_id='process_wipo',
                                     bash_command='python /project/Development/process_wipo/process_wipo.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )


get_disambig_data = BashOperator(task_id='get_disambig_input',
                                                   bash_command='bash /project/Development/disambiguation_support/get_data_for_disambig.sh {} {} {} {}'.format(host, username, new_database, disambig_input),
                                                   dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

clean_inventor = BashOperator(task_id='clean_inventor',
                                     bash_command='python /project/Development/disambiguation_support/clean_inventor.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

upload_disambig = BashOperator(task_id='upload_disambig_files',
                                     bash_command='/project/Development/disambiguation_support/upload_disambig.sh %s %s'%(keyfile, disambig_input), dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

run_lawyer_disambiguation_operator=BashOperator(task_id='run_lawyer_disambiguation',
                                           bash_command='python /project/Assignee_Lawyer_Disambiguation/lib/lawyer_disambiguation.py',
                                           dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

run_disambiguation_operator = BashOperator(task_id='run_disambiguation',
                                           bash_command='/project/Development/disambiguation_support/run_disambiguation.sh %s'%(keyfile),
                                           dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

download_disambig_operator = BashOperator(task_id='download_disambiguation',
                                          bash_command='bash /project/Development/post_process_disambiguation/download_disambig.sh %s %s'%(keyfile, disambig_output),
                                          dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

postprocess_inventor_operator = BashOperator(task_id='post_process_inventor',
                                             bash_command='python /project/Development/post_process_disambiguation/post_process_inventor.py',
                                             dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

postprocess_assignee_operator = BashOperator(task_id='post_process_assignee',
                                             bash_command='python /project/Development/post_process_disambiguation/post_process_assignee.py',
                                             dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

postprocess_location_operator = BashOperator(task_id='post_process_location',
                                             bash_command='python /project/Development/post_process_disambiguation/post_process_location.py',
                                             dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

persistent_inventor_operator = BashOperator(task_id = 'persistent_inventor',
                                            bash_command = 'python /project/Development/post_process_disambiguation/persistent_inventor.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

lookup_tables_operator = BashOperator(task_id = 'lookup_tables',
                                            bash_command = 'python /project/Development/post_process_disambiguation/create_lookup_tables.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

#check counts, has to happen after processing
count_operator = BashOperator(task_id = 'count_qa', bash_command = 'python /project/Development/QA/01_count.py',dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

null_operator = BashOperator(task_id = 'null_qa', bash_command = 'python /project/Development/QA/04_null_blanks.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )

ratios_operator = BashOperator(task_id = 'ratio_qa', bash_command = 'python /project/Development/QA/05_ratios.py', dag=dag,
                                     on_success_callback = slack_success,
                                     on_failure_callback = slack_failure
                                     )


process_xml_operator.set_upstream(download_xml_operator)
parse_xml_operator.set_upstream(process_xml_operator)

upload_new_operator.set_upstream(parse_xml_operator)
merge_new_operator.set_upstream(upload_new_operator)
merge_new_operator.set_upstream(rename_old_operator)

qa_new.set_upstream(upload_new_operator)
qa_date.set_upstream(merge_new_operator)

gi_NER.set_upstream(merge_new_operator)
gi_postprocess_NER .set_upstream(gi_NER )
gi_post_manual.set_upstream(gi_postprocess_NER )


withdrawn_operator.set_upstream(merge_new_operator)

cpc_parser_operator.set_upstream(cpc_download_operator)
cpc_parser_operator.set_upstream(cpc_class_parser_operator)
cpc_class_parser_operator.set_upstream(cpc_download_operator)

cpc_current_operator.set_upstream(cpc_parser_operator)
cpc_current_operator.set_upstream(merge_new_operator)
cpc_class_operator.set_upstream(cpc_class_parser_operator)
cpc_class_operator.set_upstream(rename_old_operator)

upload_uspc_operator.set_upstream(download_uspc_operator)
upload_uspc_operator.set_upstream(merge_new_operator)
process_wipo_operator.set_upstream(cpc_current_operator)

get_disambig_data.set_upstream(process_wipo_operator)

run_lawyer_disambiguation_operator.set_upstream(process_wipo_operator)
clean_inventor.set_upstream(get_disambig_data)
upload_disambig.set_upstream(clean_inventor)

run_disambiguation_operator.set_upstream(upload_disambig)
download_disambig_operator.set_upstream(run_disambiguation_operator)
postprocess_inventor_operator.set_upstream(download_disambig_operator)
postprocess_assignee_operator.set_upstream(download_disambig_operator)
postprocess_location_operator.set_upstream(download_disambig_operator)

persistent_inventor_operator.set_upstream(postprocess_inventor_operator)
lookup_tables_operator.set_upstream(postprocess_inventor_operator)
lookup_tables_operator.set_upstream(postprocess_assignee_operator)
lookup_tables_operator.set_upstream(postprocess_location_operator)

null_operator.set_upstream(lookup_tables_operator)
ratios_operator.set_upstream(lookup_tables_operator)
count_operator.set_upstream(lookup_tables_operator)
