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

#set up a few folders that are passed as arguments
disambig_input = '{}/disambig_inputs'.format(config['FOLDERS']['WORKING_FOLDER'])
keyfile = config['DISAMBIGUATION_CREDENTIALS']['KEY_FILE']
host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']


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
    schedule_interval='@once')

download_xml_operator = BashOperator(task_id='download_xml',
                                     bash_command='python /project/Development/xml_to_csv/bulk_downloads.py 20180920',
                                     dag=dag)

# process xml files
process_xml_operator = BashOperator(task_id='process_xml',
                                    bash_command='python /project/Development/xml_to_csv/preprocess_xml.py',
                                    dag=dag)

# parse xml into csv
parse_xml_operator = BashOperator(task_id='parse_xml',
                                  bash_command='python /project/Development/xml_to_csv/parse_patents.py',
                                  dag=dag)

#rename old database for inplace modification
rename_old_operator = BashOperator(task_id='rename_db',
                                 bash_command='python /project/Development/create_databases/rename_db.py', dag=dag)
# upload newly parsed data
upload_new_operator = BashOperator(task_id='upload_new',
                                   bash_command='python /project/Development/create_databases/upload_new.py',
                                   dag=dag)
qa_new = BashOperator(task_id='qa_new',
                      bash_command = 'python /project/Development/QA/03_temp_upload.py', dag=dag)

# merge in newly parsed data
merge_new_operator = BashOperator(task_id='merge_db',
                                  bash_command='python /project/Development/create_databases/merge_in_new_data.py',
                                  dag=dag)

#check that latest date is in merged file
qa_date = BashOperator(task_id='qa_date',
                      bash_command = 'python /project/Development/QA/02_date.py', dag=dag)

#add withdrawn patent code
withdrawn_operator = BashOperator(task_id ='note_withdrawn',
                                  bash_command = 'python /project/Development/withdrawn_patents/update_withdrawn.py', dag = dag)

#do government interest processing
gi_NER = BashOperator(task_id = 'gi_NER', 
                              bash_command = 'python /project/Development/government_interest/NER.py', dag = dag)

gi_postprocess_NER = BashOperator(task_id = 'postprocess_NER', 
                              bash_command = 'python /project/Development/government_interest/NER_to_manual.py', dag = dag)

gi_post_manual = BashOperator(task_id = 'gi_post_manual', 
                              bash_command = 'python /project/Development/government_interest/post_manual.py', dag = dag)


# download cpcs
cpc_download_operator = BashOperator(task_id='download_cpc',
                                     bash_command='python /project/Development/process_cpcs/download_cpc_ipc.py',
                                     dag=dag)

cpc_parser_operator = BashOperator(task_id='cpc_parser',
                                   bash_command='python /project/Development/process_cpcs/cpc_parser.py', dag=dag)

cpc_class_parser_operator = BashOperator(task_id='cpc_class_parser',
                                         bash_command='python /project/Development/process_cpcs/cpc_class_parser.py',
                                         dag=dag)

cpc_current_operator = BashOperator(task_id='process_cpc_current',
                                    bash_command='python /project/Development/process_cpcs/process_cpc_current.py',
                                    dag=dag)

cpc_class_operator = BashOperator(task_id='upload_cpc_class',
                                  bash_command='python /project/Development/process_cpcs/upload_cpc_class_tables.py',
                                  dag=dag)

download_uspc_operator = BashOperator(task_id='download_uspc',
                                      bash_command='python /project/Development/process_uspc/download_and_parse_uspc.py',
                                      dag=dag)

upload_uspc_operator = BashOperator(task_id='upload_uspc',
                                    bash_command='python /project/Development/process_uspc/upload_uspc.py', dag=dag)

process_wipo_operator = BashOperator(task_id='process_wipo',
                                     bash_command='python /project/Development/process_wipo/process_wipo.py', dag=dag)

get_disambig_data = BashOperator(task_id='get_disambig_input',
                                                   bash_command='bash /project/Development/disambiguation_support/get_data_for_disambig.sh {} {} {} {}'.format(host, username, new_database, disambig_input),
                                                   dag=dag)

clean_inventor = BashOperator(task_id='clean_inventor',
                                     bash_command='python /project/Development/disambiguation_support/clean_inventor.py', dag=dag)

upload_disambig = BashOperator(task_id='upload_disambig_files',
                                     bash_command='bash /project/Development/disambiguation_support/upload_disambig.sh {} {}'.format(keyfile, disambig_input), dag=dag)

run_lawyer_disambiguation_operator=BashOperator(task_id='run_lawyer_disambiguation',
                                           bash_command='python /project/Assignee_Lawyer_Disambiguation/lib/lawyer_disambiguation.py',
                                           dag=dag)

run_disambiguation_operator = BashOperator(task_id='run_disambiguation',
                                           bash_command='bash /project/Development/disambiguation_support/run_disambiguation.sh',
                                           dag=dag)



download_disambig_operator = BashOperator(task_id='download_disambiguation',
                                          bash_command='bash /project/Development/post_process_disambiguation/download_disambiguation.sh',
                                          dag=dag)

postprocess_inventor_operator = BashOperator(task_id='post_process_inventor',
                                             bash_command='python /project/Development/post_process_disambiguation/post_process_inventor.py',
                                             dag=dag)

postprocess_assignee_operator = BashOperator(task_id='post_process_assignee',
                                             bash_command='python /project/Development/post_process_disambiguation/post_process_assignee.py',
                                             dag=dag)

postprocess_location_operator = BashOperator(task_id='post_process_location',
                                             bash_command='python /project/Development/post_process_disambiguation/post_process_location.py',
                                             dag=dag)

persistent_inventor_operator = BashOperator(task_id = 'persistent_inventor',
                                            bash_command = 'python /project/Development/post_process_disambiguation/persistent_inventor.py', dag = dag)

lookup_tables_operator = BashOperator(task_id = 'lookup_tables',
                                            bash_command = 'python /project/Development/post_process_disambiguation/create_lookup_tables.py', dag = dag)

#check counts, has to happen after processing
count_operator = BashOperator(task_id = 'count_qa', bash_command = 'python /project/Development/QA/01_count.py', dag = dag)

null_operator = BashOperator(task_id = 'null_qa', bash_command = 'python /project/Development/QA/04_null_blanks.py', dag = dag)

ratios_operator = BashOperator(task_id = 'ratio_qa', bash_command = 'python /project/Development/QA/05_ratios.py', dag = dag)


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
