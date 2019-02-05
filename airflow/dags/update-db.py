from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import sys


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
# copy old database
copy_old_operator = BashOperator(task_id='copy_db',
                                 bash_command='python /project/Development/copy_databases/copy_db.py', dag=dag)
# upload newly parsed data
upload_new_operator = BashOperator(task_id='upload_new',
                                   bash_command='python /project/Development/copy_databases/upload_new_data.py',
                                   dag=dag)
# merge in newly parsed data
merge_new_operator = BashOperator(task_id='merge_db',
                                  bash_command='python /project/Development/copy_databases/merge_in_new_data.py',
                                  dag=dag)
#add withdrawn patent code
withdrawn_operator = BashOperator(task_id ='note_withdrawn',
                                  bash_command = 'python /project/Development/withdrawn_patents/update_withdrawn.py', dag = dag)

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

create_and_upload_disambig_operator = BashOperator(task_id='get_disambig_input',
                                                   bash_command='python /project/Development/disambiguation_support/get_data_for_disambig.py',
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
                                            bash_command = 'python /project/Development/post_process_disambiguation/persistent_inventor.py')

lookup_tables_operator = BashOperator(task_id = 'lookup_tables',
                                            bash_command = 'python /project/Development/post_process_disambiguation/create_lookup_tables.py')


process_xml_operator.set_upstream(download_xml_operator)
parse_xml_operator.set_upstream(process_xml_operator)

upload_new_operator.set_upstream(parse_xml_operator)
merge_new_operator.set_upstream(upload_new_operator)
merge_new_operator.set_upstream(copy_old_operator)

withdrawn_operator.set_upstream(merge_new_operator)

cpc_parser_operator.set_upstream(cpc_download_operator)
cpc_parser_operator.set_upstream(cpc_class_parser_operator)
cpc_class_parser_operator.set_upstream(cpc_download_operator)

cpc_current_operator.set_upstream(cpc_parser_operator)
cpc_current_operator.set_upstream(merge_new_operator)
cpc_class_operator.set_upstream(cpc_class_parser_operator)
cpc_class_operator.set_upstream(copy_old_operator)

upload_uspc_operator.set_upstream(download_uspc_operator)
upload_uspc_operator.set_upstream(merge_new_operator)
process_wipo_operator.set_upstream(cpc_current_operator)

create_and_upload_disambig_operator.set_upstream(process_wipo_operator)
run_disambiguation_operator.set_upstream(create_and_upload_disambig_operator)
download_disambig_operator.set_upstream(run_disambiguation_operator)
postprocess_inventor_operator.set_upstream(download_disambig_operator)
postprocess_assignee_operator.set_upstream(download_disambig_operator)
postprocess_location_operator.set_upstream(download_disambig_operator)

persistent_inventor_operator.set_upstream(postprocess_inventor_operator)
lookup_tables_operator.set_upstream(postprocess_inventor_operator)
lookup_tables_operator.set_upstream(postprocess_assignee_operator)
lookup_tables_operator.set_upstream(postprocess_location_operator)