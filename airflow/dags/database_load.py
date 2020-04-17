from airflow import DAG
from datetime import datetime, timedelta
import os

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# Helper Imports
from lib.configuration import get_config, get_backup_command, get_loader_command, get_text_table_load_command
from updater.callbacks import airflow_task_success, airflow_task_failure
from updater.create_databases.merge_in_new_data import begin_merging, begin_text_merging, post_merge, post_text_merge
from updater.create_databases.rename_db import begin_rename, post_rename
from updater.create_databases.upload_new import upload_new_data, post_upload
from updater.text_data_processor.text_table_parsing import begin_text_parsing

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

upload_dag = DAG(
    'upload_db_creation',
    description='Upload CSV files to database',
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    schedule_interval=None)
project_home = os.environ['PACKAGE_HOME']
config = get_config()

backup_old_db = BashOperator(dag=upload_dag, task_id='backup_olddb',
                             bash_command=get_backup_command(config, project_home),
                             on_success_callback=airflow_task_success,
                             on_failure_callback=airflow_task_failure)

rename_old_operator = PythonOperator(task_id='rename_db', python_callable=begin_rename, op_kwargs={'config': config},
                                     dag=upload_dag, on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure
                                     )
qc_rename_operator = PythonOperator(task_id='qc_rename_db', python_callable=post_rename, op_kwargs={'config': config},
                                    dag=upload_dag, on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure
                                    )
upload_new_operator = PythonOperator(task_id='upload_new', python_callable=upload_new_data,
                                     op_kwargs={'config': config},
                                     dag=upload_dag, on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure
                                     )
qc_upload_operator = PythonOperator(task_id='qc_upload_new', python_callable=post_upload,
                                    op_kwargs={'config': config},
                                    dag=upload_dag, on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure
                                    )
restore_old_db = BashOperator(dag=upload_dag, task_id='restore_olddb',
                              bash_command=get_loader_command(config, project_home),
                              on_success_callback=airflow_task_success,
                              on_failure_callback=airflow_task_failure)

table_creation_operator = BashOperator(dag=upload_dag, task_id='create_text_tables',
                                       bash_command=get_text_table_load_command(config, project_home),
                                       on_success_callback=airflow_task_success,
                                       on_failure_callback=airflow_task_failure)

parse_text_data_operator = PythonOperator(task_id='parse_text_data',
                                          python_callable=begin_text_parsing,
                                          dag=upload_dag, op_kwargs={'config': config},
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure)

# merge in newly parsed data
merge_new_operator = PythonOperator(task_id='merge_db',
                                    python_callable=begin_merging,
                                    op_kwargs={'config': config},
                                    dag=upload_dag,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure
                                    )
merge_text_operator = PythonOperator(task_id='merge_text_db',
                                     python_callable=begin_text_merging,
                                     op_kwargs={'config': config},
                                     dag=upload_dag,
                                     on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure
                                     )
qc_merge_operator = PythonOperator(task_id='qc_merge_db',
                                   python_callable=post_merge,
                                   op_kwargs={'config': config},
                                   dag=upload_dag,
                                   on_success_callback=airflow_task_success,
                                   on_failure_callback=airflow_task_failure
                                   )
qc_text_merge_operator = PythonOperator(task_id='qc_merge_text_db',
                                        python_callable=post_text_merge,
                                        op_kwargs={'config': config},
                                        dag=upload_dag,
                                        on_success_callback=airflow_task_success,
                                        on_failure_callback=airflow_task_failure
                                        )

rename_old_operator.set_upstream(backup_old_db)
upload_new_operator.set_upstream(backup_old_db)
qc_upload_operator.set_upstream(upload_new_operator)
qc_rename_operator.set_upstream(rename_old_operator)
restore_old_db.set_upstream(qc_rename_operator)
merge_new_operator.set_upstream(qc_upload_operator)
qc_merge_operator.set_upstream(merge_new_operator)
table_creation_operator.set_upstream(qc_upload_operator)
parse_text_data_operator.set_upstream(table_creation_operator)
merge_text_operator.set_upstream(parse_text_data_operator)
qc_text_merge_operator.set_upstream(merge_text_operator)
