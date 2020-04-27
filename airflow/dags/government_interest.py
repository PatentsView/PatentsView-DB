from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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

gi_dag = DAG(
    'disambiguation',
    description='Upload CPC files to database',
    start_date=datetime(2020, 1, 1, 0, 0, 0),
    catchup=True,
    schedule_interval=None)
gi_NER = BashOperator(task_id='gi_NER',
                      bash_command='python /project/Development/government_interest/NER.py', dag=gi_dag,
                      on_success_callback=airflow_task_success,
                      on_failure_callback=airflow_task_failure
                      )

gi_postprocess_NER = BashOperator(task_id='postprocess_NER',
                                  bash_command='python /project/Development/government_interest/NER_to_manual.py',
                                  dag=gi_dag,
                                  on_success_callback=airflow_task_success,
                                  on_failure_callback=airflow_task_failure
                                  )

gi_post_manual = BashOperator(task_id='gi_post_manual',
                              bash_command='python /project/Development/government_interest/post_manual.py', dag=gi_dag,
                              on_success_callback=airflow_task_success,
                              on_failure_callback=airflow_task_failure
                              )
