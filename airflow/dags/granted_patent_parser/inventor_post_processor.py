# import os
# from datetime import datetime, timedelta
#
# from airflow import DAG
# from airflow.operators.python import PythonOperator
#
# from lib.configuration import get_current_config, get_today_dict
# from updater.callbacks import airflow_task_failure, airflow_task_success
# from updater.post_processing.post_process_inventor import post_process_inventor, post_process_qc
#
# default_args = {
#         'owner':            'smadhavan',
#         'depends_on_past':  False,
#         'email':            ['contact@patentsview.org'],
#         'email_on_failure': False,
#         'email_on_retry':   False,
#         'retries':          3,
#         'retry_delay':      timedelta(minutes=120),
#         'concurrency':      40
#         # 'queue': 'bash_queue',
#         # 'pool': 'backfill',
#         # 'priority_weight': 10,
#         # 'end_date': datetime(2016, 1, 1),
#         }
#
# project_home = os.environ['PACKAGE_HOME']
# templates_searchpath = "{home}/resources".format(home=project_home)
# config = get_current_config(type='config.ini', supplemental_configs=None, **get_today_dict())
#
# inventor_post_processor = DAG(
#         dag_id='inventor_post_processor',
#         default_args=default_args,
#         description='Run Inventor Post Processing',
#         start_date=datetime(2021, 2, 1),
#         schedule_interval=None,
#         template_searchpath=templates_searchpath,
#         )
#
# post_process_inventor_operator = PythonOperator(task_id='post_process_inventor',
#                                                 python_callable=post_process_inventor,
#                                                 dag=inventor_post_processor,
#                                                 on_success_callback=airflow_task_success,
#                                                 on_failure_callback=airflow_task_failure)
#
# qc_post_process_inventor_operator = PythonOperator(task_id='qc_post_process_inventor',
#                                                    python_callable=post_process_qc,
#                                                    dag=inventor_post_processor,
#                                                    on_success_callback=airflow_task_success,
#                                                    on_failure_callback=airflow_task_failure)
# qc_post_process_inventor_operator.set_upstream(post_process_inventor_operator)