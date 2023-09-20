# import os
# from datetime import datetime, timedelta
#
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
#
# from daily_checks.api_maintainance.check_documentation_api import start_query_verification
# from daily_checks.aws_maintainance.rds_maintainance import check_aws_rds_space
# from lib.configuration import get_config
# from updater.callbacks import airflow_daily_check_failure, airflow_daily_check_success
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime.now(),
#     'email': ['contact@patentsview.org'],
#     'email_on_failure': True,
#     'email_on_retry': False,
#     'retries': 10,
#     'retry_delay': timedelta(minutes=5),
#     'concurrency': 4
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
# }
#
# daily_checks_dag = DAG(
#     '99_daily_checks',
#     description='Daily Checks of PatentsView Tools',
#     start_date=datetime(2020, 8, 20, 18, 0, 0),
#     catchup=False,
#     schedule_interval=timedelta(days=1), on_success_callback=airflow_daily_check_success,
#     on_failure_callback=airflow_daily_check_failure)
# project_home = os.environ['PACKAGE_HOME']
# config = get_config()
#
# api_query_check = PythonOperator(task_id='api_query_check', python_callable=start_query_verification,
#                                  dag=daily_checks_dag,
#                                  op_kwargs={
#                                      'config': config
#                                  }, queue='admin')
#
# free_space_check = PythonOperator(task_id='space_check', python_callable=check_aws_rds_space,
#                                   dag=daily_checks_dag,
#                                   op_kwargs={
#                                       'config': config
#                                   }, queue='admin')
