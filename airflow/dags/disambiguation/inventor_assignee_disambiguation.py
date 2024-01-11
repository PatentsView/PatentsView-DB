import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from lib.configuration import get_today_dict
# appending a path
from lib.utilities import chain_operators
from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.disambiguation.assignee_disambiguation.assignee_disambiguator import build_assignee_name_mentions, \
    run_hierarchical_clustering as run_assignee_hierarchical_clustering, create_uuid_map, \
    upload_results as upload_assignee_results, archive_results as archive_assignee_results, \
    finalize_assignee_clustering, qc_build_assignee_name_mentions
from updater.disambiguation.inventor_disambiguation.inventor_disambiguator import build_assignee_features, \
    build_canopies, archive_results as archive_inventor_results, build_coinventor_features, build_title_map, \
    run_hierarchical_clustering as run_inventor_hierarchical_clustering, \
    finalize_disambiguation, upload_results as upload_inventor_results, setup_inventor_assignee_disambiguation
from updater.post_processing.post_process_location import post_process_location, post_process_qc, augment_location_fips
from updater.post_processing.post_process_assignee import additional_post_processing_assignee, \
    post_process_qc as qc_post_process_assignee, post_process_assignee_patent_phase2_qc, post_process_assignee_pgpubs_phase2_qc, \
    post_process_assignee_qc_pgpubs, update_granted_rawassignee, update_pregranted_rawassignee, \
    precache_assignees, create_canonical_assignees, load_granted_location_assignee, \
    load_pregranted_location_assignee, create_patent_assignee, create_publication_assignee
from updater.post_processing.post_process_inventor import update_granted_rawinventor, update_pregranted_rawinventor, \
    precache_inventors, create_canonical_inventors, create_patent_inventor, create_publication_inventor, \
    post_process_qc as qc_inventor_post_processing, load_granted_location_inventor, load_pregranted_location_inventor, run_genderit, \
    post_process_inventor_gender, post_process_inventor_patent_phase2_qc, post_process_inventor_pgpubs_phase2_qc, post_process_inventor_qc_pgpubs
from updater.post_processing.post_process_persistent import prepare_wide_table, update_long_entity, write_wide_table
from updater.post_processing.archive_disambiguation_tables import archive_assignee_tables, archive_inventor_tables, archive_location_tables

class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


# project_home = os.environ['PACKAGE_HOME']
# templates_searchpath = "{home}/resources".format(home=project_home)
# config = get_current_config(type='config.ini', supplemental_configs=None, **get_today_dict())
# print(config)

default_args = {
    'owner': 'smadhavan',
    'depends_on_past': False,
    'email': ['contact@patentsview.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=120),
    'concurrency': 40,
    'queue': 'disambiguator'
}

disambiguation = DAG(
    dag_id='inventor_assignee_disambiguation',
    default_args=default_args,
    description='Perform inventor, assignee, & location disambiguation',
    start_date=datetime(2021, 7, 1),
    schedule_interval='@quarterly',
    template_searchpath="/project/resources",
    catchup=True,
)

quarterly_merge_completed = ExternalTaskSensor(
    task_id="qc_text_merge_quarterly_pgpubs",
    external_dag_id="merge_quarterly_updater",
    external_task_id="qc_text_merge_quarterly_pgpubs",
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode="reschedule",
)

# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# SET UP TASKS

assignee_inventor_disambig_setup = PythonOperator(task_id='Inventor_Assignee_Disambiguation_Setup',
                                             python_callable=setup_inventor_assignee_disambiguation,
                                             provide_context=True,
                                             dag=disambiguation,
                                             on_success_callback=airflow_task_success,
                                             on_failure_callback=airflow_task_failure,
                                             queue='data_collector')

archive_assignee_tables_task = PythonOperator(task_id='archive_assignee_tables',
                                             python_callable=archive_assignee_tables,
                                             provide_context=True,
                                             dag=disambiguation,
                                             on_success_callback=airflow_task_success,
                                             on_failure_callback=airflow_task_failure,
                                             queue='data_collector')

archive_location_tables_task = PythonOperator(task_id='archive_location_tables',
                                             python_callable=archive_location_tables,
                                             provide_context=True,
                                             dag=disambiguation,
                                             on_success_callback=airflow_task_success,
                                             on_failure_callback=airflow_task_failure,
                                             queue='data_collector')

archive_inventor_tables_task = PythonOperator(task_id='archive_inventor_tables',
                                             python_callable=archive_inventor_tables,
                                             provide_context=True,
                                             dag=disambiguation,
                                             on_success_callback=airflow_task_success,
                                             on_failure_callback=airflow_task_failure,
                                             queue='data_collector')

# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# INVENTOR TASKS

inv_build_assignee_features = PythonOperator(task_id='Inventor_Build_Assignee_Features',
                                             python_callable=build_assignee_features,
                                             provide_context=True,
                                             dag=disambiguation,
                                             on_success_callback=airflow_task_success,
                                             on_failure_callback=airflow_task_failure,
                                             queue='disambiguator')
inv_build_titles = PythonOperator(task_id='Inventor_Build_Titles',
                                  python_callable=build_title_map,
                                  provide_context=True,
                                  dag=disambiguation,
                                  on_success_callback=airflow_task_success,
                                  on_failure_callback=airflow_task_failure,
                                  queue='disambiguator')
inv_build_coinventor_features = PythonOperator(task_id='Inventor_Build_Co-Inventor_Features',
                                               python_callable=build_coinventor_features,
                                               provide_context=True,
                                               dag=disambiguation,
                                               on_success_callback=airflow_task_success,
                                               on_failure_callback=airflow_task_failure,
                                               queue='disambiguator')
inv_build_canopies = PythonOperator(task_id='Inventor_Build_Inventor_Canopies',
                                    python_callable=build_canopies,
                                    provide_context=True,
                                    dag=disambiguation,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    queue='disambiguator', pool='high_memory_pool')
inv_run_clustering = PythonOperator(task_id='Inventor_Run_Clustering',
                                    python_callable=run_inventor_hierarchical_clustering,
                                    provide_context=True,
                                    dag=disambiguation,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    queue='disambiguator', pool='high_memory_pool')
inv_finalize_output = PythonOperator(task_id='Inventor_Finalize',
                                     python_callable=finalize_disambiguation,
                                     provide_context=True,
                                     dag=disambiguation,
                                     on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure,
                                     queue='disambiguator')
inv_upload_results = PythonOperator(task_id='Inventor_Upload_Inventor_Results',
                                    python_callable=upload_inventor_results,
                                    provide_context=True,
                                    dag=disambiguation,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure,
                                    queue='disambiguator', pool='database_write_iops_contenders')
inv_archive_results = PythonOperator(task_id='Inventor_Archive_Inventor_Results',
                                     python_callable=archive_inventor_results,
                                     provide_context=True,
                                     dag=disambiguation,
                                     on_success_callback=airflow_task_success,
                                     on_failure_callback=airflow_task_failure,
                                     queue='disambiguator')
post_process_update_granted_rawinventor = PythonOperator(task_id='Inventor_update_granted_rawinventor',
                                                         python_callable=update_granted_rawinventor,
                                                         dag=disambiguation,
                                                         on_success_callback=airflow_task_success,
                                                         on_failure_callback=airflow_task_failure,
                                                         queue='data_collector')

post_process_update_pregranted_rawinventor = PythonOperator(task_id='Inventor_update_pregranted_rawinventor',
                                                            python_callable=update_pregranted_rawinventor,
                                                            dag=disambiguation,
                                                            on_success_callback=airflow_task_success,
                                                            on_failure_callback=airflow_task_failure,
                                                            queue='data_collector',
                                                            pool='database_write_iops_contenders')
post_process_precache_inventors = PythonOperator(task_id='Inventor_precache_inventors',
                                                 python_callable=precache_inventors,
                                                 dag=disambiguation,
                                                 on_success_callback=airflow_task_success,
                                                 on_failure_callback=airflow_task_failure,
                                                 queue='data_collector', pool='database_write_iops_contenders')
post_process_create_canonical_inventors = PythonOperator(task_id='Inventor_create_canonical_inventors',
                                                         python_callable=create_canonical_inventors,
                                                         dag=disambiguation,
                                                         on_success_callback=airflow_task_success,
                                                         on_failure_callback=airflow_task_failure,
                                                         queue='data_collector', pool='database_write_iops_contenders')
post_process_create_patent_inventor = PythonOperator(task_id='Inventor_create_patent_inventor',
                                                  python_callable=create_patent_inventor,
                                                  dag=disambiguation,
                                                  on_success_callback=airflow_task_success,
                                                  on_failure_callback=airflow_task_failure,
                                                  queue='data_collector', pool='database_write_iops_contenders')
post_process_create_pgpubs_inventor = PythonOperator(task_id='Inventor_create_pgpubs_inventor',
                                                     python_callable=create_publication_inventor,
                                                     dag=disambiguation,
                                                     on_success_callback=airflow_task_success,
                                                     on_failure_callback=airflow_task_failure,
                                                     queue='data_collector', pool='database_write_iops_contenders')
post_process_create_patent_loc_inventor = PythonOperator(task_id='Inventor_create_patent_location_inventor',
                                                     python_callable=load_granted_location_inventor,
                                                     dag=disambiguation,
                                                     on_success_callback=airflow_task_success,
                                                     on_failure_callback=airflow_task_failure,
                                                     queue='data_collector', pool='database_write_iops_contenders')
post_process_create_pgpubs_loc_inventor = PythonOperator(task_id='Inventor_create_pgpubs_location_inventor',
                                                     python_callable=load_pregranted_location_inventor,
                                                     dag=disambiguation,
                                                     on_success_callback=airflow_task_success,
                                                     on_failure_callback=airflow_task_failure,
                                                     queue='data_collector', pool='database_write_iops_contenders')

# update_granted_persistent_long_inventor = PythonOperator(
#     task_id='update_granted_persistent_long_inventor',
#     python_callable=update_long_entity,
#     op_kwargs={
#         'entity': 'inventor',
#         'database_type': 'granted_patent'
#     },
#     dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
# )

prepare_granted_persistent_wide_inventor = PythonOperator(
    task_id='prepare_granted_persistent_wide_inventor',
    python_callable=prepare_wide_table,
    op_kwargs={
        'entity': 'inventor',
        'database_type': 'granted_patent'
    },
    dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
)

create_granted_persistent_wide_inventor = PythonOperator(
    task_id='create_granted_persistent_wide_inventor',
    python_callable=write_wide_table,
    op_kwargs={
        'entity': 'inventor',
        'database_type': 'granted_patent'
    },
    dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
)

prepare_pregranted_persistent_wide_inventor = PythonOperator(
    task_id='prepare_pregranted_persistent_wide_inventor',
    python_callable=prepare_wide_table,
    op_kwargs={
        'entity': 'inventor',
        'database_type': 'pgpubs'
    },
    dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
)

create_pregranted_persistent_wide_inventor = PythonOperator(
    task_id='create_pregranted_persistent_wide_inventor',
    python_callable=write_wide_table,
    op_kwargs={
        'entity': 'inventor',
        'database_type': 'pgpubs'
    },
    dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
)
# update_pregranted_persistent_long_inventor = PythonOperator(
#     task_id='update_pregranted_persistent_long_inventor',
#     python_callable=update_long_entity,
#     op_kwargs={
#         'entity': 'inventor',
#         'database_type': 'pgpubs'
#     },
#     dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
# )

run_patent_gender = PythonOperator(task_id='patent_inventor_gender',
                                                   python_callable=run_genderit,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector',
                                                   op_kwargs={'type': 'granted_patent'})
run_pgpubs_gender = PythonOperator(task_id='pgpubs_inventor_gender',
                                                   python_callable=run_genderit,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector',
                                                   op_kwargs={'type': 'pgpubs'})
inventor_gender_post_processing = PythonOperator(task_id='inventor_gender_post_processing',
                                                   python_callable=post_process_inventor_gender,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector',
                                                   op_kwargs={'database': 'pgpubs'})
qc_post_process_inventor_operator = PythonOperator(task_id='qc_post_process_inventor',
                                                   python_callable=qc_inventor_post_processing,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector')
qc_post_process_inventor_pgpubs_operator = PythonOperator(task_id='qc_post_process_inventor_pgpubs',
                                                   python_callable= post_process_inventor_qc_pgpubs,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector')
post_process_inventor_patent_phase2_qc_operator = PythonOperator(task_id='qc_post_process_inventor_patent_phase2',
                                                   python_callable=post_process_inventor_patent_phase2_qc,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector')
post_process_inventor_pgpubs_phase2_qc_operator = PythonOperator(task_id='qc_post_process_inventor_pgpubs_phase2',
                                                   python_callable=post_process_inventor_pgpubs_phase2_qc,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector')
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# ASSIGNEE TASKS

assignee_build_assignee_features = PythonOperator(task_id='Assignee_Build_Assignee_Name_Mentions_Canopies',
                                                  python_callable=build_assignee_name_mentions,
                                                  provide_context=True,
                                                  dag=disambiguation,
                                                  on_success_callback=airflow_task_success,
                                                  on_failure_callback=airflow_task_failure,
                                                  queue='disambiguator', pool='high_memory_pool')

qc_assignee_build_assignee_features = PythonOperator(task_id='QA_Assignee_Name_Mentions_Canopies',
                                                     python_callable=qc_build_assignee_name_mentions,
                                                     provide_context=True,
                                                     dag=disambiguation,
                                                     on_success_callback=airflow_task_success,
                                                     on_failure_callback=airflow_task_failure,
                                                     queue='disambiguator')

assignee_run_clustering = PythonOperator(task_id='Assignee_Run_Clustering',
                                         python_callable=run_assignee_hierarchical_clustering,
                                         provide_context=True,
                                         dag=disambiguation,
                                         on_success_callback=airflow_task_success,
                                         on_failure_callback=airflow_task_failure,
                                         queue='disambiguator', pool='high_memory_pool')

assignee_create_uuid_map = PythonOperator(task_id='Assignee_Create_UUID_Map',
                                          python_callable=create_uuid_map,
                                          provide_context=True,
                                          dag=disambiguation,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          queue='disambiguator')
assignee_finalize_results = PythonOperator(task_id='Assignee_Finalize_Results',
                                           python_callable=finalize_assignee_clustering,
                                           provide_context=True,
                                           dag=disambiguation,
                                           on_success_callback=airflow_task_success,
                                           on_failure_callback=airflow_task_failure,
                                           queue='disambiguator')

assignee_upload_results = PythonOperator(task_id='Assignee_Upload_Results',
                                         python_callable=upload_assignee_results,
                                         provide_context=True,
                                         dag=disambiguation,
                                         on_success_callback=airflow_task_success,
                                         on_failure_callback=airflow_task_failure,
                                         queue='disambiguator', pool='database_write_iops_contenders')

assignee_archive_results = PythonOperator(task_id='Assignee_Archive_Assignee_Results',
                                          python_callable=archive_assignee_results,
                                          provide_context=True,
                                          dag=disambiguation,
                                          on_success_callback=airflow_task_success,
                                          on_failure_callback=airflow_task_failure,
                                          queue='disambiguator')

post_process_update_granted_rawassignee = PythonOperator(task_id='assignee_update_granted_rawassignee',
                                                         python_callable=update_granted_rawassignee,
                                                         dag=disambiguation,
                                                         on_success_callback=airflow_task_success,
                                                         on_failure_callback=airflow_task_failure,
                                                         queue='data_collector', pool='database_write_iops_contenders')
post_process_update_pregranted_rawassignee = PythonOperator(task_id='assignee_update_pregranted_rawassignee',
                                                            python_callable=update_pregranted_rawassignee,
                                                            dag=disambiguation,
                                                            on_success_callback=airflow_task_success,
                                                            on_failure_callback=airflow_task_failure,
                                                            queue='data_collector',
                                                            pool='database_write_iops_contenders')
post_process_precache_assignees = PythonOperator(task_id='assignee_precache_assignees',
                                                 python_callable=precache_assignees,
                                                 dag=disambiguation,
                                                 on_success_callback=airflow_task_success,
                                                 on_failure_callback=airflow_task_failure,
                                                 queue='data_collector', pool='database_write_iops_contenders')
post_process_create_canonical_assignees = PythonOperator(task_id='assignee_create_canonical_assignees',
                                                         python_callable=create_canonical_assignees,
                                                         dag=disambiguation,
                                                         on_success_callback=airflow_task_success,
                                                         on_failure_callback=airflow_task_failure,
                                                         queue='data_collector', pool='database_write_iops_contenders')
post_process_assignees = PythonOperator(task_id='assignee_additional_post_processing',
                                                         python_callable=additional_post_processing_assignee,
                                                         dag=disambiguation,
                                                         on_success_callback=airflow_task_success,
                                                         on_failure_callback=airflow_task_failure,
                                                         queue='data_collector', pool='database_write_iops_contenders')
post_process_create_patent_assignee = PythonOperator(task_id='assignee_create_patent_assignee',
                                                           python_callable=create_patent_assignee,
                                                           dag=disambiguation,
                                                           on_success_callback=airflow_task_success,
                                                           on_failure_callback=airflow_task_failure,
                                                           queue='data_collector',
                                                           pool='database_write_iops_contenders')
post_process_create_pgpubs_assignee = PythonOperator(task_id='assignee_create_pgpubs_assignee',
                                                              python_callable=create_publication_assignee,
                                                              dag=disambiguation,
                                                              on_success_callback=airflow_task_success,
                                                              on_failure_callback=airflow_task_failure,
                                                              queue='data_collector',
                                                              pool='database_write_iops_contenders')
post_process_create_patent_loc_assignee = PythonOperator(task_id='assignee_create_patent_loc_assignee',
                                                           python_callable=load_granted_location_assignee,
                                                           dag=disambiguation,
                                                           on_success_callback=airflow_task_success,
                                                           on_failure_callback=airflow_task_failure,
                                                           queue='data_collector',
                                                           pool='database_write_iops_contenders')
post_process_create_pgpubs_loc_assignee = PythonOperator(task_id='assignee_create_pgpubs_loc_assignee',
                                                              python_callable=load_pregranted_location_assignee,
                                                              dag=disambiguation,
                                                              on_success_callback=airflow_task_success,
                                                              on_failure_callback=airflow_task_failure,
                                                              queue='data_collector',
                                                              pool='database_write_iops_contenders')
qc_post_process_assignee_operator = PythonOperator(task_id='qc_post_process_assignee',
                                                   python_callable=qc_post_process_assignee,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector')
qc_post_process_assignee_pgpubs_operator = PythonOperator(task_id='qc_post_process_assignee_pgpubs',
                                                   python_callable=post_process_assignee_qc_pgpubs,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector')
post_process_assignee_patent_phase2_qc_operator = PythonOperator(task_id='qc_post_process_assignee_patent_phase2',
                                                   python_callable=post_process_assignee_patent_phase2_qc,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector')
post_process_assignee_pgpubs_phase2_qc_operator = PythonOperator(task_id='qc_post_process_assignee_pgpubs_phase2',
                                                   python_callable=post_process_assignee_pgpubs_phase2_qc,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure,
                                                   queue='data_collector')

# update_granted_persistent_long_assignee = PythonOperator(
#     task_id='update_granted_persistent_long_assignee',
#     python_callable=update_long_entity,
#     op_kwargs={
#         'entity': 'assignee',
#         'database_type': 'granted_patent'
#     },
#     dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
# )
create_granted_persistent_wide_assignee = PythonOperator(
    task_id='create_granted_persistent_wide_assignee',
    python_callable=write_wide_table,
    op_kwargs={
        'entity': 'assignee',
        'database_type': 'granted_patent'
    },
    dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
)
prepare_granted_persistent_wide_assignee = PythonOperator(
    task_id='prepare_granted_persistent_wide_assignee',
    python_callable=prepare_wide_table,
    op_kwargs={
        'entity': 'assignee',
        'database_type': 'granted_patent'
    },
    dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
)
# update_pregrant_persistent_long_assignee = PythonOperator(
#     task_id='update_pregrant_persistent_long_assignee',
#     python_callable=update_long_entity,
#     op_kwargs={
#         'entity': 'assignee',
#         'database_type': 'pgpubs'
#     },
#     dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
# )
create_pregrant_persistent_wide_assignee = PythonOperator(
    task_id='create_pregrant_persistent_wide_assignee',
    python_callable=write_wide_table,
    op_kwargs={
        'entity': 'assignee',
        'database_type': 'pgpubs'
    },
    dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
)
prepare_pregrant_persistent_wide_assignee = PythonOperator(
    task_id='prepare_pregrant_persistent_wide_assignee',
    python_callable=prepare_wide_table,
    op_kwargs={
        'entity': 'assignee',
        'database_type': 'pgpubs'
    },
    dag=disambiguation, queue='data_collector', pool='database_write_iops_contenders'
)

# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# LOCATION TASKS

# UPDATED
post_process_location_operator = PythonOperator(task_id='post_process_location',
                                                python_callable=post_process_location,
                                                dag=disambiguation,
                                                on_success_callback=airflow_task_success,
                                                on_failure_callback=airflow_task_failure, queue='admin', pool='default_pool')
# UPDATED
qc_post_process_location_operator = PythonOperator(task_id='qc_post_process_location',
                                                   python_callable=post_process_qc,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure, queue='admin',  pool='default_pool')

loc_fips_operator = PythonOperator(task_id='augment_location_fips',
                                                   python_callable=augment_location_fips,
                                                   dag=disambiguation,
                                                   on_success_callback=airflow_task_success,
                                                   on_failure_callback=airflow_task_failure, queue='admin',  pool='default_pool')

# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
# TASK DEPENDENCY MAPPING

operator_sequence = {'assignee_feat_setup': [assignee_inventor_disambig_setup, inv_build_assignee_features],
                     'coinventor_feat_setup': [assignee_inventor_disambig_setup, inv_build_coinventor_features],
                     'title_feat_setup': [assignee_inventor_disambig_setup, inv_build_titles],
                     'canopies_setup': [assignee_inventor_disambig_setup, inv_build_canopies],
                     'assignee_mentions_setup': [assignee_inventor_disambig_setup, assignee_build_assignee_features],
                     'assignee_feat': [inv_build_assignee_features, inv_run_clustering],
                     'coinventor_feat': [inv_build_coinventor_features, inv_run_clustering],
                     'title_feat': [inv_build_titles, inv_run_clustering],
                     'canopies': [inv_build_canopies, inv_run_clustering],
                     'inventor_clustering': [inv_run_clustering, inv_finalize_output, inv_upload_results,
                                             inv_archive_results, post_process_update_pregranted_rawinventor,
                                             post_process_update_granted_rawinventor, post_process_precache_inventors,
                                             post_process_create_canonical_inventors],
                     'inventor_post_processing_1': [qc_post_process_inventor_operator,
                                                    post_process_create_patent_inventor,
                                                    post_process_create_patent_loc_inventor,
                                                    prepare_granted_persistent_wide_inventor,
                                                    create_granted_persistent_wide_inventor,
                                                    # update_granted_persistent_long_inventor,
                                                    post_process_inventor_patent_phase2_qc_operator],
                     'inventor_post_processing_2': [qc_post_process_inventor_pgpubs_operator,
                                                    post_process_create_pgpubs_inventor,
                                                    post_process_create_pgpubs_loc_inventor,
                                                    prepare_pregranted_persistent_wide_inventor,
                                                    create_pregranted_persistent_wide_inventor,
                                                    # update_pregranted_persistent_long_inventor,
                                                    post_process_inventor_pgpubs_phase2_qc_operator],
                     'assignee_mention': [assignee_build_assignee_features, qc_assignee_build_assignee_features,
                                          assignee_run_clustering],
                     'cross_link_1': [inv_build_coinventor_features, assignee_run_clustering],
                     'cross_link_2': [inv_build_titles, assignee_run_clustering],
                     'cross_link_3': [inv_build_assignee_features, assignee_run_clustering],
                     'assignee_clustering': [assignee_run_clustering, assignee_create_uuid_map,
                                             assignee_finalize_results, assignee_upload_results,
                                             assignee_archive_results, post_process_update_pregranted_rawassignee,
                                             post_process_update_granted_rawassignee, post_process_precache_assignees,
                                             post_process_create_canonical_assignees,
                                             post_process_assignees
                                             ],
                     'granted_persistent': [post_process_assignees,
                                            qc_post_process_assignee_operator,
                                            post_process_create_patent_assignee,
                                            post_process_create_patent_loc_assignee,
                                            prepare_granted_persistent_wide_assignee,
                                            create_granted_persistent_wide_assignee,
                                            #update_granted_persistent_long_assignee,
                                            post_process_assignee_patent_phase2_qc_operator
                                            ],
                     'pgpubs_persistent': [post_process_assignees,
                         qc_post_process_assignee_pgpubs_operator,
                         post_process_create_pgpubs_assignee,
                         post_process_create_pgpubs_loc_assignee,
                         prepare_pregrant_persistent_wide_assignee,
                         create_pregrant_persistent_wide_assignee,
                         #update_pregrant_persistent_long_assignee,
                         post_process_assignee_pgpubs_phase2_qc_operator
                     ],
                     'location_post_processing': [post_process_location_operator, qc_post_process_location_operator],
                     'location_assignee_link': [qc_post_process_location_operator, assignee_build_assignee_features],
                     'location_assignee_granted_link': [qc_post_process_location_operator,
                                                        post_process_create_patent_loc_assignee],
                     'location_assignee_pregranted_link': [qc_post_process_location_operator,
                                                           post_process_create_pgpubs_loc_assignee],
                     'location_inventor_granted_link': [qc_post_process_location_operator,
                                                        post_process_create_patent_loc_inventor],
                     'location_inventor_pregranted_link': [qc_post_process_location_operator,
                                                           post_process_create_pgpubs_loc_inventor]
                     }


# SETUP
archive_location_tables_task.set_upstream(assignee_inventor_disambig_setup)
archive_inventor_tables_task.set_upstream(archive_location_tables_task)
archive_assignee_tables_task.set_upstream(archive_inventor_tables_task)

for dependency_group in operator_sequence:
    dependency_sequence = operator_sequence[dependency_group]
    chain_operators(dependency_sequence)

# LOCATIONS
loc_fips_operator.set_upstream(post_process_location_operator)

# INVENTOR
run_pgpubs_gender.set_upstream(post_process_create_canonical_inventors)
run_patent_gender.set_upstream(post_process_create_canonical_inventors)

inventor_gender_post_processing.set_upstream(run_patent_gender)
inventor_gender_post_processing.set_upstream(run_pgpubs_gender)

qc_post_process_inventor_operator.set_upstream(inventor_gender_post_processing)
qc_post_process_inventor_pgpubs_operator.set_upstream(inventor_gender_post_processing)


