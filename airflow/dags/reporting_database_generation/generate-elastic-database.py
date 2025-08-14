import os
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow import DAG
from reporting_database_generator.database import validate_query

from lib.configuration import get_current_config, get_today_dict

from QA.post_processing.ElasticDBTester  import run_elastic_db_qa
# from QA.post_processing.BulkDownloadsTester import run_bulk_downloads_qa

from updater.callbacks import airflow_task_failure, airflow_task_success
from updater.create_databases.create_views_for_bulk_downloads import update_view_date_ranges
from updater.create_databases.create_and_test_crosswalk import create_outer_patent_publication_crosswalk, qc_crosswalk


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['smadhavan@air.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 4,
    'queue': 'data_collector',
    'pool': 'database_write_iops_contenders'
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

project_home = os.environ['PACKAGE_HOME']
templates_searchpath = "{home}/resources".format(home=project_home)
config = get_current_config(type='granted_patent', supplemental_configs=None, **get_today_dict())

# REPORTING DB

elastic_prep_dag = DAG(dag_id="elastic_data_preparation_quarterly"
                       , default_args=default_args
                       , start_date=datetime(2023, 1, 1)
                       , schedule_interval='@quarterly'
                       , template_searchpath="/project/reporting_database_generator/elastic_scripts/"
                       , catchup=False)

operator_settings = {
    'dag': elastic_prep_dag,
    'on_success_callback': airflow_task_success,
    'on_failure_callback': airflow_task_failure,
    'on_retry_callback': airflow_task_failure
}


db_creation = SQLTemplatedPythonOperator(
    task_id='Elastic_Database_Creation',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '08_Elastic_Prep'
    },
    templates_dict={
        'source_sql': '08_Elastic_Prep.sql'
    }
)

endpoint_patent_patents_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Patent_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_01_elastic_patents_patent.sql'
    },
    templates_dict={
        'source_sql': '09_01_elastic_patents_patent.sql'
    }
)


endpoint_patent_applications_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Application_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_02_elastic_patents_application.sql'
    },
    templates_dict={
        'source_sql': '09_02_elastic_patents_application.sql'
    }
)


endpoint_patent_views = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Views',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_03_patents_views.sql'
    },
    templates_dict={
        'source_sql': '09_03_patents_views.sql'
    }
)


endpoint_patent_assignee_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Assignee_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_01_patent_assignee.sql'
    },
    templates_dict={
        'source_sql': '10_01_patent_assignee.sql'
    }
)

endpoint_patent_inventor_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Inventor_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_02_patent_inventor.sql'
    },
    templates_dict={
        'source_sql': '10_02_patent_inventor.sql'
    }
)


endpoint_patent_cpc_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_CPC_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_03_patents_cpc.sql'
    },
    templates_dict={
        'source_sql': '10_03_patents_cpc.sql'
    }
)


endpoint_patent_applicant_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Applicant_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_04_patent_applicant.sql'
    },
    templates_dict={
        'source_sql': '10_04_patent_applicant.sql'
    }
)


endpoint_patent_attorneys_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Attorneys_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_05_patents_attorneys.sql'
    },
    templates_dict={
        'source_sql': '10_05_patents_attorneys.sql'
    }
)


endpoint_patent_examiner_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Examiner_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_06_patents_examiner.sql'
    },
    templates_dict={
        'source_sql': '10_06_patents_examiner.sql'
    }
)


endpoint_patent_GI_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_GI_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_07_patents_gi.sql'
    },
    templates_dict={
        'source_sql': '10_07_patents_gi.sql'
    }
)

locations_endpoint_locations_table = SQLTemplatedPythonOperator(
    task_id='locations_Endpoint_locations_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_04_locations.sql'
    },
    templates_dict={
        'source_sql': '09_04_locations.sql'
    }
)


assignee_endpoint_assignee_table = SQLTemplatedPythonOperator(
    task_id='assignee_Endpoint_assignee_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_05_assignee.sql'
    },
    templates_dict={
        'source_sql': '09_05_assignee.sql'
    }
)


inventor_endpoint_inventor_table = SQLTemplatedPythonOperator(
    task_id='inventor_Endpoint_inventor_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_06_inventor.sql'
    },
    templates_dict={
        'source_sql': '09_06_inventor.sql'
    }
)


fcitation_endpoint_fcitation_table = SQLTemplatedPythonOperator(
    task_id='fcitation_Endpoint_fcitation_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_12_fcitation.sql'
    },
    templates_dict={
        'source_sql': '10_12_fcitation.sql'
    }
)


attorney_endpoint_attorney_table = SQLTemplatedPythonOperator(
    task_id='attorney_Endpoint_attorney_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_08_attorney.sql'
    },
    templates_dict={
        'source_sql': '09_08_attorney.sql'
    }
)


otherreference_endpoint_otherreference_table = SQLTemplatedPythonOperator(
    task_id='otherreference_Endpoint_otherreference_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_11_otherreference.sql'
    },
    templates_dict={
        'source_sql': '10_11_otherreference.sql'
    }
)


relapptext_endpoint_relapptext_table = SQLTemplatedPythonOperator(
    task_id='relapptext_Endpoint_relapptext_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_10_relapptext.sql'
    },
    templates_dict={
        'source_sql': '10_10_relapptext.sql'
    }
)


patentcitation_endpoint_patentcitation_table = SQLTemplatedPythonOperator(
    task_id='patentcitation_Endpoint_patentcitation_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_09_patentcitation.sql'
    },
    templates_dict={
        'source_sql': '10_09_patentcitation.sql'
    }
)


applicationcitation_endpoint_applicationcitation_table = SQLTemplatedPythonOperator(
    task_id='applicationcitation_Endpoint_applicationcitation_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_08_applicationcitation.sql'
    },
    templates_dict={
        'source_sql': '10_08_applicationcitation.sql'
    }
)

classifications_endpoint_classifications_table = SQLTemplatedPythonOperator(
    task_id='classifications_Endpoint_classifications_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_12_classifications.sql'
    },
    templates_dict={
        'source_sql': '09_12_classifications.sql'
    }
)


endpoint_publications_publication = SQLTemplatedPythonOperator(
    task_id='Publications_Endpoint_Publications_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '11_01_elastic_publication_publication.sql'
    },
    templates_dict={
        'source_sql': '11_01_elastic_publication_publication.sql'
    }
)

endpoint_publications_publication_views = SQLTemplatedPythonOperator(
    task_id='Publications_Endpoint_Publication_Views',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '11_02_publications_views.sql'
    },
    templates_dict={
        'source_sql': '11_02_publications_views.sql'
    }
)

endpoint_publications_assignee = SQLTemplatedPythonOperator(
    task_id='Publications_Endpoint_Publication_Assignee',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '12_01_publication_assignee.sql'
    },
    templates_dict={
        'source_sql': '12_01_publication_assignee.sql'
    }
)

endpoint_publications_inventor = SQLTemplatedPythonOperator(
    task_id='Publications_Endpoint_Publication_Inventor',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '12_02_publication_inventor.sql'
    },
    templates_dict={
        'source_sql': '12_02_publication_inventor.sql'
    }
)

endpoint_publications_cpc = SQLTemplatedPythonOperator(
    task_id='Publications_Endpoint_Publication_CPC',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '12_03_publication_cpc.sql'
    },
    templates_dict={
        'source_sql': '12_03_publication_cpc.sql'
    }
)
endpoint_publications_gi = SQLTemplatedPythonOperator(
    task_id='Publications_Endpoint_GI',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '12_04_publication_gi.sql'
    },
    templates_dict={
        'source_sql': '12_04_publication_gi.sql'
    }
)

endpoint_publications_us_parties = SQLTemplatedPythonOperator(
    task_id='Publications_Endpoint_US_Parties_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '12_05_us_parties.sql'
    },
    templates_dict={
        'source_sql': '12_05_us_parties.sql'
    }
)


endpoint_rel_app_text_pgpub = SQLTemplatedPythonOperator(
    task_id='Related_App_Text_pgpub_Endpoint',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '12_06_relapptext_pgpub.sql'
    },
    templates_dict={
        'source_sql': '12_06_relapptext_pgpub.sql'
    }
)

g_brf_sum_delta = SQLTemplatedPythonOperator(
    task_id='',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '13_01_g_brf_sum_text_increment.sql'
    },
    templates_dict={
        'source_sql': '13_01_g_brf_sum_text_increment.sql'
    }
)

g_claims_delta = SQLTemplatedPythonOperator(
    task_id='',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '13_02_g_claim_increment.sql'
    },
    templates_dict={
        'source_sql': '13_02_g_claim_increment.sql'
    }
)

g_detail_desc_delta = SQLTemplatedPythonOperator(
    task_id='',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '13_03_g_detail_desc_text_increment.sql'
    },
    templates_dict={
        'source_sql': '13_03_g_detail_desc_text_increment.sql'
    }
)

g_draw_desc_delta = SQLTemplatedPythonOperator(
    task_id='',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '13_04_g_draw_desc_text_increment.sql'
    },
    templates_dict={
        'source_sql': '13_04_g_draw_desc_text_increment.sql'
    }
)

pg_brf_sum_delta = SQLTemplatedPythonOperator(
    task_id='',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '13_05_pg_brf_sum_text_increment.sql'
    },
    templates_dict={
        'source_sql': '13_05_pg_brf_sum_text_increment.sql'
    }
)

pg_claims_delta = SQLTemplatedPythonOperator(
    task_id='',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '13_06_pg_claim_increment.sql'
    },
    templates_dict={
        'source_sql': '13_06_pg_claim_increment.sql'
    }
)

pg_detail_desc_delta = SQLTemplatedPythonOperator(
    task_id='',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '13_07_pg_detail_desc_text_increment.sql'
    },
    templates_dict={
        'source_sql': '13_07_pg_detail_desc_text_increment.sql'
    }
)

pg_draw_desc_delta = SQLTemplatedPythonOperator(
    task_id='',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '13_08_pg_draw_desc_text_increment.sql'
    },
    templates_dict={
        'source_sql': '13_08_pg_draw_desc_text_increment.sql'
    }
)

elastic_patent_db_qa = PythonOperator(
    task_id='elastic_patent_DB_QA',
    python_callable=run_elastic_db_qa,
    op_kwargs={'db_type': 'granted_patent'},  # Pass 'type' as a variable
    dag=elastic_prep_dag
)

elastic_pgpubs_db_qa = PythonOperator(
    task_id='elastic_pgpubs_DB_QA',
    python_callable=run_elastic_db_qa,
    op_kwargs={'db_type': 'pgpubs'},  # Pass 'type' as a variable
    dag=elastic_prep_dag
)

# crosswalk and download view operators
generate_crosswalk_task = PythonOperator(task_id='generate_pat_pub_crosswalk',
                                            python_callable = create_outer_patent_publication_crosswalk,
                                            **operator_settings)

qc_crosswalk_task = PythonOperator(task_id='qc_pat_pub_crosswalk',
                                            python_callable = qc_crosswalk,
                                            **operator_settings)

update_max_vi = PythonOperator(task_id='update_bulk_downloads_views', 
                        python_callable=update_view_date_ranges,
                        **operator_settings)

# require future completion
# qa_granted_bulk_downloads = PythonOperator(task_id='qa_granted_bulk_downloads', 
#                         python_callable=run_bulk_downloads_qa,
#                         **operator_settings)

# qa_pgpubs_bulk_downloads = PythonOperator(task_id='qa_pgpubs_bulk_downloads', 
#                         python_callable=run_pgpubs_bulk_downloads_qa,
#                         **operator_settings)



operator_sequence_groups = {}

operator_sequence_groups['first_step'] = [endpoint_patent_patents_table,endpoint_patent_applications_table,endpoint_patent_views,
                                          locations_endpoint_locations_table,assignee_endpoint_assignee_table, inventor_endpoint_inventor_table,
                                          attorney_endpoint_attorney_table,endpoint_publications_publication,classifications_endpoint_classifications_table ]

operator_sequence_groups['endpoint_patent_steps'] = [endpoint_patent_assignee_table,endpoint_patent_inventor_table, endpoint_patent_cpc_table,
                                                     endpoint_patent_applicant_table,endpoint_patent_attorneys_table, endpoint_patent_examiner_table,
                                                     endpoint_patent_GI_table,fcitation_endpoint_fcitation_table,otherreference_endpoint_otherreference_table,
                                                     relapptext_endpoint_relapptext_table,patentcitation_endpoint_patentcitation_table,applicationcitation_endpoint_applicationcitation_table]

operator_sequence_groups['publications_endpoint'] = [endpoint_publications_assignee,endpoint_publications_inventor,endpoint_publications_cpc,
                                                    endpoint_publications_gi,endpoint_publications_us_parties, endpoint_rel_app_text_pgpub]

operator_sequence_groups['g_text_endpoints'] = [g_brf_sum_delta, g_claims_delta, g_detail_desc_delta, g_draw_desc_delta]
operator_sequence_groups['pg_text_endpoints'] = [pg_brf_sum_delta, pg_claims_delta, pg_detail_desc_delta, pg_draw_desc_delta]

for operator in operator_sequence_groups['first_step']:
    operator.set_upstream(db_creation)
    # Set elastic_patent_db_qa upstream to each operator in 'first_step' group
    elastic_patent_db_qa.set_upstream(operator)

for operator in operator_sequence_groups['endpoint_patent_steps']:
    operator.set_upstream(endpoint_patent_patents_table)
    # Set elastic_patent_db_qa upstream to each operator in 'endpoint_patent_steps' group
    elastic_patent_db_qa.set_upstream(operator)

endpoint_publications_publication_views.set_upstream(endpoint_publications_publication)

for operator in operator_sequence_groups['publications_endpoint']:
    operator.set_upstream(endpoint_publications_publication_views)
    # Set elastic_pgpubs_db_qa upstream to each operator in 'publications_endpoint' group
    elastic_pgpubs_db_qa.set_upstream(operator)

for operator in operator_sequence_groups['g_text_endpoints']:
    operator.set_upstream(db_creation)
    elastic_patent_db_qa.set_upstream(operator)

for operator in operator_sequence_groups['pg_text_endpoints']:
    operator.set_upstream(db_creation)
    elastic_pgpubs_db_qa.set_upstream(operator)
 
# elastic tasks with crosswalk dependencies
endpoint_patent_views.set_upstream(generate_crosswalk_task)
endpoint_publications_publication_views.set_upstream(generate_crosswalk_task)

qc_crosswalk_task.set_upstream(generate_crosswalk_task)
update_max_vi.set_upstream(qc_crosswalk_task)
# qa_granted_bulk_downloads.set_upstream(update_max_vi)
# qa_pgpubs_bulk_downloads.set_upstream(update_max_vi)
