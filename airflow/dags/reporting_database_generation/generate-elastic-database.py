import configparser
import os
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from slack_sdk import WebClient
from airflow import DAG
from reporting_database_generator.database import validate_query

project_home = os.environ['PACKAGE_HOME']
config = configparser.ConfigParser()
config.read(project_home + '/config.ini')
database_name_config = {
    'raw_database': config['REPORTING_DATABASE_OPTIONS']['RAW_DATABASE_NAME'],
    'reporting_database': config['REPORTING_DATABASE_OPTIONS']['REPORTING_DATABASE_NAME'],
    'version_indicator': config['REPORTING_DATABASE_OPTIONS']['VERSION_INDICATOR'],
    'last_reporting_database': config['REPORTING_DATABASE_OPTIONS']['LAST_REPORTING_DATABASE_NAME'],
    'elastic_database_prefix': config['REPORTING_DATABASE_OPTIONS']['ELASTIC_DATABASE_PREFIX']
}


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


template_extension_config = [".sql"]

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

# REPORTING DB

elastic_prep_dag = DAG("elastic_data_preparation"
                       , default_args=default_args
                       , start_date=datetime(2023, 4, 1)
                       , schedule_interval=None
                       , template_searchpath="/project/reporting_database_generator/elastic_scripts/")

db_creation = SQLTemplatedPythonOperator(
    task_id='Elastic_Database_Creation',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '08_Elastic_Prep',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '08_Elastic_Prep.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)

endpoint_patent_patents_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Patent_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_01_elastic_patents_patent.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '09_01_elastic_patents_patent.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_patents_table.set_upstream(db_creation)

endpoint_patent_applications_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Application_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_02_elastic_patents_application.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '09_02_elastic_patents_application.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_applications_table.set_upstream(db_creation)

endpoint_patent_views = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Views',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_03_patents_views.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '09_03_patents_views.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_views.set_upstream(db_creation)

endpoint_patent_assignee_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Assignee_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_01_patent_assignee.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_01_patent_assignee.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_assignee_table.set_upstream(endpoint_patent_patents_table)
endpoint_patent_inventor_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Inventor_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_02_patent_inventor.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_02_patent_inventor.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_inventor_table.set_upstream(endpoint_patent_patents_table)

endpoint_patent_cpc_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_CPC_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_03_patents_cpc.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_03_patents_cpc.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_cpc_table.set_upstream(endpoint_patent_patents_table)

endpoint_patent_applicant_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Applicant_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_04_patent_applicant.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_04_patent_applicant.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_applicant_table.set_upstream(endpoint_patent_patents_table)

endpoint_patent_attorneys_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Attorneys_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_05_patents_attorneys.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_05_patents_attorneys.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_attorneys_table.set_upstream(endpoint_patent_patents_table)

endpoint_patent_examiner_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_Examiner_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_06_patents_examiner.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_06_patents_examiner.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_examiner_table.set_upstream(endpoint_patent_patents_table)

endpoint_patent_GI_table = SQLTemplatedPythonOperator(
    task_id='Patent_Endpoint_GI_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_07_patents_gi.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_07_patents_gi.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
endpoint_patent_GI_table.set_upstream(endpoint_patent_patents_table)
locations_endpoint_locations_table = SQLTemplatedPythonOperator(
    task_id='locations_Endpoint_locations_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_04_locations.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '09_04_locations.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
locations_endpoint_locations_table.set_upstream(db_creation)

assignee_endpoint_assignee_table = SQLTemplatedPythonOperator(
    task_id='assignee_Endpoint_assignee_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_05_assignee.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '09_05_assignee.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
assignee_endpoint_assignee_table.set_upstream(db_creation)

inventor_endpoint_inventor_table = SQLTemplatedPythonOperator(
    task_id='inventor_Endpoint_inventor_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_06_inventor.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '09_06_inventor.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
inventor_endpoint_inventor_table.set_upstream(db_creation)

fcitation_endpoint_fcitation_table = SQLTemplatedPythonOperator(
    task_id='fcitation_Endpoint_fcitation_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_12_fcitation.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_12_fcitation.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
fcitation_endpoint_fcitation_table.set_upstream(endpoint_patent_patents_table)

attorney_endpoint_attorney_table = SQLTemplatedPythonOperator(
    task_id='attorney_Endpoint_attorney_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_08_attorney.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '09_08_attorney.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
attorney_endpoint_attorney_table.set_upstream(db_creation)

otherreference_endpoint_otherreference_table = SQLTemplatedPythonOperator(
    task_id='otherreference_Endpoint_otherreference_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_11_otherreference.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_11_otherreference.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
otherreference_endpoint_otherreference_table.set_upstream(endpoint_patent_patents_table)

relapptext_endpoint_relapptext_table = SQLTemplatedPythonOperator(
    task_id='relapptext_Endpoint_relapptext_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_10_relapptext.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_10_relapptext.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
relapptext_endpoint_relapptext_table.set_upstream(endpoint_patent_patents_table)

patentcitation_endpoint_patentcitation_table = SQLTemplatedPythonOperator(
    task_id='patentcitation_Endpoint_patentcitation_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_09_patentcitation.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_09_patentcitation.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
patentcitation_endpoint_patentcitation_table.set_upstream(endpoint_patent_patents_table)

applicationcitation_endpoint_applicationcitation_table = SQLTemplatedPythonOperator(
    task_id='applicationcitation_Endpoint_applicationcitation_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '10_08_applicationcitation.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '10_08_applicationcitation.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
applicationcitation_endpoint_applicationcitation_table.set_upstream(endpoint_patent_patents_table)

classifications_endpoint_classifications_table = SQLTemplatedPythonOperator(
    task_id='classifications_Endpoint_classifications_Table',
    python_callable=validate_query.validate_and_execute,
    dag=elastic_prep_dag,
    op_kwargs={
        'filename': '09_12_classifications.sql',
        "schema_only": False
    },
    templates_dict={
        'source_sql': '09_12_classifications.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
classifications_endpoint_classifications_table.set_upstream(db_creation)
