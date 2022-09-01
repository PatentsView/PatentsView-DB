import configparser
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from slack_sdk import WebClient

from airflow.dags.granted_patent_parser.patentsview_data_updater import operator_settings
from slack_sdk.errors import SlackApiError

project_home = os.environ['PACKAGE_HOME']
config = configparser.ConfigParser()
config.read(project_home + '/config.ini')

slack_token = config["SLACK"]["API_TOKEN"]
slack_client = WebClient(slack_token)
slack_channel = config["SLACK"]["CHANNEL"]
schema_only = config["REPORTING_DATABASE_OPTIONS"]["SCHEMA_ONLY"]
if schema_only == "TRUE":
    schema_only = True
else:
    schema_only = False

from reporting_database_generator.database import validate_query

template_extension_config = [".sql"]
database_name_config = {
    'raw_database': config['REPORTING_DATABASE_OPTIONS']['RAW_DATABASE_NAME'],
    'reporting_database': config['REPORTING_DATABASE_OPTIONS']['REPORTING_DATABASE_NAME'],
    'version_indicator': config['REPORTING_DATABASE_OPTIONS']['VERSION_INDICATOR']
}


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
    'concurrency': 4
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# REPORTING DB

reporting_db_dag = DAG("reporting_database_generation"
                       , default_args=default_args
                       , start_date=datetime(2022, 8, 30)
                       , schedule_interval=None
                       , template_searchpath="/project/reporting_database_generator/")

db_creation = SQLTemplatedPythonOperator(
    task_id='Database_Creation',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '00_Creation',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '00_Creation.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)

govt_interest = SQLTemplatedPythonOperator(
    task_id='Government_Interest',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '01_01_Govt_Interest',
        "schema_only": schema_only,
        "fk_check": False
    },
    templates_dict={
        'source_sql': '01_01_Govt_Interest.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
# claims = SQLTemplatedPythonOperator(
#     task_id='Claims_Table',
#     provide_context=True,
#     python_callable=validate_query.validate_and_execute,
#     dag=reporting_db_dag,
#     op_kwargs={'filename': '01_02_Claims', 
#                "schema_only": schema_only},
#     templates_dict={'source_sql': '01_02_Claims.sql'},
#     templates_exts=template_extension_config,
#     params=database_name_config
# )
id_mappings = SQLTemplatedPythonOperator(
    task_id='ID_Mappings',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '01_03_ID_Mappings',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '01_03_ID_Mappings.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
application = SQLTemplatedPythonOperator(
    task_id='Application',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '01_04_Application',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '01_04_Application.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
wipo = SQLTemplatedPythonOperator(
    task_id='WIPO',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '01_05_Wipo',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '01_05_Wipo.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
patent = SQLTemplatedPythonOperator(
    task_id='Patent',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '02_Patent',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '02_Patent.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
location = SQLTemplatedPythonOperator(
    task_id='Location',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_01_Location',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_01_Location.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
assignee = SQLTemplatedPythonOperator(
    task_id='Assignee',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_02_Assignee',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_02_Assignee.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
inventor = SQLTemplatedPythonOperator(
    task_id='Inventor',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_03_Inventor',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_03_Inventor.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
lawyer = SQLTemplatedPythonOperator(
    task_id='Lawyer',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_04_Lawyer',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_04_Lawyer.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
examiner = SQLTemplatedPythonOperator(
    task_id='Examiner',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_05_Examiner',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_05_Examiner.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
forprior = SQLTemplatedPythonOperator(
    task_id='Foreign_Priority',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_06_Foreign_Priority',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_06_Foreign_Priority.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
pct = SQLTemplatedPythonOperator(
    task_id='PCT',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_07_PCT',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_07_PCT.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
us_appcit = SQLTemplatedPythonOperator(
    task_id='US_Application_Citation',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_08_US_App_Citation',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_08_US_App_Citation.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
us_patcit = SQLTemplatedPythonOperator(
    task_id='US_Patent_Citation',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_09_US_Patent_Citation',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_09_US_Patent_Citation.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
cpc = SQLTemplatedPythonOperator(
    task_id='CPC',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_10_CPC',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_10_CPC.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
ipcr = SQLTemplatedPythonOperator(
    task_id='ipcr',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_11_IPCR',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_11_IPCR.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
nber = SQLTemplatedPythonOperator(
    task_id='NBER',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_12_Nber',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_12_Nber.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
uspc = SQLTemplatedPythonOperator(
    task_id='USPC',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_13_uspc',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '03_13_uspc.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
rep_tbl_1 = SQLTemplatedPythonOperator(
    task_id='Reporting_Tables_1',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '04_Support',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '04_Support.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_1 = SQLTemplatedPythonOperator(
    task_id='Indexes-01',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_01_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_2 = SQLTemplatedPythonOperator(
    task_id='Indexes-02',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_02_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_3 = SQLTemplatedPythonOperator(
    task_id='Indexes-03',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_03_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_4 = SQLTemplatedPythonOperator(
    task_id='Indexes-04',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_04_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_5 = SQLTemplatedPythonOperator(
    task_id='Indexes-05',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_05_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_6 = SQLTemplatedPythonOperator(
    task_id='Indexes-06',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_06_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_7 = SQLTemplatedPythonOperator(
    task_id='Indexes-07',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_07_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_8 = SQLTemplatedPythonOperator(
    task_id='Indexes-08',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_08_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_9 = SQLTemplatedPythonOperator(
    task_id='Indexes-09',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_09_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_10 = SQLTemplatedPythonOperator(
    task_id='Indexes-10',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_10_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_11 = SQLTemplatedPythonOperator(
    task_id='Indexes-11',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_11_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
idx_12 = SQLTemplatedPythonOperator(
    task_id='Indexes-12',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_Indexes',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '05_12_index.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)
rep_tbl_2 = SQLTemplatedPythonOperator(
    task_id='Reporting_Tables_2',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '06_Reporting_Tables',
        "schema_only": schema_only
    },
    templates_dict={
        'source_sql': '06_Reporting_Tables.sql'
    },
    templates_exts=template_extension_config,
    params=database_name_config
)

# half_join_table = SQLTemplatedPythonOperator(
#     task_id='half_join_table',
#     provide_context=True,
#     python_callable=validate_query.validate_and_execute,
#     dag=reporting_db_dag,
#     op_kwargs={'filename': '07_half_join',
#                "schema_only": schema_only},
#     templates_dict={'source_sql': '07_half_join.sql'},
#     templates_exts=template_extension_config,
#     params=database_name_config
# )

# MAPPING DEPENDENCY

govt_interest.set_upstream(db_creation)
# claims.set_upstream(db_creation)
id_mappings.set_upstream(db_creation)
application.set_upstream(db_creation)
wipo.set_upstream(db_creation)

patent.set_upstream(id_mappings)

location.set_upstream(patent)
assignee.set_upstream(patent)
inventor.set_upstream(patent)
lawyer.set_upstream(patent)
examiner.set_upstream(patent)
forprior.set_upstream(patent)
pct.set_upstream(patent)
us_appcit.set_upstream(patent)
us_patcit.set_upstream(patent)
ipcr.set_upstream(patent)
nber.set_upstream(patent)

uspc.set_upstream(cpc)

cpc.set_upstream(location)
cpc.set_upstream(assignee)
cpc.set_upstream(inventor)
cpc.set_upstream(lawyer)
cpc.set_upstream(examiner)
cpc.set_upstream(forprior)
cpc.set_upstream(pct)
cpc.set_upstream(us_appcit)
cpc.set_upstream(us_patcit)

cpc.set_upstream(ipcr)
cpc.set_upstream(nber)

rep_tbl_1.set_upstream(uspc)

idx_1.set_upstream(rep_tbl_1)
idx_2.set_upstream(rep_tbl_1)
idx_3.set_upstream(rep_tbl_1)
idx_4.set_upstream(rep_tbl_1)
idx_5.set_upstream(rep_tbl_1)
idx_6.set_upstream(rep_tbl_1)
idx_7.set_upstream(rep_tbl_1)
idx_8.set_upstream(rep_tbl_1)
idx_9.set_upstream(rep_tbl_1)
idx_10.set_upstream(rep_tbl_1)
idx_11.set_upstream(rep_tbl_1)
idx_12.set_upstream(rep_tbl_1)

idx_1.set_downstream(rep_tbl_2)
idx_2.set_downstream(rep_tbl_2)
idx_3.set_downstream(rep_tbl_2)
idx_4.set_downstream(rep_tbl_2)
idx_5.set_downstream(rep_tbl_2)
idx_6.set_downstream(rep_tbl_2)
idx_7.set_downstream(rep_tbl_2)
idx_8.set_downstream(rep_tbl_2)
idx_9.set_downstream(rep_tbl_2)
idx_10.set_downstream(rep_tbl_2)
idx_11.set_downstream(rep_tbl_2)
idx_12.set_downstream(rep_tbl_2)
