from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime, timedelta

import configparser
import os

project_home = os.environ['PACKAGE_HOME']
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

from Scripts.Website_Database_Generator.database import validate_query
import pprint

template_extension_config = [".sql"]
database_name_config = {'raw_database': config['REPORTING_DATABASE_OPTIONS']['RAW_DATABASE_NAME'],
                        'reporting_database': config['REPORTING_DATABASE_OPTIONS']['REPORTING_DATABASE_NAME']}


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql',)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['smadhavan@air.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 4
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

reporting_db_dag = DAG("reporting_database_generation", default_args=default_args, start_date=datetime(2018, 12, 1),
                       schedule_interval=None, template_searchpath="/project/Scripts/Website_Database_generator/")
db_creation = SQLTemplatedPythonOperator(
    task_id='Database_Creation',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '00_Creation.sql'},
    templates_dict={'source_sql': '00_Creation.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
govt_interest = SQLTemplatedPythonOperator(
    task_id='Government_Interest',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '01_01_Govt_Interest.sql'},
    templates_dict={'source_sql': '01_01_Govt_Interest.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
claims = SQLTemplatedPythonOperator(
    task_id='Claims_Table',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '01_02_Claims.sql'},
    templates_dict={'source_sql': '01_02_Claims.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
id_mappings = SQLTemplatedPythonOperator(
    task_id='ID_Mappings',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '01_03_ID_Mappings.sql'},
    templates_dict={'source_sql': '01_03_ID_Mappings.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
application = SQLTemplatedPythonOperator(
    task_id='Application',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '01_04_Application.sql'},
    templates_dict={'source_sql': '01_04_Application.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
wipo = SQLTemplatedPythonOperator(
    task_id='WIPO',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '01_05_Wipo.sql'},
    templates_dict={'source_sql': '01_05_Wipo.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
patent = SQLTemplatedPythonOperator(
    task_id='Patent',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '02_Patent.sql'},
    templates_dict={'source_sql': '02_Patent.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
location = SQLTemplatedPythonOperator(
    task_id='Location',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_01_Location.sql'},
    templates_dict={'source_sql': '03_01_Location.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
assignee = SQLTemplatedPythonOperator(
    task_id='Assignee',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_02_Assignee.sql'},
    templates_dict={'source_sql': '03_02_Assignee.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
inventor = SQLTemplatedPythonOperator(
    task_id='Inventor',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_03_Inventor.sql'},
    templates_dict={'source_sql': '03_03_Inventor.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
lawyer = SQLTemplatedPythonOperator(
    task_id='Lawyer',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_04_Lawyer.sql'},
    templates_dict={'source_sql': '03_04_Lawyer.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
examiner = SQLTemplatedPythonOperator(
    task_id='Examiner',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_05_Examiner.sql'},
    templates_dict={'source_sql': '03_05_Examiner.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
forprior = SQLTemplatedPythonOperator(
    task_id='Foreign_Priority',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_06_Foreign_Priority.sql'},
    templates_dict={'source_sql': '03_06_Foreign_Priority.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
pct = SQLTemplatedPythonOperator(
    task_id='PCT',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_07_PCT.sql'},
    templates_dict={'source_sql': '03_07_PCT.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
us_appcit = SQLTemplatedPythonOperator(
    task_id='US_Application_Citation',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_08_US_App_Citation.sql'},
    templates_dict={'source_sql': '03_08_US_App_Citation.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
us_patcit = SQLTemplatedPythonOperator(
    task_id='US_Patent_Citation',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_09_US_Patent_Citation.sql'},
    templates_dict={'source_sql': '03_09_US_Patent_Citation.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
cpc = SQLTemplatedPythonOperator(
    task_id='CPC',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_10_CPC.sql'},
    templates_dict={'source_sql': '03_10_CPC.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
ipcr = SQLTemplatedPythonOperator(
    task_id='ipcr',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_11_IPCR.sql'},
    templates_dict={'source_sql': '03_11_IPCR.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
nber = SQLTemplatedPythonOperator(
    task_id='NBER',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_12_Nber.sql'},
    templates_dict={'source_sql': '03_12_Nber.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
uspc = SQLTemplatedPythonOperator(
    task_id='USPC',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '03_13_uspc.sql'},
    templates_dict={'source_sql': '03_13_uspc.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
rep_tbl_1 = SQLTemplatedPythonOperator(
    task_id='Reporting_Tables_1',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '04_Support.sql'},
    templates_dict={'source_sql': '04_Support.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
idx = SQLTemplatedPythonOperator(
    task_id='Indexes',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '05_Indexes.sql'},
    templates_dict={'source_sql': '05_Indexes.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)
rep_tbl_2 = SQLTemplatedPythonOperator(
    task_id='Reporting_Tables_2',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={'filename': '06_Reporting_Tables.sql'},
    templates_dict={'source_sql': '06_Reporting_Tables.sql'},
    templates_exts=template_extension_config,
    params=database_name_config
)

govt_interest.set_upstream(db_creation)
claims.set_upstream(db_creation)
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
cpc.set_upstream(patent)
ipcr.set_upstream(patent)
nber.set_upstream(patent)
uspc.set_upstream(patent)

rep_tbl_1.set_upstream(location)
rep_tbl_1.set_upstream(assignee)
rep_tbl_1.set_upstream(inventor)
rep_tbl_1.set_upstream(lawyer)
rep_tbl_1.set_upstream(examiner)
rep_tbl_1.set_upstream(forprior)
rep_tbl_1.set_upstream(pct)
rep_tbl_1.set_upstream(us_appcit)
rep_tbl_1.set_upstream(us_patcit)
rep_tbl_1.set_upstream(cpc)
rep_tbl_1.set_upstream(ipcr)
rep_tbl_1.set_upstream(nber)
rep_tbl_1.set_upstream(uspc)

idx.set_upstream(rep_tbl_1)
rep_tbl_2.set_upstream(idx)
