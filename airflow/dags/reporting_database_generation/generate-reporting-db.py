import configparser
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from slack_sdk import WebClient
from updater.callbacks import airflow_task_failure, airflow_task_success

from airflow.dags.granted_patent_parser.patentsview_data_updater import operator_settings
from slack_sdk.errors import SlackApiError

from reporting_database_generator.database import validate_query
from reporting_database_generator.create_eight_digit_patent_lookup import update_patent_id_in_patent
from reporting_database_generator.create_reporting_db import reporting_db_creation
from QA.post_processing.ReportingDBTester import run_reporting_db_qa
from lib.configuration import get_connection_string, get_required_tables, get_current_config

project_home = os.environ['PACKAGE_HOME']
config = configparser.ConfigParser()
config.read(project_home + '/config.ini')

slack_token = config["SLACK"]["API_TOKEN"]
slack_client = WebClient(slack_token)
slack_channel = config["SLACK"]["CHANNEL"]

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
    'queue': 'data_collector'
}

# REPORTING DB

reporting_db_dag = DAG("reporting_database_generation_quarterly"
                       , default_args=default_args
                       , start_date=datetime(2022, 10, 1)
                       # , end_date=datetime(2022, 7, 1)
                       , schedule_interval='@quarterly'
                       , template_searchpath="/project/reporting_database_generator/")

assignee_disambiguation_finished = ExternalTaskSensor(
    task_id="assignee_disambiguation_finished",
    external_dag_id="inventor_assignee_disambiguation",
    external_task_id="qc_post_process_assignee",
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode="reschedule",
)

inventor_disambiguation_finished = ExternalTaskSensor(
    task_id="inventor_disambiguation_finished",
    external_dag_id="inventor_assignee_disambiguation",
    external_task_id="qc_post_process_inventor",
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode="reschedule",
)

db_creation = SQLTemplatedPythonOperator(
    task_id='Database_Creation',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '00_Creation',
    },
    templates_dict={
        'source_sql': '00_Creation.sql'
    },
)
# db_creation = PythonOperator(task_id='Database_Creation',
#                                     python_callable=reporting_db_creation,
#                                     provide_context=True,
#                                     dag=reporting_db_dag,
#                                     on_success_callback=airflow_task_success,
#                                     on_failure_callback=airflow_task_failure)

rebuild_patent_lookup = PythonOperator(task_id='rebuild_patent_lookup',
                                    python_callable=update_patent_id_in_patent,
                                    provide_context=True,
                                    dag=reporting_db_dag,
                                    on_success_callback=airflow_task_success,
                                    on_failure_callback=airflow_task_failure)

govt_interest = SQLTemplatedPythonOperator(
    task_id='Government_Interest',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '01_01_Govt_Interest',
        "fk_check": False
    },
    templates_dict={
        'source_sql': '01_01_Govt_Interest.sql'
    }
)
id_mappings = SQLTemplatedPythonOperator(
    task_id='ID_Mappings',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '01_03_ID_Mappings',
    },
    templates_dict={
        'source_sql': '01_03_ID_Mappings.sql'
    }
)
application = SQLTemplatedPythonOperator(
    task_id='Application',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '01_04_Application',
    },
    templates_dict={
        'source_sql': '01_04_Application.sql'
    }
)
wipo = SQLTemplatedPythonOperator(
    task_id='WIPO',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '01_05_Wipo',
    },
    templates_dict={
        'source_sql': '01_05_Wipo.sql'
    }
)
patent = SQLTemplatedPythonOperator(
    task_id='Patent',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '02_Patent',
    },
    templates_dict={
        'source_sql': '02_Patent.sql'
    }
)
location = SQLTemplatedPythonOperator(
    task_id='Location',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_01_Location',
    },
    templates_dict={
        'source_sql': '03_01_Location.sql'
    }
)
assignee = SQLTemplatedPythonOperator(
    task_id='Assignee',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_02_Assignee',
    },
    templates_dict={
        'source_sql': '03_02_Assignee.sql'
    }
)
inventor = SQLTemplatedPythonOperator(
    task_id='Inventor',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_03_Inventor',
    },
    templates_dict={
        'source_sql': '03_03_Inventor.sql'
    }
)
lawyer = SQLTemplatedPythonOperator(
    task_id='Lawyer',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_04_Lawyer',
    },
    templates_dict={
        'source_sql': '03_04_Lawyer.sql'
    }
)
examiner = SQLTemplatedPythonOperator(
    task_id='Examiner',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_05_Examiner',
    },
    templates_dict={
        'source_sql': '03_05_Examiner.sql'
    }
)
forprior = SQLTemplatedPythonOperator(
    task_id='Foreign_Priority',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_06_Foreign_Priority',
    },
    templates_dict={
        'source_sql': '03_06_Foreign_Priority.sql'
    }
)
pct = SQLTemplatedPythonOperator(
    task_id='PCT',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_07_PCT',
    },
    templates_dict={
        'source_sql': '03_07_PCT.sql'
    }
)
us_appcit = SQLTemplatedPythonOperator(
    task_id='US_Application_Citation',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_08_US_App_Citation',
    },
    templates_dict={
        'source_sql': '03_08_US_App_Citation.sql'
    }
)
us_patcit = SQLTemplatedPythonOperator(
    task_id='US_Patent_Citation',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_09_US_Patent_Citation',
    },
    templates_dict={
        'source_sql': '03_09_US_Patent_Citation.sql'
    }
)
cpc = SQLTemplatedPythonOperator(
    task_id='CPC',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_10_CPC',
    },
    templates_dict={
        'source_sql': '03_10_CPC.sql'
    }
)
ipcr = SQLTemplatedPythonOperator(
    task_id='ipcr',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_11_IPCR',
    },
    templates_dict={
        'source_sql': '03_11_IPCR.sql'
    }
)
nber = SQLTemplatedPythonOperator(
    task_id='NBER',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_12_Nber',
    },
    templates_dict={
        'source_sql': '03_12_Nber.sql'
    }
)
uspc = SQLTemplatedPythonOperator(
    task_id='USPC',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '03_13_uspc',
    },
    templates_dict={
        'source_sql': '03_13_uspc.sql'
    }
)
assignee_cpc = SQLTemplatedPythonOperator(
    task_id='assignee_cpc',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '04_assignee_cpc',
    },
    templates_dict={
        'source_sql': '04_assignee_cpc.sql'
    }
)
assignee_inventor = SQLTemplatedPythonOperator(
    task_id='assignee_inventor',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '04_assignee_inventor',
    },
    templates_dict={
        'source_sql': '04_assignee_inventor.sql'
    }
)
assignee_nber_uspc = SQLTemplatedPythonOperator(
    task_id='assignee_nber_uspc',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '04_assignee_nber_uspc',
    },
    templates_dict={
        'source_sql': '04_assignee_nber_uspc.sql'
    }
)
inventor_step2 = SQLTemplatedPythonOperator(
    task_id='inventor_step2',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '04_inventor',
    },
    templates_dict={
        'source_sql': '04_inventor.sql'
    }
)
inventor_cpc = SQLTemplatedPythonOperator(
    task_id='inventor_cpc',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '04_inventor_cpc',
    },
    templates_dict={
        'source_sql': '04_inventor_cpc.sql'
    }
)
inventor_nber_uspc = SQLTemplatedPythonOperator(
    task_id='inventor_nber_uspc',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '04_inventor_nber_uspc',
    },
    templates_dict={
        'source_sql': '04_inventor_nber_uspc.sql'
    }
)
location_cpc = SQLTemplatedPythonOperator(
    task_id='location_cpc',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '04_location_cpc',
    },
    templates_dict={
        'source_sql': '04_location_cpc.sql'
    }
)
location_nber_uspc_year = SQLTemplatedPythonOperator(
    task_id='location_nber_uspc_year',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '04_location_nber_uspc_year',
    },
    templates_dict={
        'source_sql': '04_location_nber_uspc_year.sql'
    }
)
cpc_step2 = SQLTemplatedPythonOperator(
    task_id='cpc_step2',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_cpc',
    },
    templates_dict={
        'source_sql': '05_cpc.sql'
    }
)
nber_uspc_step2 = SQLTemplatedPythonOperator(
    task_id='nber_uspc_step2',
    provide_context=True,
    python_callable=validate_query.validate_and_execute,
    dag=reporting_db_dag,
    op_kwargs={
        'filename': '05_nber_uspc',
    },
    templates_dict={
        'source_sql': '05_nber_uspc.sql'
    }
)
reporting_db_qa = PythonOperator(task_id='reporting_DB_QA',
                                          python_callable=run_reporting_db_qa,
                                          dag=reporting_db_dag
                                          )
# MAPPING DEPENDENCY

db_creation.set_upstream(inventor_disambiguation_finished)
db_creation.set_upstream(assignee_disambiguation_finished)

govt_interest.set_upstream(db_creation)
id_mappings.set_upstream(db_creation)
application.set_upstream(db_creation)
wipo.set_upstream(db_creation)
rebuild_patent_lookup.set_upstream(db_creation)

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

uspc.set_upstream(assignee)
uspc.set_upstream(inventor)
cpc.set_upstream(assignee)
cpc.set_upstream(inventor)
nber.set_upstream(assignee)
nber.set_upstream(inventor)

assignee_cpc.set_upstream(assignee)
assignee_cpc.set_upstream(cpc)
assignee_inventor.set_upstream(assignee)
assignee_inventor.set_upstream(inventor)
assignee_nber_uspc.set_upstream(assignee)
assignee_nber_uspc.set_upstream(nber)
assignee_nber_uspc.set_upstream(uspc)

inventor_step2.set_upstream(inventor)
inventor_cpc.set_upstream(inventor)
inventor_cpc.set_upstream(cpc)
inventor_nber_uspc.set_upstream(inventor)
inventor_nber_uspc.set_upstream(nber)
inventor_nber_uspc.set_upstream(uspc)

location_cpc.set_upstream(location)
location_cpc.set_upstream(cpc)
location_nber_uspc_year.set_upstream(nber)
location_nber_uspc_year.set_upstream(uspc)
location_nber_uspc_year.set_upstream(location)

cpc_step2.set_upstream(cpc)
nber_uspc_step2.set_upstream(nber)
nber_uspc_step2.set_upstream(uspc)

# THE END OF EACH STRING OF STEPS
reporting_db_qa.set_upstream(nber_uspc_step2)
reporting_db_qa.set_upstream(cpc_step2)
reporting_db_qa.set_upstream(location_nber_uspc_year)
reporting_db_qa.set_upstream(location_cpc)
reporting_db_qa.set_upstream(inventor_nber_uspc)
reporting_db_qa.set_upstream(inventor_cpc)
reporting_db_qa.set_upstream(inventor_step2)

reporting_db_qa.set_upstream(assignee_cpc)
reporting_db_qa.set_upstream(assignee_inventor)
reporting_db_qa.set_upstream(assignee_nber_uspc)


