import os
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['contact@patentsview.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 40,
    'queue': 'data_collector'
}

bulk_download_file_generation_dag = DAG(
    dag_id="Bulk_Download_File_Generation",
    default_args=default_args,
    start_date=datetime(2025, 3, 31),
    schedule_interval='@quarterly'
)

CONFIG_DIR = "PatentsView-Downloads/config_json/"
NEW_QUARTER = "disamb_inventor_id_20250331"

GRANTED_CONFIG = os.path.join(CONFIG_DIR, "export_view_config_granted.json")
PREGANT_CONFIG = os.path.join(CONFIG_DIR, "export_view_config_pregrant.json")


def update_quarter_column(config_path, identifier):
    with open(config_path, "r") as f:
        data = json.load(f)

    for table in data:
        if "fields" in data[table]:
            last_quarter_columns = [col for col in data[table]["fields"] if identifier in col]
            if last_quarter_columns:
                data[table]["fields"].append(NEW_QUARTER)

    with open(config_path, "w") as f:
        json.dump(data, f, indent=4)


def process_granted():
    update_quarter_column(GRANTED_CONFIG, "disamb_inventor_id_")


def process_pregrant():
    update_quarter_column(PREGANT_CONFIG, "disamb_assignee_id_")


with bulk_download_file_generation_dag as dag:
    update_granted = PythonOperator(
        task_id="update_granted_config",
        python_callable=process_granted
    )

    update_pregrant = PythonOperator(
        task_id="update_pregrant_config",
        python_callable=process_pregrant
    )

update_pregrant.set_upstream(update_granted)
