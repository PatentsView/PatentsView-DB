from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import math
import os

# ----------- Configuration and Defaults -----------
json_files = [
    "export_view_config_granted",
    "export_text_tables_granted",
    "export_view_config_pregrant",
    "export_text_tables_pregrant"
]
project_home = os.environ['PACKAGE_HOME']
PV_Downloads_dir = os.getenv("PROJECT_HOME", os.path.join(os.getcwd(), "..", "PatentsView-Downloads"))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['contact@patentsview.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'concurrency': 40,
    'queue': 'disambiguator'
}


# Example: a function to check directory contents
def verify_directory_contents(**kwargs):
    project_home = os.getenv("PROJECT_HOME", os.getcwd())  # Default to current directory if PROJECT_HOME is not set
    print(f"This is the project home: {project_home}")
    print(f"Current directory: {os.getcwd()}")

    # Change directory one level up (parent directory)
    config_dir = os.path.join(project_home,"..", "PatentsView-Downloads")  # Explicitly move up one directory
    print(f"Attempting to change into: {config_dir}")

    try:
        os.chdir(config_dir)
        print(f"Successfully changed directory to: {os.getcwd()}")
        print("Directory contents:")
        for f in sorted(os.listdir('.')):
            print(f)
    except Exception as e:
        print(f"Error changing directory: {e}")


def get_relevant_year(**context):
    """
    If the execution_date is Jan/Feb/Mar, use the previous year.
    Otherwise, use the current year.
    """
    execution_date = context['execution_date']
    if execution_date.month in [1, 2, 3]:
        return execution_date.year - 1
    else:
        return execution_date.year


def get_quarter_end_str(**context):
    """
    Returns 'YYYY0331', 'YYYY0630', 'YYYY0930', or 'YYYY1231'
    based on the execution_date's quarter.
    """
    ex = context['execution_date']
    print(f"Execution date: {ex}")
    quarter = math.ceil(ex.month / 3.0)
    quarter_end_days = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
    return f"{ex.year}{quarter_end_days[quarter]}"


def create_copy_json_tasks(json_files, config_dir):
    """
    Creates a list of BashOperator tasks to copy JSON files to _temp.json files.

    :param json_files: List of JSON file names (without .json extension)
    :param config_dir: Directory where JSON files are located
    :return: List of BashOperator tasks
    """
    copy_json_tasks = []
    os.chdir(config_dir)
    for file_name in json_files:
        copy_task = BashOperator(
            task_id=f"copy_{file_name}_json",
            bash_command=f"""
                cp {file_name}.json {file_name}_temp_25.json
            """
        )
        copy_json_tasks.append(copy_task)

    return copy_json_tasks


with DAG(
        dag_id="Bulk_Download_Run_Export_Scripts",
        default_args=default_args,
        # start_date=datetime(2025, 1, 1),
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=False,
        template_searchpath="/project/bulk_download_file_generation/"
) as dag:

    test_change_directory = PythonOperator(
        task_id='verify_directory_contents',
        python_callable=verify_directory_contents
    )

    # 1) Get relevant year
    get_year = PythonOperator(
        task_id='get_year',
        python_callable=get_relevant_year
    )



    # 2) Copy each JSON file to a _temp.json file
    copy_json_tasks = create_copy_json_tasks(json_files, config_dir = os.path.join(PV_Downloads_dir, "config_json"))

    #
    # # 3) Update text_tables files with the relevant year
    # text_table_files = ["export_text_tables_granted", "export_text_tables_pregrant"]
    # update_text_tables_tasks = []
    # for file_name in text_table_files:
    #     update_task = BashOperator(
    #         task_id=f"update_{file_name}_json",
    #         bash_command="""
    #             cd {{ params.config_dir }} && \
    #             sed -i 's/_xxxx/_{{ ti.xcom_pull(task_ids="get_year") }}/g' {{ params.file_name }}_temp.json
    #         """,
    #         params={
    #             'config_dir': config_dir,
    #             'file_name': file_name
    #         }
    #     )
    #     update_text_tables_tasks.append(update_task)
    #
    # 4) Get the "quarter end" date for the -t parameter (YYYYMMDD format)
    get_quarter_end_date = PythonOperator(
        task_id='get_quarter_end_date',
        python_callable=get_quarter_end_str
    )

    test_change_directory >> get_year >> get_quarter_end_date >> copy_json_tasks
    #
    # # 5) Four separate tasks calling generate_bulk_downloads.py
    # generate_granted_text_tables = BashOperator(
    #     task_id="generate_granted_text_tables",
    #     bash_command="""
    #         cd {{ params.config_dir }} && \
    #         python generate_bulk_downloads.py \
    #             -d tab_export_settings.ini \
    #             -c cred.ini \
    #             -t {{ ti.xcom_pull(task_ids='get_quarter_end_date') }} \
    #             -e 1 -o 0 \
    #             -s config_json/export_text_tables_granted_temp.json \
    #             -g patent \
    #             -i 0
    #     """,
    #     params={'config_dir': config_dir},
    # )
    #
    # generate_granted_view_config = BashOperator(
    #     task_id="generate_granted_view_config",
    #     bash_command="""
    #         cd {{ params.config_dir }} && \
    #         python generate_bulk_downloads.py \
    #             -d tab_export_settings.ini \
    #             -c cred.ini \
    #             -t {{ ti.xcom_pull(task_ids='get_quarter_end_date') }} \
    #             -e 1 -o 0 \
    #             -s config_json/export_view_config_granted_temp.json \
    #             -g patent \
    #             -i 0
    #     """,
    #     params={'config_dir': config_dir},
    # )
    #
    # generate_pregrant_text_tables = BashOperator(
    #     task_id="generate_pregrant_text_tables",
    #     bash_command="""
    #         cd {{ params.config_dir }} && \
    #         python generate_bulk_downloads.py \
    #             -d tab_export_settings.ini \
    #             -c cred.ini \
    #             -t {{ ti.xcom_pull(task_ids='get_quarter_end_date') }} \
    #             -e 1 -o 0 \
    #             -s config_json/export_text_tables_pregrant_temp.json \
    #             -g pregrant \
    #             -i 0
    #     """,
    #     params={'config_dir': config_dir},
    # )
    #
    # generate_pregrant_view_config = BashOperator(
    #     task_id="generate_pregrant_view_config",
    #     bash_command="""
    #         cd {{ params.config_dir }} && \
    #         python generate_bulk_downloads.py \
    #             -d tab_export_settings.ini \
    #             -c cred.ini \
    #             -t {{ ti.xcom_pull(task_ids='get_quarter_end_date') }} \
    #             -e 1 -o 0 \
    #             -s config_json/export_view_config_pregrant_temp.json \
    #             -g pregrant \
    #             -i 0
    #     """,
    #     params={'config_dir': config_dir},
    # )
    #
    # # =====================
    # #  LINEAR DEPENDENCIES
    # # =====================
    #
    # # Step 1: get_year
    # # Step 2: copy_json_tasks (one after the other)
    # # Step 3: update_text_tables_tasks (one after the other)
    # # Step 4: get_quarter_end_date
    # # Step 5: four generate tasks, one after another
    #
    # # 1) get_year goes first
    # chain_head = get_year
    #
    # # 2) chain all copy tasks one by one
    # for copy_task in copy_json_tasks:
    #     chain_head >> copy_task
    #     chain_head = copy_task
    #
    # # 3) chain update_text_tables one by one
    # for update_task in update_text_tables_tasks:
    #     chain_head >> update_task
    #     chain_head = update_task
    #
    # # 4) chain get_quarter_end_date
    # chain_head >> get_quarter_end_date
    # chain_head = get_quarter_end_date
    #
    # # 5) chain the four generate tasks in sequence
    # chain_head >> generate_granted_text_tables
    # generate_granted_text_tables >> generate_granted_view_config
    # generate_granted_view_config >> generate_pregrant_text_tables
    # generate_pregrant_text_tables >> generate_pregrant_view_config
