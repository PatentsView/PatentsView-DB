from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain, cross_downstream
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import math
import os

# ----------- Configuration and Defaults -----------
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

json_files = {
    "copy_export_view_config_granted_json": "export_view_config_granted",
    "copy_export_view_config_pregrant_json": "export_view_config_pregrant",
    "copy_export_text_tables_granted_json": "export_text_tables_granted",
    "copy_export_text_tables_pregrant_json": "export_text_tables_pregrant",
}

text_table_files = {
    "update_copy_export_text_tables_granted_json": "export_text_tables_granted",
    "update_copy_export_text_tables_pregrant_json": "export_text_tables_pregrant",
}

project_home = os.environ['PACKAGE_HOME']
PV_Downloads_dir = os.getenv("PROJECT_HOME", os.path.join(os.getcwd(), "..", "PatentsView-Downloads"))


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
    execution_date = context['ds']
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
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
    Creates a dictionary of BashOperator tasks with their actual task names.

    :param json_files: Dictionary where keys are task IDs and values are JSON file names.
    :param config_dir: Directory where JSON files are located.
    :return: Dictionary of task_id -> BashOperator task
    """
    copy_json_tasks = {}
    for task_id, file_name in json_files.items():
        copy_task = BashOperator(
            task_id=task_id,
            bash_command=f"""
                cd {config_dir} && \
                cp {file_name}.json {file_name}_temp_25.json
            """
        )
        copy_json_tasks[task_id] = copy_task
    return copy_json_tasks

def create_update_text_table_tasks(text_table_files, config_dir):
    """
    Creates a dictionary of BashOperator tasks to update text table JSON files.

    :param text_table_files: Dictionary mapping task IDs to JSON file names.
    :param config_dir: Directory where JSON files are located.
    :return: Dictionary of {task_id: BashOperator task}
    """
    update_text_tables_tasks = {}  # Store tasks in a dictionary
    year = '2025'
    print(f"This is the year: {year}")

    for task_id, file_name in text_table_files.items():  # Use keys as task IDs
        update_task = BashOperator(
            task_id=task_id,  # Use the existing key as the task_id
            bash_command=f"""
                cd {config_dir} && \
                sed -i 's/_xxxx/_{year}/g' {file_name}_temp_25.json
            """,
            params={
                'config_dir': config_dir,
                'file_name': file_name
            }
        )
        update_text_tables_tasks[task_id] = update_task  # Store task in dictionary

    return update_text_tables_tasks  # ✅ Correct return type



view_config_files = {
    "update_copy_export_view_config_granted_json": "export_view_config_granted.json",
    "update_copy_export_view_config_pregrant_json": "export_view_config_pregrant.json",
}

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def create_update_view_config_tasks(view_config_files, config_dir):
    """
    Creates a dictionary of BashOperator tasks to update text table JSON files.

    :param view_config_files: Dictionary mapping task IDs to JSON file names.
    :param config_dir: Directory where JSON files are located.
    :return: Dictionary of {task_id: BashOperator task}
    """
    update_view_config_tasks = {}  # Store tasks in a dictionary
    quarter_end_date = '20250331'
    previous_quarter_end_date = '20241231'
    print(f"this is the config dir: {config_dir}")

    # Loop through the view_config_files to create tasks for each file
    for task_id, file_name in view_config_files.items():
        # Construct the bash script to be executed
        bash_command = f"""
        # Define the quarter end dates
        previous_quarter_end_date="20241231"
        quarter_end_date="20250331"

        # Loop through both identifiers
        for prefix in "disamb_assignee_id_" "disamb_inventor_id_"; do
            # Construct the new entry to be added
            new_entry="\"${{prefix}}${{quarter_end_date}}\""

            # Check if the new entry already exists in the file
            if ! grep -q "$new_entry" "{config_dir}/{file_name}"; then
                # Find the line containing the previous quarter end date (e.g., 20241231)
                last_quarter_entry=$(grep -n "\"${{prefix}}${{previous_quarter_end_date}}\"" "{config_dir}/{file_name}")

                if [ ! -z "$last_quarter_entry" ]; then
                    # Extract the line number of the last quarter entry
                    last_quarter_line_number=$(echo $last_quarter_entry | cut -d: -f1)

                    # Insert a comma and new entry on a new line, properly indented
                    sed -i "${{last_quarter_line_number}}s/\\([[:space:]]*\\)$/,\\
                    $new_entry/" "{config_dir}/{file_name}"
                fi
            fi
        done
        """

        # Add the BashOperator task to the dictionary
        update_view_config_tasks[task_id] = BashOperator(
            task_id=task_id,
            bash_command=bash_command
        )

    return update_view_config_tasks



with DAG(
        dag_id="Bulk_Download_Run_Export_Scripts",
        default_args=default_args,
        # start_date=datetime(2025, 1, 1),
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=False,
        template_searchpath="/project/bulk_download_file_generation/"
) as dag:

    # 0) Verify directory contents
    test_change_directory = PythonOperator(
        task_id='verify_directory_contents',
        python_callable=verify_directory_contents
    )

    # 1) Get relevant year
    get_year = PythonOperator(
        task_id='get_year',
        python_callable=get_relevant_year,
        provide_context=True  # Ensure it gets the execution context
    )

    # 2) Copy each JSON file to a _temp.json file
    copy_json_tasks = create_copy_json_tasks(json_files, config_dir = os.path.join(PV_Downloads_dir, "config_json"))


    # 4) Get the "quarter end" date for the -t parameter (YYYYMMDD format)
    get_quarter_end_date = PythonOperator(
        task_id='get_quarter_end_date',
        python_callable=get_quarter_end_str
    )

    # Generate update text table tasks
    config_dir = os.path.join(PV_Downloads_dir, "config_json")
    update_text_tables = create_update_text_table_tasks(text_table_files, config_dir)
    update_view_config = create_update_view_config_tasks(view_config_files, config_dir)

    test_change_directory >> get_year >> get_quarter_end_date

    get_quarter_end_date >> copy_json_tasks["copy_export_view_config_granted_json"] >> update_view_config["update_copy_export_view_config_granted_json"]
    get_quarter_end_date >> copy_json_tasks["copy_export_view_config_pregrant_json"] >> update_view_config["update_copy_export_view_config_pregrant_json"]
    get_quarter_end_date >> copy_json_tasks["copy_export_text_tables_granted_json"] >> update_text_tables["update_copy_export_text_tables_granted_json"]
    get_quarter_end_date >> copy_json_tasks["copy_export_text_tables_pregrant_json"] >> update_text_tables["update_copy_export_text_tables_pregrant_json"]

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
