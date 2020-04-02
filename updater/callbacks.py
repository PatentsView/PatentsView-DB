from QA.report import get_report_message
from lib.configuration import get_section
from lib.notifications import send_slack_notification
import os


def airflow_task_success(context):
    section = get_section(context['task'].task_id)
    from lib.configuration import get_config
    config = get_config()
    project_home = os.environ['PACKAGE_HOME']
    message = 'AIRFLOW TASK Success:\n' \
              'DAG:    {}\n' \
              'TASKS:  {}\n' \
        .format(context['task_instance'].dag_id,
                context['task_instance'].task_id)
    report_message = get_report_message(context['task'].task_id, config, project_home)
    send_slack_notification(message, config, section=section, level='success')
    send_slack_notification(report_message, config, section=section, level='success')


def airflow_task_failure(context):
    section = get_section(context['task'].task_id)
    from lib.configuration import get_config
    config = get_config()
    message = 'AIRFLOW TASK FAILURE:\n' \
              'DAG:    {}\n' \
              'TASKS:  {}\n' \
              'Reason: {}\n' \
        .format(context['task_instance'].dag_id,
                context['task_instance'].task_id,
                context['exception'])
    send_slack_notification(message, config, section=section, level='error')
