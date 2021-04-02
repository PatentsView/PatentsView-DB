from QA.reports import get_report_message
from lib.configuration import get_section
from lib.notifications import send_slack_notification


def airflow_task_success(context):
    header = get_section(context['task_instance'].dag_id, context['task'].task_id)
    section = "{header} - {dt}".format(header=header, dt=context['task_instance'].execution_date.strftime("%Y-%m-%d"))
    from lib.configuration import get_config
    config = get_config()
    message = 'AIRFLOW TASK Success:\n' \
              'DAG:    {dag_id}\n' \
              'TASKS:  {task_id}\n' \
        .format(dag_id=context['task_instance'].dag_id, task_id=context['task_instance'].task_id,
                duration=context['task_instance'].duration)
    # report_message = get_report_message(context['task'].task_id, config)
    # send_slack_notification(report_message, config, section=section, level='success')
    send_slack_notification(message, config, section=section, level='success')


def airflow_task_failure(context):
    header = get_section(context['task_instance'].dag_id, context['task'].task_id)
    section = "{header} - {dt}".format(header=header, dt=context['task_instance'].execution_date.strftime("%Y-%m-%d"))
    from lib.configuration import get_config
    config = get_config()
    message = 'AIRFLOW TASK FAILURE:\n' \
              'DAG:    {dag_id}\n' \
              'TASKS:  {task_id}\n' \
              'Duration:  {duration}\n' \
              'Reason: {exception}\n' \
        .format(dag_id=context['task_instance'].dag_id, task_id=context['task_instance'].task_id,
                duration=context['task_instance'].duration, exception=context['exception'])
    send_slack_notification(message, config, section=section, level='error')


def airflow_daily_check_success(context):
    header = get_section(context['task_instance'].dag_id, context['task'].task_id)
    section = "{header} - {dt}".format(header=header, dt=context['task_instance'].execution_date.strftime("%Y-%m-%d"))
    from lib.configuration import get_config
    config = get_config()
    message = 'AIRFLOW TASK Success:\n' \
              'DAG:    {dag_id}\n' \
              'TASKS:  {task_id}\n' \
              'Duration:  {duration}\n' \
        .format(dag_id=context['task_instance'].dag_id, task_id=context['task_instance'].task_id,
                duration=context['task_instance'].duration)
    report_message = get_report_message(context['task'].task_id, config)
    config["SLACK"]["CHANNEL"] = "pv_server_status"
    send_slack_notification(report_message, config, section=section, level='success')
    send_slack_notification(message, config, section=section, level='success')


def airflow_daily_check_failure(context):
    header = get_section(context['task_instance'].dag_id, context['task'].task_id)
    section = "{header} - {dt}".format(header=header, dt=context['task_instance'].execution_date.strftime("%Y-%m-%d"))
    from lib.configuration import get_config
    config = get_config()
    message = 'AIRFLOW TASK FAILURE:\n' \
              'DAG:    {dag_id}\n' \
              'TASKS:  {task_id}\n' \
              'Duration:  {duration}\n' \
              'Reason: {exception}\n' \
        .format(dag_id=context['task_instance'].dag_id, task_id=context['task_instance'].task_id,
                duration=context['task_instance'].duration, exception=context['exception'])
    config["SLACK"]["CHANNEL"] = "pv_server_status"
    send_slack_notification(message, config, section=section, level='error')
