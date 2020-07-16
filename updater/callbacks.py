from QA.reports import get_report_message
from lib.configuration import get_section
from lib.notifications import send_slack_notification


def airflow_task_success(context):
    section = get_section(context['task'].task_id)
    dag_id = context['dag'].dag_id
    from lib.configuration import get_config
    if dag_id in ['01_update_granted_patent', '02_gi_post_manual', '03_disambiguation_support',
                  '04_disambiguation_postprocessing']:
        type = 'granted_patent'
    else:
        type = 'application'
    config = get_config(type)
    message = 'AIRFLOW TASK Success:\n' \
              'DAG:    {dag_id}\n' \
              'TASKS:  {task_id}\n' \
              'Duration:  {duration}\n' \
        .format(dag_id=context['task_instance'].dag_id, task_id=context['task_instance'].task_id,
                duration=context['task_instance'].duration)
    report_message = get_report_message(context['task'].task_id, config)
    send_slack_notification(report_message, config, section=section, level='success')
    send_slack_notification(message, config, section=section, level='success')


def airflow_task_failure(context):
    section = get_section(context['task'].task_id)
    from lib.configuration import get_config
    dag_id = context['dag'].dag_id
    if dag_id in ['01_update_granted_patent', '02_gi_post_manual', '03_disambiguation_support',
                  '04_disambiguation_postprocessing']:
        type = 'granted_patent'
    else:
        type = 'application'
    config = get_config(type)
    message = 'AIRFLOW TASK FAILURE:\n' \
              'DAG:    {dag_id}\n' \
              'TASKS:  {task_id}\n' \
              'Duration:  {duration}\n' \
              'Reason: {exception}\n' \
        .format(dag_id=context['task_instance'].dag_id, task_id=context['task_instance'].task_id,
                duration=context['task_instance'].duration, exception=context['exception'])
    send_slack_notification(message, config, section=section, level='error')
