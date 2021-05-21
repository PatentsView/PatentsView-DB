import calendar
import datetime

from numpy import busday_count

from lib.configuration import get_current_config


def is_it_last_xday_of_third_month(end_date, atmost=2):
    if end_date.month % 3 == 0:
        month, lastday = calendar.monthrange(end_date.year, end_date.month)
        end_of_month = datetime.datetime(year=end_date.year, month=end_date.month, day=lastday)
        days_left_in_month = (end_of_month - end_date).days
        if days_left_in_month < min(atmost, 7):
            return True


def determine_update_eligibility(**context):
    execution_date = context.get('execution_date', datetime.datetime.today())
    execution_date = execution_date.date()
    if execution_date.month in [3, 6, 9, 12]:
        month, lastday = calendar.monthrange(execution_date.year, execution_date.month)
        end_of_month = datetime.datetime(year=execution_date.year, month=execution_date.month, day=lastday).date()
        if busday_count(execution_date, end_of_month, weekmask='Tue') > 1 and busday_count(execution_date, end_of_month,
                                                                                           weekmask='Thu') > 1:
            return False
        else:
            return True


def determine_granted_sensor_date(execution_date):
    from lib.configuration import get_today_dict
    kwrgs = get_today_dict(type='granted_patent', from_date=execution_date)
    return kwrgs['execution_date']


def determine_pregranted_sensor_date(execution_date):
    from lib.configuration import get_today_dict
    kwrgs = get_today_dict(type='pgpubs', from_date=execution_date)
    return kwrgs['execution_date']


def determine_update_eligibility_pregrant(**kwargs):
    configuration = get_current_config(type='pgpubs', **kwargs)
    end_date = datetime.datetime.strptime(configuration['DATES']['END_DATE'], '%Y%m%d')
    if is_it_last_xday_of_third_month(end_date, atmost=5):
        trigger_bulk_processing(end_date)


def trigger_bulk_processing(execution_date):
    from airflow.api.client.local_client import trigger_dag
    trigger_dag.trigger_dag(dag_id='supplemental_data_collection', execution_date=execution_date)


if __name__ == '__main__':
    pass
