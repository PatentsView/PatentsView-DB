import calendar
from datetime import datetime, timedelta, date

import pandas as pd
import pendulum
from numpy import busday_count
from pendulum import DateTime

from lib.configuration import get_current_config

local_tz = pendulum.timezone("America/New_York")


def previous_quarter(ref):
    if ref.month < 4:
        return date(ref.year - 1, 12, 31)
    elif ref.month < 7:
        return date(ref.year, 3, 31)
    elif ref.month < 10:
        return date(ref.year, 6, 30)
    return date(ref.year, 9, 30)


def next_quarter(ref):
    if ref.month < 4:
        return date(ref.year, 6, 30)
    elif ref.month < 7:
        return date(ref.year, 9, 30)
    elif ref.month < 10:
        return date(ref.year, 12, 31)
    return date(ref.year, 3, 31)


def get_most_recent_x_day(ref, x=calendar.TUESDAY):
    offset = (ref.weekday() - x) % 7
    last_x_day = ref - timedelta(days=offset)
    return last_x_day


def get_update_x_day(ref):
    latest_tuesday = get_most_recent_x_day(ref, calendar.TUESDAY)
    latest_thursday = get_most_recent_x_day(ref, calendar.THURSDAY)
    return max(latest_tuesday, latest_thursday)


def get_update_range(ref: DateTime):
    exec_date = ref
    quarter_latest_x_day = get_update_x_day((exec_date + pd.tseries.offsets.QuarterEnd(n=0)).date())
    # if exec_date < quarter_latest_x_day:
    #     quarter_latest_x_day = get_update_x_day(previous_quarter(exec_date))
    previous_quarter_latest_x_day = get_update_x_day(previous_quarter(quarter_latest_x_day))
    return previous_quarter_latest_x_day + timedelta(days=1), quarter_latest_x_day


#
# class DataUpdateTimeTable(Timetable):
#     def __init__(self, pipeline='granted_patent'):
#         self._pipeline = pipeline
#
#     def serialize(self) -> Dict[str, Any]:
#         return {'pipeline': self._pipeline}
#
#     @classmethod
#     def deserialize(cls, data: Dict[str, Any]) -> "Timetable":
#         return cls(data['pipeline'])
#
#     def next_dagrun_info(self, *, last_automated_data_interval: Optional[DataInterval], restriction: TimeRestriction) -> \
#             Optional[DagRunInfo]:
#
#         if last_automated_data_interval is not None:
#             start_date = last_automated_data_interval.end.date() + timedelta(days=1)
#             end_date = get_update_x_day(next_quarter(start_date))
#         else:
#             start_date = restriction.earliest
#             if start_date is None:
#                 return None
#
#     def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
#         previous_update_date, update_date = get_update_range(run_after.date())
#         start: DateTime = DateTime.combine(previous_update_date,
#                                            Time(hour=9, minute=0, second=0, microsecond=0)).replace(
#             tzinfo=local_tz)
#         end: DateTime = DateTime.combine(update_date, Time(hour=9, minute=0, second=0, microsecond=0)).replace(
#             tzinfo=local_tz)
#         return DataInterval(start=start, end=end)
#
#
# class DataUpdateTimeTablePlugin(AirflowPlugin):
#     name = 'data_update_timetable_plugin'
#     timetables = [DataUpdateTimeTable]


def is_it_last_xday_of_third_month(end_date, atmost=2):
    if end_date.month % 3 == 0:
        month, lastday = calendar.monthrange(end_date.year, end_date.month)
        end_of_month = datetime(year=end_date.year, month=end_date.month, day=lastday)
        days_left_in_month = (end_of_month - end_date).days
        if days_left_in_month < min(atmost, 7):
            return True


def determine_update_eligibility(**context):
    execution_date = context.get('execution_date', datetime.today())
    execution_date = execution_date.date()
    if execution_date.month in [3, 6, 9, 12]:
        month, lastday = calendar.monthrange(execution_date.year, execution_date.month)
        end_of_month = datetime(year=execution_date.year, month=execution_date.month, day=lastday).date()
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
    end_date = datetime.strptime(configuration['DATES']['END_DATE'], '%Y%m%d')
    if is_it_last_xday_of_third_month(end_date, atmost=5):
        trigger_bulk_processing(end_date)


def trigger_bulk_processing(execution_date):
    from airflow.api.client.local_client import trigger_dag
    trigger_dag.trigger_dag(dag_id='supplemental_data_collection', execution_date=execution_date)


if __name__ == '__main__':
    pass
