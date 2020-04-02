import datetime
import calendar


def weekday_count(start_date, end_date):
    week = {}
    for i in range((end_date - start_date).days):
        day = calendar.day_name[(start_date + datetime.timedelta(days=i + 1)).weekday()]
        week[day] = week[day] + 1 if day in week else 1
    return week
