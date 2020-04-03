import datetime
import calendar


def weekday_count(start_date, end_date):
    week = {}
    for i in range((end_date - start_date).days + 1):
        day = calendar.day_name[(start_date + datetime.timedelta(days=i)).weekday()]
        week[day] = week[day] + 1 if day in week else 1
    return week
