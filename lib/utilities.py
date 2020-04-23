import datetime
import calendar
import random
import string
import requests
from clint.textui import progress
import os
import csv


def weekday_count(start_date, end_date):
    week = {}
    for i in range((end_date - start_date).days + 1):
        day = calendar.day_name[(start_date + datetime.timedelta(days=i)).weekday()]
        week[day] = week[day] + 1 if day in week else 1
    return week


def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def download(url, filepath):
    """ Download data from a URL with a handy progress bar """

    print("Downloading: {}".format(url))
    r = requests.get(url, stream=True)

    with open(filepath, 'wb') as f:
        content_length = int(r.headers.get('content-length'))
        for chunk in progress.bar(r.iter_content(chunk_size=1024),
                                  expected_size=(content_length / 1024) + 1):
            if chunk:
                f.write(chunk)
                f.flush()


def write_csv(rows, outputdir, filename):
    """ Write a list of lists to a csv file """
    print(outputdir)
    print(os.path.join(outputdir, filename))
    writer = csv.writer(open(os.path.join(outputdir, filename), 'w', encoding='utf-8'))
    writer.writerows(rows)
