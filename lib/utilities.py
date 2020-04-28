import datetime
import calendar
import random
import re
import string
import requests
from clint.textui import progress
import os
import csv

from sqlalchemy import create_engine

from lib.configuration import get_connection_string


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


def better_title(text):
    title = " ".join(
        [item if item not in ["Of", "The", "For", "And", "On"] else item.lower() for item in str(text).title().split()])
    return re.sub('[' + string.punctuation + ']', '', title)


def write_csv(rows, outputdir, filename):
    """ Write a list of lists to a csv file """
    print(outputdir)
    print(os.path.join(outputdir, filename))
    writer = csv.writer(open(os.path.join(outputdir, filename), 'w', encoding='utf-8'))
    writer.writerows(rows)


def generate_index_statements(config, database_section, table):
    engine = create_engine(get_connection_string(config, database_section))
    db = config["DATABASE"][database_section]
    add_indexes_fetcher = engine.execute(
        "SELECT CONCAT('ALTER TABLE `',TABLE_NAME,'` ','ADD ', IF(NON_UNIQUE = 1, CASE UPPER(INDEX_TYPE) WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX' WHEN 'SPATIAL' THEN 'SPATIAL INDEX' ELSE CONCAT('INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) END, IF(UPPER(INDEX_NAME) = 'PRIMARY', CONCAT('PRIMARY KEY USING ', INDEX_TYPE ), CONCAT('UNIQUE INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) ) ), '(', GROUP_CONCAT( DISTINCT CONCAT('`', COLUMN_NAME, '`') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', ' ), ');' ) AS 'Show_Add_Indexes' FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '" + db + "' AND TABLE_NAME='" + table + "' and UPPER(INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME, INDEX_NAME ORDER BY TABLE_NAME ASC, INDEX_NAME ASC; ")
    add_indexes = add_indexes_fetcher.fetchall()

    drop_indexes_fetcher = engine.execute(
        "SELECT CONCAT( 'ALTER TABLE `', TABLE_NAME, '` ', GROUP_CONCAT( DISTINCT CONCAT( 'DROP ', IF(UPPER(INDEX_NAME) = 'PRIMARY', 'PRIMARY KEY', CONCAT('INDEX `', INDEX_NAME, '`') ) ) SEPARATOR ', ' ), ';' ) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '" + db + "' AND TABLE_NAME='" + table + "' and UPPER(INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME ORDER BY TABLE_NAME ASC")
    drop_indexes = drop_indexes_fetcher.fetchall()
    print(add_indexes)
    print(drop_indexes)

    return add_indexes, drop_indexes
