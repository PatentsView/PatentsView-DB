import calendar
import csv
import datetime
import logging
import multiprocessing as mp
import os
import random
import re
import string
import zipfile
from queue import Queue
from statistics import mean

import boto3
import requests
from bs4 import BeautifulSoup
from clint.textui import progress
from sqlalchemy import create_engine

from lib.configuration import get_connection_string


def xstr(s):
    if s is None:
        return ''
    return str(s)


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


def chunks(l, n):
    '''Yield successive n-sized chunks from l. Useful for multi-processing'''
    chunk_list = []
    for i in range(0, len(l), n):
        chunk_list.append(l[i:i + n])

    return chunk_list


def better_title(text):
    title = " ".join(
            [item if item not in ["Of", "The", "For", "And", "On"] else item.lower() for item in
             str(text).title().split()])
    return re.sub('[' + string.punctuation + ']', '', title)


def write_csv(rows, outputdir, filename):
    """ Write a list of lists to a csv file """
    print(outputdir)
    print(os.path.join(outputdir, filename))
    writer = csv.writer(open(os.path.join(outputdir, filename), 'w', encoding='utf-8'))
    writer.writerows(rows)


def generate_index_statements(config, database_section, table):
    engine = create_engine(get_connection_string(config, database_section))
    db = config["PATENTSVIEW_DATABASES"][database_section]
    add_indexes_fetcher = engine.execute(
            "SELECT CONCAT('ALTER TABLE `',TABLE_NAME,'` ','ADD ', IF(NON_UNIQUE = 1, CASE UPPER(INDEX_TYPE) WHEN "
            "'FULLTEXT' THEN 'FULLTEXT INDEX' WHEN 'SPATIAL' THEN 'SPATIAL INDEX' ELSE CONCAT('INDEX `', INDEX_NAME, "
            "'` USING ', INDEX_TYPE ) END, IF(UPPER(INDEX_NAME) = 'PRIMARY', CONCAT('PRIMARY KEY USING ', INDEX_TYPE "
            "), CONCAT('UNIQUE INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) ) ), '(', GROUP_CONCAT( DISTINCT "
            "CONCAT('`', COLUMN_NAME, '`') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', ' ), ');' ) AS 'Show_Add_Indexes' "
            "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '" + db + "' AND TABLE_NAME='" + table + "' and "
                                                                                                              "UPPER("
                                                                                                              "INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME, INDEX_NAME ORDER BY TABLE_NAME ASC, INDEX_NAME ASC; ")
    add_indexes = add_indexes_fetcher.fetchall()

    drop_indexes_fetcher = engine.execute(
            "SELECT CONCAT( 'ALTER TABLE `', TABLE_NAME, '` ', GROUP_CONCAT( DISTINCT CONCAT( 'DROP ', "
            "IF(UPPER(INDEX_NAME) = 'PRIMARY', 'PRIMARY KEY', CONCAT('INDEX `', INDEX_NAME, '`') ) ) SEPARATOR ', "
            "' ), ';' ) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '" + db + "' AND TABLE_NAME='" +
            table + "' and UPPER(INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME ORDER BY TABLE_NAME ASC")
    drop_indexes = drop_indexes_fetcher.fetchall()
    print(add_indexes)
    print(drop_indexes)

    return add_indexes, drop_indexes


def mp_csv_writer(write_queue, target_file, header):
    with open(target_file, 'w', newline='') as writefile:
        filtered_writer = csv.writer(writefile,
                                     delimiter=',',
                                     quotechar='"',
                                     quoting=csv.QUOTE_NONNUMERIC)
        filtered_writer.writerow(header)
        while 1:
            message_data = write_queue.get()
            if len(message_data) != len(header):
                # "kill" is the special message to stop listening for messages
                if message_data[0] == 'kill':
                    break
                else:
                    print(message_data)
                    raise Exception("Header and data length don't match :{header}/{data_ln}".format(header=len(header),
                                                                                                    data_ln=len(
                                                                                                            message_data)))
            filtered_writer.writerow(message_data)


def log_writer(log_queue, log_prefix="uspto_parser"):
    '''listens for messages on the q, writes to file. '''
    home_folder = os.environ['PACKAGE_HOME']
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    EXPANED_LOGFILE = datetime.datetime.now().strftime(
            '{home_folder}/logs/{prefix}_expanded_log_%Y%m%d_%H%M%S.log'.format(home_folder=home_folder,
                                                                                prefix=log_prefix))
    expanded_filehandler = logging.FileHandler(EXPANED_LOGFILE)
    expanded_filehandler.setLevel(logging.DEBUG)

    BASIC_LOGFILE = datetime.datetime.now().strftime(
            '{home_folder}/logs/{prefix}_log_%Y%m%d_%H%M%S.log'.format(home_folder=home_folder, prefix=log_prefix))
    filehandler = logging.FileHandler(BASIC_LOGFILE)
    filehandler.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    logger.addHandler(expanded_filehandler)
    logger.addHandler(filehandler)
    logger.addHandler(ch)
    while 1:
        message_data = log_queue.get()
        if message_data["message"] == 'kill':
            logger.info("Kill Signal received. Exiting")
            break
        logger.log(message_data["level"], message_data["message"])


def save_zip_file(url, name, path, counter=0, log_queue=None):
    os.makedirs(path, exist_ok=True)
    with requests.get(url, stream=True) as downloader:
        downloader.raise_for_status()
        with open(path + name, 'wb') as f:
            for chunk in downloader.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    with zipfile.ZipFile(path + name, 'r') as zip_ref:
        zip_ref.extractall(path)

    os.remove(path + name)


def download_xml_files(config, xml_template_setting_prefix='pgpubs'):
    xml_template_setting = "{prefix}_bulk_xml_template".format(prefix=xml_template_setting_prefix)
    xml_download_setting = "{prefix}_bulk_xml_location".format(prefix=xml_template_setting_prefix)
    xml_path_template = config["USPTO_LINKS"][xml_template_setting]
    start_date = datetime.datetime.strptime(config['DATES']['START_DATE'], '%Y%m%d')
    end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
    start_year = int(start_date.strftime('%Y'))
    end_year = int(end_date.strftime('%Y'))
    parallelism = int(config["PARALLELISM"]["parallelism"])
    if parallelism > 1:
        manager = mp.Manager()
        log_queue = manager.Queue()
    else:
        log_queue = Queue()
    files_to_download = []

    for year in range(start_year, end_year + 1):
        year_xml_page = xml_path_template.format(year=year)
        print(year_xml_page)
        r = requests.get(year_xml_page)
        soup = BeautifulSoup(r.content, "html.parser")
        links = soup.find_all("a", href=re.compile("[0-9]{6}\.zip"))
        idx_counter = 0
        for link in links:
            href = link.attrs['href']
            href_match = re.match(r".*([0-9]{6})", href)
            if href_match is not None:
                file_date = datetime.datetime.strptime(href_match.group(1), '%y%m%d')
                if end_date >= file_date >= start_date:
                    files_to_download.append(
                            (xml_path_template.format(year=year) + href, href, config["FOLDERS"][xml_download_setting],
                             idx_counter, log_queue))
                idx_counter += 1
    watcher = None
    pool = None
    if parallelism > 1:
        pool = mp.Pool(parallelism)
        watcher = pool.apply_async(log_writer, (log_queue,))

    p_list = []
    idx_counter = 0
    for file_to_download in files_to_download:
        if parallelism > 1:
            p = pool.apply_async(save_zip_file, file_to_download)
            p_list.append(p)
        else:
            save_zip_file(*file_to_download)
        idx_counter += 1
    if parallelism > 1:
        idx_counter = 0
        for t in p_list:
            t.get()

    idx_counter += 1
    log_queue.put({
            "level":   None,
            "message": "kill"
            })
    if parallelism > 1:
        watcher.get()
        pool.close()
        pool.join()


def rds_free_space(config, identifier):
    cloudwatch = boto3.client('cloudwatch', aws_access_key_id=config['AWS']['ACCESS_KEY_ID'],
                              aws_secret_access_key=config['AWS']['SECRET_KEY'],
                              region_name='us-east-1')

    from datetime import datetime, timedelta
    response = cloudwatch.get_metric_data(
            MetricDataQueries=[
                    {
                            'Id':         'fetching_FreeStorageSpace',
                            'MetricStat': {
                                    'Metric': {
                                            'Namespace':  'AWS/RDS',
                                            'MetricName': 'FreeStorageSpace',
                                            'Dimensions': [
                                                    {
                                                            "Name":  "DBInstanceIdentifier",
                                                            "Value": identifier
                                                            }
                                                    ]
                                            },
                                    'Period': 300,
                                    'Stat':   'Minimum'
                                    }
                            }
                    ],
            StartTime=(datetime.now() - timedelta(seconds=300 * 3)).timestamp(),
            EndTime=datetime.now().timestamp(),
            ScanBy='TimestampDescending'
            )
    return mean(response['MetricDataResults'][0]['Values'])
