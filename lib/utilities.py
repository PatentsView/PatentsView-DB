import calendar
import csv
import datetime
import logging
import multiprocessing as mp
import os
import json
import random
import re
import shutil
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


def with_keys(d, keys):
    return {x: d[x] for x in d if x in keys}


def class_db_specific_config(self, table_config, class_called):
    keep_tables = []
    for i in table_config.keys():
        if class_called in table_config[i]['TestScripts']:
            keep_tables.append(i)
    self.table_config = with_keys(table_config, keep_tables)
    if class_called[:4] == 'Text':
        pass
    else:
        print(f"The following list of tables are run for {class_called}:")
        print(self.table_config.keys())



def get_relevant_attributes(self, class_called, database_section, config):
    if database_section == "patent" or (class_called[:6] == 'Upload' and database_section[:6] == 'upload') or class_called=='GovtInterestTester':
        self.exclusion_list = ['assignee',
                               'cpc_group',
                               'cpc_subgroup',
                               'cpc_subsection',
                               'government_organization',
                               'inventor',
                               'lawyer',
                               'location',
                               'location_assignee',
                               'location_inventor',
                               'location_nber_subcategory',
                               'mainclass',
                               'nber_category',
                               'nber_subcategory',
                               'rawlocation',
                               'subclass',
                               'usapplicationcitation',
                               'uspatentcitation',
                               'wipo_field']
        self.central_entity = 'patent'
        self.category = 'type'
        self.table_config = json.load(open("{}".format(
            self.project_home + "/" + config["FOLDERS"]["resources_folder"] + "/" + config["FILES"][
                "table_config_granted"]), ))
        self.p_key = "id"
        self.f_key = "patent_id"
    elif (database_section == "pregrant_publications") or (
            class_called[:6] == 'Upload' and database_section[:6] == 'pgpubs'):
        # TABLES WITHOUT DOCUMENT_NUMBER ARE EXCLUDED FROM THE TABLE CONFIG
        self.central_entity = "publication"
        self.category = 'kind'
        self.exclusion_list = ['assignee',
                               'clean_rawlocation',
                               'inventor',
                               'location_assignee',
                               'location_inventor',
                               'rawlocation',
                               'rawlocation_geos_missed',
                               'rawlocation_lat_lon']
        self.table_config = json.load(open("{}".format(
            self.project_home + "/" + config["FOLDERS"]["resources_folder"] + "/" + config["FILES"][
                "table_config_pgpubs"]), ))
        self.p_key = "document_number"
        self.f_key = "document_number"
    elif class_called[:4] == 'Text':
        self.category = ""
        self.central_entity = ""
        self.p_key = ""
        self.exclusion_list = []

        if database_section[:6] == 'upload' or database_section == 'patent_text':
            self.table_config = json.load(open("{}".format(
                self.project_home + "/" + config["FOLDERS"]["resources_folder"] + "/" + config["FILES"][
                    "table_config_text_granted"]), ))
        elif database_section[:6] == 'pgpubs' or database_section == 'pgpubs_text':
            self.table_config = json.load(open("{}".format(
                self.project_home + "/" + config["FOLDERS"]["resources_folder"] + "/" + config["FILES"][
                    "table_config_text_pgpubs"]), ))
        else:
            raise NotImplementedError

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
        """
        SELECT CONCAT('ALTER TABLE `', TABLE_NAME, '` ', 'ADD ', IF(NON_UNIQUE = 1, CASE UPPER(INDEX_TYPE)
                                                                                        WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
                                                                                        WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
                                                                                        ELSE CONCAT('INDEX `', INDEX_NAME,
                                                                                            '` USING ', INDEX_TYPE) END,
                                                                    IF(UPPER(INDEX_NAME) = 'PRIMARY',
                                                                       CONCAT('PRIMARY KEY USING ', INDEX_TYPE),
                                                                       CONCAT('UNIQUE INDEX `', INDEX_NAME, '` USING ',
                                                                           INDEX_TYPE))),
                      '(', GROUP_CONCAT(DISTINCT CONCAT('`', COLUMN_NAME, '`') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '),
                      ');') AS 'Show_Add_Indexes'
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = '{db}'
          AND TABLE_NAME = '{table}'
          and UPPER(INDEX_NAME) <> 'PRIMARY'
        GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE, INDEX_TYPE
        ORDER BY TABLE_NAME ASC, INDEX_NAME ASC;
""".format(db=db, table=table))
    add_indexes = add_indexes_fetcher.fetchall()

    drop_indexes_fetcher = engine.execute(
        """
        SELECT CONCAT('ALTER TABLE `', TABLE_NAME, '` ', GROUP_CONCAT(DISTINCT CONCAT('DROP ',
                                                                                      IF(UPPER(INDEX_NAME) = 'PRIMARY',
                                                                                         'PRIMARY KEY',
                                                                                         CONCAT('INDEX `', INDEX_NAME, '`')))
                                                                      SEPARATOR ',
                    '), ';')
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = '{db}'
          AND TABLE_NAME = '{table}'
          and UPPER(INDEX_NAME) <> 'PRIMARY'
        GROUP BY TABLE_NAME
        ORDER BY TABLE_NAME ASC
            """.format(db=db, table=table))
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
            print(t)
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


def manage_ec2_instance(config, button='ON',identifier='xml_collector'):
    instance_id=config['AWS_WORKER'][identifier]
    ec2 = boto3.client('ec2', aws_access_key_id=config['AWS']['ACCESS_KEY_ID'],
                              aws_secret_access_key=config['AWS']['SECRET_KEY'],
                              region_name='us-east-1')
    if button=='ON':
        response=ec2.start_instances(InstanceIds=[instance_id])
    else:
        response=ec2.stop_instances(InstanceIds=[instance_id])
    return response['ResponseMetadata']['HTTPStatusCode']==200

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


def get_host_name(local=True):
    import requests
    from requests.exceptions import ConnectionError
    from airflow.utils import net
    try:
        host_key = 'local-hostname'
        if not local:
            host_key = 'public-hostname'
        r = requests.get("http://169.254.169.254/latest/meta-data/{hkey}".format(hkey=host_key))
        return r.text
    except ConnectionError:
        return net.get_host_ip_address()


def chain_operators(chain):
    for upstream, downstream in zip(chain[:-1], chain[1:]):
        downstream.set_upstream(upstream)


def archive_folder(source_folder, targets: list):
    files = os.listdir(source_folder)
    for target_folder in targets[0:-1]:
        os.makedirs(target_folder, exist_ok=True)
        for file_name in files:
            shutil.copy(os.path.join(source_folder, file_name), target_folder)
    os.makedirs(targets[-1], exist_ok=True)
    for file_name in files:
        shutil.copy(os.path.join(source_folder, file_name), targets[-1])


def link_view_to_new_disambiguation_table(connection, table_name, disambiguation_type):
    g_cursor = connection.cursor()
    index_query = 'alter table {table_name} add primary key (uuid)'.format(
        table_name=table_name)
    replace_view_query = """
        CREATE OR REPLACE VIEW {dtype}_disambiguation_mapping as SELECT uuid,{dtype}_id from {table_name}
        """.format(table_name=table_name, dtype=disambiguation_type)
    g_cursor.execute(index_query)
    g_cursor.execute(replace_view_query)
