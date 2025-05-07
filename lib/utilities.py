import calendar
import csv
import datetime
import logging
import billiard as mp
import os
import json
import random
import re
import shutil
import string
import zipfile
from queue import Queue
from statistics import mean
import pandas as pd
from time import time
import boto3
import requests
from bs4 import BeautifulSoup
from clint.textui import progress
from sqlalchemy import create_engine
from lib.xml_helpers import process_date
from lib.notifications import send_slack_notification
from lib.configuration import get_connection_string, get_current_config


def with_keys(d, keys):
    return {x: d[x] for x in d if x in keys}


def class_db_specific_config(self, table_config, class_called):
    keep_tables = []
    for i in table_config.keys():
        if class_called == 'DatabaseTester':
            if "UploadTest" in table_config[i]['TestScripts']:
                keep_tables.append(i)
        elif class_called == 'ElasticDBTester':
            keep_tables.append(i)
        else:
            if class_called in table_config[i]['TestScripts']:
                keep_tables.append(i)
    self.table_config = with_keys(table_config, keep_tables)
    if class_called[:4] == 'Text':
        pass
    else:
        if "PostProcessing" in str(self):
            tables_list = list(self.table_config.keys())
            quarter_date = self.end_date.strftime("%Y%m%d")
            for table in tables_list:
                if table in ['assignee', "assignee_disambiguation_mapping", 'location', "location_disambiguation_mapping", 'inventor',  "inventor_disambiguation_mapping", "inventor_gender", "rawinventor_gender", "rawinventor_gender_agg"]:
                    self.table_config[f'{table}_{quarter_date}'] = self.table_config.pop(f'{table}')
        print(f"The following list of tables are run for {class_called}:")
        print(self.table_config.keys())


def load_table_config(config, db='patent'):
    print(db)
    print(config["PATENTSVIEW_DATABASES"]["REPORTING_DATABASE"])
    root = config["FOLDERS"]["project_root"]
    resources = config["FOLDERS"]["resources_folder"]
    if db == 'patent':
        config_file = f"{root}/{resources}/{config['FILES']['table_config_granted']}"
    elif db == 'pgpubs':
        config_file = f"{root}/{resources}/{config['FILES']['table_config_pgpubs']}"
    elif db == 'patent_text' or db[:6] == 'upload':
        config_file = f'{root}/{resources}/{config["FILES"]["table_config_text_granted"]}'
    elif db == 'pgpubs_text' or db[:6] == 'pgpubs':
        config_file = f'{root}/{resources}/{config["FILES"]["table_config_text_pgpubs"]}'
    elif db == config["PATENTSVIEW_DATABASES"]["REPORTING_DATABASE"]:
        config_file = f'{root}/{resources}/{config["FILES"]["table_config_reporting_db"]}'
    elif db == "gender_attribution":
        config_file = f'{root}/{resources}/{config["FILES"]["table_config_inventor_gender"]}'
    elif db == 'bulk_exp_granted':
        config_file = f'{root}/{resources}/{config["FILES"]["table_config_bulk_exp_granted"]}'
    elif db == 'bulk_exp_pgpubs':
        config_file = f'{root}/{resources}/{config["FILES"]["table_config_bulk_exp_pgpubs"]}'
    elif db == 'elasticsearch_patent':
        config_file = f'{root}/{resources}/{config["FILES"]["table_config_elasticsearch_patent"]}'
    elif db == 'elasticsearch_pgpub':
        config_file = f'{root}/{resources}/{config["FILES"]["table_config_elasticsearch_pgpub"]}'

    print(f"reading table config from {config_file}")
    with open(config_file) as file:
        table_config = json.load(file)
    return table_config


def get_relevant_attributes(self, class_called, database_section, config):
    print(f"assigning class variables based on class {class_called} and database section {database_section}.")
    if (class_called == "AssigneePostProcessingQC") or (class_called == "AssigneePostProcessingQCPhase2") :
        self.database_section = database_section
        if self.database_section == 'patent':
            self.table_config = load_table_config(config, db='patent')
        else:
            self.table_config = load_table_config(config, db='pgpubs')
        self.entity_table = 'rawassignee'
        self.entity_id = 'uuid'
        self.disambiguated_id = 'assignee_id'
        self.disambiguated_table = 'assignee_'+ config['DATES']["END_DATE"]
        self.disambiguated_data_fields = ['name_last', 'name_first', 'organization']
        self.aggregator = 'main.organization'
        self.category = ""
        self.central_entity = ""
        self.p_key = ""
        self.f_key = ""
        self.exclusion_list = []
    elif (class_called == "InventorGenderPostProcessingQC"):
        self.table_config = load_table_config(config, db='gender_attribution')

    elif (class_called == "InventorPostProcessingQC") or (class_called == "InventorPostProcessingQCPhase2") :
        self.database_section = database_section
        if self.database_section == 'patent':
            self.table_config = load_table_config(config, db='patent')
        else:
            self.table_config = load_table_config(config, db='pgpubs')
        self.entity_table = 'rawinventor'
        self.entity_id = 'uuid'
        self.disambiguated_id = 'inventor_id'
        self.disambiguated_table = 'inventor'
        self.disambiguated_data_fields = ['name_last', 'name_first', 'organization']
        # self.patent_exclusion_list.extend(['assignee', 'persistent_assignee_disambig'])
        # self.add_persistent_table_to_config(database_section)
        self.category = ""
        self.aggregator = "concat(main.name_last, ', ', main.name_first)"
        self.category = ""
        self.central_entity = ""
        self.p_key = ""
        self.f_key = ""
        self.exclusion_list = []

    elif class_called == "LawyerPostProcessingQC":
        self.database_section = database_section
        self.table_config = load_table_config(config, db='patent')
        self.entity_table = 'rawlawyer'
        self.entity_id = 'uuid'
        self.disambiguated_id = 'lawyer_id'
        self.disambiguated_table = 'lawyer'
        self.disambiguated_data_fields = ['name_last', 'name_first', "organization", "country"]
        self.aggregator = 'case when main.organization is null then concat(main.name_last,", ",main.name_first) else main.organization end'
        self.disambiguated_data_fields = ['name_last', 'name_first', "organization", "country"]
        self.category = ""
        self.central_entity = ""
        self.p_key = ""
        self.f_key = ""
        self.exclusion_list = []

    elif class_called == "LocationPostProcessingQC":
        self.table_config = load_table_config(config, db='patent')
        self.disambiguated_data_fields = ['city', 'state', 'country']
        self.aggregator = "concat(main.city, ', ', main.state, ',', main.country)"
        self.entity_table = 'rawlocation'
        self.entity_id = 'id'
        self.disambiguated_id = 'location_id'
        self.disambiguated_table = 'location'
        self.category = ""
        self.central_entity = ""
        self.p_key = ""
        self.f_key = ""
        self.exclusion_list = []

    elif class_called == "CPCTest":
        self.table_config = load_table_config(config, db='patent')
        self.category = ""
        self.central_entity = ""
        self.p_key = ""
        self.f_key = ""
        self.exclusion_list = []

    elif class_called == "ReportingDBTester" or class_called == "ProdDBTester":
        self.table_config = load_table_config(config, db = config["PATENTSVIEW_DATABASES"]["REPORTING_DATABASE"]) #db should be parameterized later, not hard-coded
        self.category = ""
        self.central_entity = ""
        self.p_key = ""
        self.f_key = ""
        self.exclusion_list = []

    elif class_called == "ElasticDBTester":
        self.table_config = load_table_config(config, db = config['PATENTSVIEW_DATABASES']["ELASTICSEARCH_DB_TYPE"]) #db should be parameterized later, not hard-coded
        self.category = ""
        self.central_entity = ""
        self.p_key = ""
        self.f_key = ""
        self.exclusion_list = []

    elif database_section == "patent" or (
            database_section[:6] == 'upload' and class_called[:6] in ('Upload','GovtIn', 'MergeT')):
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
        self.table_config = load_table_config(config, db='patent')
        self.p_key = "id"
        self.f_key = "patent_id"

    elif (database_section == "pregrant_publications") or (
            database_section[:6] == 'pgpubs' and class_called[:6] in ('Upload','GovtIn', 'MergeT')):
        # TABLES WITHOUT DOCUMENT_NUMBER ARE EXCLUDED FROM THE TABLE CONFIG
        self.central_entity = "publication"
        self.category = 'kind'
        self.exclusion_list = ['assignee',
                               'clean_rawlocation',
                               'government_organization',
                               'inventor',
                               'location_assignee',
                               'location_inventor',
                               'rawlocation',
                               'rawlocation_geos_missed',
                               'rawlocation_lat_lon']
        self.table_config = load_table_config(config, db='pgpubs')
        self.p_key = "document_number"
        self.f_key = "document_number"

    elif class_called[:4] == 'Text':
        self.category = ""
        self.central_entity = ""
        self.p_key = ""
        self.f_key = ""
        self.exclusion_list = []
        if database_section[:6] == 'upload' or database_section == 'patent_text':
            self.table_config = load_table_config(config, db=database_section)
        elif database_section[:6] == 'pgpubs' or database_section == 'pgpubs_text':
            self.table_config = load_table_config(config, db=database_section)
        else:
            raise NotImplementedError

    elif class_called[:19] == 'BulkDownloadsTester':
        if 'granted' in database_section:
            self.table_config = load_table_config(config, db='bulk_exp_granted')
            self.central_entity = "patent"
            self.p_key = "patent_id"
            self.f_key = "patent_id"
        else:
            self.table_config = load_table_config(config, db='bulk_exp_pgpubs')
            self.central_entity = "publication"
            self.p_key = "pgpub_id"
            self.f_key = "pgpub_id"
        
        self.category = ""
        self.exclusion_list = []

    else:
        raise NotImplementedError

def update_to_granular_version_indicator(table, db):
    from lib.configuration import get_current_config, get_connection_string
    config = get_current_config(type=db, **{"execution_date": datetime.date(2000, 1, 1)})
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    if db == 'granted_patent':
        id = 'id'
        fk = 'patent_id'
        fact_table = 'patent'
    else:
        id = 'document_number'
        fk = 'document_number'
        fact_table = 'publications'
    query = f"""
update {table} update_table 
	inner join {fact_table} p on update_table.{fk}=p.{id}
set update_table.version_indicator=p.version_indicator     
    """
    print(query)
    query_start_time = time()
    engine.execute(query)
    query_end_time = time()
    print("This query took:", query_end_time - query_start_time, "seconds")

# Moved from AssigneePostProcessing - unused for now
def add_persistent_table_to_config(self, database_section):
    columns_query = f"""
    select COLUMN_NAME,
        case when DATA_TYPE in ('varchar', 'tinytext', 'text', 'mediumtext', 'longtext') then 'varchar' else 'int' end,                                           data_type,
        case when COLUMN_KEY = 'PRI' then 'False' else 'True' end as null_allowed, 
        'False' as category,    
    from information_schema.COLUMNS
    where TABLE_NAME = 'persistent_assignee_disambig'
      and TABLE_SCHEMA = '{database_section}'
      and column_name not in ('updated_date','created_date', 'version_indicator');
    """
    if not self.connection.open:
        self.connection.connect()
    with self.connection.cursor() as crsr:
        crsr.execute(columns_query)
        column_data = pd.DataFrame.from_records(
            crsr.fetchall(),
            columns=['column', 'data_type', 'null_allowed', 'category'])
        table_config = {
            'persistent_assignee_disambig': {
                'fields': column_data.set_index('column').to_dict(orient='index')
            }
        }
        self.table_config.update(table_config)


def trim_whitespace(config):
    from lib.configuration import get_connection_string
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    db_type = config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"][:6]
    engine = create_engine(cstr)
    print("REMOVING WHITESPACE WHERE IT EXISTS")
    project_home = os.environ['PACKAGE_HOME']
    resources_file = "{root}/{resources}/columns_for_whitespace_trim.json".format(root=project_home,
                                                                                  resources=config["FOLDERS"][
                                                                                      "resources_folder"])
    cols_tables_whitespace = json.load(open(resources_file))
    for table in cols_tables_whitespace.keys():
        if db_type in cols_tables_whitespace[table]["TestScripts"]:
            for column in cols_tables_whitespace[table]['fields']:
                trim_whitespace_query = f"""
                update {table}
                set `{column}`= TRIM(`{column}`)
                where CHAR_LENGTH(`{column}`) != CHAR_LENGTH(TRIM(`{column}`))
                """
                print(trim_whitespace_query)
                engine.execute(trim_whitespace_query)


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

    content_length = r.headers.get('content-length')
    if not content_length:
        print("\tNo Content Length Attached. Attempting download without progress bar.")
        chunker = r.iter_content(chunk_size=1024)
    else:
        chunker = progress.bar(r.iter_content(chunk_size=1024), expected_size=(int(content_length) / 1024) + 1)
    with open(filepath, 'wb') as f:
        for chunk in chunker:
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


# def generate_index_statements(config, database_section, table):
#     from lib.configuration import get_connection_string
#     engine = create_engine(get_connection_string(config, database_section))
#     db = config['PATENTSVIEW_DATABASES']["PROD_DB"]
#     add_indexes_query = f"""
#         SELECT CONCAT('ALTER TABLE `', TABLE_NAME, '` ', 'ADD ', IF(NON_UNIQUE = 1,
#         CASE UPPER(INDEX_TYPE) WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX' WHEN 'SPATIAL' THEN 'SPATIAL INDEX' ELSE CONCAT('INDEX `', INDEX_NAME,'` USING ', INDEX_TYPE) END,
#         IF(UPPER(INDEX_NAME) = 'PRIMARY', CONCAT('PRIMARY KEY USING ', INDEX_TYPE), CONCAT('UNIQUE INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE))),
#         '(', GROUP_CONCAT(DISTINCT CONCAT('`', COLUMN_NAME, '`') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');') AS 'Show_Add_Indexes'
#         FROM information_schema.STATISTICS
#         WHERE TABLE_SCHEMA = '{db}'
#           AND TABLE_NAME = '{table}'
#           and UPPER(INDEX_NAME) <> 'PRIMARY'
#         GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE, INDEX_TYPE
#         ORDER BY TABLE_NAME ASC, INDEX_NAME ASC;
# """
#     print(add_indexes_query)
#     add_indexes_fetcher = engine.execute(add_indexes_query)
#     add_indexes = add_indexes_fetcher.fetchall()
#     drop_indexes_query = f"""
#         SELECT CONCAT('ALTER TABLE `', TABLE_NAME, '` ', GROUP_CONCAT(DISTINCT CONCAT('DROP ', IF(UPPER(INDEX_NAME) = 'PRIMARY', 'PRIMARY KEY', CONCAT('INDEX `', INDEX_NAME, '`'))) SEPARATOR ', '), ';')
#         FROM information_schema.STATISTICS
#         WHERE TABLE_SCHEMA = '{db}'
#           AND TABLE_NAME = '{table}'
#           and UPPER(INDEX_NAME) <> 'PRIMARY'
#         GROUP BY TABLE_NAME
#         ORDER BY TABLE_NAME ASC
#             """
#     print(drop_indexes_query)
#     drop_indexes_fetcher = engine.execute(drop_indexes_query)
#     drop_indexes = drop_indexes_fetcher.fetchall()
#     print(add_indexes)
#     print(drop_indexes)
#
#     return add_indexes, drop_indexes


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
        zipinfo = zip_ref.infolist()
        for _file in zipinfo:
            z_nm, z_ext = os.path.splitext(name)
            f_nm, f_ext = os.path.splitext(_file.filename)
            if re.match(f"{f_nm}_r\d",z_nm):
                # revision file - can't be renamed inside the zip archive, so will extract to a temporary location and rename
                os.mkdir(f"{path}/tmp")
                zip_ref.extract(_file.filename, f"{path}/tmp")
                os.rename(f"{path}/tmp/{_file.filename}",f"{path}/{z_nm}{f_ext}")
                os.rmdir(f"{path}/tmp")
            else:
                zip_ref.extract(_file.filename, path)

    os.remove(path + name)
    print(f"{name} downloaded and extracted to {path}")
    print(f"{path} contains {os.listdir(path)}")


def download_xml_files(config, xml_template_setting_prefix='pgpubs'):

    product_id = config["USPTO_LINKS"]["product_identifier"]  # e.g., "PGPUBS"
    api_key = config["USPTO_LINKS"]["api_key"]
    download_folder = config["FOLDERS"]["pgpubs_bulk_xml_location"]

    start_date = datetime.datetime.strptime(config['DATES']['START_DATE'], '%Y%m%d')
    end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
    parallelism = int(config["PARALLELISM"]["parallelism"])

    if parallelism > 1:
        manager = mp.Manager()
        log_queue = manager.Queue()
    else:
        log_queue = Queue()

    print(f"Start date: {config['DATES']['START_DATE']}")
    print(f"End date: {config['DATES']['END_DATE']}")

    files_to_download = []
    current_date = start_date

    while current_date <= end_date:
        current_date_str = current_date.strftime('%Y-%m-%d')

        headers = {
            "X-API-KEY": api_key,
            "accept": "application/json"
        }
        params = {
            "fileDataFromDate": current_date_str,
            "fileDataToDate": current_date_str
        }

        url = f"https://api.uspto.gov/api/v1/datasets/products/{product_id}"
        try:
            r = requests.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()

            file_bag = data.get("bulkDataProductBag", [])[0].get("productFileBag", {}).get("fileDataBag", [])
            for idx, file_info in enumerate(file_bag):
                filename = file_info["fileName"]
                file_url = file_info["fileDownloadURI"]
                files_to_download.append(
                    (file_url, filename, download_folder, idx, log_queue)
                )

        except Exception as e:
            print(f"[ERROR] Fetching metadata for {current_date_str}: {e}")

        current_date += datetime.timedelta(days=1)

    # === Start downloader pool
    watcher = None
    pool = None
    if parallelism > 1:
        pool = mp.Pool(parallelism)
        watcher = pool.apply_async(log_writer, (log_queue,))

    print(f"{len(files_to_download)} files found to download: {[f[1] for f in files_to_download]}")

    p_list = []
    for file_to_download in files_to_download:
        if parallelism > 1:
            p = pool.apply_async(save_zip_file, file_to_download)
            p_list.append(p)
        else:
            save_zip_file(*file_to_download)

    if parallelism > 1:
        for p in p_list:
            p.get()

        log_queue.put({"level": None, "message": "kill"})
        watcher.get()
        pool.close()
        pool.join()



# def download_xml_files(config, xml_template_setting_prefix='pgpubs'):
#     xml_template_setting = "{prefix}_bulk_xml_template".format(prefix=xml_template_setting_prefix)
#     xml_download_setting = "{prefix}_bulk_xml_location".format(prefix=xml_template_setting_prefix)
#     xml_path_template = config["USPTO_LINKS"][xml_template_setting]
#     start_date = datetime.datetime.strptime(config['DATES']['START_DATE'], '%Y%m%d')
#     end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
#     start_year = int(start_date.strftime('%Y'))
#     end_year = int(end_date.strftime('%Y'))
#     parallelism = int(config["PARALLELISM"]["parallelism"])
#     if parallelism > 1:
#         manager = mp.Manager()
#         log_queue = manager.Queue()
#     else:
#         log_queue = Queue()
#     print(f"Start date: {config['DATES']['START_DATE']}")
#     print(f"End date: {config['DATES']['END_DATE']}")
#     files_to_download = []
#
#     #starting one year early to check for revisions to old files (particularly important at the start of a new calendar year) - should add negligible time to typical runs
#     for year in range(start_year - 1, end_year + 1):
#         year_xml_page = xml_path_template.format(year=year)
#         print(year_xml_page)
#         r = requests.get(year_xml_page)
#         soup = BeautifulSoup(r.content, "html.parser")
#         links = soup.find_all("a", href=re.compile(r".*[0-9]{6}(_r\d)?\.zip"))
#         idx_counter = 0
#         for link in links:
#             href = link.attrs['href']
#             href_match = re.match(r".*([0-9]{6})(_r\d)?\.zip", href)
#             if href_match is not None:
#                 file_datestring = href_match.group(1)
#                 file_date = datetime.datetime.strptime(file_datestring, '%y%m%d')
#                 if end_date >= file_date >= start_date:
#                     # should apply to original and revised versions
#                     files_to_download.append(
#                         (xml_path_template.format(year=year) + href, href, config["FOLDERS"][xml_download_setting],
#                          idx_counter, log_queue))
#                 idx_counter += 1
#                 if (href_match.group(2) is not None) and (file_date < start_date): # has a revision suffix and past date
#                     # check if the file has already been downloaded to the matching folder
#                     old_download_path = config["FOLDERS"][xml_download_setting].replace(config['DATES']['END_DATE'], f"20{file_datestring}") #path uses YYYY, file uses YY
#                     downloaded_files = os.listdir(old_download_path)
#                     matching_files = [f for f in downloaded_files if re.match(f'i?p[ag]{file_datestring}', f)]
#                     if (len(matching_files) == 0): # no files downloaded for week matching revised file
#                         revised_file_message = f"""
# revized XML file available for download: {href}
# no files identified as downloaded for {file_datestring} in directory {old_download_path}."""
#                         send_slack_notification(message=revised_file_message, config=config, section="UNPARSED REVISED FILE NOTICE", level='warning')
#                     elif(href[:-4] > max(matching_files)[:-4]): # more recent file than is downloaded for week matching revised file
#                         # if the file has not already been downloaded to the matching folder, send slack message indicating unparsed revision
#                         revised_file_message = f"""
# revized XML file available for download: {href}
# latest version downloaded for {file_datestring} in {old_download_path}: {max(matching_files)}"""
#                         send_slack_notification(message=revised_file_message, config=config, section="UNPARSED REVISED FILE NOTICE", level='warning')
#     watcher = None
#     pool = None
#     if parallelism > 1:
#         pool = mp.Pool(parallelism)
#         watcher = pool.apply_async(log_writer, (log_queue,))
#
#     p_list = []
#     idx_counter = 0
#     print(f"{len(files_to_download)} files found to download: {[_file[1] for _file in files_to_download]}")
#     for file_to_download in files_to_download:
#         if parallelism > 1:
#             p = pool.apply_async(save_zip_file, file_to_download)
#             p_list.append(p)
#         else:
#             save_zip_file(*file_to_download)
#         idx_counter += 1
#     if parallelism > 1:
#         idx_counter = 0
#         for t in p_list:
#             print(t)
#             t.get()
#
#     idx_counter += 1
#     log_queue.put({
#         "level": None,
#         "message": "kill"
#     })
#     if parallelism > 1:
#         watcher.get()
#         pool.close()
#         pool.join()


def manage_ec2_instance(config, button='ON', identifier='xml_collector'):
    instance_id = config['AWS_WORKER'][identifier]
    ec2 = boto3.client('ec2', aws_access_key_id=config['AWS']['ACCESS_KEY_ID'],
                       aws_secret_access_key=config['AWS']['SECRET_KEY'],
                       region_name='us-east-1')
    if button == 'ON':
        response = ec2.start_instances(InstanceIds=[instance_id])
    else:
        response = ec2.stop_instances(InstanceIds=[instance_id])
    return response['ResponseMetadata']['HTTPStatusCode'] == 200


def create_aws_boto3_session():
    """
    Creates and returns a boto3 session.
    If running in EC2, it will use the instance's IAM role.
    """
    try:
        # Create a boto3 session
        session = boto3.Session()
        print("AWS Session Created Successfully")
        return session
    except Exception as e:
        print(f"Error creating AWS session: {e}")
        return None

def rds_free_space(identifier):
    """
    Retrieve the FreeStorageSpace metric for an RDS instance.
    Args:
        identifier (str): The RDS instance identifier.
    Returns:
        dict: CloudWatch response containing storage space data.
    """
    session = create_aws_boto3_session()
    if not session:
        print("Failed to retrieve AWS credentials.")
        return None

    # Create CloudWatch client using the returned session
    cloudwatch = session.client('cloudwatch', region_name='us-east-1')

    from datetime import datetime, timedelta
    response = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'fetching_FreeStorageSpace',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'FreeStorageSpace',
                        'Dimensions': [
                            {
                                "Name": "DBInstanceIdentifier",
                                "Value": identifier
                            }
                        ]
                    },
                    'Period': 300,
                    'Stat': 'Minimum'
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
        print(target_folder)
        os.makedirs(target_folder, exist_ok=True)
        for file_name in files:
            print(file_name)
            shutil.copy(os.path.join(source_folder, file_name), target_folder)
    print(targets[-1])
    os.makedirs(targets[-1], exist_ok=True)
    for file_name in files:
        print(file_name)
        shutil.copy(os.path.join(source_folder, file_name), targets[-1])

def add_index_new_disambiguation_table(connection, table_name):
    from mysql.connector.errors import ProgrammingError
    g_cursor = connection.cursor()
    index_query = 'alter table {table_name} add primary key (uuid)'.format(
        table_name=table_name)
    print(index_query)
    try:
        g_cursor.execute(index_query)
    except ProgrammingError as e:
        from mysql.connector import errorcode
        if not e.errno == errorcode.ER_MULTIPLE_PRI_KEY:
            raise

def link_view_to_new_disambiguation_table(connection, table_name, disambiguation_type):
    from mysql.connector.errors import ProgrammingError
    g_cursor = connection.cursor()
    index_query = 'alter table {table_name} add primary key (uuid)'.format(
        table_name=table_name)
    print(index_query)
    replace_view_query = """
        CREATE OR REPLACE SQL SECURITY INVOKER VIEW {dtype}_disambiguation_mapping as SELECT uuid,{dtype}_id from {table_name}
        """.format(table_name=table_name, dtype=disambiguation_type)
    try:
        g_cursor.execute(index_query)
    except ProgrammingError as e:
        from mysql.connector import errorcode
        if not e.errno == errorcode.ER_MULTIPLE_PRI_KEY:
            raise
    print(replace_view_query)
    g_cursor.execute(replace_view_query)

def update_to_granular_version_indicator(table, db):
    from lib.configuration import get_current_config, get_connection_string
    config = get_current_config(type=db, **{"execution_date": datetime.date(2000, 1, 1)})
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    if db == 'granted_patent':
        id = 'id'
        fk = 'patent_id'
        fact_table = 'patent'
    else:
        id = 'document_number'
        fk = 'document_number'
        fact_table = 'publication'
    query = f"""
update {table} update_table 
	inner join {fact_table} p on update_table.{fk}=p.{id}
set update_table.version_indicator=p.version_indicator     
    """
    print(query)
    query_start_time = time()
    engine.execute(query)
    query_end_time = time()
    print("This query took:", query_end_time - query_start_time, "seconds")

def update_version_indicator(table, db, **kwargs):
    from lib.configuration import get_current_config, get_connection_string
    config = get_current_config(type=db, schedule="quarterly", **kwargs)
    ed = process_date(config['DATES']["end_date"], as_string=True)
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr)
    query = f"""
update {table} update_table 
set update_table.version_indicator={ed} 
    """
    print(query)
    query_start_time = time()
    engine.execute(query)
    query_end_time = time()
    print("This query took:", query_end_time - query_start_time, "seconds")


if __name__ == "__main__":
    # update_to_granular_version_indicator('uspc_current', 'granted_patent')
    print("HI")
    config = get_current_config("granted_patent", schedule='quarterly',  **{"execution_date": datetime.date(2022, 6, 30)})

