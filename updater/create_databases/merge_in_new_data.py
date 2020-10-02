import configparser
import datetime
import json
import os
import time

from sqlalchemy import create_engine

from QA.create_databases.MergeTest import MergeTest
from QA.create_databases.TextTest import TextMergeTest
from lib.configuration import get_connection_string, get_config


def get_merge_status(status_folder):
    if not os.path.exists(status_folder):
        os.makedirs(status_folder)
    status_file = '{}/{}'.format(status_folder, 'merge_status.json')
    try:
        current_status = json.load(open(status_file))
    except OSError as e:
        print(e)
        current_status = {}
    return current_status


def save_merge_status(status_folder, status):
    status_file = '{}/{}'.format(status_folder, 'merge_status.json')
    json.dump(status, open(status_file, "w"))


def update_table_data(table_name, config):
    connection_string = get_connection_string(config, "NEW_DB")
    upload_db = config["DATABASE"]["TEMP_UPLOAD_DB"]
    new_db = config["DATABASE"]["NEW_DB"]
    engine = create_engine(connection_string)
    order_by_cursor = engine.execute(
        "SELECT GROUP_CONCAT(s.COLUMN_NAME SEPARATOR ', ' ) from information_schema.tables t left join information_schema.statistics s on t.TABLE_NAME=s.TABLE_NAME where INDEX_NAME='PRIMARY' and t.TABLE_SCHEMA=s.TABLE_SCHEMA and t.TABLE_SCHEMA ='" + upload_db + "' and s.TABLE_NAME ='" + table_name + "' GROUP BY t.TABLE_NAME;")
    if table_name == 'mainclass' or table_name == 'subclass':
        order_by_clause = ''
    else:
        order_by_clause = order_by_cursor.fetchall()[0][0]
    table_data_count = engine.execute("SELEcT count(1) from " + upload_db + "." + table_name).fetchall()[0][0]
    with engine.begin() as connection:
        limit = 50000
        offset = 0
        batch_counter = 0
        while True:
            batch_counter += 1
            print('Next iteration...')

            # order by with primary key column - this has no nulls
            insert_table_command_template = "INSERT INTO {0}.{2} SELECT * FROM {1}.{2} ORDER BY " + order_by_clause + " limit {3} offset {4}"
            if table_name == "mainclass" or table_name == "subclass":
                insert_table_command_template = "INSERT IGNORE INTO {0}.{2} SELECT * FROM {1}.{2} limit {3} offset {4}"

            insert_table_command = insert_table_command_template.format(
                new_db, upload_db, table_name, limit,
                offset)
            print(insert_table_command, flush=True)
            start = time.time()

            connection.execute(insert_table_command)

            print(time.time() - start, flush=True)
            # means we have no more batches to process
            if offset > table_data_count:
                break

            offset = offset + limit


def merge_new_data(tables, project_home, update_config):
    status_folder = '{}/{}'.format(project_home, 'updater/create_databases')

    upload_status = get_merge_status(status_folder)
    try:
        for table in tables:
            if table in upload_status and upload_status[table] == 1:
                continue
            try:
                update_table_data(table, update_config)
                upload_status[table] = 1
            except Exception as e:
                upload_status[table] = 0
                raise e
    finally:
        save_merge_status(status_folder, upload_status)


def normalize_exemplary(config):
    import pandas as pd
    connection_string = get_connection_string(config, "TEMP_UPLOAD_DB")
    engine = create_engine(connection_string)
    exemplary_data = pd.read_sql_table(table_name="temp_claim_exemplary", con=engine)

    exemplary_data = exemplary_data.join(
        exemplary_data.exemplary.str.strip().str.split(",", expand=True)).drop(
        "exemplary", axis=1)

    melted_exemplary = exemplary_data.melt(id_vars=['patent_id', 'filename'], value_name='exemplary')
    melted_exemplary.exemplary = melted_exemplary.exemplary.str.strip()
    normalized_exemplary = melted_exemplary[
        (~melted_exemplary.exemplary.isnull())
        & (melted_exemplary.exemplary.str.len() > 0)]

    normalized_exemplary.to_sql(name='temp_normalized_claim_exemplary', index=False, con=engine, if_exists='append')


def update_text_data(table, update_config):
    connection_string = get_connection_string(update_config, "TEXT_DATABASE")
    engine = create_engine(connection_string)
    query = table["insert"]
    engine.execute(query)


def merge_text_data(tables, update_config):
    for table in tables:
        if 'preprocess' in tables[table]:
            tables[table]['preprocess'](update_config)
        update_text_data(tables[table], update_config)


def begin_merging(config):
    tables = ['application', 'botanic', 'detail_desc_length', 'draw_desc_text', 'figures', 'foreigncitation',
              'foreign_priority', 'government_interest', 'ipcr', 'mainclass', 'non_inventor_applicant',
              'otherreference', 'patent', 'rawassignee', 'rawexaminer', 'pct_data', 'rawinventor', 'rawlawyer',
              'rawlocation', 'rel_app_text', 'subclass', 'us_term_of_grant', 'usapplicationcitation',
              'uspatentcitation', 'usreldoc', 'uspc']
    project_home = os.environ['PACKAGE_HOME']
    merge_new_data(tables, project_home, config)


def begin_text_merging(config):
    version = config['DATABASE']['TEMP_UPLOAD_DB'].split("_")[1]
    start_year = int(datetime.datetime.strptime(config['DATES']['START_DATE'], '%Y%m%d').strftime('%Y'))
    text_table_config = {'brf_sum_text': {
        "insert": "INSERT INTO {text_db}.brf_sum_text_{year}(uuid, patent_id, text,filename, version_indicator) "
                  "SELECT uuid, patent_id, text, filename,'{database_version}' from {temp_db}.temp_brf_sum_text".format(
            text_db=config['DATABASE']['TEXT_DATABASE'], temp_db=config['DATABASE']['TEMP_UPLOAD_DB'],
            database_version=version, year=start_year)}, 'claim': {'preprocess': normalize_exemplary,
                                                                   "insert": "INSERT INTO {text_db}.claim_{year}("
                                                                             "uuid, patent_id, num, text, sequence, "
                                                                             "dependent, exemplary,filename, "
                                                                             "version_indicator, patent_date) SELECT  c.uuid, c.patent_id,c.num, c.text, c.sequence-1,  c.dependent, case when tce.exemplary is null then 0 else 1 end ,c.filename,'{database_version}', p.date from {temp_db}.temp_claim c left join {temp_db}.temp_normalized_claim_exemplary tce on tce.patent_id=c.patent_id and tce.exemplary = c.sequence  left join {temp_db}.patent p on p.id = c.patent_id".format(
                                                                       text_db=config['DATABASE']['TEXT_DATABASE'],
                                                                       temp_db=config['DATABASE']['TEMP_UPLOAD_DB'],
                                                                       database_version=version, year=start_year)},
        'draw_desc_text': {
            "insert": "INSERT INTO {text_db}.draw_desc_text_{year}(uuid patent_id, text, sequence, version_indicator) SELECT uuid, patent_id, text, sequence, '{database_version}' from {temp_db}.draw_desc_text".format(
                text_db=config['DATABASE']['TEXT_DATABASE'], temp_db=config['DATABASE']['TEMP_UPLOAD_DB'],
                database_version=version, year=start_year)}, 'detail_desc_text': {
            "insert": "INSERT INTO {text_db}.detail_desc_text_{year}(uuid, patent_id, text,length, filename, "
                      "version_indicator) SELECT uuid, patent_id, text,char_length(text), filename, "
                      "'{database_version}' from {temp_db}.temp_detail_desc_text".format(
                text_db=config['DATABASE']['TEXT_DATABASE'], temp_db=config['DATABASE']['TEMP_UPLOAD_DB'],
                database_version=version, year=start_year)}, 'detail_desc_length': {
            "insert": "INSERT INTO {new_db}.detail_desc_length( patent_id, detail_desc_length) SELECT  patent_id, CHAR_LENGTH(text) from {temp_db}.temp_detail_desc_text".format(
                new_db=config['DATABASE']['NEW_DB'], temp_db=config['DATABASE']['TEMP_UPLOAD_DB'],
                database_version=version)}}
    merge_text_data(text_table_config, config)


def post_merge(config):
    qc = MergeTest(config)
    qc.runTests()


def post_text_merge(config):
    qc = TextMergeTest(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
    # begin_merging(config)
    post_merge(config)
