import configparser
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
    status_folder = '{}/{}'.format(project_home, 'create_databases')

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


def update_text_data(table, update_config):
    connection_string = get_connection_string(update_config, "TEXT_DATABASE")
    engine = create_engine(connection_string)
    query = table["insert"]
    engine.execute(query)


def merge_text_data(tables, update_config):
    for table in tables:
        update_text_data(tables[table], update_config)


def begin_merging(config):
    tables = ['application', 'botanic', 'detail_desc_length', 'draw_desc_text', 'figures', 'foreigncitation',
              'foreign_priority', 'government_interest', 'ipcr', 'mainclass', 'non_inventor_applicant',
              'otherreference', 'patent', 'rawassignee', 'rawexaminer', 'pct_data', 'rawinventor', 'rawlawyer',
              'rawlocation', 'rel_app_text', 'subclass', 'us_term_of_grant', 'usapplicationcitation',
              'uspatentcitation', 'usreldoc', 'uspc']
    project_home = os.environ['PACKAGE_HOME']
    version = config['DATABASE']['TEMP_UPLOAD_DB'].split("_")[1]
    merge_new_data(tables, project_home, config)

    qc = MergeTest(config)
    qc.runTests()
    text_table_config = {'brf_sum_text': {
        "insert": "INSERT INTO {text_db}.brf_sum_text(uuid, patent_id, text, version_indicator) SELECT id, patent_number, text, '{database_version}' from {temp_db}.temp_brf_sum_text".format(
            text_db=config['DATABASE']['TEXT_DATABASE'], temp_db=config['DATABASE']['TEMP_UPLOAD_DB'],
            database_version=version)}, 'claim': {
        "insert": "INSERT INTO {text_db}.claim(uuid, patent_id, text, dependent, sequence, exemplary, version_indicator) SELECT c.id, c.patent_number, c.text, c.dependent, c.sequence, tce.exemplary,  '{database_version}' from {temp_db}.temp_claim c left join {temp_db}.temp_claim_exemplary tce on tce.patent_number=c.patent_number".format(
            text_db=config['DATABASE']['TEXT_DATABASE'], temp_db=config['DATABASE']['TEMP_UPLOAD_DB'],
            database_version=version)}}
    merge_text_data(text_table_config, config)

    qc = TextMergeTest(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
    begin_merging(config)
