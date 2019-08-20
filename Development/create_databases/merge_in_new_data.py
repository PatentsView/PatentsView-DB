import os
import configparser
import sys

project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers
import configparser
import json
import time

config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']

engine = general_helpers.connect_to_db(host, username, password, new_database)

# get a list of table names in the database we want to copy in
command = "select table_name from information_schema.tables where table_type = 'base table' and table_schema ='{}'".format(
    temporary_upload)
tables_data = engine.execute(command)
tables = [table['table_name'] for table in tables_data]
tables = ['application', 'botanic', 'brf_sum_text', 'claim', 'detail_desc_length', 'draw_desc_text', 'figures',
          'foreign_priority', 'ipcr', 'mainclass', 'otherreference', 'patent', 'rawassignee', 'rawexaminer',
          'rawinventor', 'rawlawyer', 'rawlocation', 'rel_app_text', 'subclass', 'us_term_of_grant',
          'usapplicationcitation', 'uspatentcitation', 'usreldoc']
engine.execute("SET FOREIGN_KEY_CHECKS=0;")

engine.dispose()

status_file = project_home + '/Development/create_databases/merge_status.json'
try:
    current_status = json.load(open(status_file))
except OSError as e:
    print(e)
    current_status = {}

# query to insert db
for table in tables:
    engine = general_helpers.connect_to_db(host, username, password, new_database)
    engine.execute("SET FOREIGN_KEY_CHECKS=0;")
    print(table, flush=True)
    if table in current_status and current_status[table] == 1:
        continue
    order_by_cursor = engine.execute(
        "SELECT GROUP_CONCAT(s.COLUMN_NAME SEPARATOR ', ' ) from information_schema.tables t left join information_schema.statistics s on t.TABLE_NAME=s.TABLE_NAME where INDEX_NAME='PRIMARY' and t.TABLE_SCHEMA=s.TABLE_SCHEMA and t.TABLE_SCHEMA ='" + temporary_upload + "' and s.TABLE_NAME ='" + table + "' GROUP BY t.TABLE_NAME;")
    order_by_clause = order_by_cursor.fetchall()[0][0]
    table_data_count = engine.execute("SELEcT count(1) from " + temporary_upload + "." + table).fetchall()[0][0]
    limit = 10000
    offset = 0
    batch_counter = 0
    try:
        while True:
            batch_counter += 1
            print('Next iteration...')

            # order by with primary key column - this has no nulls
            insert_table_command_template = "INSERT INTO {0}.{2} SELECT * FROM {1}.{2} ORDER BY " + order_by_clause + " limit {3} offset {4}"
            if table == "mainclass" or table == "subclass":
                insert_table_command_template = "INSERT IGNORE INTO {0}.{2} SELECT * FROM {1}.{2} ORDER BY " + order_by_clause + " limit {3} offset {4}"

            insert_table_command = insert_table_command_template.format(
                new_database, temporary_upload, table, limit,
                offset)
            print(insert_table_command, flush=True)
            start = time.time()

            engine.execute(insert_table_command)

            print(time.time() - start, flush=True)
            # means we have no more batches to process
            if offset > table_data_count:
                break

            offset = offset + limit
        current_status[table] = 1
    except Exception as e:
        current_status[table] = 0
        json.dump(current_status, open(status_file, "w"))
        engine.dispose()
        raise e

    json.dump(current_status, open(status_file, "w"))
    engine.dispose()

try:
    current_status = json.load(open(status_file))
except OSError as e:
    print(e)
    exit(100)

if sum(current_status.values()) < len(tables):
    exit(200)
