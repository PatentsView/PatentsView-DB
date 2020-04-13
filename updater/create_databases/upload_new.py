import os
import pandas as pd

# get a list of table names in the database we want to copy in
from sqlalchemy import create_engine

from QA.create_databases.UploadTest import UploadTest
from lib.configuration import get_connection_string, get_config


def upload_table(table_name, filepath, connection_string):
    engine = create_engine(connection_string)
    data = pd.read_csv(filepath, delimiter='\t', index_col=False)
    if table_name in ['mainclass', 'subclass']:
        table_name = "temp_" + table_name
    data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    engine.dispose()


def upload_from_timestamp_folder(timestamp_folder, connection_string):
    for table_file in os.listdir(timestamp_folder):
        if table_file not in ['error_counts.csv', 'error_data.csv']:
            table_name = table_file.replace('.csv', '')
            table_file_full_path = "{source_root}/{filename}".format(source_root=timestamp_folder, filename=table_file)
            upload_table(table_name, table_file_full_path, connection_string)


def consolidate_cpc_classes(connection_string):
    for table_name in ['mainclass', 'subclass']:
        engine = create_engine(connection_string)
        insert_statement = "INSERT IGNORE INTO {table_name} SELECT * from temp_{table_name};".format(
            table_name=table_name)
        engine.execute(insert_statement)
        engine.dispose()


def setup_database(update_config):
    connection_string = get_connection_string(update_config, "OLD_DB")
    engine = create_engine(connection_string)
    new_database = update_config["DATABASE"]["NEW_DB"]
    temp_upload_database = update_config["DATABASE"]["TEMP_UPLOAD_DB"]
    table_fetch_sql = "select table_name from information_schema.tables where table_type = 'base table' and table_schema ='{}'".format(
        new_database)
    tables_data = engine.execute(table_fetch_sql)
    tables = [table['table_name'] for table in tables_data]

    engine.execute(
        "create database if not exists {temp_upload_database} default character set=utf8mb4 default collate=utf8mb4_unicode_ci".format(
            temp_upload_database=temp_upload_database))
    for table in tables:
        con = engine.connect()
        con.execute("create table if not exists {0}.{2} like {1}.{2}".format(temp_upload_database, new_database, table))
        con.close()
    engine.dispose()


def generate_timestamp_uploads(update_config):
    working_folder = update_config['FOLDERS']['WORKING_FOLDER']
    connection_string = get_connection_string(update_config, "TEMP_UPLOAD_DB")
    parsed_data_folder = "{working_folder}/{parsed_folder}".format(working_folder=working_folder,
                                                                   parsed_folder="parsed_data")
    for timestamp_folder in os.listdir(parsed_data_folder):
        timestamp_folder_full_path = "{source_root}/{folder_name}/".format(source_root=parsed_data_folder,
                                                                           folder_name=timestamp_folder)
        upload_from_timestamp_folder(timestamp_folder_full_path, connection_string)


def begin_upload(update_config):
    setup_database(update_config)
    connection_string = get_connection_string(update_config, "TEMP_UPLOAD_DB")
    generate_timestamp_uploads(update_config)
    consolidate_cpc_classes(connection_string)


def upload_new_data(config):
    begin_upload(update_config=config)
    qc = UploadTest(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
    upload_new_data(config)
