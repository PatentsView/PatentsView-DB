import datetime
import os
import sys


import pandas as pd
# get a list of table names in the database we want to copy in
from sqlalchemy import create_engine

from QA.create_databases.UploadTest import UploadTest
from lib.configuration import get_connection_string, get_required_tables, get_current_config
from lib.utilities import load_table_config, class_db_specific_config

def upload_table(table_name, filepath, connection_string, version_indicator):
    engine = create_engine(connection_string)
    data = pd.read_csv(filepath, delimiter='\t', index_col=False, keep_default_na=False, na_values=['','NULL','null'])
    data = data.assign(version_indicator=version_indicator)
    if table_name in ['mainclass', 'subclass']:
        table_name = "temp_" + table_name
    data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    engine.dispose()


def upload_from_timestamp_folder(timestamp_folder, connection_string, version_indicator):
    for table_file in os.listdir(timestamp_folder):
        if table_file not in ['error_counts.csv', 'error_data.csv']:
            table_name = table_file.replace('.csv', '')
            table_file_full_path = "{source_root}/{filename}".format(source_root=timestamp_folder, filename=table_file)
            upload_table(table_name, table_file_full_path, connection_string, version_indicator)


def consolidate_cpc_classes(connection_string):
    for table_name in ['mainclass', 'subclass']:
        engine = create_engine(connection_string)
        insert_statement = "INSERT IGNORE INTO {table_name} (id, version_indicator) SELECT id, version_indicator from temp_{table_name};".format(
            table_name=table_name)
        engine.execute(insert_statement)
        engine.dispose()


def setup_database(update_config, drop=True, cpc_only=False):
    raw_database = update_config["PATENTSVIEW_DATABASES"]["PROD_DB"]
    temp_upload_database = update_config["PATENTSVIEW_DATABASES"]["TEMP_UPLOAD_DB"]
    if cpc_only:
        required_tables = ['cpc_current', 'cpc_group', 'cpc_subgroup', 'cpc_subsection']
    else:
        required_tables = get_required_tables(update_config)
    print("Required tables are {tlist}".format(tlist=", ".join(required_tables)))
    connection_string = get_connection_string(update_config, database="PROD_DB")
    engine = create_engine(connection_string)
    print(temp_upload_database)
    with engine.connect() as con:
        if drop:
            con.execute(f"""
                DROP DATABASE if exists {temp_upload_database}
            """)
        con.execute(f"""
                create database if not exists {temp_upload_database} default character set=utf8mb4
                default collate=utf8mb4_unicode_ci
            """)
        for table in required_tables:
            if cpc_only:
                if drop:
                    con.execute("drop table if exists {0}.{1}".format(temp_upload_database, table))
                    query = f"create table if not exists {temp_upload_database}.{table} like patent.{table}"
                    print(query)
                    con.execute(query)
            else:
                print("Creating Table : {tbl}".format(tbl=table))
                if drop:
                    con.execute("drop table if exists {0}.{1}".format(temp_upload_database, table))
                if table in ['inventor', 'assignee_disambiguation_mapping', 'inventor_disambiguation_mapping', 'assignee', 'location', 'location_disambiguation_mapping'] and raw_database=='patent':
                    # tables with version-specific base tables (will need quarterly updating)
                    query = "create table if not exists {0}.{2} like {1}.{2}_{3}".format(temp_upload_database, raw_database, table, '20211230')
                    print(query)
                    con.execute(query)
                elif table in ['government_organization']: #set any tables that should be defined as views of the production version.
                    query = f"CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS {temp_upload_database}.{table} AS SELECT * FROM {raw_database}.{table}"
                else:
                    query = "create table if not exists {0}.{2} like {1}.{2}".format(temp_upload_database, raw_database, table)
                print(query)
                con.execute(query)
        # create column for manual override of automated data qualty flag. should only be modified manually, never automatically.
        con.execute(f"ALTER TABLE {temp_upload_database}.rawlocation ADD COLUMN `qa_override` int(1) DEFAULT NULL after `longitude`;")
    engine.dispose()


def generate_timestamp_uploads(update_config):
    working_folder = update_config['FOLDERS']['WORKING_FOLDER']
    connection_string = get_connection_string(update_config, "TEMP_UPLOAD_DB")
    parsed_data_folder = "{working_folder}/{parsed_folder}".format(working_folder=working_folder,
                                                                   parsed_folder="parsed_data")
    print(f"checking for parsed data in {parsed_data_folder}...")
    parse_folders = os.listdir(parsed_data_folder)
    print(f"{len(parse_folders)} parse(s) found: {parse_folders}")
    latest_parse_folder = max(parse_folders)
    # for timestamp_folder in parse_folders:
    timestamp_folder_full_path = "{source_root}/{folder_name}/".format(source_root=parsed_data_folder,
                                                                        folder_name=latest_parse_folder)
    print(f"beginning upload of data from {timestamp_folder_full_path}...")
    upload_from_timestamp_folder(timestamp_folder_full_path, connection_string,
                                    version_indicator=update_config['DATES']['END_DATE'])


def begin_database_setup(dbtype='granted_patent' , **kwargs):
    from lib.configuration import get_current_config
    config = get_current_config(dbtype, **kwargs)
    setup_database(config)


def begin_upload(update_config):
    connection_string = get_connection_string(update_config, "TEMP_UPLOAD_DB")
    generate_timestamp_uploads(update_config)
    consolidate_cpc_classes(connection_string)


def upload_current_data(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    begin_upload(update_config=config)


def post_upload_granted(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    qc = UploadTest(config)
    qc.runTests()


def post_upload_pgpubs(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('pgpubs', **kwargs)
    qc = UploadTest(config)
    qc.runTests()

if __name__ == '__main__':
    # begin_database_setup(**{
    #         "execution_date": datetime.date(2021, 2, 5)
    #         })
    # upload_current_data(**{
    #         "execution_date": datetime.date(2020, 12, 1)
    #         })
    # post_upload(**{
    #         "execution_date": datetime.date(2020, 12, 1)
    #         })
    # config = get_current_config('pgpubs', **{
    #     "execution_date": datetime.date(2022, 6, 2)
    # })
    # setup_database(config, **{
    #         "execution_date": datetime.date(2020, 12, 14)
    #         })
    # generate_timestamp_uploads(config)
    for month, day in [
(3, 24),
(1, 27),
(1, 20),
(1, 13)
    ]:
        post_upload_pgpubs(**{
            "execution_date": datetime.date(2022, month, day)
        })

