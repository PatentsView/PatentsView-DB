import datetime
import subprocess

import pymysql
from sqlalchemy import create_engine

from QA.text_parser.AppTest import AppMergeTest
from QA.text_parser.ImportTest import RenameTest
from lib.configuration import get_connection_string, get_current_config


def subprocess_cmd(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print(proc_stdout)


def create_database(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)
    temp_db = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    host = '{}'.format(config['DATABASE_SETUP']['HOST'])
    user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
    password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
    port = int(config['DATABASE_SETUP']['PORT'])
    defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    sql_path = config["FILES"]['APP_DB_SCHEMA_FILE']
    conn = pymysql.connect(host=host, user=user, password=password, port=port)

    # CREATE TEMP DB AND LOAD IN TABLES
    conn.cursor().execute(f"Drop database if exists {temp_db};")
    conn.cursor().execute(f"CREATE DATABASE {temp_db} DEFAULT CHARACTER SET = 'utf8mb4' DEFAULT COLLATE 'utf8mb4_unicode_ci';")

    try:
        subprocess_cmd('mysql --defaults-file=' + defaults_file + ' ' + temp_db + ' < ' + sql_path)
    except:
        print('create bash command failed')
        raise


def merge_database(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)
    qavi = "'" + end_date + "'"
    vi = "'" + '-'.join([end_date[:4], end_date[4:6], end_date[6:]]) + "'"
    end_date = config['DATES']["END_DATE"]
    prod_db = '{}'.format(config['PATENTSVIEW_DATABASES']['PROD_DB'])
    sql_delete_path = config["FILES"]['APP_DELETE_FILE']

    database = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    sql_path = config["FILES"]['MERGE_SCHEMA_FILE']

    # DELETE PRIOR DATA IN DESTINATION TABLES (PROD)
    delete_version_in_destination_tables = 'yes'
    if delete_version_in_destination_tables == 'yes':
        bash_command='mysql --defaults-file=' + defaults_file + ' ' + prod_db + ' -e ' + f'"set @dbdate={vi}; set @qavi={qavi}; source {sql_delete_path};"' + ' ;'
        print(bash_command)
        try:
            subprocess_cmd(bash_command)
        except:
            print('delete bash command failed')

    try:
        subprocess_cmd('mysql --defaults-file=' + defaults_file + ' ' + database + ' < ' + sql_path)
    except:
        print('bash command failed')
        raise

def drop_database(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    database = config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB']
    engine = create_engine(cstr)
    engine.execute(
            'DROP DATABASE {};'.format(database))


def post_create_database(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)
    qc = RenameTest(config)
    qc.runTests()


def post_merge_database(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)
    qc = AppMergeTest(config)
    qc.runTests()


if __name__ == "__main__":
    # drop_database(**{
    #         "execution_date": datetime.date(2020, 12, 17)
    #         })
    create_database(**{
            "execution_date": datetime.date(2021, 12, 2)
            })
