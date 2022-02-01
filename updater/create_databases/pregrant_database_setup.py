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
    database = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    host = '{}'.format(config['DATABASE_SETUP']['HOST'])
    user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
    password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
    defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']

    conn = pymysql.connect(host=host, user=user, password=password)
    conn.cursor().execute('CREATE DATABASE {};'.format(database))
    sql_path = config["FILES"]['APP_DB_SCHEMA_FILE']
    try:
        subprocess_cmd('mysql --defaults-file=' + defaults_file + ' ' + database + ' < ' + sql_path)
    except:
        print('bash command failed')
        raise


def merge_database(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)

    database = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    defaults_file = config['DATABASE_SETUP']['CONFIG_FILE']
    sql_path = config["FILES"]['MERGE_SCHEMA_FILE']

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
    drop_database(**{
            "execution_date": datetime.date(2020, 12, 17)
            })
