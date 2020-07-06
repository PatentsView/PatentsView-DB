import configparser
import subprocess
import os
import pymysql
from sqlalchemy import create_engine

from QA.text_parser.ImportTest import RenameTest
from lib.configuration import get_config
from update_config import update_config_date
from QA.text_parser.AppTest import AppMergeTest


def subprocess_cmd(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print(proc_stdout)


def create_database(**kwargs):
    config = update_config_date(**kwargs)

    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    defaults_file = config['DATABASE']['CONFIG_FILE']

    conn = pymysql.connect(host=host, user=user, password=password)
    conn.cursor().execute('CREATE DATABASE {};'.format(database))
    sql_path = config["FILES"]['APP_DB_SCHEMA_FILE']
    try:
        subprocess_cmd('mysql --defaults-file=' + defaults_file + ' ' + database + ' < ' + sql_path)
    except:
        print('bash command failed')


def merge_database(**kwargs):
    config = update_config_date(**kwargs)

    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    defaults_file = config['DATABASE']['CONFIG_FILE']
    sql_path = config["FILES"]['MERGE_SCHEMA_FILE']

    try:
        subprocess_cmd('mysql --defaults-file=' + defaults_file + ' ' + database + ' < ' + sql_path)
    except:
        print('bash command failed')


def drop_database(**kwargs):
    config = update_config_date(**kwargs)

    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    engine = create_engine(
        'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    engine.execute(
        'DROP DATABASE {};'.format(database))


def post_create_database(**kwargs):
    config = update_config_date(**kwargs)
    qc = RenameTest(config)
    qc.runTests()


def post_merge_database(**kwargs):
    config = update_config_date(**kwargs)
    qc = AppMergeTest(config)
    qc.runTests()


if __name__ == "__main__":
    config = get_config("application")
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    sql_path = config["FILES"]['APP_DB_SCHEMA_FILE']
    defaults_file = config['DATABASE']['CONFIG_FILE']
    try:
        subprocess_cmd('mysql --defaults-file=' + defaults_file + ' ' + database + ' < ' + sql_path)
    except:
        print('bash command failed')
