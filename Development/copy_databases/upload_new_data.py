import os
import MySQLdb
import configparser
import sys
import pandas as pd
from sqlalchemy import create_engine
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read('Development/config.ini')
host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']
data_to_upload = config['FOLDERS']['PARSED_DATA']

engine = general_helpers.connect_to_db(host, username, password, new_database)

engine.execute('create schema {}'.format(temporary_upload))
engine.execute('use {}'.format(temporary_upload))

#should I just copy this directly from the schema of the other tables or do this?
with open('{}/Development/patent_schema.sql'.format(os.getcwd()), 'r') as f:
    commands = f.read().replace('\n', '').split(';')[:-1]
    for command in commands:
        engine.execute(command)
#special command to handle persistent inventor disambiguation which adds columns every time:
engine.execute('create table {}.persistent_inventor_disambig like {}.persistent_inventor_disambig'.format(temporary_upload, new_database))

for folder in os.listdir(data_to_upload):
    print(folder)
    fields = [item for item in os.listdir('{}/{}'.format(data_to_upload,folder)) if not item in ['error_counts.csv', 'error_data.csv']]
    for f in fields:
        print(f)
        data = pd.read_csv('{0}/{1}/{2}'.format(data_to_upload,folder, f), delimiter = '\t', encoding ='utf-8')
        data.to_sql(f.replace(".csv", ""), engine, if_exists = 'append', index=False)

