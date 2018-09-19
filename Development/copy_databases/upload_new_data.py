import os
import MySQLdb
import configparser
import sys
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

mydb = general_helpers.connect_to_db(host, username, password, new_database)
cursor = mydb.cursor()

#cursor.execute('create schema {}'.format(temporary_upload))
#cursor.execute('use {}'.format(temporary_upload))

#should I just copy this directly from the schema of the other tables or do this?
with open('{}/Development/patent_schema.sql'.format(os.getcwd()), 'r') as f:
    commands = f.read().replace('\n', '').split(';')[:-1]
    for command in commands:
        cursor.execute(command)
mydb.commit()

for folder in data_to_upload:
    fields = [item for item in os.listdir(folder) if not item in ['error_counts.csv', 'error_data.csv']]
    for f in fields:
        print(f)
        cursor.execute("load data local infile '{0}/{1}' ignore into table {2} CHARACTER SET utf8 fields terminated by '\t' lines terminated by '\n' ignore 1 lines".format(folder, f, f.replace(".csv", "")))
        mydb.commit()

