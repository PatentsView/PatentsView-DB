import os
import MySQLdb
import sys
import pandas as pd
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']
processed_data = '{}/parsed_data'.format(config['FOLDERS']['WORKING_FOLDER'])

engine = general_helpers.connect_to_db(host, username, password, new_database)
con = engine.connect()

#get a list of table names in the database we want to copy in
command = "select table_name from information_schema.tables where table_type = 'base table' and table_schema ='{}'".format(new_database)
tables_data = con.execute(command)
tables = [table['table_name'] for table in tables_data]

con.execute("create schema {}".format(temporary_upload))
for table in tables:
    con = engine.connect()
    con.execute("create table {0}.{2} like {1}.{2}".format(temporary_upload, new_database, table))
    con.close()

engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
con = engine.connect()

mainclass = []
subclass = []

##########################################################################################
# Added code 7.30.19 for the detail_desc_length.csv creation from detail_desc_text.csv
for folder in os.listdir(processed_data):
    # load detail_desc_text, subset cols for detail_desc_length, output to .csv
    detail_desc_text_data = pd.read_csv('{}/{}/{}'.format(processed_data, folder, 'detail_desc_text.csv'), delimiter ='\t',index_col = False)
    detail_desc_length_data = detail_desc_text_data[['patent_id','length']]

    detail_desc_length_data.to_csv('{}/{}/{}'.format(processed_data, folder, 'detail_desc_length.csv'), sep ='\t',index=False)

##########################################################################################


for folder in os.listdir(processed_data):
    fields = [item for item in os.listdir('{}/{}'.format(processed_data, folder)) if not item in ['error_counts.csv', 'error_data.csv']]
    for f in fields:
        print(folder)
        print(f)
        data = pd.read_csv('{}/{}/{}'.format(processed_data, folder, f), delimiter ='\t',index_col = False)
        if not f in ['mainclass.csv', 'subclass.csv']:
            data.to_sql(f.replace('.csv', ''), con, if_exists = 'append', index = False)
        if f == 'mainclass.csv':
             mainclass.extend(list(data['id']))
        if f == 'subclass.csv':
             subclass.extend(list(data['id']))
#mainclass and subclass get added on once because they need to be unique
mainclass = pd.DataFrame(list(set(mainclass)), columns = ['id'])
mainclass.to_sql('mainclass', engine, if_exists = 'replace', index = False)
subclass = pd.DataFrame(list(set(subclass)),columns = ['id'])
subclass.to_sql('subclass', engine, if_exists = 'replace', index = False)

