import os
import configparser
import sys
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']

engine = general_helpers.connect_to_db(host, username, password, new_database)

# #get a list of table names in the database we want to copy in
# command = "select table_name from information_schema.tables where table_type = 'base table' and table_schema ='{}'".format(new_database)
# tables_data = engine.execute(command)
# tables = [table['table_name'] for table in tables_data]

# # query to insert db
# for table in tables:
#     print(table)
#     insert_table_command = "INSERT INTO {0}.{2} SELECT * FROM {1}.{2}".format(new_database, temporary_upload, table)
#     engine.execute(insert_table_command)

# engine.execute("SET FOREIGN_KEY_CHECKS=1;")