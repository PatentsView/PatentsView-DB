import os
import MySQLdb
import configparser

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

mydb = general_helpers.connect_to_db(host, username, password, new_database)
cursor = mydb.cursor()

#get a list of table names in the database we want to copy in
command = "select table_name from information_schema.tables where table_type = 'base table' and table_schema ={}'".format(temporary_upload))
cursor.execute(command)
tables = [table[0] for table in cursor.fetchall()]]

# query to insert db
for table in tables:
    print(table)
    insert_table_command = "INSERT INTO {0}.{2} SELECT * FROM {1}.{2}".format(new_database, temporary_upload, table))
    cursor.execute(insert_table_command)
