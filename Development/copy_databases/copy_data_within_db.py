import os
import MySQLdb
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
host = config['AWS']['HOST']
username = config['AWS']['USERNAME']
password = config['AWS']['PASSWORD']
new_db = config['AWS']['NEW_DB']
temporary_upload_db = config['AWS']['TEMPORARY_UPLOAD_DB']

#a function to connect to mysql db
def connect_to_db():
    mydb = MySQLdb.connect(host=host,
    user=username,
    passwd=password,
    local_infile = 1) #must have this line

    return mydb
mydb = connect_to_db()
cursor = mydb.cursor()

#get a list of table names in the database we want to copy in
command = "select table_name from information_schema.tables where table_type = 'base table' and table_schema ='"+new_db+"'"
cursor.execute(command)
tables_tuple = cursor.fetchall()
tables = [table[0] for table in tables_tuple]

# query to insert db
for table in tables:
    print(table)
    insert_table_command = "INSERT INTO {0}.{1} SELECT * FROM {2}.{1}".format(temporary_upload_db, table, new_db)
    cursor.execute(insert_table_command)
