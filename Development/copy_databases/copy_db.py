import os
import MySQLdb
import configparser
config = configparser.ConfigParser()
config.read('config.ini')
host = config['AWS']['HOST']
username = config['AWS']['USERNAME']
password = config['AWS']['PASSWORD']
new_database = config['AWS']['NEW_DB']
old_database = config['AWS']['OLD_DB']

# connect db
def connect_to_db():
    mydb = MySQLdb.connect(host=host,
    user=username,
    passwd=password)
    return mydb


mydb = connect_to_db()
cursor = mydb.cursor()
# create a new schema
cursor.execute("create schema " + new_database)
cursor.execute("show tables from " + old_database)
# get all tables from old db
tables = [item[0] for item in cursor.fetchall() if not item[0].startswith("temp")]
# copy everything over to new db
for table in tables:
    cursor.execute("create table " + new_database + "." + table + " like " + old_database + "." + table)
    cursor.execute("insert into " + new_database + "." + table +" select * from " + old_database + "." + table)
    mydb.commit()
