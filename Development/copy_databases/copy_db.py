import os
import MySQLdb
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

# connect db
mydb = general_helpers.connect_to_db(host, username, password, old_database)
cursor = mydb.cursor()
# create a new schema
cursor.execute("create schema {}".format(new_database))
cursor.execute("show tables from {}".format(old_database))
# get the tables we need from the old database
#these are the tables that get recreated from scratch each time
tables_skipped = ['assignee', 'cpc_current', 'cpc_group', 'cpc_subgroup', 'cpc_subsection', 'inventor', 
        'lawyer', 'location_assignee', 'location_inventor', 'patent_assignee','patent_inventor', 
        'patent_lawyer', 'uspc_current', 'wipo']
tables = [item[0] for item in cursor.fetchall() if not item[0].startswith("temp") and not item[0] in tables_skipped]
# copy everything over to new db
for table in tables:
    print(table)
    cursor.execute("create table {0}.{2} like {1}.{2}".format(new_database, old_database, table))
    #cursor.execute("insert into {0}.{2} select * from {1}.{2}".format(new_database, old_database, table))
    mydb.commit()
