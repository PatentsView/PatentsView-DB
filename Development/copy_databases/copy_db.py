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
engine = general_helpers.connect_to_db(host, username, password, old_database)
# create a new schema
engine.execute("create schema {}".format(new_database))
all_tables = engine.execute("show tables from {}".format(old_database))
# get the tables we need from the old database
#these are the tables that get recreated from scratch each time
tables_skipped = ['assignee', 'cpc_current', 'cpc_group', 'cpc_subgroup', 'cpc_subsection', 'inventor', 
        'lawyer', 'location_assignee', 'location_inventor', 'patent_assignee','patent_inventor', 
        'patent_lawyer', 'uspc_current', 'wipo']
#print([item.keys() for item in all_tables])
tables = [item['Tables_in_{}'.format(old_database)] for item in all_tables if not item['Tables_in_{}'.format(old_database)].startswith("temp") and not item['Tables_in_{}'.format(old_database)] in tables_skipped]
# copy everything over to new db
for table in tables:
    print(table)
    engine.execute("create table {0}.{2} like {1}.{2}".format(new_database, old_database, table))
    #engine.execute("insert into {0}.{2} select * from {1}.{2}".format(new_database, old_database, table))
