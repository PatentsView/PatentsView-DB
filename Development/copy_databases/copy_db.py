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

engine2 = general_helpers.connect_to_db(host, username, password, new_database)
with open('{}/Development/patent_schema.sql'.format(os.getcwd()), 'r') as f:
    commands = f.read().replace('\n', '').split(';')[:-1]
    for command in commands:
        engine2.execute(command)


all_tables = engine.execute("show tables from {}".format(old_database))

#these are the tables that get recreated from scratch each time
tables_skipped = ['assignee', 'cpc_current', 'cpc_group', 'cpc_subgroup', 'cpc_subsection', 'inventor', 
        'lawyer', 'location_assignee', 'location_inventor', 'patent_assignee','patent_inventor', 
        'patent_lawyer', 'uspc_current', 'wipo']

tablenames = [item['Tables_in_{}'.format(old_database)] for item in all_tables]
tables_to_upload = [table for table in tablenames if not table.startswith("temp") and not table in tables_skipped]
# copy everything over to new db
for table in tables_to_upload:
    engine2.execute("insert into {0}.{2} select * from {1}.{2}".format(new_database, old_database, table))
