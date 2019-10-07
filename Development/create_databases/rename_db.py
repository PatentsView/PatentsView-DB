import os
import MySQLdb
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

# connect db
engine = general_helpers.connect_to_db(host, username, password, old_database)

con = engine.connect()
con.execute('create schema {}  default character set=utf8mb4 default collate=utf8mb4_unicode_ci'.format(new_database))
tables = [t[0] for t in con.execute('show tables from {}'.format(old_database)) if not t[0].startswith('temp')]
con.close()

tables_to_truncate = ['assignee', 'cpc_current','cpc_group', 'cpc_subgroup','cpc_subsection','inventor',
 'location','location_assignee','location_inventor',
 'patent_assignee','patent_inventor','patent_lawyer']
                       
for table in tables:
    con = engine.connect()
    con.execute('alter table {0}.{2} rename {1}.{2}'.format(old_database, new_database,table))
    if table in tables_to_truncate:
        con.execute('truncate table {}.{}'.format(new_database, table))
    con.close()

    
    
    
    