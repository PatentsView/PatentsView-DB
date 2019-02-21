import os
import MySQLdb
import sys
from helpers import general_helpers
project_home = os.environ['PACKAGE_HOME']
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
tables = [t[0] for t in con.execute('show tables from {}'.format(new_database))]
con.close()
for table in tables:
    con = engine.connect()
    con.execute('alter table {0}.{2} rename {1}.{2}'.format(old_database, new_database,table))
    con.close()