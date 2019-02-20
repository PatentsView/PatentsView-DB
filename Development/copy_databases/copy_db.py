import os
import MySQLdb
import sys
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
#this makes it run in airflow specifically, which is picky about paths

sys.path.append('/project/Development')
from helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read('/project/Development/config.ini')

host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']

# connect db
engine = general_helpers.connect_to_db(host, username, password, old_database)

# create a new schema
engine.execute("create schema {} CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;".format(new_database))


engine2 = general_helpers.connect_to_db(host, username, password, new_database)
conn = engine2.connect()
conn.execute("SET FOREIGN_KEY_CHECKS=0;")
conn.close()
with open('/usr/local/airflow/PatentsView-DB/Development/patent_schema.sql'.format(os.getcwd()), 'r') as f:
    commands = f.read().replace('\n', '').split(';')[:-1]
    for command in commands:
        conn = engine2.connect()
        conn.execute(command)
        conn.close()

#create persistent inventor disambig like last time because columns grow
conn = engine2.connect()
conn.execute('create table {}.persistent_inventor_disambig like {}.persistent_inventor_disambig'.format( new_database, old_database))
conn.close()

# we are manually defining this because otherwise it breaks every time there is a temp table someone forgot to name temp_
#TODO: will need to add to this if we add tables, I don't love this approach but need to think through improvement
tables_to_upload = ['application', 'botanic', 'brf_sum_text', 'claim', 'detail_desc_text', 'draw_desc_text', 
                    'figures', 'foreign_priority', 'foreigncitation', 'government_interest', 'government_organization', 'gender_lookup,
                    'ipcr', 'lawyer', 'mainclass', 'mainclass_current', 'nber', 'nber_category', 'nber_subcategory', 
                    'non_inventor_applicant', 'otherreference', 'patent', 'patent_contractawardnumber', 'patent_govintorg', 
                    'pct_data', 'rawassignee', 'rawexaminer', 'rawinventor', 'rawlawyer', 'rawlocation', 'rel_app_text', 
                    'subclass', 'subclass_current', 'us_term_of_grant', 'usapplicationcitation', 'uspatentcitation', 
                    'uspc', 'uspc_current', 'usreldoc', 'wipo', 'wipo_field']
                    
                    
for table in tables_to_upload:
    print(table) 
    conn = engine2.connect()
    conn.execute("insert into {0}.{2} select * from {1}.{2}".format(new_database, old_database, table))
    conn.close()
