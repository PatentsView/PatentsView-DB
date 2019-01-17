import os
import MySQLdb
import sys
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
#this makes it run in airflow specifically, which is picky about paths
#sys.path.append('/project/Development')
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
engine2.execute("SET FOREIGN_KEY_CHECKS=0;")
with open('/project/Development/patent_schema.sql'.format(os.getcwd()), 'r') as f:
    commands = f.read().replace('\n', '').split(';')[:-1]
    for command in commands:
        engine2.execute(command)

#create persistent inventor disambig like last time because columns grow
engine2.execute('create table {}.persistent_inventor_disambig like {}.persistent_inventor_disambig'.format( new_database, old_database))


#all_tables = engine.execute("show tables from {}".format(old_database))
#
##these are the tables that get recreated from scratch each time
#tables_skipped = ['assignee', 'cpc_current', 'cpc_group', 'cpc_subgroup', 'cpc_subsection', 'inventor', 
#        'lawyer', 'location', 'location_assignee', 'location_inventor', 'patent_assignee','patent_inventor', 
#        'patent_lawyer', 'uspc_current', 'wipo']
#
#tablenames = [item['Tables_in_{}'.format(old_database)] for item in all_tables]
#tables_to_upload = [table for table in tablenames if not table.startswith("temp") and not table in tables_skipped]
# we are manually defining this because otherwise it breaks every time there is a temp table someone forgot to name temp_
#will need to add to this if we add tables, I don't love this approach but need to think through improvement
tables_to_upload = ['application', 'botanic', 'brf_sum_text', 'claim', 'detail_desc_text', 'draw_desc_text', 
                    'figures', 'foreign_priority', 'foreigncitation', 'government_interest', 'government_organization', 
                    'ipcr', 'lawyer', 'mainclass', 'mainclass_current', 'nber', 'nber_category', 'nber_subcategory', 
                    'non_inventor_applicant', 'otherreference', 'patent', 'patent_contractawardnumber', 'patent_govintorg', 
                    'pct_data', 'rawassignee', 'rawexaminer', 'rawinventor', 'rawlawyer', 'rawlocation', 'rel_app_text', 
                    'subclass', 'subclass_current', 'us_term_of_grant', 'usapplicationcitation', 'uspatentcitation', 
                    'uspc', 'uspc_current', 'usreldoc', 'wipo', 'wipo_field']
                    
                    
for table in tables_to_upload:
    print(table) 
    engine2.execute("insert into {0}.{2} select * from {1}.{2}".format(new_database, old_database, table))
