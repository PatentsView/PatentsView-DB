import os
import configparser
import sys
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers
import configparser
import json


config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']

engine = general_helpers.connect_to_db(host, username, password, new_database)

#get a list of table names in the database we want to copy in
command = "select table_name from information_schema.tables where table_type = 'base table' and table_schema ='{}'".format(temporary_upload)
tables_data = engine.execute(command)
tables = [table['table_name'] for table in tables_data]
tables = ['mainclass', 'rawinventor_backup', 'rawassignee', 'cpc_subgroup', 'cpc_current', 'wipo', 'usreldoc', 'figures', 'brf_sum_text', 'patent', 'us_term_of_grant', 'rel_app_text', 'location_assignee', 'assignee', 'rawinventor', 'rawlocation', 'subclass_current', 'usapplicationcitation', 'location', 'nber_category', 'non_inventor_applicant_backup', 'uspatentcitation', 'patent_inventor', 'patent_govintorg_reporting', 'detail_desc_length', 'patent_lawyer', 'nber_subcategory', 'rawlawyer', 'persistent_inventor_disambig', 'patent_assignee', 'otherreference', 'claim', 'patent_contractawardnumber', 'draw_desc_text', 'wipo_backup', 'ipcr', 'uspc_current', 'location_inventor', 'cpc_group', 'rawexaminer', 'rawassignee_backup', 'subclass', 'botanic', 'cpc_subsection', 'inventor', 'foreign_priority', 'application']
engine.execute("SET FOREIGN_KEY_CHECKS=0;")

engine.dispose()


status_file=project_home + '/Development/create_databases/status.json'
try:
    current_status=json.load(open(status_file))
except OSError as e:
    print(e)
    current_status={}

# query to insert db
for table in tables:
    engine = general_helpers.connect_to_db(host, username, password, new_database)
    engine.execute("SET FOREIGN_KEY_CHECKS=0;")
    print(table, flush=True)
    if table in current_status and current_status[table]==1:
        continue
    insert_table_command = "INSERT INTO {0}.{2} SELECT * FROM {1}.{2}".format(new_database, temporary_upload, table)
    if table=="mainclass" or table=="subclass":
        insert_table_command = "INSERT IGNORE INTO {0}.{2} SELECT * FROM {1}.{2}".format(new_database, temporary_upload, table)
    engine.execute("SET SESSION net_read_timeout=3000;"+"SET SESSION net_write_timeout=3000;"+insert_table_command)
    current_status[table]=1
    json.dump(current_status, open(status_file,"w"))
    engine.dispose()
