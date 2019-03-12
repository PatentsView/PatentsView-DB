import MySQLdb
import os
import csv
import sys
import pandas as pd
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers

import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_output')
new_db = config['DATABASE']['NEW_DB']

new_id_col = 'disamb_inventor_id_{}'.format(new_db[-8:])

engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
db_con = engine.connect() 

#create inventor gender lookup
new_to_old = db_con.execute('select disamb_inventor_id_20170808, {} from persistent_inventor_disambig'.format(new_id_col))

ids_new_to_old = {}
for row in new_to_old:
    #this is hard coded because it is the year we have the gender lookup from
    ids_new_to_old[row[new_id_col]] = row['disamb_inventor_id_20170808']

gender = db_con.execute('select disamb_inventor_id_20170808, male from inventor_gender')
id_to_gender = {}
for row in gender:
    id_to_gender[row['disamb_inventor_id_20170808']] = row['male']

results = []
for new_id, old_id in ids_new_to_old.items(): 
    if old_id is not None:
        results.append((old_id, new_id, id_to_gender[old_id]))
    else:
        results.append((None,new_id, None))
gender_df = pd.DataFrame(results)
gender_df.columns = ['disamb_inventor_id_20170808',new_id_col, 'male']

db_con.execute('alter table inventor_gender rename temp_inventor_gender')
db_con.execute('create table inventor_gender like temp_inventor_gender')
cols = [col[0] for col in db_con.execute('show columns from inventor_gender')]
old_col = [col for col in cols if not col in ('disamb_inventor_id_20170808', 'male')][0]
db_con.execute('alter table inventor_gender drop column {}'.format(old_col))
db_con.execute('alter table inventor_gender add column ({} varchar(36))'.format(new_id_col))


gender_df.to_sql(con=db_con, name = 'inventor_gender', index = False, if_exists='append')
