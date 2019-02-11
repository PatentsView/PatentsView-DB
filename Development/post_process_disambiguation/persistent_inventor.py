import MySQLdb
import os
import csv
import sys
import pandas as pd
sys.path.append('/project/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers

import configparser
config = configparser.ConfigParser()
config.read('/project/Development/config.ini')
disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_out')
old_db = config['DATABASE']['OLD_DB']
new_db = config['DATABASE']['NEW_DB']
new_id_col = 'disamb_inventor_id_{}'.format(new_db[-8:])

engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
db_con = engine.connect() 

col_data = db_con.execute('show columns from {}.persistent_inventor_disambig'.format(old_db)) 
cols = [c[0] for c in col_data]
disambig_cols = [item for item in cols if item.startswith('disamb')]
cols.insert(2, new_id_col)

new_data = db_con.execute('select uuid,inventor_id from {}.rawinventor'.format(new_db))
persistent_data = db_con.execute('select * from {}.persistent_inventor_disambig'.format(old_db))

persistent_lookup = {}
for row in persistent_data:
    persistent_lookup[row['current_rawinventor_id']] = [row[item] for item in disambig_cols]
previously_existing = set(persistent_lookup.keys())

print('made lookups')

outfile = csv.writer(open(disambig_folder+'/inventor_persistent.tsv','w'),delimiter='\t')
outfile.writerow(cols)

blanks = ['' for _ in disambig_cols]

for inv in new_data:
    if inv['uuid'] in previously_existing:
        outfile.writerow([inv['uuid'], inv['uuid'], inv['inventor_id']] + persistent_lookup[inv['uuid']])
    else:
        outfile.writerow([inv['uuid'], '', inv['inventor_id']] + blanks)

db_con.execute('alter table persistent_inventor_disambig add disamb_inventor_id_{} varchar(24)'.format(new_update_date))
data = pd.read_csv('{}/inventor_persistent.tsv'.format(disambig_folder), encoding = 'utf-8', delimiter = '\t')
data.to_sql(con=db_con, name = 'persistent_inventor_disambig', index = False, if_exists='append') #append keeps the indexes
db_con.execute('create index {0}_ix on persistent_inventor_disambig ({0});'.format(new_id_col))

new_id_col = 'disamb_inventor_id_{}'.format(new_db[-8:])

query = 'create table inventor_gender (`disamb_inventor_id_{}` varchar(24) NULL, `disamb_inventor_id_20170808` varchar(24) NULL, `male` varchar(8))'.format(new_db[-8:])
db_con.execute(query)

#create inventor gender lookup
new_id_col = 'disamb_inventor_id_{}'.format(new_db[-8:])
new_to_old = con.execute('select disamb_inventor_id_20170808, {} from {}.persistent_inventor_disambig'.format(new_id_col, new_db))

ids_new_to_old = {}
for row in new_to_old:
     ids_new_to_old[row[new_id_col]] = row['disamb_inventor_id_20170808']
query = 'select id, dumale from {}.temp_inventor_gender_base_table'.format(new_db)

gender = con.execute(query)
id_to_gender = {}
for row in gender:
    id_to_gender[row['id']] = row['dumale']

results = []
for new_id, old_id in ids_new_to_old.items(): 
    if old_id is not None:
        results.append((old_id, new_id, id_to_gender[old_id]))
    else:
        results.append((None,new_id, None))
gender_df = pd.DataFrame(results)
gender_df.columns = ['disamb_inventor_id_20170808',new_id_col, 'male']

gender_df.to_sql(con=con, name = 'inventor_gender', index = False, if_exists='append')

               


