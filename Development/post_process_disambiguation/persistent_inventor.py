import MySQLdb
import os
import csv
import sys
import pandas as pd
sys.path.append('/usr/local/airflow/PatentsView-DB/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers

import configparser
config = configparser.ConfigParser()
config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')
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

#db_con.execute('alter table persistent_inventor_disambig add disamb_inventor_id_{} varchar(24)'.format(new_update_date))
data = pd.read_csv('{}/inventor_persistent.tsv'.format(disambig_folder), encoding = 'utf-8', delimiter = '\t')
data.to_sql(con=db_con, name = 'persistent_inventor_disambig', index = False, if_exists='append') #append keeps the indexes
db_con.execute('create index {0}_ix on persistent_inventor_disambig ({0});'.format(new_id_col)

#create the gender lookup table
query = '''create table gender_lookup as
select p.{} as inventor_id, t.dumale as male from
temp_inventor_gender_base_table t left join persistent_inventor_disambig p 
on t.id = p.disamb_inventor_id_20170808;'''.format(new_id_col).replace('\n', ' ')
db_con.execute(query)
db_con.close()

