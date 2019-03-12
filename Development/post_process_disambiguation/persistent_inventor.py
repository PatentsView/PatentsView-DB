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

db_con.execute('alter table persistent_inventor_disambig ADD COLUMN ({} varchar(36))'.format(new_id_col))

new_data = db_con.execute('select uuid,inventor_id from {}.rawinventor'.format(new_db))
persistent_data = db_con.execute('select * from {}.persistent_inventor_disambig'.format(new_db))

cols = [item[0] for item in db_con.execute('show columns from persistent_inventor_disambig')]
disambig_cols = [col for col in cols if col.startswith('disamb')]

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
data = pd.read_csv('{}/inventor_persistent.tsv'.format(disambig_folder), encoding = 'utf-8', delimiter = '\t')

db_con.execute('alter table persistent_inventor_disambig rename temp_persistent_inventor_disambig_backup')
db_con.execute('create table persistent_inventor_disambig like temp_persistent_inventor_disambig_backup')

data.to_sql(con=db_con, name = 'persistent_inventor_disambig', index = False, if_exists='append') #append keeps the indexes
db_con.execute('create index {0}_ix on persistent_inventor_disambig ({0});'.format(new_id_col))
