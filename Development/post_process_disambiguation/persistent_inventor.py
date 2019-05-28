import MySQLdb
import os
import csv
import sys
import pandas as pd
import tqdm 
import time 

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

#db_con.execute('alter table persistent_inventor_disambig ADD COLUMN ({} varchar(36))'.format(new_id_col))

print("getting raw inventor data....")
print('...............................')
#new_data = db_con.execute('select uuid,inventor_id from {}.rawinventor'.format(new_db))

##### new logic for offset & writeup with rawinventor
#########################################################################################
limit = 300000
offset = 0
batch_counter = 0
new_data = []
# 15,965,198 rows = 53.21 batches ~ 54 batches 
while True:
	batch_counter+=1
	print('Next iteration...')
	# order by with primary key column - no nulls
	raw_inv_chunk = db_con.execute("select uuid,inventor_id from {0}.rawinventor order by uuid limit {1} offset {2}".format(new_db, limit, offset))
	counter = 0
	for row in tqdm.tqdm(raw_inv_chunk, total=limit, desc="rawinventor processing - batch:" + str(batch_counter)):
		# row[0] = uuid, row[1] = inventor_id
		new_data.append(row)
		# keep going because we have chunks to process
		counter +=1
	# means we have no more batches to process
	if counter == 0:
		break
	# FOR TESTING
	#if batch_counter == 3:
	#	break
	offset = offset + limit


#print(new_data[0:3])



#########################################################################################
##### old logic for persistent data
#persistent_data = db_con.execute('select * from {}.persistent_inventor_disambig'.format(new_db))
#print('passes persistent data')
print("getting columns......")
print('...............................')
cols = [item[0] for item in db_con.execute('show columns from persistent_inventor_disambig')]
disambig_cols = [col for col in cols if col.startswith('disamb')]

print('making lookup.....with new logic')
print('...............................')
persistent_lookup = {}
#for row in persistent_data:
#    persistent_lookup[row['current_rawinventor_id']] = [row[item] for item in disambig_cols]

##### new logic for offset & writeup
# every 300,000 rows in a chunk
# 13,127,863 rows - 43.76 batches ~ so 44 batches
limit = 300000
offset = 0
batch_counter = 0

while True:
	batch_counter+=1
	print('Next iteration...')
	# order by with primary key column - no nulls
	persist_inv_chunk = db_con.execute("select * from {0}.persistent_inventor_disambig order by current_rawinventor_id limit {1} offset {2}".format(new_db, limit, offset))
	counter = 0
	for row in tqdm.tqdm(persist_inv_chunk, total=limit, desc="Persistent_inventor_disambig processing - batch:" + str(batch_counter)):
		#row[0] = 'current_rawinventor_id', row[2:len(row)] = disambig cols
		persistent_lookup[row[0]] = [row[2:len(row)]]
		# keep going because we have chunks to process
		counter +=1
	
	# means we have no more batches to process
	if counter == 0:
		break

	offset = offset + limit


previously_existing = set(persistent_lookup.keys())

print('made lookups')

outfile = csv.writer(open(disambig_folder+'/inventor_persistent.tsv','w'),delimiter='\t')


outfile.writerow(cols)

		
blanks = ['' for _ in disambig_cols]

for inv in new_data:
	if inv[0] in previously_existing:
		outfile.writerow([inv[0], inv[0], inv[1]] + persistent_lookup[inv[0]])
	else:
		outfile.writerow([inv[0], '', inv[1]] + blanks)
data = pd.read_csv('{}/inventor_persistent.tsv'.format(disambig_folder), encoding = 'utf-8', delimiter = '\t')

db_con.execute('alter table persistent_inventor_disambig rename temp_persistent_inventor_disambig_backup')
db_con.execute('create table persistent_inventor_disambig like temp_persistent_inventor_disambig_backup')

data.to_sql(con=db_con, name = 'persistent_inventor_disambig', index = False, if_exists='append') #append keeps the indexes
db_con.execute('create index {0}_ix on persistent_inventor_disambig ({0});'.format(new_id_col))
