import MySQLdb
import os
import csv
import sys
import pandas as pd
import tqdm

project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers

import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_output')
new_db = config['DATABASE']['NEW_DB']
old_db = config['DATABASE']['OLD_DB']

new_id_col = 'disamb_inventor_id_{}'.format(new_db[-8:])

engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
db_con = engine.connect() 

# create indexes as needed
#db_con.execute('create index ix_inv_gender on {}.inventor_gender (disamb_inventor_id_20170808, male);'.format(old_db))
#db_con.execute('create index ix_disamb_inv_id_20170808 on {}.inventor_gender (disamb_inventor_id_20170808);'.format(old_db)) 
#db_con.execute('create index ix_disamb_for_invgender on persistent_inventor_disambig (disamb_inventor_id_20170808, disamb_inventor_id_20181127, {0});'.format(new_id_col))
#db_con.execute('create index ix_disamb_20170808_invgender on persistent_inventor_disambig(disamb_inventor_id_20170808)')
print("creating inventor gender lookup")
#create inventor gender lookup
id_to_gender = {}

batch_counter = 0
limit = 300000
offset = 0
while True:
	batch_counter+=1
	print('Next iteration') 
	counter = 0
	inv_gender_chunk = db_con.execute('select disamb_inventor_id_20170808, male from {0}.inventor_gender order by disamb_inventor_id_20170808 limit {1} offset {2}'.format(old_db,limit, offset))
	for row in tqdm.tqdm(inv_gender_chunk, total=limit, desc="inventor_gender processing - batch:" + str(batch_counter)):
		id_to_gender[row[0]] = row[1]
		counter+=1
	# means we have no more batches to process
	if counter==0:
		break

	# for testing
	#if batch_counter == 1:
	#	break
	offset = offset + limit

results = []
########################################################################################
print("now getting results")
batch_counter = 0
limit = 300000
offset = 0

processed_ids = {}

while True:
	batch_counter+=1
	print('Next iteration')
	counter=0
	pid_chunk = db_con.execute('select disamb_inventor_id_20170808, disamb_inventor_id_20181127, {0} from persistent_inventor_disambig order by disamb_inventor_id_20170808 limit {1} offset {2}'.format(new_id_col,limit, offset))
	for row in tqdm.tqdm(pid_chunk, total=limit, desc="persistent inventor processing - batch:" + str(batch_counter)):
		# new id not seen before
		if row[0] not in processed_ids.keys():
			processed_ids[row[0]] = row[2]

			if row[0] is not None and row[0] in id_to_gender.keys():
				results.append((row[0], row[1], id_to_gender[row[0]], row[2]))

			else:
				results.append((None, row[1], None, row[2]))

		counter+=1
	# means we have no more batches to process
	if counter==0:
		break

	# for testing
	#if batch_counter == 3:
	#	break

	offset = offset + limit

print("now inserting into table")
gender_df = pd.DataFrame(results)
gender_df.columns = ['disamb_inventor_id_20170808','disamb_inventor_id_20181127', 'male',new_id_col]

gender_df.to_csv('{}/inventor_gender.tsv'.format(disambig_folder),encoding='utf-8', header=True,index=False)

db_con.execute('alter table inventor_gender rename temp_inventor_gender')
db_con.execute('create table inventor_gender like {}.inventor_gender'.format(old_db))

db_con.execute('alter table inventor_gender add column ({} varchar(36))'.format(new_id_col))

chunk_size_sql = 300000
gender_df.to_sql(con=db_con, name = 'inventor_gender', index = False, if_exists='append', chunksize =chunk_size_sql, method=multi)
