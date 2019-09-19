import MySQLdb
import os
import csv
import sys
import pandas as pd
import tqdm
import sqlalchemy

project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers

import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_output')
new_db = config['DATABASE']['NEW_DB']
old_db = config['DATABASE']['OLD_DB']

old_id_col = 'disamb_inventor_id_{}'.format(old_db[-8:])
new_id_col = 'disamb_inventor_id_{}'.format(new_db[-8:])

engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
db_con = engine.connect() 

########################################################################################
# STEP 1: CREATE indexes as needed for inventor_gender (2) and persistent_inventor_disambig(2)
print("First rename inventor_gender ............")

# always use 20170808 inventor id here 
try:
    db_con.execute('alter table inventor_gender rename temp_inventor_gender_{0}'.format(old_db))

# Situation where this doesn't run: 1) inventor_gender does not exist and was manually renamed to 
#  a different name or 2) dropped. In these cases, keep moving on to recreate table as planned
except sqlalchemy.exc.ProgrammingError as e:
    print("Alter table command did not work - see full error message below")
    print(e)
    
# db_con.execute('create index ix_inv_gender on temp_inventor_gender_{0} (disamb_inventor_id_20170808, male);'.format(old_db))
#
# # for order by
# db_con.execute('create index ix_disamb_inv_id_20170808 on temp_inventor_gender_{0}(disamb_inventor_id_20170808);'.format(old_db))
#
# db_con.execute('create index ix_disamb_invgender on persistent_inventor_disambig (disamb_inventor_id_20170808, {0}, {1});'.format(old_id_col,new_id_col))
#
# # for order by
# db_con.execute('create index ix_disamb_20170808_invgender on persistent_inventor_disambig(disamb_inventor_id_20170808);')


########################################################################################
# STEP 2: Create lookup with old data 

print("Creating inventor gender lookup dictionary")

id_to_gender = {}

batch_counter = 0
limit = 300000
offset = 0

while True:
	batch_counter+=1
	print('Next iteration') 
	counter = 0
    
	inv_gender_chunk = db_con.execute('select disamb_inventor_id_20170808, male from temp_inventor_gender_{0} order by disamb_inventor_id_20170808 limit {1} offset {2}'.format(old_db,limit, offset))
    
	for row in tqdm.tqdm(inv_gender_chunk, total=limit, desc="inventor_gender processing - batch:" + str(batch_counter)):
		id_to_gender[row[0]] = row[1]
		counter+=1
        
	# means we have no more batches to process
	if counter==0:
		break

	offset = offset + limit

########################################################################################
# STEP 3: Get new data , write inventor_gender.tsv
results = []
batch_counter = 0
limit = 300000
offset = 0

processed_ids = set()

while True:
	batch_counter+=1
	print('Next iteration')
	counter=0
	pid_chunk = db_con.execute('select disamb_inventor_id_20170808, {0}, {1} from persistent_inventor_disambig order by disamb_inventor_id_20170808 limit {2} offset {3}'.format(old_id_col, new_id_col, limit, offset))
	for row in tqdm.tqdm(pid_chunk, total=limit, desc="persistent inventor processing - batch:" + str(batch_counter)):
		# row[0] =  disamb_inventor_id_20170808
        # row[1:len(row) - 1] = disamb_inventor_id_201127... previous cols 
        # row[len(row) - 1] = most recent db col
        # if 20170808 id has not been seen previously already, add to processed ids list
		if row[0] not in processed_ids:
			processed_ids.add(row[0])
            
            # if 20170808 id exists and it is in the inventor_gender table, we have gender info!
			if row[0] is not None and row[0] in id_to_gender.keys():
				results.append([row[0]] + [row[1:len(row)-1]] + [row[len(row) - 1]] + [id_to_gender[row[0]]])
			


		counter+=1
	# means we have no more batches to process
	if counter==0:
		break

	# for testing
	#if batch_counter == 3:
	#	break

	offset = offset + limit

print("now creating .tsv")
gender_df = pd.DataFrame(results)
# will always take prior db id and new id with 20170808
gender_df.columns = ['disamb_inventor_id_20170808',old_id_col, new_id_col, 'male']


########################################################################################
# STEP 3: insert data from inventor_gender.tsv
gender_df.to_csv('{}/inventor_gender.tsv'.format(disambig_folder),encoding='utf-8', header=True,index=False, sep='\t')
gender_df = pd.read_csv('{}/inventor_gender.tsv'.format(disambig_folder),encoding='utf-8',delimiter='\t')

db_con.execute('create table inventor_gender_{0} like temp_inventor_gender_{1}'.format(new_db, old_db))

db_con.execute('alter table inventor_gender_{0} add column ({1} varchar(128))'.format(new_db, new_id_col))
# need to move column to be before male column to match inventor_gender.tsv
male_col_type_ig = db_con.execute("select column_type from information_schema.columns where table_schema = '{0}' and table_name = 'inventor_gender_{0}' and column_name = 'male';".format(new_db))

#only have one item, col type of male
male_col_type = [item[0] for item in male_col_type_ig]

db_con.execute('alter table inventor_gender_{0} modify column male {1} after {2}'.format(new_db, male_col_type[0], new_id_col))

print("print now inserting into table.....")
chunk_size_sql = 300000
gender_df.to_sql(con=db_con, name = 'inventor_gender_{0}'.format(new_db), index = False, if_exists='append', chunksize =chunk_size_sql)


# rename table to generic inventor gender
db_con.execute('drop table if exists inventor_gender')
db_con.execute('alter table inventor_gender_{0} rename inventor_gender'.format(new_db))

