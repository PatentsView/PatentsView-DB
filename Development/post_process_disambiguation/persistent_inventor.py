import MySQLdb
import os
import csv
import sys
import pandas as pd
import tqdm 
import time
import sqlalchemy

project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers

import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_output')
old_db = config['DATABASE']['OLD_DB']
new_db = config['DATABASE']['NEW_DB']

new_id_col = 'disamb_inventor_id_{}'.format(new_db[-8:])

engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
db_con = engine.connect() 

########################################################################################
# STEP 1: CREATE temp_rawinv_persistinvdisambig
# Note: will need to update this to add prev db column 
# rename to track with timestamps
print("First creating temp_rawinv_persistentinvdisambig table..............")
try:
    db_con.execute('alter table persistent_inventor_disambig rename temp_persistent_inventor_disambig_{0}'.format(old_db))

# Situation where this doesn't run: 1) persistent_inventor_disambig does not exist and was manually renamed to 
#  a different name or 2) dropped. In these cases, keep moving on to recreate table as planned
except sqlalchemy.exc.ProgrammingError as e:
    print("Alter table command did not work - see full error message below")
    print(e)
    
# ADD INDEXES to rawinventor
db_con.execute('create index rawinv_pid_ix on rawinventor (uuid, inventor_id);')

# 1. get column information from information schema
rawinv_col_info = db_con.execute("select column_name, column_type from information_schema.columns where table_schema = '{0}' and table_name = 'rawinventor' and column_name in ('uuid', 'inventor_id');".format(new_db))
    
pid_col_info = db_con.execute("select column_name, column_type from information_schema.columns where table_schema = '{0}' and table_name = 'temp_persistent_inventor_disambig_{1}';".format(new_db, old_db))

rawinv_create_str, rawinv_insert_str, rawinv_select_str = general_helpers.get_column_info(rawinv_col_info, "ri.")
pid_create_str, pid_insert_str, pid_select_str = general_helpers.get_column_info(pid_col_info, "pid.")

cols_create_str = general_helpers.get_full_column_strings(rawinv_create_str, pid_create_str)
cols_insert_str = general_helpers.get_full_column_strings(rawinv_insert_str, pid_insert_str)
cols_select_str = general_helpers.get_full_column_strings(rawinv_select_str, pid_select_str)

db_con.execute('create table if not exists temp_rawinv_persistinvdisambig({0});'.format(cols_create_str))
        
db_con.execute('insert into temp_rawinv_persistinvdisambig({0}) select {1} from rawinventor ri left join temp_persistent_inventor_disambig_{2} pid on ri.uuid = pid.current_rawinventor_id;'.format(cols_insert_str, cols_select_str, old_db))


# Make uuid primary key of temp_rawinv_persistinv_disambig
db_con.execute('alter table {0}.temp_rawinv_persistinvdisambig add primary key (uuid);'.format(new_db))

########################################################################################
# STEP 2: Get new data from temp_rawinv_persistinvdisambig and perform lookup with persistent_inventor_disambig
print("Now getting previous data from the persistent_inventor_disambig table..............")

print("getting columns......")
cols = [item[0] for item in db_con.execute('show columns from temp_persistent_inventor_disambig_{0}'.format(old_db))]

print("Now creating inventor_persistent.tsv")

outfile = csv.writer(open(disambig_folder +'/inventor_persistent.tsv','w'),delimiter='\t')
outfile.writerow(cols + [new_id_col])


limit = 300000
offset = 0
batch_counter = 0
blanks = [''for _ in cols]

while True:
	batch_counter+=1
	print('Next iteration...')
    
	# order by with primary key column - this has no nulls
	ri_pid_chunk = db_con.execute("select * from {0}.temp_rawinv_persistinvdisambig order by uuid limit {1} offset {2}".format(new_db, limit, offset))
    
	counter = 0
	for row in tqdm.tqdm(ri_pid_chunk, total=limit, desc="temp_rawinv_persistinvdisambig - batch:" + str(batch_counter)):
		# row[0:2] always fixed = uuid, inventor_id, current_rawinventor_id, old_rawinventor_id (not needed)
		# row[4:] always fixed = disambig cols 20171226,1003,0808,0528,1127...

		# OUTFILE ROW COLUMNS: current_rawinv_id, old_rawinv_id, disambig cols (previous), new_disambig col
		
        # if uuid matches current_rawinventor_id -> then it is previously existing
		if row[0] == row[2]:
			outfile.writerow([row[0], row[0]] + [item for item in row[4:]] + [row[1]])
		
        # uuid is new! no old_rawinventor id or old disambig cols
		else:
			outfile.writerow([row[0],''] + blanks + [row[1]])

		# keep going because we have chunks to process
		counter +=1

    # means we have no more batches to process
	if counter == 0:
		break


	offset = offset + limit


print('passes making inventor_persistent.tsv')

#########################################################################################
# STEP 3: Create empty table with new_db as suffix, add new_db column 
db_con.execute('create table if not exists persistent_inventor_disambig_{0} like temp_persistent_inventor_disambig_{1}'.format(new_db, old_db))


db_con.execute('alter table persistent_inventor_disambig_{0} ADD COLUMN ({1} varchar(128))'.format(new_db, new_id_col))


#########################################################################################
# STEP 4: Insert data into table 
print('Reading in inventor_persistent.tsv.............')
chunk_size = 300000

data_chunk = pd.read_csv('{}/inventor_persistent.tsv'.format(disambig_folder), encoding = 'utf-8', delimiter = '\t', chunksize=chunk_size)

print("Now inserting data into table")
batch_counter = 0
for chunk in tqdm.tqdm(data_chunk, desc="persistent_inventor_disambig - batch inserts:"):
    chunk.to_sql(con=db_con, name = 'persistent_inventor_disambig_{0}'.format(new_db), index = False, if_exists='append') 
    batch_counter += 1
#append keeps the indexes

# rename table to generic persistent_inventor_disambig
db_con.execute('alter table persistent_inventor_disambig_{0} rename persistent_inventor_disambig'.format(new_db))


db_con.execute('create index {0}_ix on persistent_inventor_disambig ({0});'.format(new_id_col))
