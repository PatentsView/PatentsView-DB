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
new_db = config['DATABASE']['NEW_DB']
old_db = config['DATABASE']['OLD_DB']

new_id_col = 'disamb_assignee_id_{}'.format(new_db[-8:])

engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
db_con = engine.connect()

########################################################################################
# STEP 1: CREATE temp_rawassignee_persistassignee_disambig
# rename to track with timestamps
print("First creating temp_rawassignee_persistassignee_disambig table..............")
try:
    db_con.execute('alter table persistent_assignee_disambig rename temp_persistent_assignee_disambig_{0}'.format(old_db))

# Situation where this doesn't run: 1) persistent_assignee_disambig does not exist and was manually renamed to 
#  a different name or 2) dropped. In these cases, keep moving on to recreate table as planned
except sqlalchemy.exc.ProgrammingError as e:
    print("Alter table command did not work - see full error message below")
    print(e)
    
# ADD INDEXES to rawassignee - test if this helps 
db_con.execute('create index rawassignee_pad_ix on rawassignee (uuid, assignee_id);')

# get column information from information schema, format as strings
rawassignee_col_info = db_con.execute("select column_name, column_type from information_schema.columns where table_schema = '{0}' and table_name = 'rawassignee' and column_name in ('uuid', 'assignee_id');".format(new_db))
    
pad_col_info = db_con.execute("select column_name, column_type from information_schema.columns where table_schema = '{0}' and table_name = 'temp_persistent_assignee_disambig_{1}';".format(new_db, old_db))

rawassignee_create_str, rawassignee_insert_str, rawassignee_select_str = general_helpers.get_column_info(rawassignee_col_info, "ra.")
pad_create_str, pad_insert_str, pad_select_str = general_helpers.get_column_info(pad_col_info, "pad.")


cols_create_str = general_helpers.get_full_column_strings(rawassignee_create_str, pad_create_str)
cols_insert_str = general_helpers.get_full_column_strings(rawassignee_insert_str, pad_insert_str)
cols_select_str = general_helpers.get_full_column_strings(rawassignee_select_str, pad_select_str)

db_con.execute('create table if not exists temp_rawassignee_persistassignee_disambig({0});'.format(cols_create_str))

db_con.execute('insert into temp_rawassignee_persistassignee_disambig({0}) select {1} from rawassignee ra left join temp_persistent_assignee_disambig_{2} pad on ra.uuid = pad.current_rawassignee_id;'.format(cols_insert_str, cols_select_str, old_db))

# Make uuid primary key of temp_rawassignee_persistassignee_disambig
db_con.execute('alter table {0}.temp_rawassignee_persistassignee_disambig add primary key (uuid);'.format(new_db))

########################################################################################
# STEP 2: Get new data from temp_rawassignee_persistassignee_disambig and perform lookup with persistent_assignee_disambig
print("Now getting previous data from the persistent_assignee_disambig table..............")

print("getting columns...")

cols = [item[0] for item in db_con.execute('show columns from temp_persistent_assignee_disambig_{0}'.format(old_db))]

print("Now creating assignee_persistent.tsv")

outfile = csv.writer(open(disambig_folder + '/assignee_persistent.tsv', 'w'), delimiter='\t')
outfile.writerow(cols + [new_id_col])

limit = 300000
offset = 0
batch_counter = 0
blanks = ['' for _ in cols]


while True:
	batch_counter+=1
	print('Next Iteration...')
	# order by with primary key column - this has no nulls
	ra_pad_chunk = db_con.execute("select * from {0}.temp_rawassignee_persistassignee_disambig order by uuid limit {1} offset {2}".format(new_db, limit, offset))
	counter = 0
	for row in tqdm.tqdm(ra_pad_chunk, total=limit, desc="temp_rawassignee_persistassignee_disambig - batch:" + str(batch_counter)):
		# row[0:2] always fixed = uuid, assignee_id, current_rawassignee_id, old_rawassignee_id (not needed)
		# row[4:] = disambig cols 1127...
		
		# OUTFILE ROW: current_rawassignee_id, old_rawassignee_id, disambig cols (previous), new_disambig col
		# if uuid matches current_rawassignee_id -> previously existing
        
        # if uuid matches current_rawinventor_id -> then it is previously existing
		if row[0] == row[2]:
			outfile.writerow([row[0], row[0]] + [item for item in row[4:]] + [row[1]])
            
		# uuid is new! no old_rawassignee id or old disambig cols
		else:
			outfile.writerow([row[0], ''] + blanks[2:] + [row[1]])

		# keep going because we have chunks to process
		counter +=1
        
	# means we have no more batches to process
	if counter == 0:
		break

	offset = offset + limit


print('passes making assignee_persistent.tsv')

#########################################################################################
# STEP 3: Create empty table with new_db as suffix, add new_db column 
db_con.execute('create table if not exists persistent_assignee_disambig_{0} like temp_persistent_assignee_disambig_{1}'.format(new_db, old_db))

db_con.execute('alter table persistent_assignee_disambig_{0} ADD COLUMN ({1} varchar(128))'.format(new_db, new_id_col))


#########################################################################################
# STEP 4: Insert data into table 

print('Reading in assignee_persistent.tsv.............')
chunk_size = 300000

data_chunk = pd.read_csv('{}/assignee_persistent.tsv'.format(disambig_folder), encoding = 'utf-8', delimiter='\t', chunksize=chunk_size)

batch_counter = 0
for chunk in tqdm.tqdm(data_chunk, desc='persistent_assignee_disambig - batch: ' + str(batch_counter)):
	chunk.to_sql(con=db_con, name='persistent_assignee_disambig_{0}'.format(new_db), index=False, if_exists='append') # append keeps the indexes
	batch_counter+=1

# rename table to generic persistent_assignee_disambig
db_con.execute('alter table persistent_assignee_disambig_{0} rename persistent_assignee_disambig'.format(new_db))

    
db_con.execute('create index {0}_ix on persistent_assignee_disambig ({0});'.format(new_id_col))






