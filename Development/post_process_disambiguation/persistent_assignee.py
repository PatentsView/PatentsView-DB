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
old_db = config['DATABASE']['OLD_DB']

new_id_col = 'disamb_inventor_id_{}'.format(new_db[-8:])

engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
db_con = engine.connect()

########################################################################################
# STEP 1: CREATE temp_rawassignee_persistassignee_disambig
# Note: will need to update this to add prev db column 

# ADD INDEXES to rawassignee
#db_con.execute('create index {0}_rawassignee_pad_ix on rawassignee (uuid, assignee_id);'.format(new_db))

# to test: need to add index to all columns in a table? 

db_con.execute('create table temp_rawassignee_persistassignee_disambig( uuid varchar(36), assignee_id varchar(64),current_rawassignee_id varchar(36), old_rawassignee_id varchar(36), disamb_assignee_id_20181127 varchar(36))')

db_con.execute('insert into temp_rawassignee_persistassignee_disambig(uuid, assignee_id, current_rawassignee_id, old_rawassignee_id, disamb_assignee_id_20181127, disamb_assginee_id_20190312) select ra.uuid, ra.assignee_id, pid.current_rawassignee_id, pid.old_rawassignee_id, pid.disamb_assignee_id_20181127, pid.disamb_assignee_id_20190312 from rawassignee ra left join persistent_assignee_disambig pid on ra.uuid = pid.current_rawassignee_id;')


# ADD INDEXES to uuid for temp_rawassignee_persistassignee_disambig
db_con.execute('create index {0}_rawassignee_pad_ix on temp_rawassignee_persistassignee_disambig (uuid);'.format(new_db))

########################################################################################
# STEP 2: Get new data from temp_rawassignee_persistassignee_disambig and perform lookup with persistent_assignee_disambig
print("Now getting previous data from the persistent_assignee_disambig table..............")

print("getting columns...")

cols = [item[0] for item in db_con.execute('show columns from persistent_assignee_disambig')]

print("Now creating assignee_persistent.tsv")

outfile = csv.writer(open(disambig_folder + '/assignee_persistent.tsv', 'w'), delimiter='\t')
outfile.writerow(cols)

limit = 300000
offset = 0
batch_counter = 0
blanks = ['' for _ in cols]


while True:
	batch_counter+=1
	print('Next Iteration...')
	# order by with primary key column - this has no nulls
	ra_pid_chunk = db_con.execute("select * from {0}.temp_rawassignee_persistassignee_disambig order by uuid limit {1} offset {2}".format(new_db, limit, offset))
	counter = 0
	for row in tqdm.tqdm(ra_pid_chunk, total=limit, desc="temp_rawassignee_persistassignee_disambig - batch:" + str(batch_counter)):
		# row[0:2] always fixed = uuid, assignee_id, current_rawassignee_id, old_rawassignee_id (not needed)
		# row[4:] = disambig cols 1127...
		
		# OUTFILE ROW: current_rawassignee_id, old_rawassignee_id, disambig cols (previous), new_disambig col
		# if uuid matches current_rawassignee_id -> previously existing
        
        # if uuid matches current_rawinventor_id -> then it is previously existing
		if row[0] == row[2]:
			outfile.writerow([row[0], row[0]] + row[4:] + [row[1]])
            
		# uuid is new! no old_rawassignee id or old disambig cols
		else:
			outfile.writerow([row[0], ''] + blanks + [row[1]])

		# keep going because we have chunks to process
		counter +=1
        
	# means we have no more batches to process
	if counter == 0:
		break

	offset = offset + limit


print('passes making assignee_persistent.tsv')

#########################################################################################
# STEP 3: Move prior data to renamed table with old_db as suffix, create empty table with new_db as suffix, add new_db column 
try:
    db_con.execute('alter table persistent_assignee_disambig rename temp_persistent_assignee_disambig_{}'.format(old_db))

# Situation where this doesn't run: 1) persistent_assignee_disambig does not exist and was manually renamed to 
#  a different name or 2) dropped. In these cases, keep moving on to recreate table as planned
except sqlalchemy.exc.ProgrammingError as e:
    print("Alter table command did not work - see full error message below")
    print(e)
    

db_con.execute('create table persistent_assignee_disambig_{0} like temp_persistent_assignee_disambig_{1}'.format(new_db, old_db))


db_con.execute('alter table persistent_assignee_disambig_{0} ADD COLUMN ({} varchar(64))'.format(new_db, new_id_col))


#########################################################################################
# STEP 4: Insert data into table 


print('Reading in assignee_persistent.tsv.............')
chunk_size = 300000

data_chunk = pd.read_csv('{}/assignee_persistent.tsv'.format(disambig_folder), encoding = 'utf-8', delimiter='\t', chunksize=chunk_size)

batch_counter = 0
for chunk in tqdm.tqdm(data_chunk, desc='persistent_assignee_disambig - batch: ' + str(batch_counter)):
	chunk.to_sql(con=db_con, name='persistent_assignee_disambig_{0}'.format(new_db), index=False, if_exists='append') # append keeps the indexes
	batch_counter+=1

# rename table to generic persistent_inventor_disambig
db_con.execute('alter table persistent_assignee_disambig_{0} rename persistent_assignee_disambig'.format(new_db))

    
db_con.execute('create index {0}_ix on persistent_assignee_disambig ({0});'.format(new_id_col))






