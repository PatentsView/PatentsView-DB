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
old_db = config['DATABASE']['OLD_DB']

new_id_col = 'disamb_inventor_id_{}'.format(new_db[-8:])

engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
db_con = engine.connect()

############################################################
#### need to create table first  - run with old db... separately
############################################################

# create table - using 20181127 column types 
db_con.execute('create table {}.persistent_assignee_disambig(current_rawassignee_id varchar(36), old_rawassignee_id varchar(36), disamb_assignee_id_20181127 varchar(36))'.format(old_db))

# insert into table existing data - old_rawassignee_id will be NULL - first time creation
db_con.execute('insert into {}.persistent_assignee_disambig(current_rawassignee_id, disamb_assignee_id_20181127) select uuid, assignee_id from {}.rawassignee'.format(old_db))


# create table temp_ra_persistassignee_disambig(
#   uuid varchar(36),
#   assignee_id varchar(64),
#   current_rawassignee_id varchar(36),
#   old_rawassignee_id varchar(36),
#    disamb_assignee_id_20181127 varchar(36), 
#    disamb_assignee_id_20190312 varchar(64) 
# )


# insert into temp_ra_persistassignee_disambig(uuid, assignee_id, current_rawassignee_id, old_rawassignee_id, 
#  disamb_assignee_id_20181127, disamb_assginee_id_20190312 )
 
#  explain extended select ra.uuid, ra.assignee_id, pid.current_rawassignee_id, pid.old_rawassignee_id,  
 # pid.disamb_assignee_id_20181127, pid.disamb_assignee_id_20190312 from rawassignee ra left join persistent_assignee_disambig pid on ra.uuid = pid.current_rawassignee_id;
 

# old_rawassignee_id will be NULL  - 1st time creating the table
############################################################
############################################################
# disamb_assignee_id has type varchar(64) in patent_20190312
db_con.execute('alter table persistent_assignee_disambig ADD COLUMN ({} varchar(64))').format(new_id_col)
print("getting columns...")
print('...............................')
cols = [item[0] for item in db_con.execute('show columns from persistent_assignee_disambig')]

outfile = csv.writer(open(disambig_folder + '/assignee_persistent.tsv', 'w'), delimiter='\t')
outfile.writerow(cols)


print('...............................')
print("getting data from temp_rawinv_persistinvdisambig....")
print('making lookup.....')
print('...............................')


limit - 300000
offset = 0
batch_counter = 0
# only '' applies to 1 disambig col for now
blanks = ['']


while True:
	batch_counter+=1
	print('Next Iteration...')
	# order by with primary key column - no nulls
	ri_pid_chunk = db_con.execute("select * from {0}.temp_ra_persistassignee_disambig order by uuid limit {1} offset {2}".format(new_db, limit, offset))
	counter = 0
	for row in tqdm.tqdm(ri_pid_chunk, total=limit, desc="temp_ra_persistassignee_disambig - batch:" + str(batch_counter)):
		# row[0] = uuid, row[1] = assignee_id, row[2] = current_rawassignee_id, row[3] = old_rawassignee_id
		# row[4] = disambig col 1127
		# row[5] = disambig col 0312 (null)
		
		# OUTFILE ROW: current_rawassignee_id, old_rawassignee_id, disambig cols (previous), new_disambig col
		# if uuid matches current_rawassignee_id -> previously existing
		if row[0] == row[2]:
			outfile.writerow([row[0], row[0]] + [row[4]] + [row[1]])
		# uuid is new! no old_rawassignee id or old disambig cols
		else:
			outfile.writerow([row[0], ''] + blanks + [row[1]])

		# keep going because we have chunks to process
		counter +=1
	# means we have no more batches to process
	if counter == 0:
		break
	# FOR TESTING
	#if batch_counter == 3:
	#	break

	offset = offset + limit


print('passes making assignee_persistent.tsv')
print('...............................')


data = pd.read_csv('{}/assignee_persistent.tsv'.format(disambig_folder), encoding = 'utf-8', delimiter='\t')


db_con.execute('alter table persistent_assignee_disambig rename temp_persistent_assignee_disambig_backup')
db_con.execute('create table persistent_assignee_disambig like temp_persistent_assignee_disambig_backup')

data.to_sql(con=db_con, name = 'persistent_assignee_disambig', index = False, if_exists='append') #append keeps the indexes
db_con.execute('create index {0}_ix on persistent_assignee_disambig ({0});'.format(new_id_col))




