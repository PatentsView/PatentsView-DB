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

# print("getting columns......")
# print('...............................')
# cols = [item[0] for item in db_con.execute('show columns from persistent_inventor_disambig')]

# outfile = csv.writer(open(disambig_folder+'/inventor_persistent.tsv','w'),delimiter='\t')
# outfile.writerow(cols)

# print('...............................')
# print("getting data from temp_rawinv_persistinvdisambig....")
# print('making lookup.....with new logic')
# print('...............................')
#########################################################################################
# limit = 300000
# offset = 0
# batch_counter = 0
# blanks = ['','','','','']


# # 16,788,011 rows so 55.96 --> ~ 56 batches 
# while True:
# 	batch_counter+=1
# 	print('Next iteration...')
# 	# order by with primary key column - no nulls
# 	ri_pid_chunk = db_con.execute("select * from {0}.temp_rawinv_persistinvdisambig order by uuid limit {1} offset {2}".format(new_db, limit, offset))
# 	counter = 0
# 	for row in tqdm.tqdm(ri_pid_chunk, total=limit, desc="temp_rawinv_persistinvdisambig - batch:" + str(batch_counter)):
# 		# row[0] = uuid, row[1] = inventor_id, row[2] = current_rawinventor_id, row[3] = old_rawinventor_id
# 		# row[4:8] = disambig cols 20171226,1003,0808,0528,1127
# 		# row[9] = disambig col 0312 (null) ... 4,5,6,7,8
		
# 		# OUTFILE ROW: current_rawinv_id, old_rawinv_id, disambig cols (previous), new_disambig col
# 		# if uuid matches current_rawinventor_id -> previously existing
# 		if row[0] == row[2]:
# 			outfile.writerow([row[0], row[0]] + [row[4],row[5],row[6],row[7], row[8]] + [row[1]])
# 		# uuid is new! no old_rawinventor id or old disambig cols
# 		else:
# 			outfile.writerow([row[0],''] + blanks + [row[1]])

# 		# keep going because we have chunks to process
# 		counter +=1
# 	# means we have no more batches to process
# 	if counter == 0:
# 		break
# 	# FOR TESTING
# 	#if batch_counter == 3:
# 	#	break

# 	offset = offset + limit


print('passes making persistent_data.tsv')
print('...............................')


#########################################################################################

data = pd.read_csv('{}/inventor_persistent.tsv'.format(disambig_folder), encoding = 'utf-8', delimiter = '\t')

#db_con.execute('alter table persistent_inventor_disambig rename temp_persistent_inventor_disambig_backup')
db_con.execute('create table persistent_inventor_disambig like temp_persistent_inventor_disambig_backup')

data.to_sql(con=db_con, name = 'persistent_inventor_disambig', index = False, if_exists='append') #append keeps the indexes
db_con.execute('create index {0}_ix on persistent_inventor_disambig ({0});'.format(new_id_col))
