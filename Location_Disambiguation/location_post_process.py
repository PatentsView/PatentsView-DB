import os
import csv
import sys
import pandas as pd
import string
import MySQLdb
sys.path.append("To_clone")
from ConfigFiles import config_second as config
sys.path.append("Code/PatentsView-DB/Scripts")

def fix_duplicate_location_ids(host, username, password, database):
	mydb = MySQLdb.connect(host= host, user=username, passwd=password, db =database)
	cursor = mydb.cursor()
	try:
		cursor.execute('create index raw_loc_tranformed_ix on rawlocation(location_id_transformed)')
		mydb.commit()
	except:
		pass
	locs_for_dict = pd.read_sql("select * from location;", mydb)
	list_of_loc_ids = set(list(locs_for_dict['id']))
	query1 = "select * from (select location_id_transformed, count(distinct location_id) as count from rawlocation group by location_id_transformed) as a where count >1;"
	locs_to_update = pd.read_sql(query1, mydb)
	problems = list(locs_to_update['location_id_transformed'])

	missed_locs = []
	multiple = []
	counter = 0
	for lat_long_id in problems:
	    counter +=1
	    if counter%500 == 0:
	        print counter
	    query1 = "select distinct location_id from rawlocation where location_id_transformed = '" + lat_long_id + "';"
	    raw_locs = pd.read_sql(query1, mydb)
	    found_loc_ids = list(raw_locs['location_id'])
	    num_found = 0
	    for raw_loc_id in found_loc_ids:
	        if raw_loc_id in list_of_loc_ids:
	            correct_id = raw_loc_id
	            #print raw_loc_id
	            num_found +=1
	    if num_found == 0:
	        missed_locs.append(lat_long_id)
	    if num_found >1:
	        multiple.append(lat_long_id)
	    if num_found ==1:
	        query2 = "update rawlocation set location_id= '" + correct_id + "' where location_id_transformed = '" + lat_long_id + "';"
	        cursor.execute(query2)
	        mydb.commit()
	counter =0
	for lat_long_id in multiple:
	    counter +=1
	    if counter%50 == 0:
	        print counter
	    query1 = "select distinct location_id from rawlocation where location_id_transformed = '" + lat_long_id + "';"
	    raw_locs = pd.read_sql(query1, mydb)
	    found_loc_ids = list(raw_locs['location_id'])
	    num_found = 0
	    in_location = []
	    for raw_loc_id in found_loc_ids:
	        if raw_loc_id in list_of_loc_ids:
	            in_location.append(raw_loc_id)
	            #print raw_loc_id
	            num_found +=1
	    count_locs = []
	    for found_location in in_location:
	        query2 = "select count(*) from rawlocation where location_id = '" + found_location + "'"
	        cursor.execute(query2)
	        count_locs.append([int(i[0]) for i in cursor.fetchall()][0])
	    count_zip = zip(count_locs, in_location)
	    count_zip.sort()
	    correct_id = count_zip[-1][1]
	    query2 = "update rawlocation set location_id= '" + correct_id + "' where location_id_transformed = '" + lat_long_id + "';"
	    cursor.execute(query2)
	    mydb.commit()
	to_drop = list(check['location_id_transformed'])
	for lat_long_id in to_drop:
	    query = "update rawlocation set location_id='' where location_id_transformed = '" + lat_long_id + "';"
	    cursor.execute(query)
	mydb.commit()


