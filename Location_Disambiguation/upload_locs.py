from __future__ import division
import csv,random,nltk
import MySQLdb
import re,os
import string
from unidecode import unidecode
import jaro



def upload_locs(location_folder, host, user, password, database):
	#print "Step 6: Getting correct ids -- about 4 hr 30 min for full data"
	
	fd = location_folder +'uspto_disamb_loc_latlong/'
	mydb = MySQLdb.connect(host= host, user=user, passwd = password, db = database)
	cursor = mydb.cursor()
	#this is critical to allowing the matches on latitude/longitude to work
	
	# print "Reading Input"
	# inp = csv.reader(file(fd+'location_disamb_US_google.tsv','rb'),delimiter='\t')
	# data = {}
	# for i in inp:
	# 	if float(i[4])!=0.0:
	# 		data[i[0]] = i[1:9]

	# inp = csv.reader(file(fd+'location_disamb_google.tsv','rb'),delimiter='\t')
	# for i in inp:
	# 	if float(i[4])!=0.0:
	# 		data[i[0]] = i[1:9]
	# print "Making output"
	# outp = csv.writer(open(fd+'location_disamb_all.tsv','wb'),delimiter='\t')
	# check =0
	# print "Looping over dict"
	# for k,v in data.items():
	# 	check +=1
	# 	if check%200 == 0:
	# 		print check
	# 		mydb.commit()
	# 	try:
	# 		if v[0]!='' and v[1]!='' and v[2]!='':
	# 			if v[2] == 'US':
	# 				cursor.execute('select id from location where (city="'+v[0]+'" and state="'+v[1]+'" and country="US") or (latitude='+v[3]+' AND longitude='+v[4]+')')
	# 				res = [c[0] for c in list(cursor.fetchall())]
	# 				if len(res) == 0:
	# 					outp.writerow(['']+v+[k])
	# 				elif len(res)>1:
	# 					correct = res[0]
	# 					cursor.execute('select distinct location_id_transformed from rawlocation where location_id="'+correct+'"')
	# 					res2 = [c[0] for c in list(cursor.fetchall())][0]
	# 					for r in res[1:]:
	# 						cursor.execute('update rawlocation set location_id="'+correct+'",location_id_transformed="'+res2+'" where location_id="'+r+'"')
	# 						cursor.execute('delete from location where id="'+r+'"')
	# 				else:
	# 					correct = res[0]
	# 					outp.writerow([correct]+v+[k])
	# 			else:
	# 				cursor.execute('select id from location where (city="'+v[0]+'" and country="'+v[2]+'") or (latitude='+v[3]+' AND longitude='+v[4]+')')
	# 				res = [c[0] for c in list(cursor.fetchall())]
	# 				if len(res) == 0:
	# 					outp.writerow(['']+v+[k])
	# 				elif len(res)>1:
	# 					correct = res[0]
	# 					cursor.execute('select distinct location_id_transformed from rawlocation where location_id="'+correct+'"')
	# 					res2 = [c[0] for c in list(cursor.fetchall())][0]
	# 					for r in res[1:]:
	# 						cursor.execute('update rawlocation set location_id="'+correct+'",location_id_transformed="'+res2+'" where location_id="'+r+'"')
	# 						cursor.execute('delete from location where id="'+r+'"')
	# 				else:
	# 					correct = res[0]
	# 					outp.writerow([correct]+v+[k])
	# 		elif v[0]=='' and v[1]!='' and v[2]!='':
	# 			cursor.execute('select id from location where (state="'+v[1]+'" and country="'+v[2]+'" and city is NULL) or (latitude='+v[3]+' AND longitude='+v[4]+')')
	# 			res = [c[0] for c in list(cursor.fetchall())]
	# 			if len(res) == 0:
	# 				outp.writerow(['']+v+[k])
	# 			else:
	# 				correct = res[0]
	# 				outp.writerow([correct]+v+[k])
	# 		elif v[0]!='' and v[1]=='' and v[2]!='':
	# 			cursor.execute('select id from location where (city="'+v[0]+'" and country="'+v[2]+'" and state is NULL) or (latitude='+v[3]+' AND longitude='+v[4]+')')
	# 			res = [c[0] for c in list(cursor.fetchall())]
	# 			if len(res) == 0:
	# 				outp.writerow(['']+v+[k])
	# 			else:
	# 				correct = res[0]
	# 				outp.writerow([correct]+v+[k])
	# 		elif v[2] == '' and v[0]!='':
	# 			print "_____",v
	# 			cursor.execute('select id from location where (country="IL" and city = "'+v[0]+'") or (latitude='+v[3]+' AND longitude='+v[4]+')')
	# 			res = [c[0] for c in list(cursor.fetchall())]
	# 			print res
	# 			if len(res) == 0:
	# 				outp.writerow(['']+v+[k])
	# 			else:
	# 				correct = res[0]
	# 				outp.writerow([correct]+v+[k])
	# 		else:
	# 			print "_",v
	# 			cursor.execute('select id from location where (country="'+v[2]+'" and city is NULL and state is NULL) or (latitude='+v[3]+' AND longitude='+v[4]+')')
	# 			res = [c[0] for c in list(cursor.fetchall())]
	# 			print res
	# 			if len(res) == 0:
	# 				outp.writerow(['']+v+[k])
	# 			else:
	# 				correct = res[0]
	# 				outp.writerow([correct]+v+[k])
	# 	except:
	# 		print "____",v

	# mydb.commit()

	#Step 7
	
	print "Step 7... Uploading "
	data = {} #dictionary that associates all the (corrected in previous step) disambiguated location ids with the raw location ids for the same place
	fd1 = location_folder+'/uspto_disamb_v2/'
	diri = os.listdir(fd1)
	for d in diri:
		inp = csv.reader(file(fd1+d,'rb'),delimiter='\t')
		for i in inp:
			try:
				data[i[4]].add(i[0])
			except:
				data[i[4]] = set([i[0]])

	fd =location_folder+'/uspto_disamb_loc_latlong/'
	inp = csv.reader(file(fd+'location_disamb_all.tsv','rb'),delimiter='\t')
	for e,i in enumerate(inp):
		if i[0] == '':
			idtrans = str(round(float(i[4]),5))+'|'+str(round(float(i[5]),5))
			cursor.execute('insert into location(id,city,state,country,latitude,longitude) values("'+i[-1]+'","'+'","'.join(i[1:6])+'")')
			for loc in data[i[-1]]:
				cursor.execute('update rawlocation set location_id="'+i[-1]+'",location_id_transformed="'+idtrans+'" where id="'+loc+'"')
		else:
			try:
				cursor.execute('select distinct location_id_transformed from rawlocation where location_id="'+i[0]+'"')
				res = [c[0] for c in list(cursor.fetchall())][0]
				for loc in data[i[-1]]:
					cursor.execute('update rawlocation set location_id="'+i[-1]+'",location_id_transformed="'+res+'" where id="'+loc+'"')
			except:
				print "Problem with (probably id does not exist)"
				print i[0]
				print i
		if e%500==0:
			mydb.commit()
			print str(e)
			
	
	mydb.commit()


