import MySQLdb
import os
import csv
import sys
from ConfigFiles import config
sys.path.append("Code/PatentsView-DB/Scripts")

def get_tables(host, username, password, database, output):
	mydb = MySQLdb.connect(host= host,
	user=username,
	passwd=password, db =database)
	cursor = mydb.cursor()

	table = ['cpc_current','ipcr','nber','patent','rawassignee','rawinventor','uspc_current','rawlawyer']
	for t in table:
		print "Exporting table " + t
		cursor.execute('show columns from '+t) #eventually add funcitonality to check that the columns getting exported are exactly the same
		cols = [c[0] for c in cursor.fetchall()]
		if t == 'rawinventor':
			#this removes the new rule_47 column
			cols = cols[:-1]
		if t == 'rawlawyer':
			cols = cols[:8]
		col_string= ", ".join(cols)
		cursor.execute("select " + col_string + " from "+t)
		outp = csv.writer(open(output + '/'+t+'.csv','wb'),delimiter='\t')
		outp.writerow(cols)
		outp.writerows(cursor.fetchall())
	#rawlocation done separately because it is a specially query
	#getting rawlocation
	outp = csv.writer(open(output + '/rawlocation.csv','wb'),delimiter='\t')
	cursor.execute('select id,location_id_transformed as location_id,city,state,country_transformed as country from patent_20170808.rawlocation')
	outp.writerow(['id','location_id','city','state','country'])
	outp.writerows(cursor.fetchall())
# def zipfiles(output):
# 	pass