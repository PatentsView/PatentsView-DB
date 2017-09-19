import MySQLdb
import os
def my_sql_upload_QA(db_connection, data_base, parsed_files):
	mydb = db_connection
	cursor = mydb.cursor()
	parsed = os.listdir(parsed_files)
	parsed_tables = [file.split(".")[0] for file in parsed]

	for table in parsed_tables:
		cursor.execute("select count(*) from " + data_base + "." + table)
		count = cursor.fetchone()[0]
		if count < 100 and table != 'botanic':
			print "Problem: ", table, ' has ', count, ' records'
	cursor.close()
