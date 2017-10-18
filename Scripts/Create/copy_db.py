import MySQLdb
import os
def copy(db_connection, old_database, new_database):
	cursor = db_connection.cursor()
	cursor.execute("create schema " + new_database)
	cursor.execute("show tables from " + old_database)
	tables = [item[0] for item in cursor.fetchall()]
	print tables
	for table in tables:
		cursor.execute("create table " + new_database + "." + table + " as select * from " + old_database + "." + table)
		db_connection.commit()
		print table

	# parsed = os.listdir(parsed_files)
	# parsed_tables = [file.split(".")[0] for file in parsed]

	# for table in parsed_tables:
	# 	cursor.execute("select count(*) from " + data_base + "." + table)
	# 	count = cursor.fetchone()[0]
	# 	if count < 100 and table != 'botanic':
	# 		print "Problem: ", table, ' has ', count, ' records'
	# cursor.close()