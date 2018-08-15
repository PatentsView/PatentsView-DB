import MySQLdb
import os

#you will need to adapt this to read the information from the new config file
#which is formatted differently
def connect_to_db():
    mydb = MySQLdb.connect(host= config.host,
    user=config.username,
    passwd=config.password)
    return mydb


#this chunk creates a new empty sql schema for the new database which is going to get uploaded
filterwarnings('ignore', category = MySQLdb.Warning)
mydb = connect_to_db()
cursor = mydb.cursor()
cursor.execute("create schema " + temporary_upload_db + ";")
cursor.execute("use " + temporary_upload_db + ";")
#this is a bit hacky, you are welcome to fix it if you can think of a better way, but don't spend more than 15 minutes on it
with open("Code/PatentsView-DB/Scripts/Create/patents.sql", 'r') as f:
 	commands = f.read().replace('\n', '').split(';')[:-1]
 	for command in commands:
 	 	cursor.execute(command)
 	 mydb.commit()
 	cursor.close()

 #then we need to upload the data into these temporary tables
 #if you run into encoding errors please let Sarah know right away, because I have ideas
 for folder in os.listdir(data_output_dir):
 	fields = [item for item in os.listdir(folder) if not item in ['error_counts.csv', 'error_data.csv']]
    for f in fields:  
        cursor.execute("load data local infile '{0}/{1}' into table {2} CHARACTER SET utf8 fields terminated by '\t' lines terminated by '\r\n' ignore 1 lines".format(folder, f, f.replace(".csv", "")))



#this chunk creates a new schema which is an exact copy of the existing database (called old_database)
#after we upload the newly parsed data into temporary_upload_db, we will then insert it into this new database
#this step needs to be in a separate scripte, not dependent on any of the other scripts becuase its kinda slow and it can run first
mydb = connect_to_db()
cursor = mydb.cursor()
cursor.execute("create schema " + new_database)
cursor.execute("show tables from " + old_database)
tables = [item[0] for item in cursor.fetchall() if not item[0].startswith("temp")]
for table in tables:
	cursor.execute("create table " + new_database + "." + table + " like " + old_database + "." + table)
	cursor.execute("insert into " + new_database + "." + table +" select * from " + old_database + "." + table)
	mydb.commit()
#