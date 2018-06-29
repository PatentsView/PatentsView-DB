import sys
import os
import MySQLdb
import urllib
from shutil import copyfile
from warnings import filterwarnings
sys.path.append("Code/PatentsView-DB/Scripts/Raw_Data_Parsers")

from uspto_parsers import generic_parser_2005, csv_to_mysql
sys.path.append("To_clone")
from ConfigFiles import config_second as config
sys.path.append("Code/PatentsView-DB/Scripts")
from Automated_QA import sql_upload_check
from CPC_Downloads import downloads
from Create import copy_db
from Government_Interest import extract_govint_docs, process_ipg_docs, identify_missed_orgs
sys.path.append("Code/PatentsView-DB/Scripts/Transform_script")
from script import transform_db


# #Step -1 : Add a automatic data download and upzip part

# #started Tues, Dec 26 at 5:50 pm EST; finished GI at 2:43 am
## started copy db at 10:30, finished in between 12 and 24 hours

#Step 0: reused functions
def connect_to_db():
    mydb = MySQLdb.connect(host= config.host,
    user=config.username,
    passwd=config.password)
    #eventually put password in a seperate restricted file as DDJ
    return mydb

#Step 1: Parse the raw files
print "Parsing the raw files!"
os.mkdir(config.folder)
os.mkdir(config.parsed_data_location)
generic_parser_2005.parse_patents(config.data_to_parse, config.parsed_data_location)

#Check that data parsed into all of the files
if any([os.stat(config.parsed_data_location + '/' + file).st_size <= 300 for file in os.listdir(config.parsed_data_location)]):
	print "Problem! One of the files is being parsed empty! Look at the data output file!"
	exit()

#Step 2: Creating SQL schema
print "Creating new SQL schema"
filterwarnings('ignore', category = MySQLdb.Warning)
mydb = connect_to_db()
cursor = mydb.cursor()
cursor.execute("create schema " + config.database + ";")
cursor.execute("use " + config.database + ";")
with open("Code/PatentsView-DB/Scripts/Create/patents.sql", 'r') as f:
 	commands = f.read().replace('\n', '').split(';')[:-1]
 	for command in commands:
 	 	cursor.execute(command)
 	 mydb.commit()
 	cursor.close()

#Step 3: Upload the Parsed Data to SQL
print "Loading Data into MySQL"
sql_output = config.folder + "/SQLOutput/"
os.mkdir(sql_output)
csv_to_mysql.mysql_upload(config.host,config.username,config.password,config.database,config.parsed_data_location, sql_output)
csv_to_mysql.upload_csv(config.host,config.username,config.password,config.database, sql_output)
sql_upload_check.my_sql_upload_QA(connect_to_db(), config.database, config.parsed_data_location)

#Step 4 : Run Transform Scripts
print "Transforming Data -- this takes along time"
transform_db.transform(config.persistent_files,config.host,config.username,config.password,None,config.database)


#Step 5: Run the Government Interest Python Parsing pieces -- eventually add parsing to original parse pass though
#there are several mores steps that need to happen after this
print "Starting Government Interest Parsing"
parsed_gov = config.folder + "/parsed_gov"
processed_gov = config.folder + "/processed_gov"
extract_govint_docs.main(config.data_to_parse, parsed_gov)
process_ipg_docs.main(parsed_gov, processed_gov)



#Step 6: File prep for the government interest code
#this step should be removed
#Copy the config file to where the Perl script and mysql dump can see them
#Copy the omit locations file
def write_config(file_path):
	with open(file_path, 'wb') as f:
		for value in [config.host, config.username, config.password, config.database, config.old_database, config.merged_database, config.folder, str(config.incremental_location)]:
			f.write(value + "\n")
write_config("Code/PatentsView-DB/Scripts/Government_Interest/config.txt")
write_config("To_Clone/config.txt")

#Step 7: Copy tables
#this takes forever
print "Coping the old database to the new database"
copy_db.copy(connect_to_db(), config.old_database, config.merged_database)
