import sys
import os
import MySQLdb
import urllib
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from shutil import copyfile
from warnings import filterwarnings
sys.path.append("Code/PatentsView-DB/Scripts/Raw_Data_Parsers")
from uspto_parsers import generic_parser_2005, csv_to_mysql,uspc_table,merge_db_script,cpc_table,cpc_class_tables
sys.path.append("To_clone")
from ConfigFiles import config_second as config
sys.path.append("Code/PatentsView-DB/Scripts/Transform_script")
from script import transform_db, wipo_categories, truncate_tables, get_csvs_invt_disam
sys.path.append("Code/PatentsView-DB/Scripts")
from CPC_Downloads import downloads
from Government_Interest import identify_missed_orgs, merging_cleansed_organization, load_gi
from Inventor_Post import inventor_postprocess
sys.path.append("Code/PatentsView-DB")
from Location_Disambiguation import get_locs, upload_locs
from multiprocessing import Pool, Process, freeze_support
sys.path.append("Code/PatentsView-DB/Location_Disambiguation")
from googleapi import all_locs, US_locs

# 2018, Jan , started at 10:45 am

#Step 1: Identifying the Missed government Orgs for hand match
processed_gov = config.folder + "/processed_gov"
identify_missed_orgs.find_missing(config.folder, config.persistent_files, processed_gov)
identify_missed_orgs.get_orgs(config.host,config.username,config.password,config.old_database,config.folder)

#Step 2: Merge the newly created copy database
merge_db_script.merge_db_pats(config.host,config.username,config.password,config.database, config.merged_database)

#Step 3: Truncate Tables
replace this with a filter on the copy tables part
truncate_tables.clean(config.host,config.username,config.password,config.merged_database)


#Step 4: CPC Schema and Classifications upload
print "Doing CPC Schema -- again very slow"
downloads.download_schema(config.folder)
downloads.download_input(config.folder)
cpc_input = config.folder + "/CPC_input"
cpc_schema = config.folder + "/CPC_Schema"
cpc_table.cpc_table(cpc_input)
cpc_class_tables.cpc_class_tables(cpc_input,cpc_schema)
csv_to_mysql.upload_cpc(config.host,config.username,config.password, None, config.merged_database, cpc_input)

#Step 5: Run USPC to retrospectively update classifications
print "On to USPC"
uspc_to_upload = config.folder + "/uspc_output"
uspc_table.uspc_table(config.folder)
csv_to_mysql.upload_uspc(config.host,config.username,config.password,None, config.merged_database,uspc_to_upload)


#Step 6: WIPO Classifications
print "Now WIPO"
print "Doing downloads"
downloads.download_ipc(config.folder)
print "On Lookups"
wipo_categories.wipo_lookups(config.folder, config.persistent_files, config.host, config.username, config.password, config.merged_database)
print "Uploading"
csv_to_mysql.upload_wipo(config.host,config.username,config.password, config.merged_database,  config.folder)
print "done"

#Step 7: Produce correctly formatted files for inventor disambigaution
#something may need to be done about nber
clean_inventor = config.folder + "/for_inventor_disamb"
os.mkdir(clean_inventor)
get_csvs_invt_disam.get_tables(config.host, config.username, config.password, config.merged_database, clean_inventor)




