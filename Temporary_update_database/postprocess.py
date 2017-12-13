import sys
import os
import MySQLdb
import urllib
from shutil import copyfile
from warnings import filterwarnings
sys.path.append("Code/PatentsView-DB/Scripts/Raw_Data_Parsers")
from uspto_parsers import generic_parser_2005, csv_to_mysql,uspc_table,merge_db_script,cpc_table,cpc_class_tables
sys.path.append("To_clone")
from ConfigFiles import config
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

#Step 0: Identifying the Missed government Orgs for hand match
# processed_gov = config.folder + "/processed_gov"
# identify_missed_orgs.find_missing(config.folder, config.persistent_files, processed_gov)

# #Step 1: Merge the newly created copy database
# merge_db_script.merge_db_pats(config.host,config.username,config.password,config.database, config.merged_database)

# #Step 2: Truncate Tables
# truncate_tables.clean(config.host,config.username,config.password,config.merged_database)


# #Step 3: CPC Schema and Classifications upload
# print "Doing CPC Schema -- again very slow"
# downloads.download_schema(config.folder)
# downloads.download_input(config.folder)
# cpc_input = config.folder + "/CPC_input"
# cpc_schema = config.folder + "/CPC_Schema"
# cpc_table.cpc_table(cpc_input)
# cpc_class_tables.cpc_class_tables(cpc_input,cpc_schema)
# csv_to_mysql.upload_cpc(config.host,config.username,config.password, None, config.merged_database, cpc_input)

# Step 4: Run USPC to retrospectively update classifications
# print "On to USPC"
# uspc_to_upload = config.folder + "/uspc_output"
# uspc_table.uspc_table(config.folder)
# csv_to_mysql.upload_uspc(config.host,config.username,config.password,None, config.merged_database,uspc_to_upload)


# Step 5: WIPO Classifications
# print "Now WIPO"
# print "Doing downloads"
# downloads.download_ipc(config.folder)
# print "On Lookups"
# wipo_categories.wipo_lookups(config.folder, config.persistent_files, config.host, config.username, config.password, config.merged_database)
# print "Uploading"
# csv_to_mysql.upload_wipo(config.host,config.username,config.password, config.merged_database,  config.folder)
# print "done"

#Step 8: Produce correctly formatted files for inventor disambigaution
#something may need to be done about nber
# clean_inventor = config.folder + "/for_inventor_disamb"
#os.mkdir(clean_inventor)
#get_csvs_invt_disam.get_tables(config.host, config.username, config.password, config.merged_database, clean_inventor)


#Step 7: Upload Government Interest After Hand Matching
#copy the matched organization lists to the government_manual
print "Have you done the Government Interest manual matching yet?"
done_matching = raw_input("Enter y or n: ")
if done_matching == 'y':
	print "OK, running the next government interest step"
	govt_manual = config.folder + "/government_manual"
	processed_gov = config.folder + "/processed_gov"
	#merging_cleansed_organization.upload_new_orgs(govt_manual, config.host, config.username, config.password, config.merged_database)
	merging_cleansed_organization.lookup(processed_gov, govt_manual, config.persistent_files)
	load_gi.process(config.host,config.username,config.password,config.merged_database, govt_manual)
#run the next steps here too!
# else:
# 	print "OK, skipping the Government Interest upload step for now!"

#Step 8: Run Inventor Post processing file 
# print "Have you run the inventor processing algorithm yet?"
# #then you need to copy the all-results.txt.post-processed to the disamb_inventor folder
# done_matching = raw_input("Enter y or n: ")
# if done_matching == 'y':
# 	print "OK, running inventor post processing"
# 	clean_inventor = config.folder + "/disamb_inventor"
# 	inventor_postprocess.inventor_process(clean_inventor)
# 	csv_to_mysql.upload_csv(config.host,config.username,config.password,config.merged_database,clean_inventor, "/inventor_disambig.csv" , 'inventor')
# 	csv_to_mysql.upload_csv(config.host,config.username,config.password,config.merged_database,clean_inventor, "/inventor_pairs.csv" , 'patent_inventor')

# else:
# 	print "OK, skipping the Inventor postprocessing for now upload step for now!"

#Step 6: Location Disambiguation
# loc_folder = config.folder + "/location_disambiguation/"
# get_locs.get(config.folder,config.host, config.username,config.password, config.merged_database, config.incremental_location)
# all_locs.locs(loc_folder, config.persistent_files, config.google_api_key)
# US_locs.locs(loc_folder, config.persistent_files, config.google_api_key)
# upload_locs.upload_locs(loc_folder, config.host, config.username, config.password, config.merged_database)

