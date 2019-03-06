#Date

import pandas as pd
import os
import configparser
import sys
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']
previous_qa_loc = config['FOLDERS']['OLD_QA_LOC']
new_qa_loc = config['FOLDERS']['NEW_QA_LOC']
latest_expected_date = config['DATES']['END_DATE']

engine = general_helpers.connect_to_db(host, username, password, new_database)
data = pd.read_csv("{}/01_latest_date_check.csv".format(previous_qa_loc))

def check_latest_date(newdb, last_date):
	date_dict = {}
	var = engine.execute("select max(date) from {0}.patent;".format(new_database))
	found_date = [row[0] for row in var][0]
	if str(found_date) != str(latest_expected_date):
		date_error =  "The latest date is {0}, and it should be {1}".format(str(found_date), str(latest_expected_date))
		date_dict.update({new_database:date_error})
	else:
		date_match = "date matches"
		date_dict.update({new_database:date_match})
	return date_dict
def date_check_to_excel(previous_qa_loc, date_dict, new_qa_loc):
	df = pd.read_csv('{}/01_latest_date_check.csv'.format(previous_qa_loc))
	df_2 = pd.DataFrame.from_dict(date_dict, orient = 'index')
	df_2.reset_index(inplace=True)
	df_2.rename(columns={'index':'database', 0:'latest date check'}, inplace=True)
	df_3 = df.append(df_2)
	df_3.to_csv('{}/01_latest_date_check.csv'.format(new_qa_loc), index=False)
	

date_dict = check_latest_date(new_database, latest_expected_date)
date_check_to_file(previous_qa_loc, date_dict, new_qa_loc)
