
import sys
import os
import pandas as pd
from sqlalchemy import create_engine
from warnings import filterwarnings
import csv
import re,os,random,string,codecs
import sys
from collections import Counter, defaultdict
sys.path.append('/usr/local/airflow/PatentsView-DB/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers
import configparser
   

config = configparser.ConfigParser()
config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')

db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

disambiguated_folder = "{}/disambig_out".format(config['FOLDERS']['WORKING_FOLDER'])

db_con.execute('insert into patent_inventor select patent_id, inventor_id from rawinventor')
print('patent_inventor')
db_con.execute('insert into patent_assignee select patent_id, assignee_id from rawassignee')
print('patent_assignee')
db_con.execute('insert into patent_lawyer select patent_id, lawyer_id from rawlawyer')
print('patent_lawyer')
db_con.execute('insert into location_assignee select location_id_transformed, assignee_id from rawassignee')
print('location_assignee')
#I don't understand why location assignee and location inventor have different types of location id
db_con.execute('insert into location_inventor select location_id, inventor_id from rawinventor')
print('location_inventor')
 




 
