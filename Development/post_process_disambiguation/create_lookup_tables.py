import sys
import os
import pandas as pd
from sqlalchemy import create_engine
from warnings import filterwarnings
import csv
import re,os,random,string,codecs
import sys
from collections import Counter, defaultdict
sys.path.append('/project/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers
import configparser
   

config = configparser.ConfigParser()
config.read('/project/Development/config.ini')

db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])


db_con.execute('insert ignore into patent_inventor select patent_id, inventor_id from rawinventor')
print('patent_inventor')
db_con.execute('insert ignore into patent_assignee select patent_id, assignee_id from rawassignee')
print('patent_assignee')
db_con.execute('insert ignore into patent_lawyer select patent_id, lawyer_id from rawlawyer')
print('patent_lawyer')
db_con.execute("create table temp_assignee_loc as select assignee_id, rawlocation_id, location_id_transformed as location_id from rawassignee r left join rawlocation l on r.rawlocation_id = l.id;")
print('made locaiton_assignee_temp')
db_con.execute("insert ignore into location_assignee SELECT distinct rl.location_id, ri.assignee_id from rawassignee ri left join rawlocation rl on rl.id = ri.rawlocation_id where ri.assignee_id is not NULL and rl.location_id is not NULL;")
print('location_assignee')
#I don't understand why location assignee and location inventor have different types of location id
db_con.execute("create table temp_inventor_loc as select inventor_id, location_id from rawinventor r left join rawlocation l on r.rawlocation_id = l.id;")
print('done inventor temp')

db_con.execute("insert ignore into location_inventor SELECT rl.location_id, ri.inventor_id from rawinventor ri left join rawlocation rl on rl.id = ri.rawlocation_id where ri.inventor_id is not NULL and rl.location_id is not NULL;")
print('location_inventor')




 
