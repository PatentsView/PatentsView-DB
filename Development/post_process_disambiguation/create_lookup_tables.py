
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
db_con.execute("insert ignore into location_assignee (select max(location_id) as location_id, a.assignee_id from (select assignee_id, max(location_id_count) as max_count from (select assignee_id, location_id, count(location_id) as location_id_count from temp_assignee_loc group by assignee_id, location_id) as m group by assignee_id) as a left join (select assignee_id, location_id, count(location_id) as location_id_count from temp_assignee_loc group by assignee_id, location_id) as b on a.assignee_id =b.assignee_id and a.max_count=b.location_id_count group by a.assignee_id);")
print('location_assignee')
#I don't understand why location assignee and location inventor have different types of location id
db_con.execute("create table temp_inventor_loc as select inventor_id, location_id from rawinventor r left join rawlocation l on r.rawlocation_id = l.id;")
print('done inventor temp')

db_con.execute("insert ignore into location_inventor (select max(location_id) as location_id, a.inventor_id from (select inventor_id, max(location_id_count) as max_count from (select inventor_id, location_id, count(location_id) as location_id_count from temp_inventor_loc where location_id !='' and location_id is not null group by inventor_id, location_id) as m group by inventor_id) as a left join (select inventor_id, location_id, count(location_id) as location_id_count from temp_inventor_loc where location_id !='' and location_id is not null group by inventor_id, location_id) as b on a.inventor_id =b.inventor_id and a.max_count=b.location_id_count group by a.inventor_id);")
print('location_inventor')




 
