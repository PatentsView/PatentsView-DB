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


def create_assignee_lookup(disambiguated_folder):
    
    assignee_data = csv.reader(open('{}/assignee_disambiguation.tsv'.format(disambiguated_folder), 'r'), delimiter = '\t')
    raw_to_disambiguated = {}
    disambiguated = {}
    for row in assignee_data:
        disambiguated_id = row[1] if row[1]!= '' else row[2]
        raw_to_disambiguated[row[0]] = disambiguated_id
        disambiguated[disambiguated_id] =[" ".join(row[4].split(" ")[:-1]),row[4].split(" ")[-1],row[3]]
    return raw_to_disambiguated, disambiguated

def update_raw_assignee(db_con, disambiguated_folder, lookup):
    raw_assignee_data = db_con.execute("select * from rawassignee")
    output = csv.writer(open(disambiguated_folder + "/rawassignee_updated.csv",'w', encoding = 'utf-8'),delimiter='\t')
    output.writerow(['uuid', 'patent_id', 'assignee_id', 'rawlocation_id', 'type', 'name_first', 'name_last', 'organization'])
    type_lookup = defaultdict(lambda : [])
    for row in raw_assignee_data:
        assignee_id = lookup[row['uuid']]
        type_lookup[assignee_id].append(row['type'])
        output.writerow([row['uuid'], row['patent_id'], assignee_id, row['rawlocation_id'], row['type'], row['name_first'], row['name_last'], row['organization']])
 
    return type_lookup


def upload_assignee(db_con, disambiguated_to_write, type_lookup):
    disambig_list = [] 
    for key, value in disambiguated_to_write.items():
        counts = Counter(type_lookup[key])
        type = max(type_lookup[key], key = counts.get)
        disambig_list.append([key, type, value[0], value[1], value[2]])
        if 'SealRite' in value[2]:
            print(value[2])
    disambig_data = pd.DataFrame(disambig_list)
    disambig_data.columns = ['id', 'type', 'name_first', 'name_last', 'organization']
    disambig_data.to_csv('debug.csv', encoding = 'utf-8')
    data = pd.read_csv('debug.csv')
    data.to_sql(con = db_con, name = 'assignee', index = False, if_exists = 'replace')

def upload_rawassignee(db_con, disambiguated_folder):
    raw_assignee = pd.read_csv('{}/rawassignee_updated.csv'.format(disambiguated_folder), delimiter = '\t')
    raw_assignee.to_sql(con=db_con, name = 'rawassignee_2', index = False, if_exists='replace') 

if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    disambiguated_folder = "{}/disambig_out".format(config['FOLDERS']['WORKING_FOLDER'])    


    raw_to_disambiguated, disambiguated_to_write = create_assignee_lookup(disambiguated_folder)
    print('done lookup')
    type_lookup = update_raw_assignee(db_con, disambiguated_folder, raw_to_disambiguated)
    print('done raw update')
    upload_assignee(db_con, disambiguated_to_write, type_lookup)
    upload_rawassignee(db_con, disambiguated_folder)
