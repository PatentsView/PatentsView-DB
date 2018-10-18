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
    os.system('mv assignee_disambiguation.tsv {}/assignee_disambiguation.tsv'.format(disambiguated_folder))    
    assignee_data = csv.reader(open('{}/assignee_disambiguation.tsv'.format(disambiguated_folder), 'r'), delimiter = '\t')
    raw_to_disambiguated = {}
    disambiguated = {}
    for row in assignee_data:
        disambiguated_id = row[1] if row[1]!= '' else row[2]
        raw_to_disambiguated[row[0]] = disambiguated_id
        disambiguated[disambiguated_id] =[" ".join(row[4].split(" ")[:-1]),row[4].split(" ")[-1],row[3]]
    return raw_to_disambiguated, disambiguated

def update_raw_assignee(db_con, disambiguated_folder, lookup, disambiguated):
    raw_assignee_data = db_con.execute("select * from rawassignee")
    output = csv.writer(open(disambiguated_folder + "/rawassignee_updated.csv",'w', encoding = 'utf-8'),delimiter='\t')
    output.writerow(['uuid', 'patent_id', 'assignee_id', 'rawlocation_id', 'type', 'name_first', 'name_last', 'organization'])
    type_lookup = defaultdict(lambda : [])
    counter = 0
    for row in raw_assignee_data:
        if row['uuid'] in lookup.keys(): #some rawassignees don't have a disambiguated assignee becasue of no location    
            assignee_id = lookup[row['uuid']]
            type_lookup[assignee_id].append(row['type'])
        else:
            assignee_id = general_helpers.id_generator()
            disambiguated[assignee_id] = [row['name_first'], row['name_last'], row['organization']]
            type_lookup[assignee_id].append(row['type'])
        output.writerow([row['uuid'], row['patent_id'], assignee_id, row['rawlocation_id'], row['type'], row['name_first'], row['name_last'], row['organization']])
    return type_lookup, disambiguated


def upload_assignee(db_con, disambiguated_to_write, type_lookup):
    disambig_list = [] 
    for assignee_id, assignee_info in disambiguated_to_write.items():
        if assignee_id in type_lookup.items():
            counts = Counter(type_lookup[assignee_id])
            type = max(type_lookup[key], key = counts.get)
        else:
            type = ''
        disambig_list.append([assignee_id, type, assignee_info[0], assignee_info[1], assignee_info[2]])
    disambig_data = pd.DataFrame(disambig_list)
    disambig_data.columns = ['id', 'type', 'name_first', 'name_last', 'organization']
    disambig_data.to_sql(con = db_con, name = 'assignee', index = False, if_exists = 'replace')

def upload_rawassignee(db_con, disambiguated_folder):
    db_con.execute('alter table rawassignee rename temp_rawassignee_backup')
    raw_assignee = pd.read_csv('{}/rawassignee_updated.csv'.format(disambiguated_folder), encoding = 'utf-8', delimiter = '\t')
    raw_assignee.to_sql(con=db_con, name = 'rawassignee', index = False, if_exists='replace') 

if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    disambiguated_folder = "{}/disambig_out".format(config['FOLDERS']['WORKING_FOLDER'])    

    raw_to_disambiguated, disambiguated_to_write = create_assignee_lookup(disambiguated_folder)
    print(len(raw_to_disambiguated))
    print(len(disambiguated_to_write))
    print('done lookup')
    type_lookup, disambiguated_to_write = update_raw_assignee(db_con, disambiguated_folder, raw_to_disambiguated, disambiguated_to_write)
    print('done raw update')
    upload_assignee(db_con, disambiguated_to_write, type_lookup)
    print('Uploaded assignee')
    upload_rawassignee(db_con, disambiguated_folder)
