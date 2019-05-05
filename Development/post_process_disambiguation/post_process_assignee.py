import sys
import os
project_home = os.environ['PACKAGE_HOME']
import pandas as pd
from sqlalchemy import create_engine
from warnings import filterwarnings
import csv
import re,os,random,string,codecs
import sys
import time
from collections import Counter, defaultdict
from Development.helpers import general_helpers
import operator
import tqdm

def create_assignee_lookup(disambiguated_folder):  
    assignee_data = csv.reader(open('{}/assignee_disambiguation.tsv'.format(disambiguated_folder), 'r'), delimiter = '\t')
    raw_to_disambiguated = {}
    disambiguated = {}
    for row in tqdm.tqdm(assignee_data, 6163970):
        disambiguated_id = row[1] if row[1]!= '' else row[2]
        raw_to_disambiguated[row[0]] = disambiguated_id
        disambiguated[disambiguated_id] =[" ".join(row[4].split(" ")[:-1]),row[4].split(" ")[-1],row[3]]
    return raw_to_disambiguated, disambiguated
# String helper function
def xstr(s):
    if s is None:
        return ''
    return str(s)
def update_raw_assignee(db_con, disambiguated_folder, lookup, disambiguated):
    raw_assignee_data = db_con.execute("select * from rawassignee")
    output = csv.writer(open(disambiguated_folder + "/rawassignee_updated.csv",'w', encoding = 'utf-8'),delimiter='\t')
    output.writerow(['uuid', 'patent_id', 'assignee_id', 'rawlocation_id', 'type', 'name_first', 'name_last', 'organization', 'sequence'])
    type_lookup = defaultdict(lambda : [])
    assignee_id_set = set() #need this to get rid of assignees that don't appear in the raw assignee table
    counter = 0
    canonical_name_count = defaultdict(lambda: defaultdict(lambda: 0))
    canonical_org_count = defaultdict(lambda: defaultdict(lambda: 0))
    for row in tqdm.tqdm(raw_assignee_data):
        if row['uuid'] in lookup.keys(): #some rawassignees don't have a disambiguated assignee becasue of no location    
            assignee_id = lookup[row['uuid']]
            type_lookup[assignee_id].append(row['type'])
        else:
            assignee_id = general_helpers.id_generator()
            disambiguated[assignee_id] = [row['name_first'], row['name_last'], row['organization']]
            type_lookup[assignee_id].append(row['type'])
        name=xstr(row['name_first']) + "|" + xstr(row['name_last'])
        if row['name_first'] is None and row['name_last'] is None:
            name= None
        canonical_name_count[assignee_id][name] += 1
        canonical_org_count[assignee_id][row['organization']] += 1
        assignee_id_set.add(assignee_id)
        output.writerow([row['uuid'], row['patent_id'], assignee_id, row['rawlocation_id'], row['type'], row['name_first'], row['name_last'], row['organization'], row['sequence']])

    for disambiguated_id in tqdm.tqdm(disambiguated.keys(), total=len(disambiguated.keys())):
        frequent_org=disambiguated[disambiguated_id][2]
        if disambiguated_id in canonical_org_count:
            frequent_org = max(canonical_org_count[disambiguated_id].items(), key=operator.itemgetter(1))[0]
        frequent_name = disambiguated[disambiguated_id][1]
        if disambiguated_id in canonical_name_count:
            frequent_name = max(canonical_name_count[disambiguated_id].items(), key=operator.itemgetter(1))[0]
        if frequent_name is not None:
            disambiguated[disambiguated_id] = [
                " ".join(frequent_name.split("|")[:-1]), frequent_name.split(" ")[-1], frequent_org]
        else:
            disambiguated[disambiguated_id] = [
                None, None, frequent_org]


    return type_lookup, disambiguated, assignee_id_set


def upload_rawassignee(db_con, disambiguated_folder, db):

    db_con.execute('create table rawassignee_inprogress like rawassignee_backup')
    add_indexes_fetcher=db_con.execute("SELECT CONCAT('ALTER TABLE `',TABLE_NAME,'` ','ADD ', IF(NON_UNIQUE = 1, CASE UPPER(INDEX_TYPE) WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX' WHEN 'SPATIAL' THEN 'SPATIAL INDEX' ELSE CONCAT('INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) END, IF(UPPER(INDEX_NAME) = 'PRIMARY', CONCAT('PRIMARY KEY USING ', INDEX_TYPE ), CONCAT('UNIQUE INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) ) ), '(', GROUP_CONCAT( DISTINCT CONCAT('`', COLUMN_NAME, '`') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', ' ), ');' ) AS 'Show_Add_Indexes' FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '"+db+"' AND TABLE_NAME='rawassignee_inprogress' and UPPER(INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME, INDEX_NAME ORDER BY TABLE_NAME ASC, INDEX_NAME ASC; ")
    add_indexes=add_indexes_fetcher.fetchall()

    drop_indexes_fetcher=db_con.execute("SELECT CONCAT( 'ALTER TABLE `', TABLE_NAME, '` ', GROUP_CONCAT( DISTINCT CONCAT( 'DROP ', IF(UPPER(INDEX_NAME) = 'PRIMARY', 'PRIMARY KEY', CONCAT('INDEX `', INDEX_NAME, '`') ) ) SEPARATOR ', ' ), ';' ) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '"+db+"' AND TABLE_NAME='rawassignee_inprogress' and UPPER(INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME ORDER BY TABLE_NAME ASC")
    drop_indexes=drop_indexes_fetcher.fetchall()

    for drop_sql in drop_indexes:
        db_con.execute(drop_sql)
    start = time.time()
    n = 100  # chunk row size
    raw_assignees= pd.read_csv('{}/rawassignee_updated.csv'.format(disambiguated_folder), encoding = 'utf-8', delimiter = '\t')
    raw_assignees.sort_values(by="uuid", inplace=True, axis=1)
    raw_assignees.reset_index(inplace=True)
    n = 100  # chunk row size
    for i in range(0, raw_assignees.shape[0], n):
        current_chunk = raw_assignees[i:i + n]
        with db_con.begin() as conn:
            current_chunk.to_sql(con=conn, name='rawassignee_inprogress', index=False, if_exists='append',
                                 method="multi")  # append keeps the index
    end = time.time()
    print("Load Time:" + str(round(end - start)))

    for add_sql in add_indexes:
        db_con.execute(add_sql)
    stamp=str(round(time.time()))
    db_con.execute('alter table rawassignee rename temp_rawassignee_backup_'+stamp)
    db_con.execute('alter table rawassignee_inprogress rename rawassignee' + stamp)



def upload_assignee(db_con, disambiguated_to_write, type_lookup, assignee_id_set):
    disambig_list = [] 
    for assignee_id, assignee_info in disambiguated_to_write.items():
        if assignee_id in type_lookup.items():
            counts = Counter(type_lookup[assignee_id])
            type = max(type_lookup[key], key = counts.get)
        else:
            type = ''
        if assignee_id in assignee_id_set: #only add the ones that are actually used
            disambig_list.append([assignee_id, type, assignee_info[0], assignee_info[1], assignee_info[2]])
    disambig_data = pd.DataFrame(disambig_list)
    disambig_data.columns = ['id', 'type', 'name_first', 'name_last', 'organization']
    del disambig_list
    start=time.time()
    n = 100  # chunk row size
    for i in range(0, disambig_data.shape[0], n):
        current_chunk = disambig_data[i:i + n]
        with db_con.begin() as conn:
            current_chunk.to_sql(con = conn, name = 'assignee', index = False, if_exists = 'append',
                        method="multi")#append keeps the index
    end= time.time()
    print("Load Time:" + str(round(end-start)))
    
if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    disambiguated_folder = "{}/disambig_output".format(config['FOLDERS']['WORKING_FOLDER'])
    start = time.time()
    raw_to_disambiguated, disambiguated_to_write = create_assignee_lookup(disambiguated_folder)
    print(len(raw_to_disambiguated))
    print(len(disambiguated_to_write))
    print('done lookup')
    end = time.time()
    print("Lookup Time:" + str(round(end - start)))
    start = time.time()
    type_lookup, disambiguated_to_write, assignee_ids_to_use = update_raw_assignee(db_con, disambiguated_folder, raw_to_disambiguated, disambiguated_to_write)
    print('done raw update')
    end = time.time()
    print("Raw update Time:" + str(round(end - start)))
    start = time.time()
    upload_assignee(db_con, disambiguated_to_write, type_lookup, assignee_ids_to_use)
    print('Uploaded assignee')
    end = time.time()
    print("Assignee Upload Time:" + str(round(end - start)))
    start = time.time()
    upload_rawassignee(db_con, disambiguated_folder,config['DATABASE']['NEW_DB'])
    end = time.time()
    print("Raw Assignee Upload Time:" + str(round(end - start)))
