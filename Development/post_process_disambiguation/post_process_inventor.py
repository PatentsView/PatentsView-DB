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


def make_lookup(disambiguated_folder):
    #os.system('mv inventor_disambiguation.tsv {}/inventor_disambiguation.tsv'.format(disambiguated_folder))
    rawinventors = csv.reader(open(disambiguated_folder + "/inventor_disambiguation.tsv",'r'),delimiter='\t')
    lookup = {}
    inventors_to_write = {}
    for i in rawinventors: 
        lookup[i[2]] = i[3] #lookup between raw id and inventor id
        #put together first and last names
        if i[5]!='':
            first = i[4]+' '+i[5]
        else:
            first = i[4]
        if i[7] != '':
            last = i[6]+', '+i[7]
        else:
            last = i[6]
        
        inventors_to_write[i[3]] = [first, last] #get the first and last name

    return lookup, inventors_to_write

def write_inventor(inventors_to_write, disambiguated_folder):
    inventor_list = []
    for inv_id, inventor in inventors_to_write.items():
        inventor_list.append([inv_id] + inventor)
    inventor_data = pd.DataFrame(inventor_list)
    inventor_data.to_sql(con=db_con, name = 'inventor', if_exists = 'replace', index = False)
    inventor_data.to_csv("{}/inventor.csv".format(disambiguated_folder), index = False)


def update_raw(db_con, disambiguated_folder, lookup):
    raw_inventor = db_con.execute("select * from rawinventor")
    db_con.execute('alter table rawinventor rename temp_rawinventor_backup')
    output = csv.writer(open(disambiguated_folder + "/rawinventor_updated.csv",'w'),delimiter='\t')
    for row in raw_inventor:
        inventor_id =lookup[row[0]]
        output.writerow([row['uuid'], row['patent_id'], inventor_id, row['rawlocation_id'], row['name_first'], row['name_last'], row['sequence'], row['rule_47']])
    raw_data = pd.read_csv(disambiguated_folder + "/rawinventor_updated.csv")
    raw_data.to_sql(con=db_con, name = 'rawinventor', if_exists = 'append', index = False)
 


if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    disambiguated_folder = "{}/disambig_out".format(config['FOLDERS']['WORKING_FOLDER'])    

    lookup, inventors = make_lookup(disambiguated_folder)
    print('done lookup ')
    print(len(lookup))
    print(len(inventors))
    write_inventor(inventors, disambiguated_folder)
    print('written inventor')
    update_raw(db_con, disambiguated_folder, lookup)
