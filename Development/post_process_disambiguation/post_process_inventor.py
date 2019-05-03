import sys
import os
import pandas as pd
from sqlalchemy import create_engine
from warnings import filterwarnings
import csv
import re, os, random, string, codecs
import sys
from collections import Counter, defaultdict
import time

project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers


def make_lookup(disambiguated_folder):
    rawinventors = csv.reader(open(disambiguated_folder + "/inventor_disambiguation.tsv", 'r'), delimiter='\t')
    lookup = {}
    inventors_to_write = {}
    for i in rawinventors:
        lookup[i[2]] = i[3]  # lookup between raw id and inventor id
        # put together first and last names
        if i[5] != '':
            first = i[4] + ' ' + i[5]
        else:
            first = i[4]
        if i[7] != '':
            last = i[6] + ', ' + i[7]
        else:
            last = i[6]

        inventors_to_write[i[3]] = [first, last]  # get the first and last name

    return lookup, inventors_to_write


def write_inventor(inventors_to_write, disambiguated_folder):
    inventor_list = []
    for inv_id, inventor in inventors_to_write.items():
        inventor_list.append([inv_id] + inventor)
    inventor_data = pd.DataFrame(inventor_list)
    inventor_data.columns = ['id', 'name_first', 'name_last']
    start = time.time()
    inventor_data.to_sql(con=db_con, name='inventor', if_exists='append', index=False, chunksize=500, method="multi")
    print("TO SQL: Time")
    end = time.time()
    print(end - start)
    inventor_data.to_csv("{}/inventor.csv".format(disambiguated_folder), index=False)


def update_raw(db_con, disambiguated_folder, lookup):
    db_con.execute('create table rawinventor_inprogress like rawinventor')
    raw_inventor = db_con.execute("select * from rawinventor")
    print('got raw inventor')
    output = csv.writer(open(disambiguated_folder + "/rawinventor_updated.csv", 'w'), delimiter='\t')
    output.writerow(
        ['uuid', 'patent_id', 'inventor_id', 'rawlocation_id', 'name_first', 'name_last', 'sequence', 'rule_47'])
    for row in raw_inventor:
        inventor_id = lookup[row[0]]
        output.writerow(
            [row['uuid'], row['patent_id'], inventor_id, row['rawlocation_id'], row['name_first'], row['name_last'],
             row['sequence'], row['rule_47']])
    raw_data = pd.read_csv(disambiguated_folder + "/rawinventor_updated.csv", delimiter='\t')
    start = time.time()
    raw_data.to_sql(con=db_con, name='rawinventor_inprogress', if_exists='append', index=False, chunksize=500,
                    method="multi")
    print("TO SQL: Time")
    end = time.time()
    print(end - start)
    timestamp = round(end)
    backup_name = "temp_rawinventor_backup_" + str(timestamp)
    db_con.execute('alter table rawinventor rename '+backup_name)
    db_con.execute('alter table rawinventor_inprogress rename rawinventor')


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'],
                                           config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    disambiguated_folder = "{}/disambig_output".format(config['FOLDERS']['WORKING_FOLDER'])

    start = time.time()
    lookup, inventors = make_lookup(disambiguated_folder)
    end = time.time()
    print('done lookup ')
    print("Time Taken:")
    print(end - start)

    start = time.time()
    write_inventor(inventors, disambiguated_folder)
    end = time.time()
    print('written inventor')
    print("Write Total Time")
    print(end - start)

    start = time.time()
    update_raw(db_con, disambiguated_folder, lookup)
    end = time.time()
    print('written rawinventor')
    print("Write Total Time")
    print(end - start)
