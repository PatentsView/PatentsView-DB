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
import tqdm
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

    # steps: write to output, clear df, read in chunks
    inventor_data.to_csv("{}/inventor.csv".format(disambiguated_folder), index=False)
    del inventor_data

    print("processed inventor list, now chunk inserting", flush=True)

    inv_chunk_size = 300000
    data_chunk = pd.read_csv("{}/inventor.csv".format(disambiguated_folder), encoding='utf-8', chunksize=inv_chunk_size)

    start = time.time()
    batch_counter = 0
    for chunk in tqdm.tqdm(data_chunk, desc='inventor table - batch inserts: '+ str(batch_counter)):
        chunk.to_sql(con=db_con, name='inventor', if_exists='append', index=False, method="multi")
        batch_counter += 1

    print("TO SQL: Time", flush=True)
    end = time.time()
    print(end - start, flush=True)


def update_raw(db_con, disambiguated_folder, lookup):

    print("opening rawinventor updated for writing...", flush=True)

    output = csv.writer(open(disambiguated_folder + "/rawinventor_updated.csv", 'w'), delimiter='\t')
    output.writerow(
        ['uuid', 'patent_id', 'inventor_id', 'rawlocation_id', 'name_first', 'name_last', 'sequence', 'rule_47'])

    limit = 300000
    offset = 0
    batch_counter = 0

    print("now batch processing", flush=True)

    while True:
        batch_counter += 1
        print('Next iteration...', flush=True)

        # order by with primary key column - this has no nulls
        raw_inv_chunk = db_con.execute("select * from rawinventor order by uuid limit {0} offset {1}".format(limit, offset))

        counter = 0
        for row in tqdm.tqdm(raw_inv_chunk, total=limit,
                             desc="rawinventor - batch:" + str(batch_counter)):

            inventor_id = lookup[row[0]]
            output.writerow(
                [row['uuid'], row['patent_id'], inventor_id, row['rawlocation_id'], row['name_first'], row['name_last'],
                 row['sequence'], row['rule_47']])

            # keep going because we have chunks to process
            counter += 1

        # means we have no more batches to process
        if counter == 0:
            break

        offset = offset + limit


    print("done updating rawinventor...", flush=True)



## Keeping this method separate to avoid memory issues
def upload_raw(db_con,disambiguated_folder, db) :

    print("now creating index syntax", flush=True)

    db_con.execute('create table rawinventor_inprogress like rawinventor')
    add_indexes_fetcher=db_con.execute("SELECT CONCAT('ALTER TABLE `',TABLE_NAME,'` ','ADD ', IF(NON_UNIQUE = 1, CASE UPPER(INDEX_TYPE) WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX' WHEN 'SPATIAL' THEN 'SPATIAL INDEX' ELSE CONCAT('INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) END, IF(UPPER(INDEX_NAME) = 'PRIMARY', CONCAT('PRIMARY KEY USING ', INDEX_TYPE ), CONCAT('UNIQUE INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) ) ), '(', GROUP_CONCAT( DISTINCT CONCAT('`', COLUMN_NAME, '`') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', ' ), ');' ) AS 'Show_Add_Indexes' FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '"+db+"' AND TABLE_NAME='rawinventor_inprogress' and UPPER(INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME, INDEX_NAME ORDER BY TABLE_NAME ASC, INDEX_NAME ASC; ")
    add_indexes=add_indexes_fetcher.fetchall()

    drop_indexes_fetcher=db_con.execute("SELECT CONCAT( 'ALTER TABLE `', TABLE_NAME, '` ', GROUP_CONCAT( DISTINCT CONCAT( 'DROP ', IF(UPPER(INDEX_NAME) = 'PRIMARY', 'PRIMARY KEY', CONCAT('INDEX `', INDEX_NAME, '`') ) ) SEPARATOR ', ' ), ';' ) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '"+db+"' AND TABLE_NAME='rawinventor_inprogress' and UPPER(INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME ORDER BY TABLE_NAME ASC")
    drop_indexes=drop_indexes_fetcher.fetchall()

    for drop_sql in drop_indexes:
        db_con.execute(drop_sql[0])

    start = time.time()

    print("Now chunk processing", flush=True)

    inv_chunk_size = 300000
    data_chunk = pd.read_csv(disambiguated_folder + "/rawinventor_updated.csv", encoding='utf-8', delimiter='\t',
                               keep_default_na=False, chunksize=inv_chunk_size)

    batch_counter = 0
    for chunk in tqdm.tqdm(data_chunk, desc='desc=rawinventor_inprogress - batch: ' + str(batch_counter)):

        chunk.sort_values(by="uuid", inplace=True)
        chunk.reset_index(inplace=True, drop=True)

        chunk.to_sql(con=db_con, name='rawinventor_inprogress', if_exists='append', index=False, method="multi")
        batch_counter += 1

    print("TO SQL: Time", flush=True)
    end = time.time()

    print("Load Time: " + str(round(end - start)), flush=True)

    print("now adding indexes", flush=True)

    for add_sql in add_indexes:
        print(add_sql[0])
        db_con.execute(add_sql[0])
    timestamp = round(end)
    backup_name = "temp_rawinventor_backup_" + str(timestamp)
    db_con.execute('alter table rawinventor rename ' + backup_name)
    db_con.execute('alter table rawinventor_inprogress rename rawinventor')

if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'],
                                           config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    disambiguated_folder = "{}/disambig_output".format(config['FOLDERS']['WORKING_FOLDER'])

    print('making lookup', flush=True)
    start = time.time()
    lookup, inventors = make_lookup(disambiguated_folder)
    end = time.time()
    print('done lookup', flush=True)
    print("Time Taken:", flush=True)
    print(end - start, flush=True)

    print('writing inventor', flush=True)
    start = time.time()
    write_inventor(inventors, disambiguated_folder)
    end = time.time()
    print('written inventor', flush=True)
    print("Write Total Time", flush=True)
    print(end - start, flush=True)


    print("starting update raw", flush=True)
    start = time.time()
    update_raw(db_con, disambiguated_folder, lookup)
    end = time.time()
    print('finished update raw', flush=True)
    print("Total Time", flush=True)
    print(end - start, flush=True)


    print("starting upload raw", flush=True)
    start = time.time()
    upload_raw(db_con, disambiguated_folder, config['DATABASE']['TEMP_UPLOAD_DB'])
    end = time.time()
    print('finished upload raw', flush=True)
    print("Write Total Time", flush=True)
    print(end - start, flush=True)
