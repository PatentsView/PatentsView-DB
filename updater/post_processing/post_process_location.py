import uuid

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import csv
import os
from collections import Counter, defaultdict

from lib.configuration import get_config, get_connection_string

import tqdm
import time


def make_lookup(disambiguated_folder):
    inp = csv.reader(open("{}/location_disambiguation.tsv".format(disambiguated_folder), 'r'), delimiter='\t')
    print('read in', flush=True)
    name_id_lookup = {}  # place to lat/long/id lookup
    undisambiguated = set()  # place for locations without a lat/long
    lookup = {}  # raw location id to disambiguated id
    # for some reason column 1 and 2 both have disambiguated place names
    # create the latitude/longitude to id mappings
    id_lat_long_lookup = {}
    lat_long_name_count = defaultdict(lambda: defaultdict(lambda: 0))
    counter = 0
    for row in inp:
        counter += 1
        if counter % 1000 == 0:
            print(counter)
        clean_loc = tuple([None if i == 'NULL' else i for i in row[1].split('|')])
        try:
            if len(row) < 5:  # some of that long pairs are ented as a pipe separated pair in the 4th column
                lat_long = row[3].split('|')
                row[3] = lat_long[0]
                row.append(lat_long[1])
            if row[3] != 'NULL':  # if there is a lat-long pair
                lat_long = "{0}|{1}".format(np.round(float(row[3]), 4), np.round(float(row[4]), 4))
            else:
                lat_long = 'undisambiguated'  # this handles the ~13,000 undisambiguated
        except ValueError as e:
            print(row, flush=True)
            print(len(row), flush=True)
            print(e, flush=True)
            raise e
        lat_long_name_count[lat_long][clean_loc] += 1
        id_lat_long_lookup[row[0]] = lat_long
    print('first loop', flush=True)

    lat_long_cannonical_name = {}
    for lat_long, name_list in lat_long_name_count.items():
        if not lat_long == 'undisambiguated':  # don't clump the undisambiguated ones together
            name = sorted([(name, count) for name, count in name_list.items()], key=lambda x: x[1])[-1][0]
            lat_long_cannonical_name[lat_long] = {'place': name, 'id': str(uuid.uuid4())}
        else:
            lat_long_cannonical_name[lat_long] = {'place': (None, None, None), 'id': str(uuid.uuid4())}

    print('second loop', flush=True)
    return id_lat_long_lookup, lat_long_cannonical_name


def create_fips_lookups(persistent_files):
    # TODO: maybe just store this as a json file that directly becomes a dict?
    county_lookup = pd.read_csv('{}/county_lookup.csv'.format(persistent_files))
    city = county_lookup['city']
    state = county_lookup['state']
    county = county_lookup['county']
    county_fips = county_lookup['county_fips']
    county_fips_dict = {}
    county_dict = {}
    for i in range(len(county)):
        county_dict[(state[i], city[i])] = county[i]
        county_fips_dict[(state[i], city[i])] = county_fips[i]
    state_df = pd.read_csv('{}/state_fips.csv'.format(persistent_files), dtype=object)
    state_dict = dict(zip(list(state_df['State']), list(state_df['State_FIPS'])))

    return [county_dict, county_fips_dict, state_dict]


def lookup_fips(city, state, country, lookup_dict, lookup_type='city'):
    result = None
    if lookup_type == 'city':
        if country == 'US' and (state, city) in lookup_dict:
            result = lookup_dict[(state, city)]
    elif country == 'US' and state in lookup_dict:
        result = lookup_dict[state]
    return result


def upload_location(db_con, lat_long_cannonical_name, disambiguated_folder, fips_lookups):
    county_dict, county_fips_dict, state_dict = fips_lookups

    location = []
    for lat_long, v in lat_long_cannonical_name.items():
        if not lat_long == 'undisambiguated':
            county = lookup_fips(v['place'][0], v['place'][1], v['place'][2], county_dict, 'city')
            county_fips = lookup_fips(v['place'][0], v['place'][1], v['place'][2], county_fips_dict, 'city')
            state_fips = lookup_fips(v['place'][0], v['place'][1], v['place'][2], state_dict, 'state')
            location.append(
                [v['id'], v['place'][0], v['place'][1], v['place'][2], lat_long.split('|')[0], lat_long.split('|')[1],
                 county, state_fips, county_fips])
    location_df = pd.DataFrame(location)
    location_df.columns = ['id', 'city', 'state', 'country', 'latitude', 'longitude', 'county', 'state_fips',
                           'county_fips']
    location_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    print(location_df.head())
    location_df.to_csv('{}/location.csv'.format(disambiguated_folder))
    start = time.time()
    n = 10000  # chunk row size
    for i in tqdm.tqdm(range(0, location_df.shape[0], n), desc="Location Load"):
        current_chunk = location_df[i:i + n]
        with db_con.begin() as conn:
            current_chunk.to_sql(con=conn, name='location', index=False, if_exists='append',
                                 method="multi")  # append keeps the index
    end = time.time()
    print("Load Time:" + str(round(end - start)), flush=True)


def process_rawlocation(db_con, lat_long_cannonical_name, id_lat_long_lookup, disambiguated_folder):
    raw_loc_count_cursor = db_con.execute("select count(*) from rawlocation")
    raw_loc_count = raw_loc_count_cursor.fetchall()[0][0]
    updated = csv.writer(open(disambiguated_folder + "/rawlocation_updated.csv", 'w'), delimiter='\t')
    limit = 300000
    offset = 0
    total_undisambiguated = 0
    total_missed = []
    batch_counter = 0
    while True:
        batch_counter += 1
        print("Next Iteration", flush=True)
        rawl_loc_fetch_chunk = db_con.execute(
            "select * from rawlocation order by id limit {} offset {}".format(limit, offset))
        counter = 0
        for i in tqdm.tqdm(rawl_loc_fetch_chunk, total=limit,
                           desc="Raw location Part Processing" + str(batch_counter) + "/" + str(raw_loc_count / limit)):
            if i['id'] in id_lat_long_lookup.keys():
                lat_long = id_lat_long_lookup[i['id']]
                loc_id = lat_long_cannonical_name[lat_long]['id']
                updated.writerow(
                    [i['id'], loc_id, i['city'], i['state'], i['country'], i['country_transformed'], lat_long])
            else:
                updated.writerow([i['id'], None, i['city'], i['state'], i['country'], i['country_transformed'], None])
            counter += 1
        if counter == 0:
            break
        offset = offset + limit


def upload_rawloc(db_con, disambiguated_folder, db):
    db_con.execute('create table rawlocation_inprogress like rawlocation')
    add_indexes_fetcher = db_con.execute(
        "SELECT CONCAT('ALTER TABLE `',TABLE_NAME,'` ','ADD ', IF(NON_UNIQUE = 1, CASE UPPER(INDEX_TYPE) WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX' WHEN 'SPATIAL' THEN 'SPATIAL INDEX' ELSE CONCAT('INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) END, IF(UPPER(INDEX_NAME) = 'PRIMARY', CONCAT('PRIMARY KEY USING ', INDEX_TYPE ), CONCAT('UNIQUE INDEX `', INDEX_NAME, '` USING ', INDEX_TYPE ) ) ), '(', GROUP_CONCAT( DISTINCT CONCAT('`', COLUMN_NAME, '`') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', ' ), ');' ) AS 'Show_Add_Indexes' FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '" + db + "' AND TABLE_NAME='rawlocation_inprogress' and UPPER(INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME, INDEX_NAME ORDER BY TABLE_NAME ASC, INDEX_NAME ASC; ")
    add_indexes = add_indexes_fetcher.fetchall()

    drop_indexes_fetcher = db_con.execute(
        "SELECT CONCAT( 'ALTER TABLE `', TABLE_NAME, '` ', GROUP_CONCAT( DISTINCT CONCAT( 'DROP ', IF(UPPER(INDEX_NAME) = 'PRIMARY', 'PRIMARY KEY', CONCAT('INDEX `', INDEX_NAME, '`') ) ) SEPARATOR ', ' ), ';' ) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '" + db + "' AND TABLE_NAME='rawlocation_inprogress' and UPPER(INDEX_NAME) <> 'PRIMARY' GROUP BY TABLE_NAME ORDER BY TABLE_NAME ASC")
    drop_indexes = drop_indexes_fetcher.fetchall()

    for drop_sql in drop_indexes:
        db_con.execute(drop_sql[0])

    raw_data_chunks = pd.read_csv(disambiguated_folder + "/rawlocation_updated.csv", delimiter='\t', chunksize=10000)
    print('uploading', flush=True)
    start = time.time()
    for raw_data in tqdm.tqdm(raw_data_chunks, desc="Raw Location Upload"):
        raw_data.columns = ["id", "location_id", "city", "state", "country", "country_transformed",
                            "location_id_transformed"]
        raw_data.sort_values(by="id", inplace=True)
        raw_data.reset_index(inplace=True, drop=True)
        with db_con.begin() as conn:
            raw_data.to_sql(con=conn, name='rawlocation_inprogress', index=False, if_exists='append',
                            method="multi")  # append keeps the index
    end = time.time()
    print("Load Time:" + str(round(end - start)), flush=True)
    for add_sql in add_indexes:
        db_con.execute(add_sql[0])
    stamp = str(round(time.time()))
    db_con.execute('alter table rawlocation rename temp_rawlocation_backup_' + stamp)
    db_con.execute('alter table rawlocation_inprogress rename rawlocation')


def post_process_location(config):
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    disambiguated_folder = "{}/disambig_output".format(config['FOLDERS']['WORKING_FOLDER'])
    print('here!', flush=True)
    id_lat_long_lookup, lat_long_cannonical_name = make_lookup(disambiguated_folder)
    print('made lookup', flush=True)
    fips_lookups = create_fips_lookups(config['FOLDERS']['PERSISTENT_FILES'])
    upload_location(engine, lat_long_cannonical_name, disambiguated_folder, fips_lookups)
    print('done locupload ', flush=True)
    process_rawlocation(engine, lat_long_cannonical_name, id_lat_long_lookup, disambiguated_folder)
    print('done process', flush=True)
    upload_rawloc(engine, disambiguated_folder, config['DATABASE']['NEW_DB'])


if __name__ == '__main__':
    config = get_config()
    post_process_location(config)
