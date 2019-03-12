import sys
import numpy as np
import os
import pandas as pd
from sqlalchemy import create_engine
from warnings import filterwarnings
import csv
import re,os,random,string,codecs
import sys
from collections import Counter, defaultdict
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers

def make_lookup(disambiguated_folder):
    inp = csv.reader(open( "{}/location_disambiguation.tsv".format(disambiguated_folder),'r'),delimiter='\t')
    print('read in')
    name_id_lookup = {} #place to lat/long/id lookup
    undisambiguated =set() #place for locations without a lat/long
    lookup = {} #raw location id to disambiguated id
    #for some reason column 1 and 2 both have disambiguated place names
    #create the latitude/longitude to id mappings
    id_lat_long_lookup = {}
    lat_long_name_count = defaultdict(lambda : defaultdict(lambda: 0))
    for row in inp:
        clean_loc = tuple([None if i =='NULL' else i for i in row[1].split('|')])
        if len(row) < 5: #some of the lat long pairs are ented as a pipe separated pair in the 4th column
            lat_long = row[3].split('|')
            row[3] = lat_long[0]
            row.append(lat_long[1])
        if row[3] != 'NULL':#if there is a lat-long pair
            lat_long = "{0}|{1}".format(np.round(float(row[3]),4), np.round(float(row[4]),4))
        else:
            lat_long = 'undisambiguated' #this handles the ~13,000 undisambiguated
        lat_long_name_count[lat_long][clean_loc] +=1  
        id_lat_long_lookup[row[0]] = lat_long
    print('first loop')

    lat_long_cannonical_name = {}
    for lat_long, name_list in lat_long_name_count.items():
        if not lat_long == 'undisambiguated':#don't clump the undisambiguated ones together
            name = sorted([(name, count) for name, count in name_list.items()], key = lambda x: x[1])[-1][0]
            lat_long_cannonical_name[lat_long] = {'place': name, 'id': general_helpers.id_generator(12)}
        else:
            lat_long_cannonical_name[lat_long] = {'place': ('', '', ''), 'id': general_helpers.id_generator(12)}

    print('second loop')
    return id_lat_long_lookup, lat_long_cannonical_name

def create_fips_lookups(persistent_files):
    #TODO: maybe just store this as a json file that directly becomes a dict?
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

def lookup_fips(city, state, country, lookup_dict, lookup_type = 'city'):
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
            county = lookup_fips(v['place'][0],v['place'][1],v['place'][2], county_dict, 'city')
            county_fips = lookup_fips(v['place'][0],v['place'][1],v['place'][2], county_fips_dict, 'city')
            state_fips = lookup_fips(v['place'][0],v['place'][1],v['place'][2], state_dict, 'state')
            location.append([v['id'], v['place'][0],v['place'][1],v['place'][2],lat_long.split('|')[0], lat_long.split('|')[1], county, state_fips, county_fips])
    location_df = pd.DataFrame(location)
    location_df.columns = ['id', 'city', 'state', 'country', 'latitude', 'longitude', 'county', 'state_fips', 'county_fips']
    location_df.to_csv('{}/location.csv'.format(disambiguated_folder))
    location_df.to_sql(con=db_con, name = 'location', if_exists = 'append', index = False)
    

def process_rawlocation(db_con, lat_long_cannonical_name, id_lat_long_lookup, disambiguated_folder):
   
    db_con.execute('alter table rawlocation rename temp_rawlocation_backup')
    print('getting data for rawloc')
    db_con.execute('create table rawlocation like temp_rawlocation_backup')
    raw_loc_data = db_con.execute("select * from temp_rawlocation_backup")
    print('got data')
    updated = csv.writer(open(disambiguated_folder + "/rawlocation_updated.csv",'w'),delimiter='\t')
    counter = 0
    total_undisambiguated = 0
    total_missed = []
    for i in raw_loc_data:
        counter +=1
        if counter%500000==0:
            print(str(counter))
        if i['id'] in id_lat_long_lookup.keys():
            lat_long = id_lat_long_lookup[i['id']]
            loc_id = lat_long_cannonical_name[lat_long]['id']
            updated.writerow([i['id'],loc_id, i['city'], i['state'], i['country'], i['country_transformed'], lat_long])
        else:
            updated.writerow([i['id'],'', i['city'], i['state'], i['country'], i['country_transformed'],''])

def upload_rawloc(db_con, disambiguated_folder):
    raw_data = pd.read_csv(disambiguated_folder + "/rawlocation_updated.csv", delimiter = '\t')
    print('uploading')
    raw_data.to_sql(con=db_con, name = 'rawlocation', if_exists = 'append', index = False)


if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    
    disambiguated_folder = "{}/disambig_output".format(config['FOLDERS']['WORKING_FOLDER'])
    print('here!')
    id_lat_long_lookup, lat_long_cannonical_name = make_lookup(disambiguated_folder)
    print('made lookup')
    fips_lookups = create_fips_lookups(config['FOLDERS']['PERSISTENT_FILES'])
    upload_location(db_con, lat_long_cannonical_name, disambiguated_folder, fips_lookups)
    print('done locupload ')
    process_rawlocation(db_con, lat_long_cannonical_name, id_lat_long_lookup, disambiguated_folder)
    print('done process')
    upload_rawloc(db_con, disambiguated_folder)



