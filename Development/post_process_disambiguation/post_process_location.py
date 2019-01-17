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
sys.path.append('/project/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers

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
            lat_long = '|' #this handles the ~13,000 undisambiguated
        lat_long_name_count[lat_long][clean_loc] +=1  
        id_name_lookup[row[0]] = lat_long
    print('first loop')

    lat_long_cannonical_name = {}
    for lat_long, name_list in lat_long_name_count.items():
        name = sorted([(name, count) for name, count in name_list.items()], key = lambda x: x[1])[-1]
        lat_long_cannonical_name[lat_long] = {'place': name, 'id': general_helpers.id_generator(12)}

    print('second loop')
    return id_lat_long_lookup, lat_long_cannonical_name

def upload_location(db_con, lat_long_cannonical_name, disambiguated_folder):
    location = []          
    for lat_long, v in lat_long_cannonical_name.items():
        location.append([v['id'], v['place'][0],v['place'][1],v['place'][2],lat_long.split('|')[0], lat_long.split('|')[1], None, None, None])
    location_df = pd.DataFrame(location)
    location_df.columns = ['id', 'city', 'state', 'country', 'latitude', 'longitude', 'county', 'state_fips', 'county_fips']
    location_df.to_sql(con=db_con, name = 'location', if_exists = 'append', index = False)
    location_df.to_csv('{}/location.csv'.format(disambiguated_folder))
    

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
    config.read('/project/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    
    disambiguated_folder = "{}/disambig_out".format(config['FOLDERS']['WORKING_FOLDER'])
    print('here!')
    print('data inserted')
    id_lat_long_lookup, lat_long_cannonical_name = make_lookup(disambiguated_folder)
    print('made lookup')
    upload_location(db_con, lat_long_cannonical_name, disambiguated_folder)
    print('done locupload ')
    process_rawlocation(db_con, lat_long_cannonical_name, d_lat_long_lookup, disambiguated_folder)
    print('done process')
    #upload_rawloc(db_con, disambiguated_folder)


