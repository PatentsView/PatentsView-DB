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
sys.path.append('/usr/local/airflow/PatentsView-DB/Development')
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
    counter = 0
    for row in inp:
        counter +=1 
        if counter%500000 == 0:
           print(counter)
        #TODO: need to check if this is the correct structure 
        if not row[1] in name_id_lookup.keys():
            clean_loc = tuple([None if i =='NULL' else i for i in row[1].split('|')])
            if len(row) < 5: #some of the lat long pairs are ented as a pipe separated pair in the 4th column
                lat_long = row[3].split('|')
                row[3] = lat_long[0]
                row.append(lat_long[1])
            if row[3] != 'NULL':#if there is a lat-long pair
                lat_long = "{0}|{1}".format(np.round(float(row[3]),4), np.round(float(row[4]),4))
            else:
                lat_long = '|'
            name_id_lookup[row[1]] = {'place': clean_loc, 'lat_long':lat_long, 'id':general_helpers.id_generator(12)}
    print('first loop')
    #have to read the file in again to go back over it
    inp = csv.reader(open("{}/location_disambiguation.tsv".format(disambiguated_folder),'r'),delimiter='\t')
    #second round to produced looked up info
    for e, row in enumerate(inp):    
         lookup[row[0]] = row[1]
    print('second loop')
    return name_id_lookup, lookup

def upload_location(db_con, name_id_lookup, disambiguated_folder):
    location = []          
    for k, v in name_id_lookup.items():
        try:
            location.append([v['id'], v['place'][0],v['place'][1],v['place'][2],v['lat_long'].split('|')[0], v['lat_long'].split('|')[1], None, None, None])
        except:
            print('key: ', k)
            print('value: ', v)
    location_df = pd.DataFrame(location)
    location_df.columns = ['id', 'city', 'state', 'country', 'latitude', 'longitude', 'county', 'state_fips', 'county_fips']
    location_df.to_sql(con=db_con, name = 'location', if_exists = 'append', index = False)
    location_df.to_csv('{}/location.csv'.format(disambiguated_folder))
    

def process_rawlocation(db_con, lookup, name_id_lookup, disambiguated_folder):
   
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
        if i['id'] in lookup.keys():
            cannonical_name = lookup[i['id']]
            lat_long = name_id_lookup[cannonical_name]['lat_long']
            loc_id = name_id_lookup[cannonical_name]['id']
            updated.writerow([i['id'],loc_id, i['city'], i['state'], i['country'], i['country_transformed'], lat_long])
        else:
            updated.writerow([i['id'],'', i['city'], i['state'], i['country'
], i['country_transformed'],''])

def upload_rawloc(db_con, disambiguated_folder):
    raw_data = pd.read_csv(disambiguated_folder + "/rawlocation_updated.csv", delimiter = '\t')
    print('uploading')
    raw_data.to_sql(con=db_con, name = 'rawlocation', if_exists = 'append', index = False)


if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    
    disambiguated_folder = "{}/disambig_out".format(config['FOLDERS']['WORKING_FOLDER'])
    '''
    print('here!')
    print('data inserted')
    name_id_lookup, lookup = make_lookup(disambiguated_folder)
    print('made lookup')
    upload_location(db_con, name_id_lookup, disambiguated_folder)
    print('done locupload ')
    process_rawlocation(db_con, lookup, name_id_lookup, disambiguated_folder)
    print('done process')
    '''
    upload_rawloc(db_con, disambiguated_folder)
