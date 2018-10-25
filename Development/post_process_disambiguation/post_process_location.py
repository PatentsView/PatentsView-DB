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
    #os.system('mv location_all.tsv {}/location_disambiguation.tsv'.format(disambiguated_folder))
    inp = csv.reader(open( "{}/location_disambiguation.tsv".format(disambiguated_folder),'r'),delimiter='\t')
    lat_name_lookup = {} #place to lat/long/id lookup
    undisambiguated =set() #place for locations without a lat/long
    lookup = {} #raw location id to disambiguated id
    #for some reason column 1 and 2 both have disambiguated place names
    #create the latitude/longitude to id mappings
    for row in inp:
        #TODO: need to check if this is the correct structure 
        clean_loc = tuple([None if i =='NULL' else i for i in row[1].split('|')])
        if row[3] != 'NULL':#if there is a lat-long pair
            lat_long = "{0}|{1}".format(np.round(float(row[3]),4), np.round(float(row[4]),4))
            lat_name_lookup[lat_long] = {'place': clean_loc, 'id':general_helpers.id_generator(12)}
        else: #for the place without a lat long lookup
            undisambiguated.add(row[0])

    #have to read the file in again to go back over it
    inp = csv.reader(open("{}/location_disambiguation.tsv".format(disambiguated_folder),'r'),delimiter='\t')
    #second round to produced looked up info
    for e, row in enumerate(inp):    
        if row[3] != 'NULL':
            lat_long = "{0}|{1}".format(np.round(float(row[3]),4), np.round(float(row[4]),4))
            lookup[row[0]] = {'id': lat_name_lookup[lat_long]['id'], 'lat_long':lat_long}
    return lat_name_lookup, lookup, undisambiguated

def upload_location(db_con, lat_name_lookup, disambiguated_folder):
    location = []          
    for k, v in lat_name_lookup.items():
        location.append([v['id'], v['place'][0],v['place'][1],v['place'][2],k.split('|')[0], k.split('|')[1], None, None, None])
    location_df = pd.DataFrame(location)
    location_df.columns = ['id', 'city', 'state', 'country', 'latitude', 'longitude', 'county', 'state_fips', 'county_fips']
    location_df.to_sql(con=db_con, name = 'location', if_exists = 'replace', index = False)
    location_df.to_csv('{}/location.csv'.format(disambiguated_folder))
    

def upload_rawlocation(db_con, lookup, lat_name_lookup, disambiguated_folder):
    db_con.execute('alter table rawlocation rename temp_rawlocation_backup')
    db_con.execute('create table rawlocation like temp_rawlocation_backup')
    raw_loc_data = db_con.execute("select * from temp_rawlocation_backup")
    updated = csv.writer(open(disambiguated_folder + "/rawlocation_updated.csv",'w'),delimiter='\t')
    counter = 0
    total_undisambiguated = 0
    total_missed = []
    for row in raw_loc_data:
        counter +=1
        if counter%500000==0:
            print(str(counter))
        try: 
            loc_id = lookup[i['id']]['id']
            lat_long =lookup[i['id']]['lat_long']
        except:
            loc_id = None
            lat_long = None
            #need to update the rest to null and count them
            if i[0] in nondisamb:
                total_undisambigged +=1
            else:
                total_missed.append(i[0])
        outp.writerow([row['id'],loc_id, row['city'], row['state'], row['country'], row['country_transformed'], lat_long])
    raw_data = pd.read_csv(disambiguated_folder + "/rawlocation_updated.csv", delimiter = '\t')
    raw_data.to_sql(con=db_con, name = 'rawlocation', if_exists = 'replace', index = False)
if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')

    #db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], 'patent_20180828_dev')
    
    disambiguated_folder = "{}/disambig_out".format(config['FOLDERS']['WORKING_FOLDER'])

    lat_name_lookup, lookup, undisambiguated = make_lookup(disambiguated_folder)
    print('made lookup')
    #upload_location(db_con, lat_name_lookup, disambiguated_folder)
    print('done locupload ')
    upload_rawlocation(db_con, lookup, lat_name_lookup, disambiguated_folder)
    print('done')
    
