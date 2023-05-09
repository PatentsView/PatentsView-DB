import os
import pandas as pd
import csv
from math import ceil
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine
from tqdm import tqdm

project_home = os.environ['PACKAGE_HOME']

with open(f'{project_home}/resources/location_references/state_abr_to_name.csv', mode='r', encoding='utf-8-sig') as f:
    r = csv.DictReader(f)
    st_ab_to_nm = next(r) 
iso_csv = pd.read_csv(f'{project_home}/resources/location_references/iso-country-codes.csv',keep_default_na=False)
# co_ab_to_nm = {row['Alpha-2 code']: row['English short name lower case'] for i,row in iso_csv.iterrows()} #ISO-2 codes and names - replaced by OSM equivalents
iso3_to_iso2 = {row['Alpha-3 code']: row['Alpha-2 code'] for i,row in iso_csv.iterrows()}
osm_codes_names = {row['ISO-2']: row['name'] for i,row in pd.read_csv(f'{project_home}/resources/location_references/OSM-country-codes.csv',keep_default_na=False).iterrows()}


def generate_es_query(row):
    musts = []
    shoulds = []
    search_string = None
    # store city, state, and country outside of the row to prevent overwriting
    country = row['country']
    state = row['state']
    city = row['city']
    
    if country is not None and len(country.strip()) > 0 and country.strip().lower() not in ('unknown','omitted'):
        if len(country.strip()) > 2:
            try:
                country = iso3_to_iso2[country]
            except:
                country = country[:2] # many length 3 codes in historical data are the ISO-2 code plus 'X' rather than the ISO-3 code
        if country == 'HK':
            musts.append({'match': {'country_code': {'query': 'CN'}}})
            musts.append({'match': {'state': {'query': 'Hong Kong'}}})
            search_string = 'Hong Kong'
        elif country in osm_codes_names:
            musts.append({'match': {'country_code': {'query': country}}})
            search_string = osm_codes_names[country]

    if state is not None and len(state.strip()) > 0:
        if country is None and state.strip().upper() in st_ab_to_nm: #in US or Canada but country omitted
            if state.strip().upper() in ['ON', 'QC','NS','NB','MB','BC','PE','SK','AB','NL']: #Canada
                country = 'CA'
                musts.append({'match': {'country_code': {'query': 'CA'}}})
            else: # US
                country = 'US'
                musts.append({'match': {'country_code': {'query': 'US'}}})
        try:
            if country.strip().upper() in ['US','CA']:
                musts.append({'match': {'state': {'query': st_ab_to_nm[state]}}})
                search_string = st_ab_to_nm[state]
        except KeyError: # if state is invalid
            pass
        except AttributeError: # if country is None and no US/Canadian state
            pass

    if city is not None and len(city.strip()) > 0:
        search_string = city
        
    if search_string is not None:
        shoulds.append({"match": {"name": {"query": search_string}}})
        musts.append({"match": {"name": {"query": search_string, "fuzziness":"AUTO"}}})

    if len(shoulds) + len(musts) == 0 :
        return None
    esqry = {"query": {"bool": {}}}
    if len(musts) > 0:
        esqry['query']['bool']['must'] = musts
    if len(shoulds) > 0:
        esqry['query']['bool']['should'] = shoulds
        
    esqry['sort'] = ["_score", {"importance":"desc"}]

    return esqry

def match_locations(locations_to_search, es_con, **kwargs):
    chunksize = kwargs.get('chunksize', 100)
    verbose = kwargs.get('verbose', False)
    unique_locations = locations_to_search[['city','state','country']].drop_duplicates(ignore_index=True)
    unique_locations['query'] = unique_locations.apply(generate_es_query, axis=1)
    unique_locations.dropna(subset=['query'], inplace=True) # don't bother searching null queries
    unique_locations.reset_index(drop=True, inplace=True)

    print(f'matching {unique_locations.shape[0]} unique locations...')

    if verbose: 
        print("unique locations details:")
        print(unique_locations.info())

    # search locations individually (slow)
    if chunksize in [0,1,None]:
        if verbose:
            print('matching locations individually...')
        tophits = []
        for qry in (tqdm(unique_locations['query']) if verbose else unique_locations['query']):
            res = es_con.search(index='locations', body=qry)
            if res['hits']['total']['value'] > 0:
                tophits.append({'matched_name': res['hits']['hits'][0]['_source']['name'], 
                                # 'OSM_ID': res['hits']['hits'][0]['_id'], 
                                # 'ES_score': res['hits']['hits'][0]['_score'],
                                'latitude': res['hits']['hits'][0]['_source']['lat'], 
                                'longitude': res['hits']['hits'][0]['_source']['lon']
                                })
            else:
                tophits.append({'matched_name': None, 'lat': None, 'long': None, 'OSM_ID': None, 'ES_score': None})

        hitframe = pd.DataFrame.from_records(tophits)
        
    # search all records at once (fast but potentially high memory or timeout for large sets):    
    elif chunksize == -1:
        search_bodies = []
        for qry in unique_locations['query']:
            search_bodies.append({"index": "locations"})
            search_bodies.append(qry)

        results = es_con.msearch(body=search_bodies)['responses']

        hitframe = pd.DataFrame.from_records([{'matched_name': res['hits']['hits'][0]['_source']['name'], 
                                                # 'OSM_ID': res['hits']['hits'][0]['_id'], 
                                                # 'ES_score': res['hits']['hits'][0]['_score'],
                                                'latitude': res['hits']['hits'][0]['_source']['lat'], 
                                                'longitude': res['hits']['hits'][0]['_source']['lon']
                                                } 
                                            if res['hits']['total']['value'] > 0 else 
                                            {'matched_name': None, 
                                                # 'OSM_ID': None, 
                                                # 'ES_score': None,
                                                'latitude': None, 
                                                'longitude': None
                                                } for res in results])
    # nontrivial chunksize
    else:
        print(f"peforming chunked matching with chunksize {chunksize}")
        divs = [chunksize * n for n in range(ceil(unique_locations.shape[0]/chunksize))]
        divs.append(unique_locations.shape[0])
        hitframe = pd.DataFrame()
        for i in (tqdm(range(len(divs)-1)) if verbose else range(len(divs)-1)):
            chunk = unique_locations[divs[i]:divs[i+1]]
            search_bodies = []
            for qry in chunk['query']:
                search_bodies.append({"index": "locations"})
                search_bodies.append(qry)

            results = es_con.msearch(body=search_bodies)['responses']

            hit_temp = pd.DataFrame.from_records([{'matched_name': res['hits']['hits'][0]['_source']['name'], 
                                                    # 'OSM_ID': res['hits']['hits'][0]['_id'], 
                                                    # 'ES_score': res['hits']['hits'][0]['_score'],
                                                    'latitude': res['hits']['hits'][0]['_source']['lat'], 
                                                    'longitude': res['hits']['hits'][0]['_source']['lon']
                                                    } 
                                                if res['hits']['total']['value'] > 0 else 
                                                {'matched_name': None, 
                                                    # 'OSM_ID': None, 
                                                    # 'ES_score': None,
                                                    'latitude': None, 
                                                    'longitude': None
                                                    } for res in results])
            
            if verbose: 
                print("chunk results details:")
                print(hit_temp.info())

            hitframe = pd.concat((hitframe,hit_temp), axis = 0, ignore_index=True)

    assert unique_locations.shape[0] == hitframe.shape[0], f"""number of results mismatch: 
                {unique_locations.shape[0]} unique raw records, 
                {hitframe.shape[0]} results returned"""

    # hitframe['location_id_transformed'] = [f"{row['latitude']}|{row['longitude']}" if row['latitude'] is not None else None for i,row in hitframe.iterrows()]
    if verbose: 
        print("total match data details:")
        print(hitframe.info())

    hitframe = pd.concat((unique_locations, hitframe),axis=1)
    if verbose: 
        print("unique data details:")
        print(hitframe.info())

    return(hitframe)
    # # merge deduplicated ES results with parse results (many to one)
    # mergeframe = locations_to_search.merge(hitframe, on=['city','state','country'], how='left')
    # if verbose: 
    #     print("mapped data details:")
    #     print(mergeframe.info())

    # return(mergeframe)


def create_location_match_table(config, **kwargs):
    database = config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB']

    host = '{}'.format(config['DATABASE_SETUP']['HOST'])
    user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
    password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
    port = '{}'.format(config['DATABASE_SETUP']['PORT'])
    engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    print('retrieving locations to match...')
    locations_to_search = pd.read_sql(f"SELECT id, city, state, country FROM rawlocation", con=engine)

    es_hostname = config['ELASTICSEARCH']['HOST']
    es_username = config['ELASTICSEARCH']['USER']
    es_password = config['ELASTICSEARCH']['PASSWORD']
    es_con = Elasticsearch(hosts=es_hostname, http_auth=(es_username, es_password), timeout=100)

    matched_locations = match_locations(locations_to_search, es_con, **kwargs)
    matched_data = locations_to_search.merge(matched_locations, on=['city','state','country'], how='left')

    # recreate engine because match_locations() can take long enough for the mysql connection to reset.
    engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database)) 

    print('creating table for matched locations...')
    create_sql = """
    CREATE TABLE IF NOT EXISTS `matched_rawlocation` (
  `id`  varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `matched_name` varchar(128) COLLATE utf8mb4_unicode_ci,
  `latitude` float DEFAULT NULL,
  `longitude` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    print(create_sql)
    engine.execute(create_sql)
    print('populating match table...')
    matched_data[['id','matched_name','latitude','longitude']].to_sql(name='matched_rawlocation' ,schema=database ,con=engine, if_exists='replace', index=False)

    print('propagating match results to rawlocation...')
    update_sql = """
    UPDATE `rawlocation` r 
    JOIN `matched_rawlocation` mr ON (r.id = mr.id)
    SET r.latitude = mr.latitude,
    r.longitude = mr.longitude
    WHERE mr.latitude IS NOT NULL
    """
    print(update_sql)
    engine.execute(update_sql)

def geocode_by_osm(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    create_location_match_table(config)

if __name__ == '__main__':
    from lib.configuration import get_current_config, get_today_dict
    config = get_current_config(type='config.ini', supplemental_configs=None, **get_today_dict())
    create_location_match_table(config)
