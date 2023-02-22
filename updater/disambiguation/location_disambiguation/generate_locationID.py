from lib.configuration import get_connection_string, get_current_config, get_unique_connection_string
# from lib import utilities
# import pymysql
import datetime
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from math import radians, cos, sin, asin, sqrt
from QA.post_processing.LocationUploadQA import LocationUploadTest


def get_temp_db_config(config):
    """ Gets database credentials for read/write connection
        :keyword config: credentials for our databases
        :return: sqlalchemy pythonic way to connect to databases, database name
    """
    temp_db = 'TEMP_UPLOAD_DB'
    cstr = get_connection_string(config, temp_db)
    engine = create_engine(cstr)
    db = config['PATENTSVIEW_DATABASES'][temp_db]
    return engine, db

def get_exact_match_update_query(geo_type):
    """ Gets query to update rawlocation object with a location_id from an exact string match to city, state, country (U.S. locations) or city, country (foreign)
        :keyword geo_type: domestic or foreign
        :return: query to update rawlocation records based on canonical locations, '=' or '!=' for later use filtering to U.S. locations or non-U.S. locations
    """
    if geo_type == 'domestic':
        query_string = """
update {db}.rawlocation a 
inner join geo_data.curated_locations b on a.city=b.location_name
inner join geo_data.state_codes d on b.state=d.`State/Possession`
inner join geo_data.country_codes c on a.`country`=c.`alpha-2`
        and b.`country` = c.name and a.state = d.Abbreviation
inner join (
    select location_name, `state`, b.country, count(*)
    from geo_data.curated_locations b 
    where place= '{loc}' and b.`country` {filter} 'United States of America' 
    group by 1, 2, 3
    having count(*)=1                 
    ) as dedup on b.location_name=dedup.location_name and b.state=dedup.state and b.country=dedup.country
set location_id= b.uuid
## Adding in the lat/long distance back in because there are some exact matches with widely inaccurate lat/long data
where 
# approx. ~ 126 miles 
(latitude-lat) >= -1.83
and (latitude-lat) <= 1.83
# approx. ~ 79 miles 
and (longitude-lon) >= -1.45
and (longitude-lon) <= 1.45
and a.location_id is null
and b.place= '{loc}'"""
        filter = '='
    elif geo_type == 'foreign':
        query_string = """
update {db}.rawlocation a 
inner join geo_data.non_us_unique_city_countries b on a.city=b.location_name
inner join geo_data.country_codes c on a.`country`=c.`alpha-2`
        and b.`country` = c.name
set location_id= b.uuid
## Adding in the lat/long distance restriction between canonical locations and geocoded rawlocations because there are some exact matches with widely inaccurate lat/long data
where 
# approx. ~ 126 miles 
(latitude-lat) >= -1.83
and (latitude-lat) <= 1.83
# approx. ~ 79 miles 
and (longitude-lon) >= -1.45
and (longitude-lon) <= 1.45
and a.location_id is null 
and b.place= '{loc}'"""
        filter = '!='
    else:
        raise Exception("geography type not recognized")
    return query_string, filter

def generate_locationID_exactmatch(config, geo_type='domestic'):
    """ Associates location_id to rawlocation objects where there is an exact text match to city, state, and country (domestic) or for city and country matches (foreign)
        :keyword config: credentials for our databases
        :keyword geo_type: credentials for our databases
    """
    engine, db = get_temp_db_config(config)
    # Grab Update Query & Relevant Filter
    query, filter = get_exact_match_update_query(geo_type=geo_type)
    # Total Rows Available for Disambiguation
    total_rows = engine.execute(f"""SELECT count(*) from rawlocation where country {filter} 'US';""")
    total_rows_affected = total_rows.first()[0]
    # Our canonical locations table includes the following location_types.
    # Because locations may be in our database tagged as city and town, We iterate through the location_types starting with the largest population size first with city
    location_types = ['city', 'town', 'village', 'hamlet']
    for loc in location_types:
        with engine.connect() as connection:
            exactmatch_query = eval(f'f"""{query}"""')
            connection.execute(exactmatch_query)
            rows_affected = connection.execute(
                f"""SELECT count(*) from rawlocation where location_id is not null and country {filter} 'US';""")
            if loc == 'city':
                previous = 0
            # Cumulative Rows Affected Over the 4 location Types
            notnull_rows = rows_affected.first()[0]
            previous = notnull_rows
            new_rows_affected = (notnull_rows - previous)
            perc = notnull_rows / total_rows_affected
            print(f"{new_rows_affected} Rows Have Been Updated from {loc}")
            print(f"A Total of {notnull_rows} OUT OF {total_rows_affected} or {perc} of {geo_type} Rawlocations have Been Updated")
            save_aggregate_results_to_qa_table(config, loc, db, "", geo_type, new_rows_affected)


def get_unique_list_of_geos(config, geo_type):
    """ Return a list of states (U.S. locations) or a list of countries (Non-U.S. locations) for a given week of rawlocations
        :keyword config: credentials for our databases
        :keyword geo_type: domestic or foreign
        :return: A list of states of countries
    """
    engine, db = get_temp_db_config(config)
    # Iterate by State for U.S. Locations
    if geo_type == 'domestic':
        df = pd.read_sql("""
select distinct state 
from rawlocation a 
    inner join geo_data.state_codes b on a.state=b.`Abbreviation` 
where location_id is null and country = 'US';""", con=engine)
        geo_list = df['state'].unique()
    # Iterate by Country for NON U.S. Locations
    elif geo_type == 'foreign':
        df = pd.read_sql("""
select distinct country 
from rawlocation a 
    inner join geo_data.country_codes b on a.country=b.`alpha-2` 
where location_id is null and country != 'US';""", con=engine)
        geo_list = df['country'].unique()
    print(f"There are {len(geo_list)} Entities To Process")
    return geo_list

def get_rawlocations_and_canonical_locations_for_NN_comparison(config, geo_type, geo):
    """ Returns rawlocations and canonical locations dataframes that are within the same region for comparison
        :keyword config: credentials for our databases
        :keyword geo_type: domestic or foreign
        :keyword geo: a state or country
        :return: all rawlocation for a given geo (state or country), a list of locations for a given geo (state or country)
    """
    engine, db = get_temp_db_config(config)
    if geo_type == "domestic":
        # Pull all rawlocations (parsed from USPTO's weekly published XML files) within a given state
        rawlocations = pd.read_sql(
            f"""
        select id, latitude, longitude
        from {db}.rawlocation
        where country = 'US' 
        and state = '{geo}'
        and location_id is null 
        and latitude is not null 
        and longitude is not null""", con=engine)
        # Pull all canonical locations (derived from OSM) associated with a given state
        canonical_locations = pd.read_sql(
            f"""
        select uuid, lat, lon
        from geo_data.curated_locations b 
        inner join geo_data.country_codes c on  b.`country` = c.name 
        inner join geo_data.state_codes d on b.state=d.`State/Possession`
        where c.`alpha-2` = 'US' and d.Abbreviation='{geo}' """, con=engine)
    elif geo_type == 'foreign':
        # Pull all rawlocations (parsed from USPTO's weekly published XML files) within a given country
        rawlocations = pd.read_sql(f"""
                select id, latitude, longitude
                from {db}.rawlocation
                where country = '{geo}' 
                and location_id is null 
                and latitude is not null 
                and longitude is not null
                """, con=engine)
        # Pull all canonical locations (derived from OSM) for a given country
        canonical_locations = pd.read_sql(f"""
            select uuid, lat, lon
            from geo_data.curated_locations b 
            inner join geo_data.country_codes c on  b.`country` = c.name 
            where c.`alpha-2` = '{geo}'""", con=engine)
    return rawlocations, canonical_locations

def haversince(lat1, long1, lat2, long2):
    """ Calculate the great circle distance in miles between two points on the earth (specified in decimal degrees)
        :keyword lat1: latitude of the first location
        :keyword long1: longitude of the first location
        :keyword lat2: latitude of the second location
        :keyword long2: longitude of the second location
        :return: distance (miles)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [long1, lat1, long2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 3956  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r

def get_canonical_locations_for_hav_distance(canonical_locations, lat1, long1):
    """ Returns a list of canonical locations for comparison to a given rawlocation coordinate
        :keyword canonical_locations:
        :keyword lat1: rawlocation latitude
        :keyword long1: rawlocation longitude
        :return: a list of canonical locations within approx. ~126 miles latitude and approx. ~79 miles longitude
    """
    can_locs_to_search = canonical_locations[['uuid', 'lat', 'lon']][
        (canonical_locations['lat'] <= (lat1 + 1.83)) & (canonical_locations['lat'] >= (lat1 - 1.83)) & (
                canonical_locations['lon'] <= (long1 + 1.45)) & (
                canonical_locations['lon'] >= (long1 - 1.45))].to_records(index=False)
    return can_locs_to_search

def find_shortest_distance(can_locs_to_search, lat1, long1):
    """ Finds the closest canonical location to a given rawlocation record
        :keyword can_locs_to_search: a list of canonical locations within approx. ~ 126 miles latitude and approx. ~ 79 miles longitude
        :keyword lat1: rawlocation latitude
        :keyword long1: rawlocation longitude
        :return: the closest canonical location to a given rawlocation record
    """
    country_nn_lookup = []
    for cur_uuid, lat2, long2 in can_locs_to_search:
        distance = haversince(lat1, long1, lat2, long2)
        country_nn_lookup.append([cur_uuid, lat2, long2, distance])
    country_nn_lookup_df = pd.DataFrame(country_nn_lookup)
    # Find the closest distance from the rawlocation geocodes to our canonical locations list
    nearest = country_nn_lookup_df[3].min()
    nearest_df = country_nn_lookup_df[country_nn_lookup_df[3] == nearest]
    if nearest_df.shape[0] > 1:
        raise Exception("We have two identical curated records that are nearest instead of one")
    else:
        curated_location_id = nearest_df.iloc[0][0]
    return curated_location_id

def update_batch(engine, db):
    """ Update a batch (defined by a state or country) of rawlocations with location_id
    :keyword engine: sqlalchemy pythonic way to connect to databases
    :keyword db: database name
    :return: the number of records updated this batch
    """
    with engine.connect() as connection:
        # Update our rawlocations table in our temporary weekly parsed database
        update_nn_query = f"""
    update {db}.rawlocation a 
    inner join {db}.rawlocation_intermediate b on a.id=b.rawlocation_id
    set a.location_id=b.uuid
                    """
        print(update_nn_query)
        connection.execute(update_nn_query)

        # Save the Number of Records Updated for QA
        rows_affected = connection.execute(f"""SELECT count(*) from rawlocation_intermediate;""")
        notnull_rows = rows_affected.first()[0]
    return notnull_rows


def find_nearest_latlong(config, geo_type='domestic'):
    """ Iterate through all remaining unattributed rawlocations to match the nearest canonical locations
        :keyword config: sqlalchemy pythonic way to connect to databases
        :keyword geo_type: domestic or foreign
    """
    engine, db = get_temp_db_config(config)
    geo_list = get_unique_list_of_geos(config, geo_type)
    geo_counter = 1
    for geo in geo_list:
        rawlocations, canonical_locations = get_rawlocations_and_canonical_locations_for_NN_comparison(config, geo_type, geo)
        total_rawloc = rawlocations.shape[0]
        print(f"There are {total_rawloc} rawlocations to process for {geo}")
        raw_latlongs = rawlocations[['id', 'latitude', 'longitude']].to_records(index=False)
        # nn_final is a temporary list that stores rawlocation_id and location_id (uuid in our DB) for each iteration
        nn_final = []
        rawlocation_counter = 1
        # ambiguous_rawlocations is a temporary list that stores rawlocation_ids where location_id was not found
        ambiguous_rawlocations = []
        # Skip rawlocation records if we have no canonical records nearby
        if canonical_locations.empty or rawlocations.empty:
            continue
        else:
            # Iterate through each rawlocations
            for raw_id, lat1, long1 in raw_latlongs:
                print(f"Processing {rawlocation_counter} of {total_rawloc}")
                # filter to a list of curated locations within a given distance range of the rawlocation
                can_locs_to_search = get_canonical_locations_for_hav_distance(canonical_locations, lat1, long1)
                # saving unattributed rawlocations
                if can_locs_to_search.size == 0:
                    ambiguous_rawlocations.append([raw_id])
                else:
                    curated_location_id = find_shortest_distance(can_locs_to_search, lat1, long1)
                    nn_final.append([raw_id, curated_location_id])
                rawlocation_counter = rawlocation_counter+1

            # Save RawLocations that are still Ambiguous after location disambiguation attempt to table called rawlocations_ambiguous
            ambiguous_rawlocations_df = pd.DataFrame(ambiguous_rawlocations, columns=['rawlocation_id'])
            ambiguous_rawlocations_df.to_sql('rawlocations_ambiguous', engine, if_exists='append', index=False)

            # Create a temporary table rawlocation_intermediate used to update rawlocations iteratively for each geo
            rawlocation_with_nn = pd.DataFrame(nn_final, columns=['rawlocation_id', 'uuid'])
            rawlocation_with_nn.to_sql('rawlocation_intermediate', engine, if_exists='replace', index=False)

            notnull_rows = update_batch(engine, db)

            state = geo if geo_type == 'domestic' else ""
            country = geo if geo_type != 'domestic' else ""

            save_aggregate_results_to_qa_table(config, "NN", db, state, country, notnull_rows)
            geo_counter = geo_counter + 1


def save_aggregate_results_to_qa_table(config, join_type, db, state, country, new_rows_affected):
    """ Save metadata on the processing of rawlocation records
        :keyword config: sqlalchemy pythonic way to connect to databases
        :keyword join_type: NN (nearest neighbor or city, town, village, hamlet)
        :keyword db: database name (e.g. upload_20221108 which will contain parsed USPTO patent data from 2022-11-01 through 2022-11-01)
        :keyword state: state code if iterating through U.S. locations
        :keyword country: country code
        :keyword new_rows_affected: number of records updated in the current batch
    """
    data = {
        "join_type": [join_type],
        'state': [state],
        'country': [country],
        'version_indicator': [db],
        'count': [new_rows_affected]
    }
    table_frame = pd.DataFrame(data)
    qa_connection_string = get_connection_string(config, 'QA_DATABASE', connection='APP_DATABASE_SETUP')
    qa_engine = create_engine(qa_connection_string)
    qa_table = 'DataMonitor_LocDisambig'

    try:
        table_frame.to_sql(name=qa_table, if_exists='append', con=qa_engine, index=False)
    except SQLAlchemyError as e:
        table_frame.to_csv("errored_qa_data" + qa_table, index=False)
        raise e


def location_disambig_mapping_update(config, dbtype, **kwargs):
    """ Creates and appends a table called location_disambiguation_mapping containing rawlocation.uuid and location_id
        :keyword config: sqlalchemy pythonic way to connect to databases
        :keyword dbtype: granted_patent or pgpubs
        :keyword kwargs: execution_date (the last day in a weeks worth of parsed data)
    """
    weekly_config = get_current_config('granted_patent', **kwargs)
    cstr = get_connection_string(weekly_config, "PROD_DB")
    engine = create_engine(cstr)
    current_end_date = weekly_config["DATES"]["END_DATE"]
    db = config["PATENTSVIEW_DATABASES"]["PROD_DB"]

    from lib.is_it_update_time import get_update_range
    q_start_date, q_end_date = get_update_range(kwargs['execution_date'] + datetime.timedelta(days=7))
    end_of_quarter = q_end_date.strftime('%Y%m%d')

    with engine.connect() as connection:
        query = f"""
CREATE TABLE if not exists {db}.`location_disambiguation_mapping_{end_of_quarter}` (
  `id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `location_id` varchar(256) COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`id`),
  KEY `location_id_2` (`location_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"""
        query2 = f"""
insert into {db}.location_disambiguation_mapping_{end_of_quarter} (id, location_id)
select id, location_id from {dbtype}_{current_end_date}.rawlocation
        """
        for q in [query, query2]:
            print(q)
            connection.execute(q)

def run_location_disambiguation(dbtype, **kwargs):
    config = get_current_config(dbtype, **kwargs)
    generate_locationID_exactmatch(config, geo_type='domestic')
    generate_locationID_exactmatch(config, geo_type='foreign')
    find_nearest_latlong(config, geo_type='domestic')
    find_nearest_latlong(config, geo_type='foreign')
    location_disambig_mapping_update(config, dbtype, **kwargs)

def run_location_disambiguation_tests(dbtype, **kwargs):
    config = get_current_config(dbtype, **kwargs)
    tests = LocationUploadTest(config)
    tests.runTests()

if __name__ == "__main__":
    d = datetime.date(2022, 11, 1)
    run_location_disambiguation("granted_patent", **{
            "execution_date": d
        })
