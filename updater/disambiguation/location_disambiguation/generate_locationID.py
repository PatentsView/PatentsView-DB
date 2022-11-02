from lib.configuration import get_connection_string, get_current_config
# from lib import utilities
# import pymysql
import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from math import radians, cos, sin, asin, sqrt
from QA.post_processing.LocationUploadQA import LocationUploadTest


def generate_US_locationID_exactmatch(config):
    temp_db = 'TEMP_UPLOAD_DB'
    cstr = get_connection_string(config, temp_db)
    engine = create_engine(cstr)
    db = config['PATENTSVIEW_DATABASES'][temp_db]
    print(db)
    location_types = ['city', 'town', 'village', 'hamlet']
    total_rows = engine.execute("""SELECT count(*) from rawlocation where country_transformed='US';""")
    total_rows_affected = total_rows.first()[0]
    query_dict = {"city/state/country": """
        update {db}.rawlocation a 
        inner join geo_data.curated_locations b on a.city=b.location_name
        inner join patent.state_codes d on b.state=d.`State/Possession`
        inner join patent.country_codes c on a.`country_transformed`=c.`alpha-2`
                and b.`country` = c.name and a.state = d.Abbreviation
        inner join 
                (
                    select location_name, `state`, b.country, count(*)
                    from geo_data.curated_locations b 
                    where place= '{loc}' and b.`country`='United States of America' 
                    group by 1, 2, 3
                    having count(*)=1                 
                ) as dedup on b.location_name=dedup.location_name and b.state=dedup.state and b.country=dedup.country
        set location_id= b.uuid
        
        ## Adding in the lat/long distance back in because there are some exact matches with widely inaccurate lat/long data
        where (latitude-lat) >= -1.83
        and (latitude-lat) <= 1.83
        and (longitude-lon) >= -1.45
        and (longitude-lon) <= 1.45
        """}
    for key in query_dict:
        for loc in location_types:
            with engine.connect() as connection:
                exactmatch_query = eval(f'f"""{query_dict[key]}"""')
                print(exactmatch_query)
                connection.execute(exactmatch_query)
                rows_affected = connection.execute(
                    """SELECT count(*) from rawlocation where location_id is not null and country_transformed='US';""")
                if loc == 'city':
                    previous = 0
                notnull_rows = rows_affected.first()[0]
                new_rows_affected = (notnull_rows - previous)
                perc = notnull_rows / total_rows_affected
                print(f"{new_rows_affected} Rows Have Been Updated from {loc} on join {key}")
                print(f"A Total of {notnull_rows} OUT OF {total_rows_affected} or {perc} of US Rawlocations have Been Updated")
                data = {
                    "join_type": [key+'/'+loc],
                    'state': [""],
                    'country': ['US'],
                    'version_indicator': [db],
                    'count': [new_rows_affected]
                }
                table_frame = pd.DataFrame(data)
                qa_connection_string = get_connection_string(config, 'QA_DATABASE', connection='APP_DATABASE_SETUP')
                qa_engine = create_engine(qa_connection_string)
                qa_table = 'DataMonitor_LocDisambig'
                previous = notnull_rows

                try:
                    table_frame.to_sql(name=qa_table, if_exists='append', con=qa_engine, index=False)
                except SQLAlchemyError as e:
                    table_frame.to_csv("errored_qa_data" + qa_table, index=False)
                    raise e


def haversince(lat1, long1, lat2, long2):
    """
    Calculate the great circle distance in miles between two points
    on the earth (specified in decimal degrees)
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


def find_nearest_latlong(config, geo_type_list=['domestic', 'foreign']):
    temp_db = 'TEMP_UPLOAD_DB'
    cstr = get_connection_string(config, temp_db)
    engine = create_engine(cstr)
    db = config['PATENTSVIEW_DATABASES'][temp_db]
    print(db)
    for geo_type in geo_type_list:
        if geo_type == 'domestic':
            df = pd.read_sql("""select distinct state from rawlocation where location_id is null and country = 'US';""", con=engine)
            geo_list = df['state'].unique()
        elif geo_type == 'foreign':
            df = pd.read_sql("""select distinct country from rawlocation where location_id is null and country != 'US';""", con=engine)
            geo_list = df['country'].unique()
        print(f"There are {len(geo_list)} Entities To Process")
        highlevel_counter = 1
        for geo in geo_list:
            print(" --------------------- --------------------- --------------------- ")
            print(f"Processing {highlevel_counter} of {len(geo_list)} of {geo_type}")
            print(" --------------------- --------------------- --------------------- ")
            if geo_type == 'domestic':
                curated_latlongs = pd.read_sql(
                    f"""
                select uuid, lat, lon
                    from geo_data.curated_locations b 
                    inner join patent.country_codes c on  b.`country` = c.name 
                    inner join patent.state_codes d on b.state=d.`State/Possession`
                where c.`alpha-2` = 'US' and d.Abbreviation='{geo}' """, con=engine)
                state_list = df['state'].unique()
                rawlocations = pd.read_sql(
                    f"""
                select id, latitude, longitude
                    from rawlocation
                where country = 'US' 
                    and state = '{geo}'
                    and location_id is null 
                    and latitude is not null 
                    and longitude is not null""", con=engine)

            elif geo_type == 'foreign':
                curated_latlongs = pd.read_sql(
    f"""
select uuid, lat, lon
    from geo_data.curated_locations b 
    inner join patent.country_codes c on  b.`country` = c.name 
where c.`alpha-2` = '{geo}'""", con=engine)
                rawlocations = pd.read_sql(
                    f"""
select id, latitude, longitude
    from rawlocation
where country = '{geo}' and location_id is null and latitude is not null and longitude is not null""",
                    con=engine)
            total_rawloc = rawlocations.shape[0]
            print(f"\tThere are {total_rawloc} rawlocations to process for {geo}")
            raw_latlongs = rawlocations[['id', 'latitude', 'longitude']].to_records(index=False)
            nn_final = []
            counter = 1
            wrong_latlongs = []
            for raw_id, lat1, long1 in raw_latlongs:
                print(f"Processing {counter} of {total_rawloc}")
                country_nn_lookup = []

                curated_latlongs_temp = curated_latlongs[
                    (curated_latlongs['lat'] <= (lat1 + 1.83)) & (curated_latlongs['lat'] >= (lat1 - 1.83)) & (
                                curated_latlongs['lon'] <= (long1 + 1.45)) & (curated_latlongs['lon'] >= (long1 - 1.45))]
                cur_latlongs = curated_latlongs_temp[['uuid', 'lat', 'lon']].to_records(index=False)
                if curated_latlongs_temp.empty==True:
                    # print("\t\tTHIS ID NEEDS REVIEWING: ", raw_id, lat1, long1)
                    wrong_latlongs.append([raw_id])
                else:
                    for cur_uuid, lat2, long2 in cur_latlongs:
                        distance = haversince(lat1, long1, lat2, long2)
                        country_nn_lookup.append([cur_uuid, lat2, long2, distance])
                    country_nn_lookup_df = pd.DataFrame(country_nn_lookup)
                    nearest = country_nn_lookup_df[3].min()
                    nearest_df = country_nn_lookup_df[country_nn_lookup_df[3] == nearest]
                    if nearest_df.shape[0] > 1:
                        raise Exception("We have two identical curated records that are nearest instead of one")
                        # curated_location_id = nearest_df.iloc[0][0]
                    else:
                        curated_location_id = nearest_df.iloc[0][0]
                    nn_final.append([raw_id, curated_location_id])
                counter = counter+1

            wrong_latlongs_df = pd.DataFrame(wrong_latlongs, columns=['rawlocation_id'])
            wrong_latlongs_df.to_sql('rawlocation_latlong_unmatched', engine, if_exists='append', index=False)

            rawlocation_with_nn = pd.DataFrame(nn_final, columns=['rawlocation_id', 'uuid'])
            rawlocation_with_nn.to_sql('rawlocation_intermediate', engine, if_exists='replace', index=False)
            with engine.connect() as connection:
                update_nn_query = """
update rawlocation a 
inner join rawlocation_intermediate b on a.id=b.rawlocation_id
set a.location_id=b.uuid
                """
                print(update_nn_query)
                connection.execute(update_nn_query)

                rows_affected = connection.execute(
                    f"""SELECT count(*) from rawlocation_intermediate;""")
                notnull_rows = rows_affected.first()[0]
                # print("--------------------------------------------------------")
                # print(f"{notnull_rows} UPDATED FOR {geo}")
                # print("--------------------------------------------------------")
                if geo_type == 'domestic':
                    data = {
                        "join_type": ['NN'],
                        'country': ['US'],
                        'state': [geo],
                        'version_indicator': [db],
                        'count': [notnull_rows]
                    }
                elif geo_type == 'foreign':
                    data = {
                        "join_type": ['NN'],
                        'country': [geo],
                        'state': [''],
                        'version_indicator': [db],
                        'count': [notnull_rows]
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
            highlevel_counter = highlevel_counter + 1


def run_location_disambiguation(dbtype, **kwargs):
    config = get_current_config(dbtype, **kwargs)
    generate_US_locationID_exactmatch(config)
    find_nearest_latlong(config, geo_type_list=['domestic', 'foreign'])

def run_location_disambiguation_tests(dbtype, **kwargs):
    config = get_current_config(dbtype, **kwargs)
    tests = LocationUploadTest(config)
    tests.runTests()


if __name__ == "__main__":
    # config = get_current_config('granted_patent', **{
    #     "execution_date": datetime.date(2022, 5, 24)
    # })
    # config = get_current_config('pgpubs', **{
    #     "execution_date": datetime.date(2022, 6, 2)
    # })
    # generate_US_locationID_exactmatch(config)
    # find_nearest_latlong_us(config)
    # find_nearest_latlong(config)
    # tests = LocationUploadTest(config)
    # tests.runTests()

    run_location_disambiguation(dbtype='granted_patent', **{
        "execution_date": datetime.date(2022, 5, 31)
    })
    # run_location_disambiguation_tests(dbtype='granted_patent', **{
    #     "execution_date": datetime.date(2022, 5, 24)
    # })
    # HOW TO "UNDO" THIS CODE --- CAREFUL
    # drop rows from table patent_QA.DataMonitor_LocDisambig;
    # update {temp_db}.rawlocation set location_id = null;

