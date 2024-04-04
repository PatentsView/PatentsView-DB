from datetime import datetime, timedelta, date

import pandas as pd
from sqlalchemy import create_engine, inspect
import re
import shapefile
from shapely.geometry import shape, Point
from tqdm import tqdm

from QA.post_processing.LocationPostProcessing import LocationPostProcessingQC
from lib.configuration import get_connection_string, get_current_config, get_today_dict, get_unique_connection_string
from lib.is_it_update_time import get_update_range
from lib.utilities import weekday_count

tqdm.pandas()

def update_rawlocation(update_config):
    prod_db = update_config["PATENTSVIEW_DATABASES"]['PROD_DB']
    engine = create_engine(get_connection_string(update_config, database="PROD_DB"))
    end_date = update_config['DATES']['END_DATE']
    update_statement = f"""
        UPDATE {prod_db}.rawlocation rl 
            inner join location_disambiguation_mapping_{end_date} ldm on ldm.id = rl.id
        set rl.location_id = ldm.location_id
        where rl.location_id is null
    """
    print(update_statement)
    engine.execute(update_statement)


def precache_locations(config):
    query0 = """drop table if exists unique_locations;"""
    query1 = """create table unique_locations 
select distinct location_id from (
select location_id from patent.rawlocation
union all
select location_id from pregrant_publications.rawlocation) as UNDERLYING;"""
    query2 = f""" alter table unique_locations add index location_id (location_id);"""
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    for q in [query0, query1, query2]:
        print(q)
        engine.execute(q)


def create_location(update_config):
    engine = create_engine(get_connection_string(update_config, "RAW_DB"))
    end_date = update_config["DATES"]["END_DATE"].strip("-")
    print(f"CREATING location_{end_date} ... ")

    # create table in RAW_DB (patent) and insert unique locations with curated information.
    query0 = f"""
    create table location_{end_date}
    select uuid as location_id 
    , g.id as curated_location_id 
    , location_name as city
    , g.state as curated_locations_state
    , cc.`Alpha-2` as country
    , g.country as curated_locations_country
    , lat as latitude
    , lon as longitude
    from patent.unique_locations u 
    inner join geo_data.curated_locations g on g.uuid=u.location_id
    inner join geo_data.country_codes cc on g.country=cc.name
    """
    # add indices
    query1 = f""" alter table patent.location_{end_date} add index location_id (location_id) """
    query2 = f""" alter table patent.location_{end_date} add index curated_locations_state (curated_locations_state) """
    # add column to populate in next step
    query3 = f"""alter table patent.location_{end_date} add column state nvarchar(200), add column state_fips nvarchar(200)"""
    # set state and state FIPS value in new location table using geo_data references (US and Canada only)
    # set state abbreviation based on curated state name
    # set state FIPS based on above abbreviation
    query4 = f"""
update patent.location_{end_date} a 
inner join geo_data.state_codes sc on a.curated_locations_state=sc.`State/Possession`
left join geo_data.census_fips cf on Abbreviation=cf.State
set a.state_fips = cf.STATE_FIPS, a.state = Abbreviation
where a.country = 'US' or a.country = 'CA'"""
    # add column to populate in next step
    query5 = f"""alter table patent.location_{end_date} add column county nvarchar(200), add column county_fips nvarchar(200)"""
    # set county and county FIPS value in new location table by matching with state and city name in geo_data reference
    query6 = f""" 
update patent.location_{end_date} a 
inner join geo_data.county_lookup coun on a.state=coun.state and a.city=coun.city
set a.county_fips = RIGHT(coun.county_fips, 3), 
a.county = coun.county
where a.country = 'US'
     """
    for q in [query0, query1, query2, query3, query4, query5, query6]:
        print(q)
        engine.execute(q)

    print(f"CREATING location ... ")
    # remove existing location view
    query1 = """
    Drop view if exists location;
    """
    # create new view referencing current update location table
    query2 = f"""
    CREATE SQL SECURITY INVOKER VIEW `location` AS SELECT
   `patent`.`location_{end_date}`.`location_id` AS `id`,
   `patent`.`location_{end_date}`.`curated_location_id` AS `curated_location_id`,
   `patent`.`location_{end_date}`.`city` AS `city`,
   `patent`.`location_{end_date}`.`state` AS `state`,
   `patent`.`location_{end_date}`.`country` AS `country`,
   `patent`.`location_{end_date}`.`latitude` AS `latitude`,
   `patent`.`location_{end_date}`.`longitude` AS `longitude`,
   `patent`.`location_{end_date}`.`county` AS `county`,
   `patent`.`location_{end_date}`.`state_fips` AS `state_fips`,
   `patent`.`location_{end_date}`.`county_fips` AS `county_fips`,
    convert('{end_date}' using utf8mb4) COLLATE utf8mb4_unicode_ci AS `version_indicator`
FROM `patent`.`location_{end_date}`;
    """
    # add indices
    query3 = f""" alter table `location_{end_date}` add index city (city); """
    query4 = f""" alter table `location_{end_date}` add index state (state); """
    query5 = f""" alter table `location_{end_date}` add index country (country); """
    for q in [query1, query2, query3, query4, query5]:
        print(q)
        engine.execute(q)

def lookup_fips(db_record, county_shapes):
    """
    checks geographic point for membership in county shapes and returns related state and county FIPS codes
    :param db_record: pandas DataFrame row containing a shapely.geometry Point under the index 'pt' and the location_id the point represents
    :param all_county_shapes: the set of shape objects to check if db_record.pt falls inside
    """
    fips = {'location_id': db_record.location_id,'county_fips': None, 'state_fips': None}
    counties = [x for x in county_shapes if shape(x.shape).contains(db_record.pt)]
    # do not assign a code if there are zero or multiple containing counties
    if len(counties) == 1:
        fips['county_fips'] = counties[0].record.COUNTYFP
        fips['state_fips'] = counties[0].record.STATEFP
    return pd.Series(fips)

def fips_geo_patch(config):
    """
    fills in state and county FIPS codes for locations that were not coded by lookup but could be coded by latitude/longitude
    :param config: config object containing crucial date, file, and database information
    """
    end_date = config["DATES"]["END_DATE"].strip("-")
    
    print("reading shapefiles...")
    shapefile_source = config['FILES']['COUNTY_SHAPEFILES']
    rdr = shapefile.Reader(shapefile_source)
    county_shapes = [s for s in rdr.iterShapeRecords()]

    engine = create_engine(get_connection_string(config, "RAW_DB"))

    print('retrieving US locations without county FIPS code:')
    missing_fips_query = f"""
    SELECT *
    FROM patent.location_{end_date}
    WHERE country = 'US'
    AND county_fips IS NULL
    """
    print(missing_fips_query)
    missing_fips_records = pd.read_sql(missing_fips_query, con=engine)
    print(f"{missing_fips_records.shape[0]} records found.")

    if missing_fips_records.shape[0] > 0:
        print("mapping locations...")
        missing_fips_records = missing_fips_records.assign(pt=missing_fips_records.apply(lambda x: Point(x.longitude, x.latitude), axis=1))

        lookup_results = missing_fips_records.progress_apply(lookup_fips, axis=1, args=(county_shapes,))

        engine = create_engine(get_connection_string(config, "RAW_DB")) # re-connecting as failsafe in case record matching takes too long
        print("uploading matched locations...")
        lookup_results.to_sql(f"fips_geocode_patch_log_{end_date}", con=engine, index=False)

        print("merging geo-matched locations into main table:")
        merge_query = f"""
        UPDATE patent.location_{end_date} loc
        JOIN patent.fips_geocode_patch_log_{end_date} patch ON loc.location_id = patch.location_id
        LEFT JOIN geo_data.census_fips fips ON (patch.state_fips = fips.state_fips AND patch.county_fips = fips.county_fips)
        SET loc.state_fips = patch.state_fips,
        loc.county_fips = patch.county_fips,
        loc.county = fips.county_name,
        loc.state = fips.state
        WHERE loc.country = 'US'
        AND loc.county_fips IS NULL
        AND patch.county_fips IS NOT NULL
        """
        print(merge_query)
        mergeres = engine.execute(merge_query)
        print(f"{mergeres.rowcount} records updated")

    else:
        print("no locations required geographic FIPS assignment.")


def consolidate_location_disambiguation_quarterly(config):
    prod_db = config["PATENTSVIEW_DATABASES"]['PROD_DB']
    engine = create_engine(get_connection_string(config, "PROD_DB"))
    dbtype = 'pgpubs' if prod_db=='pregrant_publications' else 'granted_patent'
    inspector = inspect(engine)
    quarter_end = config['DATES']['END_DATE']
    quarter_start = config['DATES']['START_DATE']
    # quarter_start, quarter_end = get_update_range(kwargs['execution_date'])
    print(f"consolidating location disambiguation tables for date range {quarter_start} to {quarter_end}")
    weekly_prefix = config['PATENTSVIEW_DATABASES'][f"{dbtype}_upload_db"]
    db_list = [db for db in inspector.get_schema_names() if re.fullmatch(f"{weekly_prefix}\\d{{8}}", db) and quarter_start <= db[-8:] <= quarter_end]
    print(f"{len(db_list)} databases identified in range: {db_list}")
    expected_db_count = weekday_count(datetime.strptime(quarter_start, '%Y%m%d').date(), datetime.strptime(quarter_end, '%Y%m%d').date())['Thursday' if dbtype == 'pgpubs' else 'Tuesday']
    if len(db_list) != expected_db_count:
        raise Exception(f"number of weekly DBs does not match expected value:\n{len(db_list)} weekly DBs observed; {expected_db_count} weekly DBs expected.")

    quarter_map_create = f"""
    CREATE TABLE IF NOT EXISTS {prod_db}.location_disambiguation_mapping_{quarter_end} (
    `id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    `location_id` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `version_indicator` date NOT NULL,
    `created_date` timestamp NOT NULL DEFAULT current_timestamp(),
    `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
    PRIMARY KEY (`id`),
    KEY `location_id_2` (`location_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"""
    print(quarter_map_create)
    engine.execute(quarter_map_create)

    for weekly_db in db_list:
        incorporate_week_query = f"""
            INSERT INTO {prod_db}.location_disambiguation_mapping_{quarter_end} 
            (id, location_id, version_indicator)
            SELECT id, location_id, version_indicator
            FROM {weekly_db}.location_disambiguation_mapping"""
        print(incorporate_week_query)
        engine.execute(incorporate_week_query)

def update_dis_location_mapping(config):
    engine = create_engine(get_connection_string(config, "PROD_DB"))
    end_date = config['DATES']['END_DATE']
    prod_db = config["PATENTSVIEW_DATABASES"]['PROD_DB']
    query0 = """show create view location_disambiguation_mapping"""
    current = pd.read_sql(query0, engine)
    query_list = []
    current_view_q = current["Create View"][0]
    new_table_addition = f" union all select `id` as `id`, `location_id` as `location_id` from `location_disambiguation_mapping_{end_date}` ;  "
    drop_current = f"""drop view {prod_db}.location_disambiguation_mapping"""
    query_list.append(drop_current)
    loc_dis_mapping = current_view_q + new_table_addition
    loc_dis_mapping.replace("CREATE ALGORITHM=UNDEFINED DEFINER=`pipeline_user`@`%` SQL SECURITY DEFINER VIEW", "CREATE SQL SECURITY INVOKER VIEW")
    query_list.append(loc_dis_mapping)
    for q in query_list:
        print(q)
        engine.execute(q)


def post_process_location(**kwargs):
    patent_config = get_current_config(schedule="quarterly", **kwargs)
    pgpubs_config = get_current_config(type='pgpubs', schedule="quarterly", **kwargs)
    consolidate_location_disambiguation_quarterly(patent_config)
    consolidate_location_disambiguation_quarterly(pgpubs_config)
    # update_dis_location_mapping(patent_config)
    # update_dis_location_mapping(pgpubs_config)
    update_rawlocation(patent_config)
    update_rawlocation(pgpubs_config)
    precache_locations(patent_config)
    create_location(patent_config)

def augment_location_fips(**kwargs):
    patent_config = get_current_config(schedule="quarterly", **kwargs)
    fips_geo_patch(patent_config)

def post_process_qc(**kwargs):
    config = get_current_config(schedule="quarterly", **kwargs)
    qc = LocationPostProcessingQC(config)
    qc.runTests(config)


if __name__ == '__main__':
    post_process_location(**{
            "execution_date": date(2024, 4, 5)
            })
    # config = get_current_config(schedule="quarterly", **{
    #         "execution_date": date(2023, 10, 1)
    #         })
    # consolidate_location_disambiguation_quarterly(config)




