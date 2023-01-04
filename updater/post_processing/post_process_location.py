import datetime
import time

import numpy as np
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from tqdm import tqdm

from QA.post_processing.LocationPostProcessing import LocationPostProcessingQC
from lib.configuration import get_connection_string, get_current_config
from updater.post_processing.create_lookup import load_lookup_table


def update_rawlocation(update_config, end_date, database='RAW_DB'):
    engine = create_engine(get_connection_string(update_config, database))
    db = update_config['PATENTSVIEW_DATABASES'][database]
    update_statement = f"""
        UPDATE {db}.rawlocation rl inner join location_disambiguation_mapping_{end_date} ldm
            on ldm.id = rl.id
        set rl.location_id = ldm.location_id
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
    # from lib.is_it_update_time import get_update_range
    # q_start_date, q_end_date = get_update_range(update_config['DATES']['START_DATE'] + datetime.timedelta(days=6))

    # query1 = f"""insert ignore into unique_locations (select distinct location_id, city, state, country from rawlocation where location_id is not null and version_indicator >= '{q_start_date}') """

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
    query1 = f""" alter table patent.location_{end_date} add index location_id (location_id) """
    query2 = f""" alter table patent.location_{end_date} add index curated_locations_state (curated_locations_state) """
    query3 = f"""alter table patent.location_{end_date} add column state nvarchar(200), add column state_fips nvarchar(200)"""
    query4 = f"""
update patent.location_{end_date} a 
inner join geo_data.state_codes sc on a.curated_locations_state=sc.`State/Possession`
left join geo_data.census_fips cf on Abbreviation=cf.State
set a.state_fips = cf.STATE_FIPS, a.state = Abbreviation
where a.country = 'US' or a.country = 'CA'"""
    query5 = f"""alter table patent.location_{end_date} add column county nvarchar(200), add column county_fips nvarchar(200)"""
    query6 = f""" 
update patent.location_{end_date} a 
inner join geo_data.county_lookup coun on a.state=coun.state and a.city=coun.city
set a.county_fips = coun.county_fips, a.county = coun.county
where a.country = 'US'
     """
    for q in [query0, query1, query2, query3, query4, query5, query6]:
        print(q)
        engine.execute(q)

    print(f"CREATING location ... ")
    query1 = """
    Drop view if exists location;
    """
    query2 = f"""
    CREATE  SQL SECURITY INVOKER  VIEW `location` AS SELECT
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
   '{end_date}' AS `version_indicator`
FROM `patent`.`location_{end_date}`;
    """
    query3 = f""" alter table `location_{end_date}` add index city (city); """
    query4 = f""" alter table `location_{end_date}` add index state (state); """
    query5 = f""" alter table `location_{end_date}` add index country (country); """
    for q in [query1, query2, query3, query4, query5]:
        print(q)
        engine.execute(q)


def post_process_location(**kwargs):
    config = get_current_config(schedule="quarterly", **kwargs)
    end_date = config['DATES']['END_DATE']
    update_rawlocation(config, end_date)
    update_rawlocation(config, end_date, database='PGPUBS_DATABASE')
    precache_locations(config)
    create_location(config)


def post_process_qc(**kwargs):
    config = get_current_config(**kwargs)
    qc = LocationPostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    # post_process_qc(**{
    #         "execution_date": datetime.date(2021, 6, 22)
    #         })
    # config = get_current_config(schedule='quarterly', **{
    #         "execution_date": datetime.date(2022, 7, 1)
    #         })
    # engine = create_engine(get_connection_string(config, "RAW_DB"))
    post_process_location(**{
            "execution_date": datetime.date(2022, 7, 1)
            })

