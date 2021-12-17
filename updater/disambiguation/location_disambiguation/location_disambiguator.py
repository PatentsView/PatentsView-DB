from sqlalchemy import create_engine

from lib.configuration import get_current_config, get_connection_string
from updater.disambiguation.location_disambiguation.NearestNeighbor import find_nearest_neighbor_for_source
from updater.disambiguation.location_disambiguation.geo_search import search_for_lat_lon


def assign_existing_location_ids(config, database_section):
    update_query = """
           update rawlocation rl join location l on l.city <=> rl.city and l.country <=> rl.country_transformed and
                                                    l.state <=> rl.state <=> l.state
           set rl.location_id=l.id, rl.location_id_transformed=CONCAT(l.latitude,'|', l.longitude)
           where rl.location_id is null and rl.version_indicator between  '{start_dt}' and '{end_date}';
       """.format(start_dt=config['DATES']['START_DATE'], end_date=config['DATES']['END_DATE'])
    cstr = get_connection_string(config=config, database=database_section)
    engine = create_engine(cstr)
    with engine.connect() as c:
        c.execute(update_query)


def update_rawlocation_for_granted(**kwargs):
    config = get_current_config(schedule='quarterly', type='granted_patent', **kwargs)
    assign_existing_location_ids(config, 'RAW_DB')


def update_rawlocation_for_pregranted(**kwargs):
    config = get_current_config(schedule='quarterly', type='pgpubs', **kwargs)
    assign_existing_location_ids(config, 'PGPUBS_DATABASE')


def update_latitude_longitude_for_granted(**kwargs):
    config = get_current_config(schedule='quarterly', type='pgpubs', **kwargs)
    search_for_lat_lon(config, source='RAW_DB')


def update_latitude_longitude_for_pregranted(**kwargs):
    config = get_current_config(schedule='quarterly', type='pgpubs', **kwargs)
    search_for_lat_lon(config, source='PGPUBS_DATABASE')


def find_nearest_neighbor_for_pregranted(**kwargs):
    config = get_current_config(schedule='quarterly', type='granted_patent', **kwargs)
    find_nearest_neighbor_for_source(config, 'PGPUBS_DATABASE')


def find_nearest_neighbor_for_granted(**kwargs):
    config = get_current_config(schedule='quarterly', type='granted_patent', **kwargs)
    find_nearest_neighbor_for_source(config, 'RAW_DB')
