import pandas as pd
from sqlalchemy import create_engine


def generate_search_json(row):
    filters = []
    search_string = None
    if row[3] is not None and len(row[3].strip()) > 0:
        filters.append({'match': {'country_code': {'query': row[3]}}})
        search_string = row[3]
    if row[2] is not None and len(row[2].strip()) > 0:
        filters.append({'match': {'state': {'query': row[2]}}})
        search_string = row[2]
    if row[1] is not None and len(row[1].strip()) > 0:
        search_string = row[1]
    if search_string is not None:
        filters.append({"match": {"name": {'query': search_string}}})
        return {
            "query": {
                "bool": {
                    #                     "filter": filters,
                    "must": filters
                }
            }
        }
    return None


def generate_next_record_set(c, limit, offset, start_dt, end_dt):
    rawlocation_query_template = """
SELECT 
  rl.id, 
  rl.city, 
  sc.`State/Possession`,        
  c.name
FROM 
  rawlocation rl
           join patent.country_codes c on c.`alpha-2` = rl.country_transformed
  LEFT JOIN patent.state_codes sc ON sc.Abbreviation = rl.state 
WHERE 
(  rl.city IS NOT NULL 
  OR sc.`State/Possession` IS NOT NULL 
  OR rl.country_transformed IS NOT NULL ) and location_id_transformed is null and rl.version_indicator between '{start_dt}' and '{end_dt}'
ORDER BY 
  id
LIMIT 
  {limit}
OFFSET
  {offset}
"""
    rawlocation_query = rawlocation_query_template.format(limit=limit, offset=offset, start_dt=start_dt, end_dt=end_dt)
    with c.connect() as rawlocation_cursor:
        rawlocation_records = rawlocation_cursor.execute(rawlocation_query)
        for record in rawlocation_records:
            yield record


def get_total_records(c, start_dt, end_dt):
    count_statement = """
    SELECT 
      count(1) 
    FROM
        rawlocation 
    WHERE 
    (  city IS NOT NULL 
      OR state IS NOT NULL 
      OR country_transformed IS NOT NULL) and location_id_transformed is null and version_indicator between '{start_dt}' and '{end_dt}'
    """.format(start_dt=start_dt, end_dt=end_dt)
    result = c.execute(count_statement)
    total_rows = result.fetchall()[0][0]
    return total_rows


def get_db_connection(config, source):
    from lib.configuration import get_connection_string
    cstr = get_connection_string(config, source)
    return create_engine(cstr)


def search_for_lat_lon(config, source):
    from lib.configuration import get_es
    es = get_es(config)
    connection = get_db_connection(config, source)
    limit = 10000
    offset = 0
    total_rows = get_total_records(connection, config['DATES']['START_DATE'], config['DATES']['END_DATE'])
    suffix = config['DATES']['END_DATE']
    target_table = 'rawlocation_lat_lon_{suffix}'.format(suffix=suffix)
    create_sql = """
    CREATE TABLE `{target_table}` (
  `lat` float DEFAULT NULL,
  `lon` float DEFAULT NULL,
  `id`  varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """.format(target_table=target_table)
    connection.execute(create_sql)
    view_sql = """
    CREATE OR REPLACE VIEW rawlocation_lat_lon as SELECT id, lat, lon from {target_table}
    """.format(target_table=target_table)
    while True:
        if offset >= total_rows:
            break
        ids = []
        geo_records = []
        #     try:
        search_bodies = []
        for db_record in generate_next_record_set(connection, limit, offset, config['DATES']['START_DATE'],
                                                  config['DATES']['END_DATE']):
            if not all([x is None or len(x) < 1 for x in db_record[1:]]):
                ids.append(db_record[0])
                search_body = generate_search_json(db_record)
                if search_body is not None:
                    search_bodies.append({"index": "locations"})
                    search_bodies.append(search_body)
        offset = offset + limit
        if len(search_bodies) < 1:
            continue
        search_hits = es.msearch(search_bodies)
        for search_hit in search_hits['responses']:
            if search_hit is None or search_hit['hits']['total']['value'] < 1:
                geo_records.append({'lat': None, 'lon': None})
                continue
            geo_records.append({
                'lat':
                    search_hit['hits']['hits'][0]['_source']['lat'],
                'lon':
                    search_hit['hits']['hits'][0]['_source']['lon']
            })
        geo_frame = pd.DataFrame(geo_records)
        geo_frame = geo_frame.assign(id=ids)
        #     tries = 0
        geo_frame.to_sql(name=target_table, con=connection,
                         index=False, if_exists='append')
    connection.execute(view_sql)
    update_lat_lon(connection, target_table)


def update_lat_lon(engine, source_table):
    index_query = """
    ALTER TABLE {source_table} add primary key(id)
    """.format(source_table=source_table)
    engine.execute(index_query)
    update_query = """
    UPDATE rawlocation rl join {source_table} l on l.id = rl.id set rl.location_id_transformed = CONCAT(lat,'|',lon) where lat is not null
    """.format(source_table=source_table)
    engine.execute(update_query)
