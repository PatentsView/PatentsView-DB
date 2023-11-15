import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from lib.utilities import get_current_config, get_connection_string
from sqlalchemy import create_engine
from time import time

def export_parquet_file(rawassignee_records, letter):
    export_name = f"rawassignee_{letter}.parquet"
    table = pa.Table.from_pandas(rawassignee_records)
    try:
        pq.write_table(table, export_name)
    except:
        print(f"writing {export_name} failed")

def get_rawassignee_data(config):
    from string import ascii_lowercase as alc
    cstr = get_connection_string(config, 'PROD_DB')
    quarter_ed = config["DATES"]["END_DATE"]
    engine = create_engine(cstr)
    for letter in alc:
        # alter table rawassignee add index first_char_organization (organization(1));
        q = f"""
        select uuid
        	, name_first
        	 , name_last
        	 , organization
        	 , c.city as disambig_city
        	 , c.state as disambig_state
        	 , c.country as disambig_country
        	 , c.latitude as disambig_lat
        	 , c.longitude as disambig_long
        from rawassignee a 
        	inner join rawlocation b on a.rawlocation_id =b.id
        	inner join location_{quarter_ed} c on b.location_id=c.location_id
        	where left(organization, 1) = "{letter}"
            """
        query_start_time = time()
        rawassignee_records = pd.read_sql_query(sql=q, con=engine)
        print(q)
        query_end_time = time()
        print("\t\tThis query took:", (query_end_time-query_start_time)/60, "minutes")
        export_parquet_file(rawassignee_records, letter)

def get_rawassignee_null_orgs_data(config):
    from string import ascii_lowercase as alc
    cstr = get_connection_string(config, 'PROD_DB')
    quarter_ed = config["DATES"]["END_DATE"]
    engine = create_engine(cstr)
    q = f"""
    select uuid
        , name_first
         , name_last
         , organization
         , c.city as disambig_city
         , c.state as disambig_state
         , c.country as disambig_country
         , c.latitude as disambig_lat
         , c.longitude as disambig_long
    from rawassignee a 
        inner join rawlocation b on a.rawlocation_id =b.id
        inner join location_{quarter_ed} c on b.location_id=c.location_id
        where organization is null
        """
    query_start_time = time()
    rawassignee_records = pd.read_sql_query(sql=q, con=engine)
    print(q)
    query_end_time = time()
    print("\t\tThis query took:", (query_end_time-query_start_time)/60, "minutes")
    export_parquet_file(rawassignee_records, "null_org")

if __name__ == '__main__':
    type = 'granted_patent'
    config = get_current_config(type, schedule="quarterly", **{"execution_date": datetime.date(2023, 7, 1)})
    get_rawassignee_data(config)
    get_rawassignee_null_orgs_data(config)
