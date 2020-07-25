from sqlalchemy import create_engine

from lib.configuration import get_connection_string


def create_lookup_tables(config):
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    with engine.connect() as cursor:
        cursor.execute('insert ignore into patent_inventor select patent_id, inventor_id from rawinventor where inventor_id is not null')
        print('patent_inventor')
        cursor.execute('insert ignore into patent_assignee select patent_id, assignee_id from rawassignee where assignee_id is not null')
        print('patent_assignee')
        cursor.execute('insert ignore into patent_lawyer select patent_id, lawyer_id from rawlawyer where lawyer_id is not null')
        print('patent_lawyer')
        cursor.execute(
            "create table temp_assignee_loc as select assignee_id, rawlocation_id, location_id_transformed as location_id from rawassignee r left join rawlocation l on r.rawlocation_id = l.id;")
        print('made locaiton_assignee_temp')
        cursor.execute(
            "insert ignore into location_assignee SELECT distinct rl.location_id, ri.assignee_id from rawassignee ri left join rawlocation rl on rl.id = ri.rawlocation_id where ri.assignee_id is not NULL and rl.location_id is not NULL;")
        print('location_assignee')
        # I don't understand why location assignee and location inventor have different types of location id
        cursor.execute(
            "create table temp_inventor_loc as select inventor_id, location_id from rawinventor r left join rawlocation l on r.rawlocation_id = l.id;")
        print('done inventor temp')

        cursor.execute(
            "insert ignore into location_inventor SELECT rl.location_id, ri.inventor_id from rawinventor ri left join rawlocation rl on rl.id = ri.rawlocation_id where ri.inventor_id is not NULL and rl.location_id is not NULL;")
        print('location_inventor')
