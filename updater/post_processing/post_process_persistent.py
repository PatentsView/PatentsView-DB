import datetime

import pandas as pd
#########################################################################################################
# UPDATE LONG TABLE
#########################################################################################################
# REQUIRES: db_con, RAW_DB, long table name
# MODIFIES: nothing
# EFFECTS: returns list of long persistent entity columns
import pymysql
from sqlalchemy import create_engine

from lib.configuration import get_config, get_connection_string, get_current_config
from lib.xml_helpers import process_date

def update_long_entity(entity, database_type='granted_patent', **kwargs):
    config = get_current_config(schedule='quarterly',type=database_type, **kwargs)
    section = get_database_section(database_type)
    connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                 user=config['DATABASE_SETUP']['USERNAME'],
                                 password=config['DATABASE_SETUP']['PASSWORD'],
                                 db=config['PATENTSVIEW_DATABASES'][section],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    update_version = config['DATES']['END_DATE']
    version_indicator = process_date(update_version, as_string=True)
    source_entity_table = 'raw_{entity}'.format(entity=entity)
    source_entity_field = '{entity}_id'.format(entity=entity)

    id = 'document_number'
    if database_type == 'granted_patent':
        id = 'patent_id'
    target_persistent_table = 'persistent_{entity}_disambig_long'.format(entity=entity)

    entity_update_query = f"""
    INSERT INTO {target_persistent_table} (uuid, database_update, {source_entity_field}, version_indicator, {id}) SELECT uuid, {update_version},{source_entity_field}, {version_indicator}, {id} from {source_entity_table}
    """
    print(entity_update_query)
    if not connection.open:
        connection.connect()
    with connection.cursor() as persiste_update_cursor:
        persiste_update_cursor.execute(entity_update_query)


def generate_wide_header(connection, entity, config,section):
    ############ 1. Create output file for wide format and needed wide/long column lists
    # get disambig cols from old db's persistent_inventor_disambig
    current_rawentity = 'current_raw{0}_id'.format(entity)
    old_rawentity = 'old_raw{0}_id'.format(entity)
    persistent_table = 'persistent_{entity}_disambig'.format(entity=entity)
    if not connection.open:
        connection.connect()
    with connection.cursor() as header_cursor:
        column_query = """
        select column_name from information_schema.columns where table_schema = '{0}' and table_name = '{1}';
        """.format(
                config['PATENTSVIEW_DATABASES'][section], persistent_table)
        header_cursor.execute(column_query)
        pid_cols = [r[0] for r in header_cursor.fetchall()]
    disambig_cols = [x for x in pid_cols if x.startswith('disamb')]
    disambig_cols.sort()

    # Add new column for this data update:
    header = [current_rawentity, old_rawentity] + disambig_cols
    header_df = pd.DataFrame(columns=header)
    return header_df


def get_database_section(database_type='granted_patent'):
    if database_type == 'granted_patent':
        return 'RAW_DB'
    elif database_type == 'pgpubs':
        return 'PGPUBS_DATABASE'
    else:
        return None


def prepare_wide_table(entity, database_type='granted_patent', **kwargs):
    config = get_current_config(schedule='quarterly',type=database_type, **kwargs)
    section = get_database_section(database_type)
    connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                 user=config['DATABASE_SETUP']['USERNAME'],
                                 password=config['DATABASE_SETUP']['PASSWORD'],
                                 db=config['PATENTSVIEW_DATABASES'][section],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    update_version = config['DATES']['END_DATE']
    wide_table_name = "persistent_{entity}_disambig".format(entity=entity)
    RAW_DB = config['PATENTSVIEW_DATABASES'][section]
    if not connection.open:
        connection.connect()
    with connection.cursor() as wide_table_prep_cursor:
        # only read header for creating table
        wide_pid_df = generate_wide_header(connection, entity, config,section)

        alter_stmt = 'alter table {0}.{1} add column disamb_{2}_id_{3} varchar(256) null after {4} '.format(
            RAW_DB,
            wide_table_name,
            entity,
            update_version,
            wide_pid_df.columns[-1])
        # create_with_columns = get_create_syntax(entity, pid_cols, create_stmt)
        # primary_key_stmt = 'PRIMARY KEY (`{current_rawentity}`));'.format(current_rawentity=current_rawentity)
        #
        # complete_create_statement = create_with_columns + primary_key_stmt
        wide_table_prep_cursor.execute(alter_stmt)


def write_wide_table(entity, database_type='granted_patent', **kwargs):
    config = get_current_config(schedule='quarterly',type=database_type, **kwargs)
    section = get_database_section(database_type=database_type)
    connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                 user=config['DATABASE_SETUP']['USERNAME'],
                                 password=config['DATABASE_SETUP']['PASSWORD'],
                                 db=config['PATENTSVIEW_DATABASES'][section],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    update_version = config['DATES']['END_DATE']
    source_entity_table = 'raw{entity}'.format(entity=entity)
    source_entity_field = '{entity}_id'.format(entity=entity)
    # fixed
    current_rawentity = 'current_raw{0}_id'.format(entity)
    disamb_col = 'disamb_{}_id_{}'.format(entity, update_version)
    persistent_wide_table = 'persistent_{0}_disambig'.format(entity)
    id_col = '{0}_id'.format(entity)
    uuid = 'id'
    id = 'document_number'
    if database_type == 'granted_patent':
        id = 'patent_id'
        uuid = 'uuid'
    upsert_query = f"""
    INSERT INTO {persistent_wide_table} 
    ({current_rawentity},{disamb_col},version_indicator, {id}, sequence) 
    select 
    {uuid}, {id_col},'{update_version}', {id}, sequence 
    from {source_entity_table} 
    ON DUPLICATE  
    KEY UPDATE {disamb_col} = VALUES({disamb_col})
    """
    print(upsert_query)

    cstr = get_connection_string(config, database=section)
    engine = create_engine(cstr)
    with engine.connect() as connection:
        connection.execute(upsert_query)


if __name__ == '__main__':
    config = get_config()
    # write_wide_table('assignee', **{
    #         "execution_date": datetime.date(2021, 3, 23)
    #         })
    update_long_entity(entity='assignee', database_type='granted_patent', **{
            "execution_date": datetime.date(2021, 10, 1)
            })