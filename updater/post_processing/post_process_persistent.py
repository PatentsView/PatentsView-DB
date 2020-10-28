import time

import pandas as pd
#########################################################################################################
# UPDATE LONG TABLE
#########################################################################################################
# REQUIRES: db_con, new_db, long table name
# MODIFIES: nothing
# EFFECTS: returns list of long persistent entity columns
import pymysql
from sqlalchemy import create_engine

from lib.configuration import get_config, get_connection_string


def get_long_entity_cols(db_con, new_db, persistent_long_table):
    result = db_con.execute(
            "select column_name from information_schema.columns where table_schema = '{0}' and table_name = '{"
            "1}';".format(
                    new_db, persistent_long_table))
    result_cols = [r[0] for r in result]
    # skip auto-increment column, we don't need it
    long_cols = [x for x in result_cols if x != 'id']
    print(long_cols)

    return long_cols


# REQUIRES: entity, varchar, entity database id cols, sql create statement
# MODIFIES: nothing
# EFFECTS: creates persistent entity table syntax
def get_create_syntax(entity, entity_db_cols, create_stmt):
    current_rawentity = 'current_raw{0}_id'.format(entity)
    old_rawentity = 'old_raw{0}_id'.format(entity)

    # loop through to construct create syntax for entity disambig cols
    for col in entity_db_cols:

        # regardless of entity, uuid column fixed at 32
        if col == current_rawentity or col == old_rawentity:
            add_col_stmt = "`{0}` varchar(64),".format(col)

        # if assignee is entity - then disambig cols need to be varchar 64
        elif entity == 'assignee':
            add_col_stmt = "`{0}` varchar(64), ".format(col)

        # inventor entity -  disambig cols need to be varchar 16
        else:
            add_col_stmt = "`{0}` varchar(16), ".format(col)

        create_stmt += add_col_stmt

    return create_stmt


def update_long_entity(config, entity):
    connection = pymysql.connect(host=config['DATABASE']['HOST'],
                                 user=config['DATABASE']['USERNAME'],
                                 password=config['DATABASE']['PASSWORD'],
                                 db=config['DATABASE']['NEW_DB'],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    update_version = config['DATES']['END_DATE']
    source_entity_table = '{entity}_disambiguation_mapping'.format(entity=entity)
    source_entity_field = '{entity}_id'.format(entity=entity)

    target_persistent_table = 'persistent_{entity}_disambig_long'.format(entity=entity)

    entity_update_query = "INSERT INTO {target_table} (uuid, database_update, {entity_id}) SELECT uuid, {db_version}, " \
                          "{entity_id} from {source_table}".format(
            target_table=target_persistent_table, source_table=source_entity_table, entity_id=source_entity_field,
            db_version=update_version)
    print(entity_update_query)
    if not connection.open:
        connection.connect()
    with connection.cursor() as persiste_update_cursor:
        persiste_update_cursor.execute(entity_update_query)


def generate_wide_header(connection, entity, update_version, config):
    ############ 1. Create output file for wide format and needed wide/long column lists
    # get disambig cols from old db's persistent_inventor_disambig
    current_rawentity = 'current_raw{0}_id'.format(entity)
    old_rawentity = 'old_raw{0}_id'.format(entity)
    persistent_table = 'persistent_{entity}_disambig'.format(entity=entity)
    if not connection.open:
        connection.connect()
    with connection.cursor() as header_cursor:
        column_query = "select column_name from information_schema.columns where table_schema = '{0}' and table_name " \
                       "= '{1}';".format(
                config['DATABASE']['OLD_DB'], persistent_table)
        header_cursor.execute(column_query)
        pid_cols = [r[0] for r in header_cursor.fetchall()]
    disambig_cols = [x for x in pid_cols if x.startswith('disamb')]
    disambig_cols.sort()

    # Add new column for this data update:
    header = [current_rawentity, old_rawentity] + disambig_cols + [
            'disamb_' + entity + '_id_' + update_version]
    header_df = pd.DataFrame(columns=header)
    return header_df


def prepare_wide_table(config, entity):
    connection = pymysql.connect(host=config['DATABASE']['HOST'],
                                 user=config['DATABASE']['USERNAME'],
                                 password=config['DATABASE']['PASSWORD'],
                                 db=config['DATABASE']['NEW_DB'],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    update_version = config['DATES']['END_DATE']
    wide_table_name = "persistent_{entity}_disambig".format(entity=entity)
    old_db = config['DATABASE']['OLD_DB']
    new_db = config['DATABASE']['NEW_DB']
    current_rawentity = 'current_raw{0}_id'.format(entity)
    if not connection.open:
        connection.connect()
    with connection.cursor() as wide_table_prep_cursor:
        wide_table_prep_cursor.execute('drop table if exists {}.{}'.format(new_db, wide_table_name))
        # only read header for creating table
        wide_pid_df = generate_wide_header(connection, entity, update_version, config)
        pid_cols = list(wide_pid_df.columns.values)

        create_stmt = 'create table {0}.{1} ( '.format(new_db, wide_table_name)
        create_with_columns = get_create_syntax(entity, pid_cols, create_stmt)
        primary_key_stmt = 'PRIMARY KEY (`{current_rawentity}`));'.format(current_rawentity=current_rawentity)

        complete_create_statement = create_with_columns + primary_key_stmt
        wide_table_prep_cursor.execute(complete_create_statement)


def get_raw_count(connection, raw_table, new_db):
    total_rows = 0
    if not connection.open:
        connection.connect()
    with connection.cursor() as count_cursor:
        count_cursor.execute('select count(*) from {0}.{1}'.format(new_db, raw_table))
        total_rows = count_cursor.fetchall()[0][0]
    return total_rows


def get_next_raw_chunk(connection, entity, limit, offset, config):
    old_db = config['DATABASE']['OLD_DB']
    new_db = config['DATABASE']['NEW_DB']

    persistent_long_table = 'persistent_{0}_disambig_long'.format(entity)
    current_rawentity = 'current_raw{0}_id'.format(entity)
    old_rawentity = 'old_raw{0}_id'.format(entity)
    id_col = '{0}_id'.format(entity)
    raw_table = 'raw{0}'.format(entity)

    chunk_cols = [current_rawentity, old_rawentity, 'database_update', id_col]

    sql_stmt_inner = "(select uuid from {0}.{1} order by uuid limit {2} offset {3}) raw".format(new_db, raw_table,
                                                                                                limit, offset)
    sql_stmt_template = "select lf.uuid as {0}, raw_old.uuid as {1}, lf.database_update, lf.{2} from {3} left join {" \
                        "4}.{5} lf on raw.uuid = lf.uuid left join {6}.{7} raw_old on lf.uuid = raw_old.uuid;".format(
            current_rawentity, old_rawentity, id_col, sql_stmt_inner, new_db, persistent_long_table, old_db, raw_table)
    print(sql_stmt_template)
    if not connection.open:
        connection.connect()
    with connection.cursor() as count_cursor:
        count_cursor.execute(sql_stmt_template)
        chunk_results = [r for r in count_cursor]
        chunk_df = pd.DataFrame(chunk_results, columns=chunk_cols)
        return chunk_df


def write_wide_table(config, entity):
    connection = pymysql.connect(host=config['DATABASE']['HOST'],
                                 user=config['DATABASE']['USERNAME'],
                                 password=config['DATABASE']['PASSWORD'],
                                 db=config['DATABASE']['NEW_DB'],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
    update_version = config['DATES']['END_DATE']
    # fixed
    current_rawentity = 'current_raw{0}_id'.format(entity)
    old_rawentity = 'old_raw{0}_id'.format(entity)
    disamb_str = 'disamb_{}_id_'.format(entity)
    persistent_wide_table = 'persistent_{0}_disambig'.format(entity)
    raw_table = 'raw{0}'.format(entity)
    id_col = '{0}_id'.format(entity)

    new_db = config['DATABASE']['NEW_DB']
    # fixed
    total_rows = get_raw_count(connection, raw_table, new_db)
    header_df = generate_wide_header(connection, entity, update_version, config)
    ############ 2. Convert long -> wide and output .tsv: grab all uuid rows together for a set of uuids
    limit = 100
    offset = 0

    start = time.time()
    itr = 0

    print('Estimated # of rounds: ', total_rows / limit)
    if_exists = 'append'
    while True:
        print('###########################################\n')
        print('Next iteration... ', itr)

        chunk_df = get_next_raw_chunk(connection, entity, limit, offset, config)
        if chunk_df.shape[0] == 0:
            break

        # 0. Preprocess dataupdate column to add prefix + save current/old uuid lookup

        chunk_df['database_update'] = disamb_str + chunk_df['database_update']

        uuid_lookup = chunk_df[[current_rawentity, old_rawentity]].drop_duplicates()

        # 1. Pivot, reset index & get back uuid as column, rename axis & remove database_update axis value
        pivoted_chunk_df = chunk_df.pivot_table(index=current_rawentity, columns='database_update',
                                          values=id_col, aggfunc='first').reset_index()
        pd.options.display.max_columns = 100

        # 2. Merge back old rawinventor id column
        merged_df = pd.merge(pivoted_chunk_df, uuid_lookup, on=current_rawentity)

        # 3. Concat with sort = False (preserves desired col order from header_df)
        formatted_chunk_df = pd.concat([header_df, merged_df], sort=False)

        # 4. Write to outfile
        cstr = get_connection_string(config, database='NEW_DB')
        engine = create_engine(cstr)
        formatted_chunk_df.to_sql(name=persistent_wide_table, index=False, if_exists=if_exists, con=engine)
        if_exists = 'append'
        offset += limit
        itr += 1

        if itr == 1:
            print('Time for 1 iteration: ', time.time() - start, ' seconds')
        print('###########################################\n')

    print('###########################################\n')
    print('total time taken:', round(time.time() - start, 2), ' seconds')
    print('###########################################\n')

    return


if __name__ == '__main__':
    config = get_config()
    write_wide_table(config, 'inventor')
