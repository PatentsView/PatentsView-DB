import pandas as pd
import time

#########################################################################################################
# UPDATE LONG TABLE
#########################################################################################################
# REQUIRES: db_con, new_db, long table name
# MODIFIES: nothing
# EFFECTS: returns list of long persistent entity columns
from sqlalchemy import create_engine

from lib.configuration import get_connection_string


def get_long_entity_cols(db_con, new_db, persistent_long_table):
    result = db_con.execute(
        "select column_name from information_schema.columns where table_schema = '{0}' and table_name = '{1}';".format(
            new_db, persistent_long_table))
    result_cols = [r[0] for r in result]
    # skip auto-increment column, we don't need it
    long_cols = [x for x in result_cols if x != 'id']
    print(long_cols)

    return long_cols


# REQUIRES: db_con, new_db, raw entity table name, id_col (assignee/inventor id), total rows in raw entity table, new_db timestamp, outfile path, long table header cols
# MODIFIES: nothing
# EFFECTS: returns list of long persistent entity columns
def write_long_outfile_newrows(db_con, new_db, raw_table, id_col, total_rows, new_db_timestamp, outfile_fp,
                               header_long):
    ############# 2.output rawinventor rows from newdb
    limit = 300000
    offset = 0

    start = time.time()
    itr = 0

    print('Estimated # of rounds: ', total_rows / 300000)

    while True:
        print('###########################################\n')
        print('Next iteration... ', itr)

        sql_stmt_template = "select uuid, {0} from {1}.{2} order by uuid limit {3} offset {4};".format(id_col, new_db,
                                                                                                       raw_table, limit,
                                                                                                       offset)

        print(sql_stmt_template)
        result = db_con.execute(sql_stmt_template)

        # r = tuples of (uuid, new db timestamp, entity id)
        chunk_results = [(r[0], new_db_timestamp, r[1]) for r in result]

        # means we have no more result batches to process! done
        if len(chunk_results) == 0:
            break

        chunk_df = pd.DataFrame(chunk_results, columns=header_long)
        chunk_df.to_csv(outfile_fp, index=False, header=False, mode='a', sep='\t')

        # continue
        offset += limit
        itr += 1

        if itr == 1:
            print('Time for 1 iteration: ', time.time() - start, ' seconds')
            print('###########################################\n')

    print('###########################################')
    print('total time taken:', round(time.time() - start, 2), ' seconds')
    print('###########################################')

    return


# REQUIRES: entity (either inventor or assignee)
# MODIFIES: nothing
# EFFECTS: updates persistent long entity table with new rows from db update
def update_persistent_long_entity(entity, config):
    db_con = create_engine(get_connection_string('NEW_DB') + '&local_infile=1')
    new_db = config['DATABASE']['NEW_DB']
    new_db_timestamp = new_db.replace('patent_', '')
    disambig_folder = "{}/disambig_output/".format(config['FOLDERS']['WORKING_FOLDER'])

    outfile_name_long = 'persistent_{0}_long_{1}.tsv'.format(entity, new_db_timestamp)
    outfile_fp_long = disambig_folder + outfile_name_long

    # set of values that change depending on entity
    persistent_long_table = 'persistent_{0}_disambig_long'.format(entity)
    raw_table = 'raw{0}'.format(entity)
    id_col = '{0}_id'.format(entity)

    header_long = get_long_entity_cols(db_con, new_db, persistent_long_table)

    # generate header for output file
    header_df = pd.DataFrame(columns=header_long)
    header_df.to_csv(outfile_fp_long, index=False, header=True, sep='\t')

    # get total rows in raw entity table
    result = db_con.execute('select count(*) from {0}.{1}'.format(new_db, raw_table))
    total_rows = [r[0] for r in result][0]

    write_long_outfile_newrows(db_con, new_db, raw_table, id_col, total_rows, new_db_timestamp, outfile_fp_long,
                               header_long)

    ######### 3. load data
    db_con.execute(
        "LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1}.{2} FIELDS TERMINATED BY '\t' NULL DEFINED BY '' IGNORE 1 lines (uuid, database_update, {3});".format(
            outfile_fp_long, new_db, persistent_long_table, id_col))



# REQUIRES: entity, varchar, entity database id cols, sql create statement
# MODIFIES: nothing
# EFFECTS: creates persistent entity table syntax
def get_create_syntax(db_con, entity, entity_db_cols, create_stmt):
    current_rawentity = 'current_raw{0}_id'.format(entity)
    old_rawentity = 'old_raw{0}_id'.format(entity)

    # loop through to construct create syntax for entity disambig cols
    for col in entity_db_cols:

        # regardless of entity, uuid column fixed at 32
        if col == current_rawentity or col == old_rawentity:
            add_col_stmt = "`{0}` varchar(32),".format(col)

        # if assignee is entity - then disambig cols need to be varchar 64
        elif entity == 'assignee':
            add_col_stmt = "`{0}` varchar(64), ".format(col)

        # inventor entity -  disambig cols need to be varchar 16
        else:
            add_col_stmt = "`{0}` varchar(16), ".format(col)

        create_stmt += add_col_stmt

    return create_stmt

