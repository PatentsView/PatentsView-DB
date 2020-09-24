import time
import pandas as pd
from sqlalchemy import create_engine

from lib.configuration import get_connection_string


#########################################################################################################
# WIDE TABLE CREATION BELOW
#########################################################################################################
# REQUIRES: db_con, old_db, new_db, entity, raw entity table name, total rows in raw entity table, new_db timestamp
# MODIFIES: nothing
# EFFECTS: returns list of wide persistent entity columns
def get_wide_entity_disambig_cols(db_con, old_db, persistent_disambig_table):
    result = db_con.execute(
        "select column_name from information_schema.columns where table_schema = '{0}' and table_name = '{1}';".format(
            old_db, persistent_disambig_table))
    result_cols = [r[0] for r in result]
    disambig_cols = [x for x in result_cols if x.startswith('disamb')]
    disambig_cols.sort()
    print(disambig_cols)

    return disambig_cols


# REQUIRES: db_con, old_db, new_db, entity, raw entity table name, total rows in raw entity table, new_db timestamp
# MODIFIES: nothing
# EFFECTS: writes .tsv of table in wide format
def write_wide_outfile(db_con, new_db, old_db, entity, persistent_long_table, raw_table, id_col, total_rows, outfile_fp,
                       header_df):
    # fixed
    current_rawentity = 'current_raw{0}_id'.format(entity)
    old_rawentity = 'old_raw{0}_id'.format(entity)
    disamb_str = 'disamb_{}_id_'.format(entity)

    # fixed
    chunk_cols = [current_rawentity, old_rawentity, 'database_update', id_col]

    ############ 2. Convert long -> wide and output .tsv: grab all uuid rows together for a set of uuids
    limit = 300000
    offset = 0

    start = time.time()
    itr = 0

    print('Estimated # of rounds: ', total_rows / 300000)

    while True:

        print('###########################################\n')

        print('Next iteration... ', itr)

        sql_stmt_inner = "(select uuid from {0}.{1} order by uuid limit {2} offset {3}) raw".format(new_db, raw_table,
                                                                                                    limit, offset)
        sql_stmt_template = "select lf.uuid as {0}, raw_old.uuid as {1}, lf.database_update, lf.{2} from {3} left join {4}.{5} lf on raw.uuid = lf.uuid left join {5}.{6} raw_old on lf.uuid = raw_old.uuid;".format(
            current_rawentity, old_rawentity, id_col, sql_stmt_inner, new_db, persistent_long_table, old_db, raw_table)

        print(sql_stmt_template)
        result = db_con.execute(sql_stmt_template)

        chunk_results = [r for r in result]

        # no more result batches to process! done
        if len(chunk_results) == 0:
            break

        # 0. Preprocess dataupdate column to add prefix + save current/old uuid lookup
        chunk_df = pd.DataFrame(chunk_results, columns=chunk_cols)
        chunk_df['database_update'] = disamb_str + chunk_df['database_update']

        uuid_lookup = chunk_df[[current_rawentity, old_rawentity]].drop_duplicates()

        # 1. Pivot, reset index & get back uuid as column, rename axis & remove database_update axis value
        pivoted_chunk_df = chunk_df.pivot(index=current_rawentity, columns='database_update',
                                          values=id_col).reset_index().rename_axis(None, 1)

        # 2. Merge back old rawinventor id column
        merged_df = pd.merge(pivoted_chunk_df, uuid_lookup)

        # 3. Concat with sort = False (preserves desired col order from header_df)
        formatted_chunk_df = pd.concat([header_df, merged_df], sort=False)

        # 4. Write to outfile
        formatted_chunk_df.to_csv(outfile_fp, index=False, header=False, mode='a', sep='\t', na_rep=None)

        offset += limit
        itr += 1

        if itr == 1:
            print('Time for 1 iteration: ', time.time() - start, ' seconds')
        print('###########################################\n')

    print('###########################################\n')
    print('total time taken:', round(time.time() - start, 2), ' seconds')
    print('###########################################\n')

    return


# REQUIRES: db_con, entity, persistent entity table, outfile folder path
# MODIFIES: nothing
# EFFECTS: creates persistent entity table for new database
def create_wide_table_database(db_con, entity, persistent_disambig_table, outfile_fp):
    ####### 3. create table in database
    db_con.execute('drop table if exists {}.{}'.format(new_db, persistent_disambig_table))

    current_rawentity = 'current_raw{0}_id'.format(entity)

    # only read header for creating table
    wide_df = pd.read_csv(outfile_fp, sep='\t', nrows=1)
    entity_db_cols = list(wide_df.columns.values)

    create_stmt = 'create table {0}.{1} ( '.format(new_db, persistent_disambig_table)
    primary_key_stmt = 'PRIMARY KEY (`{0}`));'.format(current_rawentity)

    create_stmt = get_create_syntax(db_con, entity, entity_db_cols, create_stmt)

    create_stmt = create_stmt + primary_key_stmt
    print(create_stmt)
    db_con.execute(create_stmt)

    return


def create_persistent_wide_entity(config, entity):
    db_con = create_engine(get_connection_string(config, 'NEW_DB')+'&local_infile=1')
    disambig_folder = "{}/disambig_output/".format(config['FOLDERS']['WORKING_FOLDER'])

    old_db = config['DATABASE']['OLD_DB']
    new_db = config['DATABASE']['NEW_DB']
    new_db_timestamp = new_db.replace('patent_', '')

    # set of values that change depending on entity
    persistent_long_table = 'persistent_{0}_disambig_long'.format(entity)
    raw_table = 'raw{0}'.format(entity)
    id_col = '{0}_id'.format(entity)

    outfile_name_wide = 'persistent_{}_wide.tsv'.format(entity)
    outfile_fp_wide = disambig_folder + outfile_name_wide

    persistent_disambig_table = 'persistent_{0}_disambig'.format(entity)

    # get disambig cols from old db's persistent_inventor_disambig
    disambig_cols = get_wide_entity_disambig_cols(db_con, old_db, persistent_disambig_table)

    # Add new column for this data update:
    raw_cols = ['current_{0}_id'.format(raw_table), 'old_{0}_id'.format(raw_table)]
    header_wide = [raw_cols[0], raw_cols[1]] + disambig_cols + ['disamb_{0}_id_'.format(entity) + new_db_timestamp]
    print(header_wide)
    header_df = pd.DataFrame(columns=header_wide)
    header_df.to_csv(outfile_fp_wide, index=False, header=True, sep='\t')
    # get total rows in raw entity table
    result = db_con.execute('select count(*) from {0}.{1}'.format(new_db, raw_table))
    total_rows = [r[0] for r in result][0]
    write_wide_outfile(db_con, new_db, old_db, entity, persistent_long_table, raw_table, id_col, total_rows,
                       outfile_fp_wide, header_df)

    ####### 3. create table in database
    create_wide_table_database(db_con, entity, persistent_disambig_table, outfile_fp_wide)

    ######### 4. load data
    db_con.execute(
        "LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1}.{2} FIELDS TERMINATED BY '\t' NULL DEFINED BY '' IGNORE 1 lines;".format(
            outfile_fp_wide, new_db, persistent_disambig_table))

    return True
