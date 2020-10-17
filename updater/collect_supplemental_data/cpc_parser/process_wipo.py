import os
import time

import pandas as pd
from sqlalchemy import create_engine

from lib.configuration import get_config, get_connection_string


def get_ipc_tech_code_field_map(ipc_tech_file):
    # Read in the CSV file
    ipc_technology_data = pd.read_csv(ipc_tech_file)
    # Remove trailing % (likely wildcard format)
    cleaned_ipc_tech_data = ipc_technology_data.assign(
            IPC_Code_stripped=ipc_technology_data.IPC_code.str.replace("%", "").str.replace(' ', ''))
    # Select and Rename columns
    ipc_tech_map_frame = cleaned_ipc_tech_data[[
            "IPC_Code_stripped", "Field_number"
            ]].rename({
                              "IPC_Code_stripped": "IPC_Code"
                              }, axis=1)
    # Convert to lookup dict
    #     ipc_code_to_field_map = ipc_tech_map_frame.set_index(
    #         'IPC_Code').to_dict()["Field_number"]
    return ipc_tech_map_frame


def get_ipc_cpc_ipc_concordance_map(concordance_file):
    cpc_ipc_concordance_data = pd.read_csv(concordance_file,
                                           header=None,
                                           sep="\t")
    cpc_ipc_concordance_data.columns = [
            'cpc_code', 'unknown_column_1', 'ipc_code', 'unknown_column_2',
            'unknown_column_3'
            ]
    cpc_ipc_known_data = cpc_ipc_concordance_data.drop(
            ['unknown_column_1', 'unknown_column_2', 'unknown_column_3'], axis=1)
    #     cpc_ipc_concordance_map = cpc_ipc_known_data.set_index(
    #         'cpc_code').to_dict()["ipc_code"]
    return cpc_ipc_known_data


# @profile()
def extract_wipo_data(cpc_chunk, cpc_ipc_concordance, ipc_tech_map, config):
    # Obtain IPC Concordance for each patent based on cpc subgrou ID
    cpc_current_with_concordance = cpc_chunk.merge(right=cpc_ipc_concordance,
                                                   how='left',
                                                   left_on='subgroup_id',
                                                   right_on='cpc_code').drop(
            "cpc_code", axis=1)
    # If concordance does not exist, use subgroup id as IPC code
    cpc_current_with_concordance.ipc_code.fillna(
            cpc_current_with_concordance.subgroup_id, inplace=True)
    # Create lookup fields for IPC Wipo technology field id
    cpc_current_with_concordance = cpc_current_with_concordance.assign(
            section=cpc_current_with_concordance.ipc_code.str.slice(0, 4))
    cpc_current_with_concordance = cpc_current_with_concordance.assign(
            group=cpc_current_with_concordance.ipc_code.str.split("/",
                                                                  expand=True)[0])
    # Lookup IPC Tech field ID (WIpo field id)
    # First lookup using "section" column
    cpc_current_with_wito_merge_1 = cpc_current_with_concordance.merge(
            right=ipc_tech_map,
            how='left',
            left_on='section',
            right_on='IPC_Code').drop('IPC_Code',axis=1).rename({"Field_number": "field_id"},axis=1)
    # For failed lookups use "group" field
    secondary_lookup = cpc_current_with_wito_merge_1.merge(
            right=ipc_tech_map,
            how='left',
            left_on='group',
            right_on='IPC_Code')
    # Merge the secondary lookup with main dataset (Merge by index)
    wipo_data_with_merge = cpc_current_with_wito_merge_1.join(
            secondary_lookup[["Field_number"]])
    wipo_data_with_merge.field_id.fillna(wipo_data_with_merge.Field_number,
                                         inplace=True)
    # Clean UP
    wipo_data = wipo_data_with_merge.dropna(subset=['field_id'], axis=0).drop(
            [
                    "subgroup_id", "ipc_code", "section", "group", "Field_number"
                    ],
            axis=1)
    # Counter for Each Field ID for each patent
    wipo_count = wipo_data.groupby(["patent_id",
                                    "field_id"]).size().to_frame('wipo_count')
    wipo_count = wipo_count.reset_index()
    # Retain Top 3 most frequent Wipo field IDs
    wipo_filtered_data = wipo_count.groupby("patent_id").apply(
        lambda _df: _df.nlargest(3, 'wipo_count', keep='all')).reset_index(drop=True)
    # Assign Sequence
    wipo_filtered_data_sequenced = wipo_filtered_data.drop(["wipo_count"], axis=1).assign(
            sequence=wipo_filtered_data.groupby(['patent_id']).cumcount())
    cstr = get_connection_string(config, "TEMP_UPLOAD_DB")
    print(cstr)
    engine = create_engine(cstr)
    with engine.begin() as conn:
        wipo_filtered_data_sequenced.to_sql('wipo', conn, if_exists='append', index=False, method="multi")


def wipo_chunk_processor(cpc_current_data, ipc_tech_field_map, cpc_ipc_concordance_map, config):
    extract_wipo_data(cpc_current_data, cpc_ipc_concordance_map, ipc_tech_field_map, config)


def consolidate_wipo(config):
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    insert_query = "INSERT IGNORE INTO wipo SELECT * from {temp_db}.wipo".format(
            temp_db=config["DATABASE"]["TEMP_UPLOAD_DB"])
    engine.execute(insert_query)


def process_and_upload_wipo(config):
    myengine = create_engine(get_connection_string(config, "NEW_DB"))
    wipo_output = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],
                                 'wipo_output')
    if not os.path.exists(wipo_output):
        os.mkdir(wipo_output)
    persistent_files = config['FOLDERS']['PERSISTENT_FILES']
    ipc_tech_file = '{}/ipc_technology.csv'.format(persistent_files)
    ipc_tech_field_map = get_ipc_tech_code_field_map(ipc_tech_file)

    concordance_file = '{}/{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],
                                         'cpc_input', 'ipc_concordance.txt')

    cpc_ipc_concordance_map = get_ipc_cpc_ipc_concordance_map(concordance_file)

    limit = 10000
    offset = 0
    batch_counter = 0
    base_query_template = "SELECT id from patent order by id limit {limit} offset {offset}"
    cpc_query_template = "SELECT c.patent_id, c.subgroup_id from cpc_current c join ({base_query}) p on p.id = " \
                         "c.patent_id"
    while True:
        start = time.time()
        batch_counter += 1
        base_query = base_query_template.format(limit=limit, offset=offset)
        cpc_join_query = cpc_query_template.format(base_query=base_query)
        cpc_current_data = pd.read_sql_query(con=myengine, sql=cpc_join_query)
        if cpc_current_data.shape[0] < 1:
            break
        wipo_chunk_processor(cpc_current_data, ipc_tech_field_map, cpc_ipc_concordance_map, config)
        offset = offset + limit
        end = time.time()
        print("Chunk Time:" + str(round(end - start)))
    consolidate_wipo(config)


if __name__ == '__main__':
    config = get_config()
    process_and_upload_wipo(config)
