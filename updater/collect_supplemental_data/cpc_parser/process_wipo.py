import time
import pandas as pd
from sqlalchemy import create_engine

from lib.configuration import get_connection_string


def extract_wipo_data(cpc_chunk, cpc_ipc_concordance, ipc_tech_map):
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
        right_on='IPC_Code').drop('IPC_Code',
                                  axis=1).rename({"Field_number": "wipo"},
                                                 axis=1)
    # For failed lookups use "group" field
    secondary_lookup = cpc_current_with_wito_merge_1[
        cpc_current_with_wito_merge_1.wipo.isnull()].merge(
        right=ipc_tech_map,
        how='inner',
        left_on='group',
        right_on='IPC_Code')
    # Merge the secondary lookup with main dataset (Merge by index)
    wipo_data_with_merge = cpc_current_with_wito_merge_1.join(
        secondary_lookup[["Field_number"]])
    wipo_data_with_merge.wipo.fillna(wipo_data_with_merge.Field_number,
                                     inplace=True)
    # Clean UP
    wipo_data = wipo_data_with_merge.dropna(subset=['wipo'], axis=0).drop(
        [
            "subgroup_id", "category", "section_id", "subsection_id",
            "group_id", "uuid", "ipc_code", "section", "group", "Field_number"
        ],
        axis=1)
    # Counter for Each Field ID for each patent
    wipo_count = wipo_data.groupby(["patent_number",
                                    "wipo"]).size().to_frame('wipo_count')
    # Retain Top 3 most frequent Wipo field IDs
    wipo_filtered_data = wipo_count.sort_values("wipo_count").groupby(
        "patent_number").head(3).reset_index()
    # Assign Sequence
    wipo_filtered_data_sequenced = wipo_filtered_data.assign(
        sequence=wipo_filtered_data.groupby(['patent_number']).cumcount())
    return wipo_filtered_data_sequenced


def upload_wipo(db_con, wipo_chunk):
    start = time.time()
    with db_con.begin() as conn:
        wipo_chunk.to_sql('wipo', conn, if_exists='append', index=False, method="multi")
    end = time.time()
    print("Chunk Load Time:" + str(round(end - start)))


def wipo_chunk_processor(cpc_chunk, config):
    engine = create_engine(get_connection_string(config, "TEMP_UPLOAD_DB"))
    wipo_data = extract_wipo_data(cpc_chunk, config)
    upload_wipo(engine, wipo_data)


def consolidate_wipo(config):
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    insert_query = "INSERT IGNORE INTO wipo SELECT  * from {temp_db}.wipo".format(
        temp_db=config["DATABASES"]["TEMP_UPLOAD_DB"])
    engine.execute(insert_query)


def process_and_upload_wipo(config):
    myengine = create_engine(get_connection_string(config, "NEW_DB"))
    limit = 300000
    offset = 0
    batch_counter = 0
    while True:
        batch_counter += 1
        cpc_current_data = pd.read_sql_query(con=myengine,
                                             sql="select * from cpc_current order by uuid limit {} offset {}".format(
                                                 limit, offset))
        if cpc_current_data.shape[0] < 1:
            break
        wipo_chunk_processor(cpc_current_data, config)
        offset = offset + limit

    consolidate_wipo(config)
    
