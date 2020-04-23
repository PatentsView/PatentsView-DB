import sys
import os
import pandas as pd
import re, os
import multiprocessing
import time
import uuid

from sqlalchemy import create_engine

from lib.configuration import get_connection_string, get_config


def extract_cpc_current(chunk_frame):
    # Melt Primary and Additional columns into rows
    first_level_melt = chunk_frame.melt(id_vars=['patent_number'],
                                        value_vars=['cpc_primary', 'cpc_additional'],
                                        value_name='subgroup_id',
                                        var_name='cpccategory')
    # Explode multiple classifications into columns
    first_level_melt_expanded = first_level_melt.join(
        first_level_melt.subgroup_id.str.split(';', expand=True)).drop(
        'subgroup_id', axis=1)
    # Melt the exploded columns into rows; use column name for sub sequencing (within category sequence)
    second_level_melt = first_level_melt_expanded.melt(
        id_vars=['patent_number', 'cpccategory'],
        value_name='subgroup_id',
        var_name='subsequence')
    # Remove None/NaN
    cpc_data_filtered = second_level_melt[~second_level_melt.subgroup_id.
        isnull()]
    # use the patent id, sequencing order to assign overall sequence
    # Use sort descending to get cpc_primary listed first before cpc_additional
    cpc_data_filtered_sequenced = cpc_data_filtered.assign(
        sequence=cpc_data_filtered.sort_values(
            ['patent_number', 'cpccategory', 'subsequence'],
            ascending=[True, False, True]).groupby(['patent_number'
                                                    ]).cumcount())
    # Set category values
    cpc_data = cpc_data_filtered_sequenced.assign(
        category=cpc_data_filtered_sequenced.cpccategory.map(
            {
                "cpc_primary": "inventional",
                "cpc_additional": "additional"
            })).drop(["cpccategory", "subsequence"], axis=1)
    # Split subgroup ID into section, subsection and group IDs
    cpc_data = cpc_data.assign(section_id=cpc_data.subgroup_id.str.slice(0, 1))
    cpc_data = cpc_data.assign(
        subsection_id=cpc_data.subgroup_id.str.slice(0, 3))
    cpc_data = cpc_data.assign(group_id=cpc_data.subgroup_id.str.slice(0, 4))
    # Assign UUID
    cpc_data = cpc_data.assign(uuid=cpc_data.apply(lambda _: str(uuid.uuid4()), axis=1))
    # Rename patent number to patent_id
    cpc_data = cpc_data.rename({'patent_number': 'patent_id'}, axis=1)
    return cpc_data


def upload_cpc_current(db_con, cpc_chunk):
    start = time.time()
    with db_con.begin() as conn:
        cpc_chunk.to_sql('cpc_current', conn, if_exists='append', index=False, method="multi")
    end = time.time()
    print("Chunk Load Time:" + str(round(end - start)))


def consolidate_cpc_data(config):
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    delete_query = "DELETE cpc FROM cpc_current cpc LEFT JOIN patent p on p.id = cpc.patent_id WHERE p.id is null"
    engine.execute(delete_query)


def cpc_chunk_processor(granted_patent_class_chunk, config):
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    cpc_current_chunk = extract_cpc_current(granted_patent_class_chunk)
    upload_cpc_current(engine, cpc_current_chunk)


def process_and_upload_cpc_current(config):
    cpc_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_output')

    granted_patent_classification_chunks = pd.read_csv("{0}/grants_classes.csv".format(cpc_folder),
                                                       dtype={
                                                           'patent_number': str,
                                                           'cpc_primary': str,
                                                           'cpc_additional': str
                                                       },
                                                       chunksize=100000)
    for granted_patent_class_chunk in granted_patent_classification_chunks:
        cpc_chunk_processor(granted_patent_class_chunk, config)
    consolidate_cpc_data(config)


if __name__ == '__main__':
    config = get_config()
    process_and_upload_cpc_current(config)
