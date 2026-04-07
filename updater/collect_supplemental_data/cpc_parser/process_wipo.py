import datetime
import os
import time

import pandas as pd
from sqlalchemy import create_engine, types

from lib.configuration import get_connection_string, get_current_config


def get_ipc_tech_code_field_map(ipc_tech_file):
    # Read in the CSV file
    ipc_technology_data = pd.read_csv(ipc_tech_file, dtype=str)
    # Remove trailing % (likely wildcard format)
    ipc_technology_data["IPC_Code_stripped"] = ipc_technology_data["IPC_code"].apply(
        lambda x: x.split("%")[0].replace(" ", "").strip()
    )
    # Select and Rename columns
    ipc_tech_map_frame = ipc_technology_data[
        ["IPC_Code_stripped", "Field_number"]
    ].rename({"IPC_Code_stripped": "IPC_Code"}, axis=1)
    # Convert to lookup dict
    #     ipc_code_to_field_map = ipc_tech_map_frame.set_index(
    #         'IPC_Code').to_dict()["Field_number"]
    return ipc_tech_map_frame


def get_ipc_cpc_ipc_concordance_map(concordance_file):
    print(concordance_file)
    cpc_ipc_concordance_data = pd.read_csv(
        concordance_file, header=None, sep="\t", dtype=str
    )
    cpc_ipc_concordance_data.columns = [
        "cpc_code",
        "unknown_column_1",
        "ipc_code",
        "unknown_column_2",
        "unknown_column_3",
    ]
    cpc_ipc_known_data = cpc_ipc_concordance_data.drop(
        ["unknown_column_1", "unknown_column_2", "unknown_column_3"], axis=1
    )
    # cpc_ipc_concordance_map = cpc_ipc_known_data.set_index('cpc_code').to_dict()["ipc_code"]
    return cpc_ipc_known_data


# @profile()
def extract_wipo_data(
    cpc_chunk, cpc_ipc_concordance, ipc_tech_map, config, unique_id, target_table
):
    # Obtain IPC Concordance for each patent based on cpc subgroup ID
    cpc_current_with_concordance = cpc_chunk.merge(
        right=cpc_ipc_concordance,
        how="left",
        left_on="subgroup_id",
        right_on="cpc_code",
    ).drop("cpc_code", axis=1)
    # If concordance does not exist, use subgroup id as IPC code
    cpc_current_with_concordance["ipc_code"] = cpc_current_with_concordance[
        "ipc_code"
    ].fillna(cpc_current_with_concordance["subgroup_id"])
    # Lookup IPC Tech field ID using SQL LIKE semantics (prefix match for trailing %)
    # against the concordance-derived IPC code (or subgroup fallback).
    lookup_values = (
        cpc_current_with_concordance["ipc_code"]
        .fillna("")
        .str.replace(" ", "", regex=False)
    )
    ipc_lookup = ipc_tech_map[["IPC_Code", "Field_number"]].dropna().copy()
    ipc_lookup["IPC_Code"] = ipc_lookup["IPC_Code"].str.replace(" ", "", regex=False)
    ipc_lookup = ipc_lookup.sort_values(
        by="IPC_Code", key=lambda s: s.str.len(), ascending=False
    )

    wipo_data_with_merge = cpc_current_with_concordance.copy()
    wipo_data_with_merge["field_id"] = pd.NA
    for row in ipc_lookup.itertuples(index=False):
        prefix = row.IPC_Code
        unmatched_rows = wipo_data_with_merge["field_id"].isna()
        matches = lookup_values.str.startswith(prefix)
        wipo_data_with_merge.loc[unmatched_rows & matches, "field_id"] = (
            row.Field_number
        )

    wipo_data_with_merge["field_id"] = pd.to_numeric(
        wipo_data_with_merge["field_id"], errors="coerce"
    )
    # Clean UP
    wipo_data = wipo_data_with_merge.dropna(subset=["field_id"], axis=0).drop(
        ["subgroup_id", "ipc_code"], axis=1
    )
    # Use source version indicator when provided by upstream query; otherwise fallback.
    source_has_version_indicator = "version_indicator" in wipo_data.columns
    if not source_has_version_indicator:
        wipo_data = wipo_data.assign(version_indicator=config["DATES"]["END_DATE"])

    # Counter for each Field ID for each record/version pair.
    group_keys = [unique_id, "version_indicator", "field_id"]
    sequence_keys = [unique_id, "version_indicator"]
    wipo_count = wipo_data.groupby(group_keys).size().to_frame("wipo_count")
    wipo_count = wipo_count.reset_index()
    # Retain top 3 most frequent WIPO field IDs per record, preserving ties.
    wipo_count = wipo_count.sort_values(
        [unique_id, "version_indicator", "wipo_count"], ascending=[True, True, False]
    )
    wipo_count = wipo_count.assign(
        count_rank=wipo_count.groupby(sequence_keys)["wipo_count"].rank(
            method="dense", ascending=False
        )
    )
    wipo_filtered_data = wipo_count.loc[wipo_count["count_rank"] <= 3].drop(
        columns=["count_rank"]
    )
    # Assign Sequence
    wipo_filtered_data_sequenced = wipo_filtered_data.drop(
        ["wipo_count"], axis=1
    ).assign(sequence=wipo_filtered_data.groupby(sequence_keys).cumcount())
    cstr = get_connection_string(config, "PROD_DB")
    # print(cstr)
    engine = create_engine(cstr)
    with engine.begin() as conn:
        wipo_filtered_data_sequenced.to_sql(
            target_table,
            conn,
            if_exists="append",
            index=False,
            method="multi",
            dtype={
                unique_id: types.NVARCHAR(length=20),
                "field_id": types.INTEGER(),
                "sequence": types.INTEGER(),
                "version_indicator": types.Date(),
            },
        )


def wipo_chunk_processor(
    cpc_current_data, ipc_tech_field_map, cpc_ipc_concordance_map, config, unique_id
):
    extract_wipo_data(
        cpc_current_data, cpc_ipc_concordance_map, ipc_tech_field_map, config, unique_id
    )


def consolidate_wipo(config):
    engine = create_engine(get_connection_string(config, "PROD_DB"))
    start_date = datetime.datetime.strptime(config["DATES"]["START_DATE"], "%Y%m%d")
    suffix = (start_date - datetime.timedelta(days=1)).strftime("%Y%m%d")
    rename_raw_statement = """
    rename table {raw_db}.wipo to {raw_db}.wipo_{suffix}
    """.format(
        raw_db=config["PATENTSVIEW_DATABASES"]["PROD_DB"], suffix=suffix
    )
    rename_upload_statement = """
    rename table {upload_db}.wipo to {raw_db}.wipo
    """.format(
        raw_db=config["PATENTSVIEW_DATABASES"]["PROD_DB"],
        upload_db=config["PATENTSVIEW_DATABASES"]["TEMP_UPLOAD_DB"],
    )
    add_vi_index_statement = """
    alter table wipo add index version_indicator_index (version_indicator)
    """
    print(rename_raw_statement)
    engine.execute(rename_raw_statement)
    print(rename_upload_statement)
    engine.execute(rename_upload_statement)
    print(add_vi_index_statement)
    engine.execute(add_vi_index_statement)


def process_and_upload_wipo(db, **kwargs):
    print(db)
    config = get_current_config(db, schedule="quarterly", **kwargs)
    myengine = create_engine(get_connection_string(config, "PROD_DB"))
    wipo_output = "{}/{}".format(config["FOLDERS"]["WORKING_FOLDER"], "wipo_output")
    version_indicator = config["DATES"]["END_DATE"]
    os.makedirs(wipo_output, exist_ok=True)
    persistent_files = config["FOLDERS"]["PERSISTENT_FILES"]
    ipc_tech_file = "{}/ipc_technology.csv".format(persistent_files)
    ipc_tech_field_map = get_ipc_tech_code_field_map(ipc_tech_file)

    concordance_file = "{}/{}/{}".format(
        config["FOLDERS"]["WORKING_FOLDER"], "cpc_input", "ipc_concordance.txt"
    )
    print(concordance_file)
    cpc_ipc_concordance_map = get_ipc_cpc_ipc_concordance_map(concordance_file)

    limit = 30_000
    offset = 0
    if db == "granted_patent":
        base_query_template = "SELECT id from patent where version_indicator <= '{vind}' order by id limit {limit} offset {offset} "
        cpc_query_template = "SELECT c.patent_id, c.subgroup_id from cpc_current c join ({base_query}) p on p.id = c.patent_id"

        patent_batches_query = f"select round((count(*)/{limit}),0) from patent where version_indicator <= '{version_indicator}'"
        patent_batches = pd.read_sql_query(con=myengine, sql=patent_batches_query)
        num_batches = int(patent_batches.iloc[:, 0][0])
        unique_id = "patent_id"
    elif db == "pgpubs":
        base_query_template = "SELECT document_number from publication where version_indicator <= '{vind}' order by document_number limit {limit} offset {offset} "
        cpc_query_template = "SELECT c.document_number, c.subgroup_id from cpc_current c join ({base_query}) p on p.document_number = c.document_number"

        pgpubs_batches_query = f"select round((count(*)/{limit}),0) from publication where version_indicator <= '{version_indicator}'"
        pgpubs_batches = pd.read_sql_query(con=myengine, sql=pgpubs_batches_query)
        num_batches = int(pgpubs_batches.iloc[:, 0][0])
        unique_id = "document_number"
    else:
        raise Exception(
            f"DB name provided: {db}. expected 'granted_patent' or 'pgpubs'"
        )
    for batch in range(num_batches):
        start = time.time()
        base_query = base_query_template.format(
            limit=limit, offset=offset, vind=version_indicator
        )
        cpc_join_query = cpc_query_template.format(base_query=base_query)
        cpc_current_data = pd.read_sql_query(con=myengine, sql=cpc_join_query)
        print(
            f"For {batch}: {offset}, with {cpc_join_query}, the count is {cpc_current_data.shape[0]}"
        )
        offset = offset + limit
        if cpc_current_data.shape[0] < 1:
            continue
        wipo_chunk_processor(
            cpc_current_data,
            ipc_tech_field_map,
            cpc_ipc_concordance_map,
            config,
            unique_id,
        )
        end = time.time()
        perc_complete = (batch + 1) / num_batches
        chunk_time = str(round(end - start))
        print(
            f"We are on batch: {batch} of {num_batches}, or {perc_complete} complete which took {chunk_time} seconds"
        )
    consolidate_wipo(config)


if __name__ == "__main__":
    process_and_upload_wipo(
        db="pgpubs", **{"execution_date": datetime.date(2022, 6, 1)}
    )
