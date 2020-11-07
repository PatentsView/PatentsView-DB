import csv
import time

import pandas as pd
from sqlalchemy import create_engine

from QA.post_processing.InventorPostProcessing import InventorPostProcessingQC
from lib.configuration import get_config, get_connection_string


def update_rawinventor(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    update_statement = "UPDATE rawinventor ri left join inventor_disambiguation_mapping idm on idm.uuid = ri.uuid set " \
                       "" \
                       "" \
                       "" \
                       "" \
                       "" \
                       "" \
                       "" \
                       "ri.inventor_id=idm.inventor_id "
    engine.execute(update_statement)


def get_inventor_stopwords(config):
    stop_word_file = "{folder}/post_processing/inventor_name_stopwords.txt".format(folder=config["FOLDERS"][
        "persistent_files"])
    with open(stop_word_file) as f:
        stopwords = f.read().splitlines()
    return stopwords


def inventor_clean(inventor_group, stopwords):
    inventor_group['name_first'] = inventor_group['name_first'].apply(
            lambda x: x if pd.isnull(x) else ' '.join(
                    [word for word in x.split() if word not in stopwords]))
    inventor_group['name_last'] = inventor_group['name_last'].apply(
            lambda x: x if pd.isnull(x) else ' '.join(
                    [word for word in x.split() if word not in stopwords]))
    inventor_group['patent_date'] = pd.to_datetime(inventor_group['patent_date']).dt.date
    return inventor_group


def generate_disambiguated_inventors(engine, limit, offset):
    inventor_core_template = """
    SELECT inventor_id
    from disambiguated_inventor_ids order by inventor_id
    limit {limit} offset {offset}
    """

    inventor_data_template = """
    SELECT ri.inventor_id, name_first, name_last, p.date as patent_date
    from rawinventor ri
             join patent p on p.id = ri.patent_id
             join ({inv_core_query}) inventor on inventor.inventor_id = ri.inventor_id
    where ri.inventor_id is not null;
    """
    inventor_core_query = inventor_core_template.format(limit=limit,
                                                        offset=offset)
    inventor_data_query = inventor_data_template.format(
            inv_core_query=inventor_core_query)

    current_inventor_data = pd.read_sql_query(sql=inventor_data_query, con=engine)
    return current_inventor_data


#
# def inventor_reduce(inventor_group):
#     name_sizes = inventor_group.groupby(
#             ["inventor_id", "name_first", "name_last"], dropna=False).agg({
#             'patent_date': [len, max]
#             }).reset_index()
#     name_sizes.columns = [
#             "inventor_id", "name_first", "name_last", "patent_count",
#             "latest_patent_date"
#             ]
#     name_sizes.sort_values(by=["patent_count", "latest_patent_date"],
#                            ascending=[False, False],
#                            inplace=True)
#     return name_sizes.head(1).drop(["latest_patent_date", "patent_count"], axis=1)


def inventor_reduce(inventor_data):
    inventor_data['help'] = inventor_data.groupby(['inventor_id', 'name_first', 'name_last'])[
        'inventor_id'].transform('count')
    out = inventor_data.sort_values(['help', 'patent_date'], ascending=[False, False],
                                    na_position='last').drop_duplicates(
            'inventor_id', keep='first').drop(
            ['help', 'patent_date'], 1)
    return out


def precache_inventors(config):
    inventor_cache_query = """
    INSERT INTO disambiguated_inventor_ids (inventor_id)  SELECT distinct inventor_id from rawinventor;
    """
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    engine.execute(inventor_cache_query)


def create_inventor(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    limit = 10000
    offset = 0
    while True:
        start = time.time()
        current_inventor_data = generate_disambiguated_inventors(engine, limit, offset)
        if current_inventor_data.shape[0] < 1:
            break
        step_time = time.time() - start
        start = time.time()

        # current_inventor_data = current_inventor_data.sort_values(by=['patent_date'])
        # current_inventor_data['inventor_name'] = current_inventor_data['name_first'] + "|" + current_inventor_data[
        #     'name_last']
        # canonical_assignments = current_inventor_data.groupby(['inventor_id'])['inventor_name'].agg(
        #     pd.Series.mode).reset_index()
        # inventor_data = canonical_assignments.join(
        #     canonical_assignments.inventor_name.str.split("|", expand=True).rename({
        #                                                                                    0: 'name_first',
        #                                                                                    1: 'name_last'
        #                                                                                    })).drop("inventor_name",
        #                                                                                             axis=1)
        # canonical_assignments = current_inventor_data.groupby("inventor_id").apply(
        #         inventor_reduce).reset_index(drop=True).rename({
        #         "inventor_id": "id"
        #         }, axis=1)
        step_time = time.time() - start
        canonical_assignments = inventor_reduce(current_inventor_data).rename({
                                                                                      "inventor_id": "id"
                                                                                      }, axis=1)
        canonical_assignments.to_sql(name='inventor', con=engine,
                                     if_exists='append',
                                     index=False)
        current_iteration_duration = time.time() - start
        offset = limit + offset


def upload_disambig_results(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    disambig_output_file = "{wkfolder}/disambig_output/{disamb_file}".format(
            wkfolder=update_config['FOLDERS']['WORKING_FOLDER'], disamb_file="inventor_disambiguation.tsv")
    disambig_output = pd.read_csv(disambig_output_file, sep="\t", chunksize=300000, header=None, quoting=csv.QUOTE_NONE,
                                  names=['unknown_1', 'uuid', 'inventor_id', 'name_first', 'name_middle', 'name_last',
                                         'name_suffix'])
    count = 0
    for disambig_chunk in disambig_output:
        engine.connect()
        start = time.time()
        count += disambig_chunk.shape[0]
        disambig_chunk[["uuid", "inventor_id"
                        ]].to_sql(name='inventor_disambiguation_mapping',
                                  con=engine,
                                  if_exists='append',
                                  index=False,
                                  method='multi')
        end = time.time()
        print("It took {duration} seconds to get to {cnt}".format(duration=round(
                end - start, 3),
                cnt=count))
        engine.dispose()


def post_process_inventor(config):
    upload_disambig_results(config)
    update_rawinventor(config)
    precache_inventors(config)
    create_inventor(config)


def post_process_qc(config):
    qc = InventorPostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
    # post_process_inventor(config)
    post_process_qc(config)
