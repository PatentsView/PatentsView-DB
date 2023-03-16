import csv
import datetime
import time

import pandas as pd
from sqlalchemy import create_engine

from QA.post_processing.InventorPostProcessing import InventorPostProcessingQC
from lib.configuration import get_connection_string, get_current_config, get_disambig_config
from updater.post_processing.create_lookup import load_lookup_table


def update_rawinventor_for_type(update_config, incremental="1", database='RAW_DB', uuid_field='uuid'):
    engine = create_engine(get_connection_string(update_config, database))
    filter = '1=1'
    if incremental == "1":
        filter = 'ri.inventor_id is null'
    update_statement = """
        UPDATE rawinventor ri join inventor_disambiguation_mapping idm
            on idm.uuid =  ri.{uuid_field}
        set ri.inventor_id=idm.inventor_id where {filter} 
    """.format(uuid_field=uuid_field, filter=filter)
    print(update_statement)
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


def generate_disambiguated_inventors(config, engine, limit, offset):
    inventor_core_template = """
        SELECT inventor_id
        from disambiguated_inventor_ids order by inventor_id
        limit {limit} offset {offset}
    """

    inventor_data_template = """
        SELECT ri.inventor_id, ri.name_first, ri.name_last, p.date as patent_date
        from rawinventor ri
                 join patent p on p.id = ri.patent_id
                 join ({inv_core_query}) inventor on inventor.inventor_id = ri.inventor_id
        where ri.inventor_id is not null
        UNION 
        SELECT ri2.inventor_id, ri2.name_first, ri2.name_last, a.date as patent_date
        from {pregrant_db}.rawinventor ri2
                 join {pregrant_db}.application a on a.document_number = ri2.document_number
                 join ({inv_core_query}) inventor on inventor.inventor_id = ri2.inventor_id
        where ri2.inventor_id is not null;
    """
    inventor_core_query = inventor_core_template.format(limit=limit,
                                                        offset=offset)
    inventor_data_query = inventor_data_template.format(
        inv_core_query=inventor_core_query, pregrant_db=config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'])

    current_inventor_data = pd.read_sql_query(sql=inventor_data_query, con=engine)
    return current_inventor_data


def inventor_reduce(inventor_data):
    inventor_data['help'] = inventor_data.groupby(['inventor_id', 'name_first', 'name_last'])[
        'inventor_id'].transform('count')
    out = inventor_data.sort_values(['help', 'patent_date'], ascending=[False, False],
                                    na_position='last').drop_duplicates(
        'inventor_id', keep='first').drop(
        ['help', 'patent_date'], 1)
    return out


def precache_inventors_ids(config):
    suffix = config['DATES']['END_DATE']
    create_query = """
    CREATE TABLE disambiguated_inventor_ids_{suffix} (inventor_id varchar(256),  PRIMARY KEY (`inventor_id`))
    """.format(suffix=suffix)
    view_query = """
    CREATE OR REPLACE VIEW disambiguated_inventor_ids as select inventor_id from disambiguated_inventor_ids_{suffix}
    """.format(suffix=suffix)
    inventor_cache_query = """
        INSERT IGNORE INTO disambiguated_inventor_ids_{suffix} (inventor_id)
        SELECT inventor_id
        from {granted_db}.rawinventor
        UNION
        SELECT inventor_id
        from {pregrant_db}.rawinventor;
    """.format(pregrant_db=config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'],
               granted_db=config['PATENTSVIEW_DATABASES']['RAW_DB'], suffix=suffix)
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    print(create_query)
    engine.execute(create_query)
    print(inventor_cache_query)
    engine.execute(inventor_cache_query)
    print(view_query)
    engine.execute(view_query)


def create_inventor(update_config):
    engine = create_engine(get_connection_string(update_config, "RAW_DB"))
    version_indicator = update_config['DATES']['END_DATE']
    suffix = update_config['DATES']['END_DATE']
    rename_name = "inventor_{tstamp}".format(tstamp=suffix)
    create_sql = """
        CREATE TABLE {rename_name} (
            `id` varchar(256) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
            `name_first` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            `name_last` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
            `male_flag` int(11) DEFAULT NULL,
            `attribution_status` int(11) DEFAULT NULL,
            `version_indicator` date NOT NULL DEFAULT '2020-09-29',
            `created_date` timestamp NOT NULL DEFAULT current_timestamp(),
            `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
            PRIMARY KEY (`id`),
            KEY `inventor_version_indicator_index` (`version_indicator`),
            KEY `inventor_name_last_name_first_index` (`name_last`(256),`name_first`(256))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """.format(rename_name=rename_name)
    print(create_sql)
    view_sql = """
    CREATE OR REPLACE SQL SECURITY INVOKER VIEW inventor as select * from inventor_{suffix}
    """.format(suffix=suffix)
    print(view_sql)
    engine.execute(create_sql)
    engine.execute(view_sql)
    limit = 10000
    offset = 0
    while True:
        start = time.time()
        current_inventor_data = generate_disambiguated_inventors(update_config, engine, limit, offset)
        if current_inventor_data.shape[0] < 1:
            break
        step_time = time.time() - start
        canonical_assignments = inventor_reduce(current_inventor_data).rename({
            "inventor_id": "id"
        }, axis=1)
        canonical_assignments = canonical_assignments.assign(version_indicator=version_indicator)
        canonical_assignments.to_sql(name=rename_name, con=engine,
                                     if_exists='append',
                                     index=False)
        current_iteration_duration = time.time() - start
        offset = limit + offset


def upload_disambig_results(update_config):
    engine = create_engine(get_connection_string(update_config, "RAW_DB"))
    disambig_output_file = "{wkfolder}/disambig_output/{disamb_file}".format(
        wkfolder=update_config['FOLDERS']['WORKING_FOLDER'],
        disamb_file="inventor_disambiguation.tsv")
    disambig_output = pd.read_csv(disambig_output_file, sep="\t",
                                  chunksize=300000, header=None, quoting=csv.QUOTE_NONE,
                                  names=['unknown_1', 'uuid', 'inventor_id', 'name_first',
                                         'name_middle', 'name_last', 'name_suffix'])
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


def update_granted_rawinventor(**kwargs):
    config = get_disambig_config(schedule='quarterly', **kwargs)
    update_rawinventor_for_type(config, config['DISAMBIGUATION']['INCREMENTAL'], database='RAW_DB', uuid_field='uuid')


def update_pregranted_rawinventor(**kwargs):
    config = get_disambig_config(schedule='quarterly', **kwargs)
    update_rawinventor_for_type(config, config['DISAMBIGUATION']['INCREMENTAL'], database='PGPUBS_DATABASE',
                                uuid_field='id')


def precache_inventors(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    precache_inventors_ids(config)


def create_canonical_inventors(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    create_inventor(config)


def load_granted_lookup(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    load_lookup_table(update_config=config, database='RAW_DB', parent_entity='patent',
                      parent_entity_id='patent_id', entity='inventor',
                      include_location=True, location_strict=False)

def load_granted_location_inventor(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    load_lookup_table(update_config=config, database='RAW_DB', parent_entity='location',
                      parent_entity_id='location_id', entity='inventor',
                      include_location=True, location_strict=True)


def load_pregranted_lookup(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    load_lookup_table(update_config=config, database='PGPUBS_DATABASE', parent_entity='publication',
                      parent_entity_id='document_number', entity="inventor",
                      include_location=True)

def load_pregranted_location_inventor(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    load_lookup_table(update_config=config, database='PGPUBS_DATABASE', parent_entity='location',
                      parent_entity_id='location_id', entity='inventor',
                      include_location=True, location_strict=True)


def post_process_inventor(**kwargs):
    update_granted_rawinventor(**kwargs)
    update_pregranted_rawinventor(**kwargs)
    precache_inventors(**kwargs)
    create_canonical_inventors(**kwargs)
    load_granted_lookup(**kwargs)
    load_pregranted_lookup(**kwargs)


def post_process_qc(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    qc = InventorPostProcessingQC(config)
    qc.runTests()

def evaluate_inventor_clustering(**kwargs):
    config = get_current_config('granted_patent', schedule='quarterly', **kwargs)
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    pquery = "select patent_id, patent_date from patentsview_export_granted.g_patent"
    print(pquery)
    patent = pd.read_sql_query(sql=pquery, con=engine)
    rquery = "select patent_id, sequence, inventor_id from patent.rawinventor "
    print(rquery)
    rawinventor = pd.read_sql_query(sql=rquery, con=engine)
    patent_date = pd.DatetimeIndex(patent.patent_date)
    patent["patent_date"] = patent_date.year.astype(int)
    joined = rawinventor.merge(patent, on="patent_id", how="left")
    joined["mention_id"] = "US" + joined.patent_id + "-" + joined.sequence
    breakpoint()
    joined = joined.query('patent_date >= 1975 and patent_date <= 2022')
    current_disambiguation = joined.set_index("mention_id")["inventor_id"]

    from er_evaluation.estimators import pairwise_precision_design_estimate, pairwise_recall_design_estimate
    from er_evaluation.summary import cluster_sizes
    from pv_evaluation.benchmark import load_binette_2022_inventors_benchmark
    pairwise_precision_design_estimate(current_disambiguation, load_binette_2022_inventors_benchmark(),
                                       weights=1 / cluster_sizes(load_binette_2022_inventors_benchmark()))



if __name__ == '__main__':
    # post_process_inventor(config)
    # post_process_inventor(**{
    #     "execution_date": datetime.date(2020, 12, 29)
    # })
    # post_process_qc(**{
    #         "execution_date": datetime.date(2020, 12, 29)
    #         })
    # evaluate_inventor_clustering(**{
    #     "execution_date": datetime.date(2020, 12, 29)
    # })
    load_pregranted_location_inventor(**{
        "execution_date": datetime.date(2022, 7, 1)
    })
