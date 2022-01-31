import csv
import datetime
import time

import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import ResultProxy

from QA.post_processing.InventorPostProcessing import InventorPostProcessingQC
from lib.configuration import get_connection_string, get_current_config
from updater.post_processing.create_lookup import load_lookup_table


def update_rawinventor(update_config, database='PROD_DB', uuid_field='uuid'):
    engine = create_engine(get_connection_string(update_config, database))
    update_statement = """
        UPDATE rawinventor ri join inventor_disambiguation_mapping idm
            on idm.uuid =  ri.{uuid_field}
        set ri.inventor_id=idm.inventor_id where ri.inventor_id is null
    """.format(uuid_field=uuid_field)
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


def precache_inventors(config):
    inventor_cache_query = """
        INSERT IGNORE INTO disambiguated_inventor_ids (inventor_id)
        SELECT inventor_id
        from patent.rawinventor
        UNION
        SELECT inventor_id
        from pregrant_publications.rawinventor;
    """
        # .format(pregrant_db=config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'],
        #        granted_db=config['PATENTSVIEW_DATABASES']['RAW_DB'])
    engine = create_engine(get_connection_string(config, "PROD_DB"))
    print(inventor_cache_query)
    engine.execute(inventor_cache_query)


def create_inventor(update_config, version_indicator):
    engine = create_engine(get_connection_string(update_config, "PROD_DB"))
    rename_name = "inventor_{tstamp}".format(tstamp=version_indicator)
    rename_sql = """
    RENAME TABLE inventor TO {target}
    """.format(target=rename_name)
    create_sql = """
    CREATE TABLE inventor like {target}
    """.format(target=rename_name)
    try:
        engine.execute(rename_sql)
    except exc.SQLAlchemyError as e:
        count_sql = "select count(1) from rename_name"
        x: ResultProxy = engine.execute(count_sql)
    try:
        engine.execute(create_sql)
    except exc.SQLAlchemyError as e:
        count_sql = "select count(1) from inventor"
        x: ResultProxy = engine.execute(count_sql)
        if x.fetchall()[0][0] > 0:
            raise
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
        canonical_assignments.to_sql(name='inventor', con=engine,
                                     if_exists='append',
                                     index=False)
        current_iteration_duration = time.time() - start
        offset = limit + offset


def upload_disambig_results(update_config):
    engine = create_engine(get_connection_string(update_config, "PROD_DB"))
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


def post_process_inventor(**kwargs):
    pgpubs_config = get_current_config('pgpubs', **kwargs)
    patent_config = get_current_config('granted_patent', **kwargs)
    patent_version_indicator = patent_config['DATES']['END_DATE']
    pgpubs_version_indicator = pgpubs_config['DATES']['END_DATE']
    update_rawinventor(pgpubs_config, database='PROD_DB', uuid_field='id')
    update_rawinventor(patent_config, database='PROD_DB', uuid_field='uuid')
    precache_inventors(patent_config)
    create_inventor(patent_config, version_indicator=patent_version_indicator)
    load_lookup_table(update_config=patent_config, database='PROD_DB', parent_entity='patent',
                      parent_entity_id='patent_id', entity='inventor', version_indicator=patent_version_indicator,
                      include_location=True, location_strict=False)
    load_lookup_table(update_config=pgpubs_config, database='PROD_DB', parent_entity='publication',
                      parent_entity_id='document_number', entity="inventor", version_indicator=pgpubs_version_indicator,
                      include_location=True)


def post_process_qc(**kwargs):
    config = get_current_config(**kwargs)
    qc = InventorPostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    # post_process_inventor(config)
    post_process_inventor(**{
            "execution_date": datetime.date(2020, 12, 29)
            })
    # post_process_qc(**{
    #         "execution_date": datetime.date(2020, 12, 29)
    #         })
