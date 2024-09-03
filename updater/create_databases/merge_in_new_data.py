import datetime
import json
import os
import time

from sqlalchemy import create_engine

from QA.create_databases.MergeTestQuarterly import MergeTestQuarterly
from QA.create_databases.MergeTestWeekly import MergeTestWeekly
from QA.create_databases.TextTest import TextMergeTest
from lib.configuration import get_connection_string, get_current_config, get_lookup_tables, get_merge_table_candidates


def get_merge_status(status_folder, run_id):
    if not os.path.exists(status_folder):
        os.makedirs(status_folder)
    status_file = '{}/{}'.format(status_folder, 'merge_status.json')
    try:
        current_status = json.load(open(status_file))
        current_run_status = current_status[str(run_id)]
    except OSError as e:
        print(e)
        current_run_status = {}
    except KeyError as e:
        print(e)
        current_run_status = {}
    return current_run_status


def save_merge_status(status_folder, status, run_id):
    status_file = '{}/{}'.format(status_folder, 'merge_status.json')
    saved_status = get_merge_status(status_folder, run_id)
    saved_status[run_id] = status
    json.dump(saved_status, open(status_file, "w"))


def update_table_data(table_name, config, lookup=False):
    insert_clause = "INSERT"
    if lookup:
        insert_clause = "INSERT IGNORE"
    connection_string = get_connection_string(config, "PROD_DB")
    upload_db = config["PATENTSVIEW_DATABASES"]["TEMP_UPLOAD_DB"]
    raw_db = config["PATENTSVIEW_DATABASES"]["PROD_DB"]
    engine = create_engine(connection_string)
    order_by_cursor = engine.execute(
            "SELECT GROUP_CONCAT(s.COLUMN_NAME SEPARATOR ', ' ) from information_schema.tables t left join information_schema.statistics s on t.TABLE_NAME=s.TABLE_NAME where INDEX_NAME='PRIMARY' and t.TABLE_SCHEMA=s.TABLE_SCHEMA and t.TABLE_SCHEMA ='" + upload_db + "' and s.TABLE_NAME ='" + table_name + "' GROUP BY t.TABLE_NAME;")
    if table_name == 'mainclass' or table_name == 'subclass':
        order_by_clause = ''
    else:
        order_by_clause = "ORDER BY {order_field}".format(order_field=order_by_cursor.fetchall()[0][0])
    field_list_cursor = engine.execute(
            """
SELECT CONCAT('`', GROUP_CONCAT(COLUMN_NAME SEPARATOR '`, `'), '`')
from information_schema.COLUMNS c
where c.TABLE_SCHEMA = '{database}'
  and c.TABLE_NAME = '{table}'
  and COLUMN_NAME not in ('created_date', 'updated_date')            
            """.format(database=upload_db, table=table_name))
    field_list = field_list_cursor.fetchall()[0][0]
    table_data_count = engine.execute("SELEcT count(1) from " + upload_db + "." + table_name).fetchall()[0][0]
    with engine.begin() as connection:
        limit = 50000
        offset = 0
        batch_counter = 0
        while True:
            batch_counter += 1
            print('Next iteration...')

            # order by with primary key column - this has no nulls
            insert_table_command = """
{insert_clause} INTO {raw_database}.{table_name} ({field_list})
SELECT {field_list}
FROM {upload_database}.{table_name}
{order_clause}
limit {limit} offset {offset}
            """.format(insert_clause=insert_clause, raw_database=raw_db, table_name=table_name, field_list=field_list,
                       upload_database=upload_db,
                       order_clause=order_by_clause, limit=limit, offset=offset)
            print(insert_table_command, flush=True)
            start = time.time()

            connection.execute(insert_table_command)

            print(time.time() - start, flush=True)
            # means we have no more batches to process
            if offset > table_data_count:
                break

            offset = offset + limit


def merge_new_data(tables, project_home, update_config, run_id):
    status_folder = '{}/{}'.format(project_home, 'updater/create_databases')
    lookup_table_list = get_lookup_tables(update_config)
    upload_status = get_merge_status(status_folder, run_id)
    print(upload_status)
    try:
        for table in tables:
            if table in upload_status and upload_status[table] == 1:
                continue
            try:
                lookup = False
                if table in lookup_table_list:
                    lookup = True
                update_table_data(table, update_config, lookup)
                upload_status[table] = 1
            except Exception as e:
                upload_status[table] = 0
                raise e
    finally:
        save_merge_status(status_folder, upload_status, run_id)


def normalize_exemplary(config):
    import pandas as pd
    connection_string = get_connection_string(config, "TEMP_UPLOAD_DB")
    engine = create_engine(connection_string)
    exemplary_data = pd.read_sql_table(table_name="claim_exemplary_{year}".format(
            year=datetime.datetime.strptime(config["DATES"]["END_DATE"], "%Y%m%d").strftime('%Y')),
            con=engine)

    exemplary_data = exemplary_data.join(
            exemplary_data.exemplary.str.strip().str.split(",", expand=True)).drop(
            "exemplary", axis=1)

    melted_exemplary = exemplary_data.melt(id_vars=['patent_id', 'version_indicator'], value_name='exemplary')
    melted_exemplary.exemplary = melted_exemplary.exemplary.str.strip()
    normalized_exemplary = melted_exemplary[
        (~melted_exemplary.exemplary.isnull())
        & (melted_exemplary.exemplary.str.len() > 0)]

    normalized_exemplary.to_sql(name='temp_normalized_claim_exemplary', index=False, con=engine, if_exists='replace')


def update_text_data(table, update_config):
    connection_string = get_connection_string(update_config, "TEXT_DB")
    # qa_connection_string = get_connection_string(update_config, 'QA_DATABASE', connection='QA_DATABASE_SETUP')
    engine = create_engine(connection_string)
    query = table["insert"]
    print(query)
    engine.execute(query)


def merge_text_data(tables, update_config):
    for table in tables:
        if 'preprocess' in tables[table]:
            tables[table]['preprocess'](update_config)
        update_text_data(tables[table], update_config)


def begin_merging(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    tables_dict = get_merge_table_candidates(config)
    tables = tables_dict.keys()
    project_home = os.environ['PACKAGE_HOME']
    merge_new_data(tables, project_home, config, kwargs['run_id'])


def begin_text_merging(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    version = config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'].split("_")[1]
    text_db = config['PATENTSVIEW_DATABASES']['TEXT_DB']
    temp_db = config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB']
    raw_db = config['PATENTSVIEW_DATABASES']['PROD_DB']
    end_date=datetime.datetime.strptime(config['DATES']['END_DATE'], "%Y%m%d")
    year = int(end_date.strftime('%Y'))
    text_table_config = {
            'brf_sum_text':       {
                    "insert": f"""
INSERT INTO {text_db}.brf_sum_text_{year}(uuid, patent_id, summary_text, version_indicator)
SELECT uuid, patent_id, summary_text, version_indicator
from {temp_db}.brf_sum_text_{year}
on duplicate key update
	`uuid` = VALUES(`uuid`),
	`summary_text` = VALUES(`summary_text`),
	`version_indicator` = VALUES(`version_indicator`)
                    """
                    },

            'claim':              {
                    'preprocess': normalize_exemplary,
                    "insert":     f"""
INSERT INTO {text_db}.claims_{year}(uuid, patent_id, claim_number, claim_text, claim_sequence, dependent, exemplary, version_indicator)
SELECT c.uuid,
       c.patent_id,
       c.claim_number,
       c.claim_text,
       c.claim_sequence - 1,
       c.dependent,
       case when tce.exemplary is null then 0 else 1 end,
       c.version_indicator
from {temp_db}.claims_{year} c
         left join {temp_db}.temp_normalized_claim_exemplary tce
                   on tce.patent_id = c.patent_id and tce.exemplary = c.claim_sequence
on duplicate key update
	`uuid` = VALUES(`uuid`),
	`claim_number` = VALUES(`claim_number`),
	`claim_text` = VALUES(`claim_text`),
	`claim_sequence` = VALUES(`claim_sequence`),
	`dependent` = VALUES(`dependent`),
	`exemplary` = VALUES(`exemplary`),
	`version_indicator` = VALUES(`version_indicator`)
                    """
                    },

            'draw_desc_text':     {
                    "insert": f"""
INSERT INTO {text_db}.draw_desc_text_{year}(uuid, patent_id, draw_desc_text, draw_desc_sequence, version_indicator)
SELECT uuid, patent_id, draw_desc_text, draw_desc_sequence, version_indicator
from {temp_db}.draw_desc_text_{year}
                    """
                    },

            'detail_desc_text':   {
                    "insert": f"""
INSERT INTO {text_db}.detail_desc_text_{year}(uuid, patent_id, description_text, description_length, version_indicator)
SELECT uuid, patent_id, description_text, char_length(description_text), version_indicator
from {temp_db}.detail_desc_text_{year}
                              """
                    },

            'detail_desc_length': {
                    "insert": f"INSERT INTO {raw_db}.detail_desc_length( patent_id, detail_desc_length, version_indicator) SELECT  patent_id, CHAR_LENGTH(description_text), version_indicator from {temp_db}.detail_desc_text_{year}"
                    }
            }
    merge_text_data(text_table_config, config)


def begin_text_merging_pgpubs(**kwargs):
    config = get_current_config('pgpubs', **kwargs)
    version = config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'].split("_")[1]
    temp_db = config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB']
    text_db = config['PATENTSVIEW_DATABASES']['TEXT_DB']
    prod_db = config['PATENTSVIEW_DATABASES']['PROD_DB']
    end_date=datetime.datetime.strptime(config['DATES']['END_DATE'], "%Y%m%d")
    year = int(end_date.strftime('%Y'))
    text_table_config = {
            'brf_sum_text':       {
                    "insert": """
INSERT INTO {text_db}.brf_sum_text_{year}(id, pgpub_id, summary_text, version_indicator)
SELECT id, pgpub_id, summary_text, version_indicator
from {temp_db}.brf_sum_text_{year}
                    """.format(text_db=text_db,
                               temp_db=temp_db,
                               year=year)
                    },

            'claim':              {
                    "insert":     """
INSERT INTO {text_db}.claim_{year}(id, pgpub_id, claim_text, claim_sequence, dependent, claim_number, version_indicator)
SELECT c.id,
       c.pgpub_id,
       c.claim_text,
       c.claim_sequence - 1,
       c.dependent,
       c.claim_number,
       c.version_indicator
from {temp_db}.claim_{year} c
         left join {temp_db}.temp_normalized_claim_exemplary tce
                   on tce.pgpub_id = c.pgpub_id 
                    """.format(
                            text_db=text_db,
                            temp_db=temp_db, year=year)
                    },

            'draw_desc_text':     {
                    "insert": """
INSERT INTO {text_db}.draw_desc_text_{year}(id, pgpub_id, draw_desc_text, draw_desc_sequence, version_indicator)
SELECT id, pgpub_id, draw_desc_text, draw_desc_sequence, version_indicator
from {temp_db}.draw_desc_text_{year}
                    """.format(text_db=text_db,
                               temp_db=temp_db,
                               year=year)
                    },

            'detail_desc_text':   {
                    "insert": """
INSERT INTO {text_db}.detail_desc_text_{year}(id, pgpub_id, description_text, description_length, version_indicator)
SELECT id, pgpub_id, description_text, char_length(description_text), version_indicator
from {temp_db}.detail_desc_text_{year}
                              """.format(text_db=text_db,
                                         temp_db=temp_db,
                                         year=year)
                    },

            'detail_desc_length': {
                    "insert": "INSERT INTO {raw_db}.detail_desc_length( pgpub_id, detail_desc_length, version_indicator) SELECT  pgpub_id, CHAR_LENGTH(description_text), version_indicator from {temp_db}.detail_desc_text_{year}".format(
                            raw_db=prod_db,
                            temp_db=temp_db, year=year)
                    }
            }
    merge_text_data(text_table_config, config)

def post_merge_weekly_granted(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    #run_id = kwargs.get('run_id')
    qc = MergeTestWeekly(config)
    qc.runStandardTests()

def post_merge_quarterly_granted(**kwargs):
    config = get_current_config('granted_patent', schedule='quarterly', **kwargs)
    run_id = kwargs.get('run_id')
    qc = MergeTestQuarterly(config)
    qc.runStandardTests()

def post_merge_weekly_pgpubs( **kwargs):
    config = get_current_config('pgpubs', **kwargs)
    # run_id = kwargs.get('run_id')
    qc = MergeTestWeekly(config)
    qc.runStandardTests()

def post_merge_quarterly_pgpubs(**kwargs):
    config = get_current_config('pgpubs', schedule='quarterly', **kwargs)
    run_id = kwargs.get('run_id')
    qc = MergeTestQuarterly(config)
    qc.runStandardTests()


def post_text_merge(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    run_id = kwargs.get('run_id')
    qc = TextMergeTest(config)
    qc.runStandardTests()


if __name__ == '__main__':
    # config = get_current_config('granted_patent', **{
    #     "execution_date": datetime.date(2021, 11, 4)
    # })
    # config = get_current_config('pgpubs', **{
    #     "execution_date": datetime.date(2021, 12, 2)
    # })
    # begin_merging(**{
    #         "execution_date": datetime.date(2020, 12, 1),
    #         "run_id":         1
    #         }, )
    post_merge_weekly_pgpubs(**{
            "execution_date": datetime.date(2024, 1, 11),
            "run_id" : "scheduled__2024-01-11T09:00:00+00:00"
            })
    # begin_text_merging(**{
    #         "execution_date": datetime.date(2020, 12, 1),
    #         "run_id":         "testing"
    #         })
    # post_merge_quarterly_granted( **{
    #     "execution_date": datetime.date(2021, 12, 2)
    # })
    # begin_text_merging_pgpubs(**{
    #         "execution_date": datetime.date(2021, 12, 2),
    #         })
    # update_table_data("application", config)
    print("TESTING POST MERGE")
