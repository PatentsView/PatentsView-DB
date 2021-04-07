import datetime
import json
import os
import time

from sqlalchemy import create_engine

from QA.create_databases.MergeTest import MergeTest
from QA.create_databases.TextTest import TextMergeTest
from lib.configuration import get_connection_string, get_current_config, get_lookup_tables, get_parsed_tables_dict, \
    get_upload_tables_dict


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
    connection_string = get_connection_string(config, "RAW_DB")
    upload_db = config["PATENTSVIEW_DATABASES"]["TEMP_UPLOAD_DB"]
    raw_db = config["PATENTSVIEW_DATABASES"]["RAW_DB"]
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

    normalized_exemplary.to_sql(name='temp_normalized_claim_exemplary', index=False, con=engine, if_exists='append')


def update_text_data(table, update_config):
    connection_string = get_connection_string(update_config, "TEXT_DATABASE")
    engine = create_engine(connection_string)
    query = table["insert"]
    engine.execute(query)


def merge_text_data(tables, update_config):
    for table in tables:
        if 'preprocess' in tables[table]:
            tables[table]['preprocess'](update_config)
        update_text_data(tables[table], update_config)


def begin_merging(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    tables_dict = get_parsed_tables_dict(config)
    tables = tables_dict.keys()
    project_home = os.environ['PACKAGE_HOME']
    merge_new_data(tables, project_home, config, kwargs['run_id'])


def begin_text_merging(**kwargs):
    config = get_current_config(**kwargs)
    version = config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'].split("_")[1]
    year = int(kwargs['execution_date'].strftime('%Y'))
    text_table_config = {
            'brf_sum_text':       {
                    "insert": """
INSERT INTO {text_db}.brf_sum_text_{year}(uuid, patent_id, `text`, version_indicator)
SELECT uuid, patent_id, text, version_indicator
from {temp_db}.brf_sum_text_{year}
                    """.format(text_db=config['PATENTSVIEW_DATABASES']['TEXT_DATABASE'],
                               temp_db=config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'],
                               year=year)
                    },
            'claim':              {
                    'preprocess': normalize_exemplary,
                    "insert":     """
INSERT INTO {text_db}.claim_{year}(uuid, patent_id, num, `text`, `sequence`, dependent, exemplary, version_indicator, patent_date)
SELECT c.uuid,
       c.patent_id,
       c.num,
       c.text,
       c.sequence - 1,
       c.dependent,
       case when tce.exemplary is null then 0 else 1 end,
       version_indicator,
       p.date
from {temp_db}.claim_{year} c
         left join {temp_db}.temp_normalized_claim_exemplary tce
                   on tce.patent_id = c.patent_id and tce.exemplary = c.sequence
         left join {temp_db}.patent p
                   on p.id = c.patent_id
                    """.format(
                            text_db=config['PATENTSVIEW_DATABASES']['TEXT_DATABASE'],
                            temp_db=config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'],year=year)
                    },
            'draw_desc_text':     {
                    "insert": """
INSERT INTO {text_db}.draw_desc_text_{year}(uuid, patent_id, text, sequence, version_indicator)
SELECT uuid, patent_id, text, sequence, version_indicator
from {temp_db}.draw_desc_text_{year}
                    """.format(text_db=config['PATENTSVIEW_DATABASES']['TEXT_DATABASE'],
                               temp_db=config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'],
                               year=year)
                    },
            'detail_desc_text':   {
                    "insert": """
INSERT INTO {text_db}.detail_desc_text_{year}(uuid, patent_id, text,length,version_indicator)
SELECT uuid, patent_id, text,char_length(text), version_indicator
from {temp_db}.detail_desc_text_{year}
                              """.format(text_db=config['PATENTSVIEW_DATABASES']['TEXT_DATABASE'],
                                         temp_db=config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'],
                                         year=year)
                    },
            'detail_desc_length': {
                    "insert": "INSERT INTO {raw_db}.detail_desc_length( patent_id, detail_desc_length, version_indicator) SELECT  patent_id, CHAR_LENGTH(text), version_indicator from {temp_db}.detail_desc_text_{year}".format(
                            raw_db=config['PATENTSVIEW_DATABASES']['RAW_DB'],
                            temp_db=config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'], year=year)
                    }
            }
    merge_text_data(text_table_config, config)


def post_merge(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    run_id = kwargs.get('run_id')
    if run_id.startswith("backfill"):
        print("Skipping QC")
    else:
        qc = MergeTest(config, run_id=kwargs['run_id'])
        qc.runTests()


def post_text_merge(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    run_id = kwargs.get('run_id')
    if run_id.startswith("backfill"):
        print("Skipping QC")
    else:
        qc = TextMergeTest(config)
        qc.runTests()


if __name__ == '__main__':
    # begin_merging(**{
    #         "execution_date": datetime.date(2020, 12, 1),
    #         "run_id":         1
    #         }, )
    post_merge(**{
            "execution_date": datetime.date(2020, 12, 1),
            "run_id":         1
            })
