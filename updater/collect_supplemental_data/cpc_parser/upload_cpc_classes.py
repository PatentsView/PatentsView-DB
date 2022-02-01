import csv
import datetime
import os
import re

import pandas as pd
from sqlalchemy import create_engine

from lib.configuration import get_connection_string, get_current_config
from updater.create_databases.upload_new import setup_database


def upload_cpc_small_tables(db_con, db, folder, version_indicator):
    '''
    db_conn: sql alchemy connection engine
    db : name of the database
    folder: where the cpc_group/subsection folders are
    '''
    # Upload CPC subsection data
    mainclass = csv.reader(open(os.path.join(folder, 'cpc_subsection.csv'), 'r'))
    counter = 0
    for m in mainclass:
        towrite = [re.sub('"', "'", item) for item in m]
        query = 'insert into {}.cpc_subsection(id, title, version_indicator) values("{}", "{}", "{}")'.format(db,
                                                                                                              towrite[
                                                                                                                  0],
                                                                                                              towrite[
                                                                                                                  1],
                                                                                                              version_indicator)
        db_con.execute(query)

    # Upload CPC group data
    subclass = csv.reader(open(os.path.join(folder, 'cpc_group.csv')))
    exist = set()
    for m in subclass:
        towrite = [re.sub('"', "'", item) for item in m]
        if not towrite[0] in exist:
            exist.add(towrite[0])
            query = 'insert into {}.cpc_group(id, title, version_indicator) values("{}", "{}", "{}")'.format(db,
                                                                                                             towrite[0],
                                                                                                             towrite[1],
                                                                                                             version_indicator)
            db_con.execute(query)


def upload_cpc_subgroup(db_con, db, folder, version_indicator):
    '''
    db_con : sql alchemy connection engine
    db: new/updated database
    folder: where the cpc_group/subsection folders are
    This is a separate function because the one-by-one insert is too slow
    So instead post-process and then upload as a csv
    '''
    subgroup = csv.reader(open(os.path.join(folder, 'cpc_subgroup.csv'), 'r'))
    subgroup_out = csv.writer(open(os.path.join(folder, 'cpc_subgroup_clean.csv'), 'w'), delimiter='\t')
    subgroup_out.writerow(['id', 'title'])
    exist = set()
    for m in subgroup:
        towrite = [re.sub('"', "'", item) for item in m]
        if not towrite[0] in exist:
            exist.add(towrite[0])
            clean = [i if not i == "NULL" else "" for i in towrite]
            subgroup_out.writerow(clean)
    print('now uploading')
    data = pd.read_csv('{0}/{1}'.format(folder, 'cpc_subgroup_clean.csv'), delimiter='\t', encoding='utf-8')
    data = data.assign(version_indicator=version_indicator)
    data.to_sql('cpc_subgroup', db_con, if_exists='append', index=False)


def update_raw_db(db_con, temp_db):
    upsert_query_template = """
INSERT INTO `{table}`(`id`, `title`, `version_indicator`)
SELECT id, title, version_indicator
from `{temp_db}`.`{table}`
ON DUPLICATE KEY UPDATE title = VALUES(title),
                        version_indicator= VALUES(version_indicator);
"""
    cpc_subsection_upsert_query = upsert_query_template.format(temp_db=temp_db, table='cpc_subsection')
    cpc_group_upsert_query = upsert_query_template.format(temp_db=temp_db, table='cpc_group')
    cpc_subgroup_upsert_query = upsert_query_template.format(temp_db=temp_db, table='cpc_subgroup')
    with db_con.connect() as connection:
        connection.execute(cpc_subsection_upsert_query)
        connection.execute(cpc_group_upsert_query)
        connection.execute(cpc_subgroup_upsert_query)


def upload_cpc_classes(**kwargs):
    config = get_current_config('granted_patent', schedule='quarterly', **kwargs)

    cstr = get_connection_string(config, "TEMP_UPLOAD_DB")
    db_con = create_engine(cstr)
    cpc_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_output')
    setup_database(config, drop=False)
    upload_cpc_small_tables(db_con, config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'], cpc_folder,
                            config['DATES']['END_DATE'])
    upload_cpc_subgroup(db_con, config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'], cpc_folder,
                        config['DATES']['END_DATE'])

    raw_cstr = get_connection_string(config, "PROD_DB")
    raw_db_con = create_engine(raw_cstr)
    update_raw_db(raw_db_con, config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])


if __name__ == '__main__':
    upload_cpc_classes(**{
        "execution_date": datetime.date(2020, 12, 15)
    })
