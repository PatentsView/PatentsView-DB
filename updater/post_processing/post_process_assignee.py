import csv
import datetime
import time

import pandas as pd
from sqlalchemy import create_engine

from QA.post_processing.AssigneePostProcessing import AssigneePostProcessingQC
from lib.configuration import get_connection_string, get_current_config, get_version_indicator
from updater.post_processing.create_lookup import load_lookup_table


def upload_disambig_results(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    disambig_output_file = "{wkfolder}/disambig_output/{disamb_file}".format(
            wkfolder=update_config['FOLDERS']['WORKING_FOLDER'],
            disamb_file="assignee_disambiguation.tsv")
    disambig_output = pd.read_csv(
            disambig_output_file,
            sep="\t",
            chunksize=300000,
            header=None,
            quoting=csv.QUOTE_NONE,
            names=['uuid', 'organization_id', 'person_id', 'organization', 'name'])
    count = 0
    engine.execute("TRUNCATE TABLE assignee_disambiguation_mapping")
    for disambig_chunk in disambig_output:
        disambig_chunk = disambig_chunk.assign(
                assignee_id=disambig_chunk.organization_id)
        disambig_chunk.assignee_id.fillna(disambig_chunk.person_id, inplace=True)
        disambig_chunk.drop(["organization_id", "person_id"], axis=1, inplace=True)
        count += disambig_chunk.shape[0]
        engine.connect()
        start = time.time()
        disambig_chunk[["uuid", "assignee_id"]].to_sql(name='assignee_disambiguation_mapping',
                                                       con=engine,
                                                       if_exists='append',
                                                       index=False,
                                                       method='multi')
        end = time.time()
        print("It took {duration} seconds to get to {cnt}".format(duration=round(
                end - start, 3),
                cnt=count))
        engine.dispose()


def update_rawassignee(update_config, database='RAW_DB', uuid_field='uuid'):
    engine = create_engine(get_connection_string(update_config, database))
    update_statement = """
        UPDATE rawassignee ra left join {granted_db}.assignee_disambiguation_mapping adm
            on adm.uuid = ra.{uuid_field} 
        set  ra.assignee_id = adm.assignee_id where ra.assignee_id is null
    """.format(uuid_field=uuid_field,
               granted_db=config['PATENTSVIEW_DATABASES']['RAW_DB'])
    print(update_statement)
    engine.execute(update_statement)


def generate_disambiguated_assignees(engine, limit, offset):
    assignee_core_template = """
        SELECT assignee_id
        from disambiguated_assignee_ids order by assignee_id
        limit {limit} offset {offset}
    """

    assignee_data_template = """
        SELECT ra.assignee_id, ra.type, ra.name_first, ra.name_last, ra.organization, p.date as patent_date
        from rawassignee ra
                 join patent p on p.id = ra.patent_id
                 join ({assign_core_query}) assignee on assignee.assignee_id = ra.assignee_id
        where ra.assignee_id is not null
        UNION 
        SELECT ra2.assignee_id, ra2.type, ra2.name_first, ra2.name_last, ra2.organization, a.date as patent_date
        from {pregrant_db}.rawassignee ra2
                 join {pregrant_db}.application a on a.document_number = ra2.document_number
                 join ({assign_core_query}) assignee on assignee.assignee_id = ra2.assignee_id
        where ra2.assignee_id is not null;
    """
    assignee_core_query = assignee_core_template.format(limit=limit,
                                                        offset=offset)
    assignee_data_query = assignee_data_template.format(
            assign_core_query=assignee_core_query, pregrant_db=config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'])

    current_assignee_data = pd.read_sql_query(sql=assignee_data_query, con=engine)
    return current_assignee_data


def create_assignee(update_config, version_indicator):
    engine = create_engine(get_connection_string(update_config, "RAW_DB"))
    limit = 10000
    offset = 0
    while True:
        start = time.time()
        current_assignee_data = generate_disambiguated_assignees(engine, limit, offset)
        if current_assignee_data.shape[0] < 1:
            break
        step_time = time.time() - start
        start = time.time()

        step_time = time.time() - start
        canonical_assignments = assignee_reduce(current_assignee_data).rename({
                "assignee_id": "id"
                }, axis=1)
        canonical_assignments = canonical_assignments.assign(version_indicator=version_indicator)
        canonical_assignments.to_sql(name='assignee', con=engine,
                                     if_exists='append',
                                     index=False)
        current_iteration_duration = time.time() - start
        offset = limit + offset


def precache_assignees(config):
    assignee_cache_query = """
        INSERT IGNORE INTO disambiguated_assignee_ids (assignee_id)
        SELECT assignee_id
        from {granted_db}.rawassignee
        UNION
        SELECT assignee_id
        from {pregrant_db}.rawassignee;
    """.format(pregrant_db=config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'],
               granted_db=config['PATENTSVIEW_DATABASES']['RAW_DB'])
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    print(assignee_cache_query)
    engine.execute(assignee_cache_query)


def assignee_reduce(assignee_data):
    assignee_key_fields = ['assignee_id', 'type', 'name_first', 'name_last', 'organization']
    assignee_data['help'] = assignee_data.groupby(assignee_key_fields)['assignee_id'].transform('count')
    out = assignee_data.sort_values(['help', 'patent_date'], ascending=[False, False],
                                    na_position='last').drop_duplicates(
            'assignee_id', keep='first').drop(
            ['help', 'patent_date'], 1)
    return out


def post_process_assignee(**kwargs):
    config = get_current_config(**kwargs)
    version_indicator = get_version_indicator(**kwargs)
    update_rawassignee(config, database='PGPUBS_DATABASE', uuid_field='id')
    update_rawassignee(config, database='RAW_DB', uuid_field='uuid')
    precache_assignees(config)
    create_assignee(config, version_indicator=version_indicator)
    load_lookup_table(update_config=config, database='RAW_DB', parent_entity='patent',
                      parent_entity_id='patent_id', entity='assignee', version_indicator=version_indicator,
                      include_location=True)
    load_lookup_table(update_config=config, database='PGPUBS_DATABASE', parent_entity='publication',
                      parent_entity_id='document_number', entity="assignee", version_indicator=version_indicator,
                      include_location=True)


def post_process_qc(config):
    qc = AssigneePostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_current_config(**{
            "execution_date": datetime.date(2020, 12, 29)
            })
    # post_process_assignee(config)
    post_process_qc(config)
