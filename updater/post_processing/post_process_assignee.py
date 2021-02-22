import csv
import time

from sqlalchemy import create_engine
import pandas as pd
import configparser

from QA.post_processing.AssigneePostProcessing import AssigneePostProcessingQC
from lib.configuration import get_config, get_connection_string


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
    engine.execute("TRUNCATE TABLE temp_assignee_disambiguation_mapping")
    for disambig_chunk in disambig_output:
        disambig_chunk = disambig_chunk.assign(
                assignee_id=disambig_chunk.organization_id)
        disambig_chunk.assignee_id.fillna(disambig_chunk.person_id, inplace=True)
        disambig_chunk.drop(["organization_id", "person_id"], axis=1, inplace=True)
        count += disambig_chunk.shape[0]
        engine.connect()
        start = time.time()
        disambig_chunk[["uuid", "assignee_id"]].to_sql(name='temp_assignee_disambiguation_mapping',
                                                       con=engine,
                                                       if_exists='append',
                                                       index=False,
                                                       method='multi')
        end = time.time()
        print("It took {duration} seconds to get to {cnt}".format(duration=round(
                end - start, 3),
                cnt=count))
        engine.dispose()


def update_rawassignee(update_config, database='RAW_DB', uuid_field = 'uuid'):
    engine = create_engine(get_connection_string(update_config, database))
    update_statement = "UPDATE rawassignee ra left join {granted_db}.temp_assignee_disambiguation_mapping adm on adm.uuid = ra.{uuid_field} set " \
                       " ra.assignee_id = adm.assignee_id".format(uuid_field=uuid_field, granted_db=config['PATENTSVIEW_DATABASES']['RAW_DB'])
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


# def create_assignee(update_config):
#     engine = create_engine(get_connection_string(update_config, "NEW_DB"))
#     assignee_name_with_count = pd.read_sql_query(
#             "SELECT assignee_id, name_first, name_last, type, count(1) name_count from rawassignee where assignee_id "
#             "is not null and organization is null group by assignee_id, name_first, name_last, type;",
#             con=engine)
#     assignee_name_data = assignee_name_with_count.sort_values("name_count", ascending=False).groupby(
#             "assignee_id").head(
#             1).reset_index(drop=True)
#     assignee_name_data = assignee_name_data.drop("name_count", axis=1).assign(organization=None)
#     assignee_name_data.rename({"assignee_id": "id"}, axis=1).to_sql(name='assignee', con=engine, if_exists='append',
#                                                                     index=False)
#
#     assignee_organization_with_count = pd.read_sql_query(
#             "SELECT assignee_id, organization, type, count(1) org_count from rawassignee where assignee_id is not "
#             "null and organization is not null group by assignee_id, organization, type;",
#             con=engine)
#     assignee_org_data = assignee_organization_with_count.sort_values("org_count", ascending=False).groupby(
#             "assignee_id").head(
#             1).reset_index(drop=True)
#     assignee_org_data = assignee_org_data.drop("org_count", axis=1).assign(name_first=None, name_last=None)
#     assignee_org_data.rename({"assignee_id": "id"}, axis=1).to_sql(name='assignee', con=engine, if_exists='append',
#                                                                    index=False)


def create_assignee(update_config):
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
        canonical_assignments.to_sql(name='assignee', con=engine,
                                     if_exists='append',
                                     index=False)
        current_iteration_duration = time.time() - start
        offset = limit + offset

def precache_assignees(config):
    assignee_cache_query = """
    INSERT IGNORE INTO disambiguated_assignee_ids (assignee_id)  SELECT  assignee_id from {granted_db}.rawassignee UNION SELECT  assignee_id from {pregrant_db}.rawassignee;
    """.format(pregrant_db=config['PATENTSVIEW_DATABASES']['PGPUBS_DATABASE'],
               granted_db=config['PATENTSVIEW_DATABASES']['RAW_DB'])
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    print(assignee_cache_query)
    engine.execute(assignee_cache_query)

def assignee_reduce(assignee_data):
    assignee_data['help'] = assignee_data.groupby(['assignee_id', 'type', 'name_first', 'name_last', 'organization'])[
        'assignee_id'].transform('count')
    out = assignee_data.sort_values(['help', 'patent_date'], ascending=[False, False],
                                    na_position='last').drop_duplicates(
            'assignee_id', keep='first').drop(
            ['help', 'patent_date'], 1)
    return out

def post_process_assignee(config):
    upload_disambig_results(config)
    update_rawassignee(config, database='PGPUBS_DATABASE', uuid_field='id')
    update_rawassignee(config)
    precache_assignees(config)
    create_assignee(config)


def post_process_qc(config):
    qc = AssigneePostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
    post_process_assignee(config)
    post_process_qc(config)
