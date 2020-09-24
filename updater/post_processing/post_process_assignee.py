import csv
import time

from sqlalchemy import create_engine
import pandas as pd

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


def update_rawassignee(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    update_statement = "UPDATE rawassignee ra left join assignee_disambiguation_mapping adm on adm.uuid = ra.uuid set " \
                       " ra.assignee_id = adm.assignee_id"
    engine.execute(update_statement)


def create_assignee(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    assignee_name_with_count = pd.read_sql_query(
            "SELECT assignee_id, name_first, name_last, type, count(1) name_count from rawassignee where assignee_id "
            "is not null and organization is null group by assignee_id, name_first, name_last, type;",
            con=engine)
    assignee_name_data = assignee_name_with_count.sort_values("name_count", ascending=False).groupby(
            "assignee_id").head(
            1).reset_index(drop=True)
    assignee_name_data = assignee_name_data.drop("name_count", axis=1).assign(organization=None)
    assignee_name_data.rename({"assignee_id": "id"}, axis=1).to_sql(name='assignee', con=engine, if_exists='append',
                                                                    index=False)

    assignee_organization_with_count = pd.read_sql_query(
            "SELECT assignee_id, organization, type, count(1) org_count from rawassignee where assignee_id is not "
            "null and organization is not null group by assignee_id, organization, type;",
            con=engine)
    assignee_org_data = assignee_organization_with_count.sort_values("org_count", ascending=False).groupby(
            "assignee_id").head(
            1).reset_index(drop=True)
    assignee_org_data = assignee_org_data.drop("org_count", axis=1).assign(name_first=None, name_last=None)
    assignee_org_data.rename({"assignee_id": "id"}, axis=1).to_sql(name='assignee', con=engine, if_exists='append',
                                                                   index=False)


def post_process_assignee(config):
    upload_disambig_results(config)
    update_rawassignee(config)
    create_assignee(config)


def post_process_qc(config):
    qc = AssigneePostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
    # post_process_assignee(config)
    post_process_qc(config)
