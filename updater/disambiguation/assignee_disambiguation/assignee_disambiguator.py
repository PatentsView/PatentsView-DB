from datetime import datetime
from pendulum import DateTime

import pv
from lib.configuration import get_disambig_config
from lib.utilities import archive_folder, link_view_to_new_disambiguation_table
from pv.QA.AssigneeDisambiguationPipelineTester import AssigneeDisambiguationPipelineTester
from pv.disambiguation.util.config_util import prepare_config


def build_assignee_name_mentions(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    pv.disambiguation.assignee.build_assignee_name_mentions_consolidated.generate_assignee_mentions(config)


def qc_build_assignee_name_mentions(**kwargs):
    airflow_run_date = datetime.fromisoformat(kwargs['ts'])
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    tester = AssigneeDisambiguationPipelineTester(config=config)
    tester.test_assignee_mentions_step(airflow_run_date)
    # pv.disambiguation.assignee.qc.assignee_disambiguation_pipeline_qc.test_assignee_mentions_step(config,
    #                                                                                               airflow_run_date)
def run_hierarchical_clustering(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.assignee.run_clustering.run_clustering(config)


def create_uuid_map(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.assignee.create_uuid_map.generate_uuid_map(config)


def finalize_assignee_clustering(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.assignee.finalize.finalize_results(config)


def upload_results(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.assignee.upload.upload(config)


def archive_results(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    # print("Archiving files")
    config = prepare_config(config)
    # folder = config['DATES']['END_DATE']
    # source_folder = "data/current/assignee"
    # targets = ["data/{folder}/assignee/".format(folder=folder)]
    # archive_folder(source_folder, targets)
    print("Mapping tables")
    cnx_g = pv.disambiguation.util.db.connect_to_disambiguation_database(config, dbtype='granted_patent_database')
    link_view_to_new_disambiguation_table(connection=cnx_g, table_name=config['ASSIGNEE_UPLOAD']['target_table'],
                                          disambiguation_type='assignee')
    cnx_pg = pv.disambiguation.util.db.connect_to_disambiguation_database(config, dbtype='pregrant_database')
    link_view_to_new_disambiguation_table(connection=cnx_pg, table_name=config['ASSIGNEE_UPLOAD']['target_table'],
                                          disambiguation_type='assignee')


if __name__ == '__main__':
    build_assignee_name_mentions(**{'execution_date': DateTime(year=2022, month=7, day=1)})
