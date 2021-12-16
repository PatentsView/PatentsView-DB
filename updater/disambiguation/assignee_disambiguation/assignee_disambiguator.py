from pendulum import DateTime

import pv
from lib.configuration import get_disambig_config
from lib.utilities import archive_folder, link_view_to_new_disambiguation_table
from pv.disambiguation.util.config_util import prepare_config


def build_assignee_name_mentions(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/consolidated_config.ini'],
                                 **kwargs)
    pv.disambiguation.assignee.build_assignee_name_mentions_consolidated.generate_assignee_mentions(config)


def run_hierarchical_clustering(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.assignee.run_clustering.run_clustering(config)


def create_uuid_map(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.assignee.create_uuid_map.generate_uuid_map(config)


def upload_results(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.assignee.upload.upload(config)


def archive_results(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    folder = config['DATES']['END_DATE']
    source_folder = "data/current/assignee"
    targets = ["data/{folder}/assignee/".format(folder=folder)]
    archive_folder(source_folder, targets)
    cnx_g = pv.disambiguation.util.db.connect_to_disambiguation_database(config, dbtype='granted_patent_database')
    link_view_to_new_disambiguation_table(connection=cnx_g, table_name=config['ASSIGNEE_UPLOAD']['target_table'],
                                          disambiguation_type='assignee')


if __name__ == '__main__':
    run_hierarchical_clustering(**{'execution_date': DateTime(year=2021, month=7, day=1)})
