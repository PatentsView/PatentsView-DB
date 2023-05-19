from pendulum import DateTime

import os
import pv
from lib.configuration import get_disambig_config
from lib.utilities import archive_folder, link_view_to_new_disambiguation_table
from pv.disambiguation.util.config_util import prepare_config


def setup_inventor_assignee_disambiguation(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini', '/project/config.ini'],
                                 **kwargs)
    breakpoint()
    end_date = config['DATES']['END_DATE']
    os.makedirs(os.path.dirname(f"{config['BASE_PATH']['inventor']}".format(end_date=end_date)), exist_ok=True)
    os.makedirs(os.path.dirname(f"{config['BASE_PATH']['assignee']}".format(end_date=end_date)), exist_ok=True)
    print(f"NEW PATH CREATED ---- {config['BASE_PATH']['inventor']}".format(end_date=end_date))


def build_assignee_features(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    pv.disambiguation.inventor.build_assignee_features_consolidated.generate_assignee_mentions(config)


def build_coinventor_features(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    pv.disambiguation.inventor.build_coinventor_features_consolidated.generate_coinventor_mentions(config)


def build_title_map(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    pv.disambiguation.inventor.build_title_map_consolidated.generate_title_maps(config)


def build_canopies(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    pv.disambiguation.inventor.build_canopies_consolidated.generate_inventor_canopies(config)


def run_hierarchical_clustering(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.inventor.run_clustering.run_clustering(config)


def finalize_disambiguation(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.inventor.finalize.finalize(config)


def upload_results(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    pv.disambiguation.inventor.upload.upload(config)


def archive_results(**kwargs):
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/new_consolidated_config.ini'],
                                 **kwargs)
    config = prepare_config(config)
    # incremental = True if config['DISAMBIGUATION']['INCREMENTAL'] == "1" else False
    # folders = [config['DATES']['END_DATE']]
    # if incremental:
    #     folders.append("full_disambiguation")
    #     print("Running Full Disambiguation!")
    # source_folder = "data/current/inventor"
    # targets = ["data/{folder}/inventor/".format(folder=x) for x in folders]
    # archive_folder(source_folder, targets)
    print('Mapping tables')
    cnx_g = pv.disambiguation.util.db.connect_to_disambiguation_database(config, dbtype='granted_patent_database')
    link_view_to_new_disambiguation_table(connection=cnx_g, table_name=config['INVENTOR_UPLOAD']['target_table'],
                                          disambiguation_type='inventor')
    cnx_g = pv.disambiguation.util.db.connect_to_disambiguation_database(config, dbtype='pregrant_database')
    link_view_to_new_disambiguation_table(connection=cnx_g, table_name=config['INVENTOR_UPLOAD']['target_table'],
                                          disambiguation_type='inventor')


if __name__ == '__main__':
    # config = get_disambig_config(schedule='quarterly',
    #                              supplemental_configs=['config/new_consolidated_config.ini'],
    #                              **{'execution_date': DateTime(year=2021, month=7, day=1)})
    # config = prepare_config(config)
    # import pprint
    #
    # archive_results(**{'execution_date': DateTime(year=2021, month=7, day=1)})
    # pprint.pprint({section: dict(config[section]) for section in config.sections()})
    # build_title_map(**{'execution_date': DateTime(year=2021, month=7, day=1)})
    setup_inventor_assignee_disambiguation(**{'execution_date': DateTime(year=2022, month=7, day=1)})
