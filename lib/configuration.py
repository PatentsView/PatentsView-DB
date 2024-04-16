import datetime
import json
import os

from elasticsearch import Elasticsearch
from pendulum import DateTime


def get_config():
    import os
    project_home = os.environ['PACKAGE_HOME']
    import configparser

    config = configparser.ConfigParser()
    filename = 'config.ini'
    config_file = "{home}/{filename}".format(home=project_home, filename=filename)
    config.read(config_file)
    return config


def set_config(config, type='granted_patent'):
    import os
    project_home = os.environ['PACKAGE_HOME']
    filename = 'config.ini'
    if type == 'granted_patent':
        filename = 'config.ini'
    elif type == 'application':
        filename = 'app_config.ini'
    config_file = "{home}/{filename}".format(home=project_home, filename=filename)
    with open(config_file, "w") as f:
        config.write(f)
    return config


def get_section(dag_id, task_id):
    section_lookup = {
            'granted_patent_updater':       {
                    "merge_db":           "Granted Patent - Data Processing",
                    "merge_text_db":      "Granted Patent - Data Processing",
                    "parse_xml":          "Granted Patent - XML Parsing",
                    "qc_parse_text_data": "Granted Patent - XML Parsing (QC)",
                    "qc_upload_new":      "Granted Patent - Data Processing (QC)",
                    "GI_QC":              "Granted Patent - GI Processing (QC)"
                    },
            'pregrant_publication_updater': {
                    "create_pgpubs_database": "PGPUBS Parser - Database Setup",
                    "drop_database":          "PGPUBS Parser - Database Setup",
                    "merge_database":         "PGPUBS Parser - Data Processing",
                    "parse_pgpubs_xml":       "PGPUBS Parser - XML Parsing",
                    "post_process":           "PGPUBS Parser - Data Processing"
                    },
            '99_daily_checks':              {
                    'api_query_check': 'System Check - API',
                    'space_check':     'System Check - Free space on Ingest MySQL'
                    }

            }
    section = None
    if dag_id in section_lookup:
        if task_id in section_lookup[dag_id]:
            section = section_lookup[dag_id][task_id]
    return section


def get_connection_string(config, database='TEMP_UPLOAD_DB', connection='DATABASE_SETUP'):
    database = '{}'.format(config['PATENTSVIEW_DATABASES'][database])
    host = '{}'.format(config[connection]['HOST'])
    user = '{}'.format(config[connection]['USERNAME'])
    password = '{}'.format(config[connection]['PASSWORD'])
    port = '{}'.format(config[connection]['PORT'])
    return 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database)

def get_unique_connection_string(config, connection='DATABASE_SETUP', database='unique_name'):
    database = f'{database}'
    host = '{}'.format(config[connection]['HOST'])
    user = '{}'.format(config[connection]['USERNAME'])
    password = '{}'.format(config[connection]['PASSWORD'])
    port = '{}'.format(config[connection]['PORT'])
    return 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database)

def get_backup_command(**kwargs):
    command = "mydumper"
    config = get_current_config(**kwargs)
    conf_parameter = config['DATABASE_SETUP']['CONFIG_FILE']
    directory_parameter = "{datahome}/{database}_backup".format(datahome=config["FOLDERS"]["WORKING_FOLDER"],
                                                                database=config["PATENTSVIEW_DATABASES"]["PROD_DB"])
    database_parameter = "{database}".format(database=config["PATENTSVIEW_DATABASES"]["PROD_DB"])
    verbosity = 3
    thread = 6

    backup_command = """
{command} --defaults-file={conf_parameter} -F 50 -s 5000000 -l 1999999999 -v {verbosity} -t {thread} -B {database} -o 
{directory_parameter} --lock-all-tables
    """.format(command=command, conf_parameter=conf_parameter, verbosity=verbosity,
               thread=thread, directory_parameter=directory_parameter, database=database_parameter)
    return backup_command


def get_loader_command(config, project_home):
    command = "bash"
    script = "{home}/lib/loader/index_optimized_loader".format(home=project_home)
    conf_parameter = "{home}/resources/sql.conf".format(home=project_home)
    directory_parameter = "{datahome}/{database}_backup".format(datahome=config["FOLDERS"]["WORKING_FOLDER"],
                                                                database=config["PATENTSVIEW_DATABASES"]["PROD_DB"])
    database_parameter = "{database}".format(database=config["PATENTSVIEW_DATABASES"]["PROD_DB"])
    verbosity = 3
    thread = 6

    loader_command = """
{command} {script} {conf_parameter} -d {directory_parameter} -s {database_parameter} -v {verbosity} -t {thread} -o
    """.format(command=command, script=script, conf_parameter=conf_parameter,
               directory_parameter=directory_parameter, database_parameter=database_parameter,
               verbosity=verbosity, thread=thread)

    return loader_command


def get_text_table_load_command(project_home, **kwargs):
    command = 'mysql'
    config = get_current_config(**kwargs)
    defaults_parameter = config['DATABASE_SETUP']['CONFIG_FILE']
    script_to_load = "{home}/resources/text_table_triggers.sql".format(home=project_home)
    database = config["PATENTSVIEW_DATABASES"]['TEMP_UPLOAD_DB']
    create_command = "{command} --defaults-file={default_param} {database} < {script_to_load}".format(
            command=command, default_param=defaults_parameter,
            database=database, script_to_load=script_to_load)
    return create_command


def get_today_dict(type='granted_patent', from_date=datetime.date.today()):
    day_offset = 1
    if type == 'pgpubs':
        day_offset = 3
    offset = (from_date.weekday() - day_offset) % 7
    latest_release_day = from_date - datetime.timedelta(days=offset)
    return {
            'execution_date': latest_release_day
            }


def get_table_config(update_config):
    project_home = os.environ['PACKAGE_HOME']
    dbtype = 'pgpubs' if update_config["PATENTSVIEW_DATABASES"]['PROD_DB']=='pregrant_publications' else 'patent'
    resources_file = "{root}/{resources}/raw_db_tables_{dbtype}.json".format(root=project_home,
                                                                    resources=update_config["FOLDERS"]["resources_folder"],
                                                                    dbtype=dbtype)
    raw_db_table_settings = json.load(open(resources_file))
    return raw_db_table_settings


def get_required_tables(update_config):
    raw_db_table_settings = get_table_config(update_config)
    return raw_db_table_settings['table_list'].keys()


def get_upload_tables_dict(update_config):
    raw_db_table_settings = get_table_config(update_config)
    required_tables = {x: False for x in raw_db_table_settings["table_list"] if not
    raw_db_table_settings["table_list"][x]["bulk_generated"]}
    return required_tables


def get_parsed_tables_dict(update_config):
    raw_db_table_settings = get_table_config(update_config)
    required_tables = {x: False for x in raw_db_table_settings["table_list"] if
                       raw_db_table_settings["table_list"][x]["raw_data"] and not
                       raw_db_table_settings["table_list"][x]["direct_load"]}
    return required_tables


def get_merge_table_candidates(update_config):
    raw_db_table_settings = get_table_config(update_config)
    required_tables = {x: False for x in raw_db_table_settings["table_list"] if
                       raw_db_table_settings["table_list"][x]["raw_data"]}
    return required_tables


def get_lookup_tables(update_config):
    raw_db_table_settings = get_table_config(update_config)
    lookup_tables = [x for x in raw_db_table_settings["table_list"] if raw_db_table_settings["table_list"][x]["lookup"]]
    return lookup_tables


def get_version_indicator(**kwargs):
    execution_date = kwargs['execution_date']
    return execution_date.strftime('%Y%m%d')


def get_disambig_config(schedule='quarterly', supplemental_configs=None, **kwargs):
    disambiguation_root = os.environ['DISAMBIGUATION_ROOT']
    print(disambiguation_root)
    import configparser, pprint

    config = get_config()
    execution_date: DateTime = kwargs['execution_date']
    if schedule == 'weekly':
        current_week_start = datetime.timedelta(days=1)
        current_week_end = datetime.timedelta(days=7)
        start_date = (execution_date + current_week_start)
        end_date = (execution_date + current_week_end)
    else:
        from lib.is_it_update_time import get_update_range_full_quarter
        start_date, end_date_dash = get_update_range_full_quarter(execution_date)
    start_date = start_date.strftime('%Y%m%d')
    end_date = end_date_dash.strftime('%Y%m%d')
    config['DATES'] = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "END_DATE_DASH": end_date_dash
    }
    config['DISAMBIG_TABLES'] = {
        "INVENTOR": f"inventor_disambiguation_mapping_{end_date}",
        "ASSIGNEE": f"assignee_disambiguation_mapping_{end_date}",
    }
    print("Start Date is {start}".format(start=config['DATES']['START_DATE']))
    print("End date is {end}".format(end=config['DATES']['END_DATE']))
    if supplemental_configs is not None:
        for supplemental_config in supplemental_configs:
            s_config = configparser.ConfigParser()
            config_file = "{disambiguation_root}/{filename}".format(disambiguation_root=disambiguation_root,
                                                                    filename=supplemental_config)
            print(config_file)
            s_config.read(config_file)
            config.update(s_config)
        print("Canopy Settings are {canopy_setting}".format(
            canopy_setting=pprint.pformat(config['INVENTOR_BUILD_CANOPIES'])))

    incremental = 0
    # if end_date.month == 12:
    #     incremental = 0
    config['DISAMBIGUATION']['INCREMENTAL'] = str(incremental)
    print("Incremental Setting is {incremental}".format(incremental=config['DISAMBIGUATION']['INCREMENTAL']))
    return config


def get_current_config(type='granted_patent', schedule='weekly', **kwargs):
    """
    Update config file start and end date to first and last day of the supplied week
    :param supplemental_configs:
    :param type: XML type
    :type cfg: object
    :type yr: int
    :type mth: int
    :type day: int
    :param yr: Year for start and end date
    :param mth: Month for start and end date
    :param day: Day for start and end date
    :param cfg: config to update
    :return: updated config
    """

    config = get_config()
    config_prefix = "upload_"

    if type == 'pgpubs':
        config_prefix = 'pgpubs_'
    execution_date: DateTime = kwargs['execution_date']
    print(f"""
    generating config with parameters: 
    type: {type}
    schedule: {schedule}
    execution date: {execution_date.strftime('%Y-%m-%d')}""")
    if schedule == 'weekly':
        current_week_start = datetime.timedelta(days=1)
        current_week_end = datetime.timedelta(days=7)
        start_date = (execution_date + current_week_start)
        end_date = (execution_date + current_week_end)
    else:
        from lib.is_it_update_time import get_update_range_full_quarter
        start_date, end_date = get_update_range_full_quarter(execution_date)
    temp_date = end_date.strftime('%Y%m%d')

    config['DATES'] = {
        "START_DATE": start_date.strftime('%Y%m%d'),
        "END_DATE": end_date.strftime('%Y%m%d'),
        "END_DATE_DASH": end_date
    }
    prefixed_string = "{prfx}{date}".format(prfx=config_prefix, date=temp_date)
    config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"] = prefixed_string
    config['PATENTSVIEW_DATABASES']["PROD_DB"] = 'pregrant_publications'
    config['PATENTSVIEW_DATABASES']["TEXT_DB"] = 'pgpubs_text'

    if type == 'pgpubs':
        config['PATENTSVIEW_DATABASES']["REPORTING_DATABASE"] = 'PatentsView_' + end_date.strftime('%Y%m%d')
        config['PATENTSVIEW_DATABASES']["ELASTICSEARCH_DB"] = 'elastic_production_pgpub_'+ end_date.strftime('%Y%m%d')
        config['PATENTSVIEW_DATABASES']["ELASTICSEARCH_DB_TYPE"] = 'elasticsearch_pgpub'
    config['FOLDERS']["WORKING_FOLDER"] = "{data_root}/{prefix}".format(
        prefix=prefixed_string,
        data_root=config['FOLDERS']['data_root'])
    if type == 'granted_patent':
        config['FOLDERS']['granted_patent_bulk_xml_location'] = '{working_folder}/raw_data/'.format(
            working_folder=config['FOLDERS']['WORKING_FOLDER'])
        config['FOLDERS']['long_text_bulk_xml_location'] = '{working_folder}/raw_data/'.format(
            working_folder=config['FOLDERS']['WORKING_FOLDER'])
        config['PATENTSVIEW_DATABASES']["PROD_DB"] = 'patent'
        config['PATENTSVIEW_DATABASES']["TEXT_DB"] = 'patent_text'
        config['PATENTSVIEW_DATABASES']["REPORTING_DATABASE"] = 'PatentsView_' + end_date.strftime('%Y%m%d')
        config['PATENTSVIEW_DATABASES']["ELASTICSEARCH_DB"] = 'elastic_production_patent_' + end_date.strftime('%Y%m%d')
        config['PATENTSVIEW_DATABASES']["ELASTICSEARCH_DB_TYPE"] = 'elasticsearch_patent'

    latest_thursday = get_today_dict(type='pgpubs', from_date=end_date)
    latest_tuesday = get_today_dict(type='granted_patent', from_date=end_date)

    return config


def get_es(config):
    es_hostname = config['ELASTICSEARCH']['HOST']
    username = config['ELASTICSEARCH']['USER']
    password = config['ELASTICSEARCH']['PASSWORD']
    es = Elasticsearch(hosts=es_hostname, http_auth=(username, password),timeout=1200)
    return es

if __name__ == '__main__':
    # pgpubs, granted_patent
    # config = get_current_config('pgpubs', schedule="quarterly", **{
    #     "execution_date": datetime.date(2021, 8, 4)
    # })
    # get_backup_command(**{
    #     "execution_date": datetime.date(2021, 11, 4)
    # })
    # print(config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"])
    config = get_disambig_config(type='pgpubs', schedule="quarterly", **{
        "execution_date": datetime.date(2021, 10, 1)
    })
    # print(config['PATENTSVIEW_DATABASES']["PROD_DB"])
    # print(config['PATENTSVIEW_DATABASES']["TEXT_DB"])
    # print(config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"][:6])
    # print(config['DATES']['END_DATE'])
