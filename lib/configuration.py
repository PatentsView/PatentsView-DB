import time

import pymysql


def get_config(type='granted_patent'):
    import os
    project_home = os.environ['PACKAGE_HOME']
    import configparser

    config = configparser.ConfigParser()
    filename = 'config.ini'
    if type == 'granted_patent':
        filename = 'config.ini'
    elif type == 'application':
        filename = 'app_config.ini'
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


def get_section(task_id):
    section_lookup = {'download_xml': "XML Processing", 'process_xml': "XML Processing", 'parse_xml': "XML Processing",
                      "backup_olddb": "Database Setup", "rename_db": "Database Setup", "upload_new": "Database Setup",
                      "restore_olddb": "Database Setup", "merge_db": "Database Setup",
                      "create_text_tables": "Description Parsing", "parse_text_data": "Supplemental Data Collection",
                      "download_cpc": "Supplemental Data Collection",
                      "qc_download_cpc": "QC - Supplemental Data Collection",
                      "cpc_class_parser": "Supplemental Data Collection",
                      "qc_cpc_class_parser": "QC - Supplemental Data Collection",
                      "cpc_parser": "Supplemental Data Collection",
                      "qc_cpc_parser": "QC - Supplemental Data Collection",
                      "cpc_current_processor": "Supplemental Data Collection",
                      "wipo_processor": "Supplemental Data Collection",
                      "cpc_class_uploader": "Supplemental Data Collection",
                      "qc_cpc_current_wipot": "QC - Supplemental Data Collection",
                      "qc_rename_db": "QC - Database Setup",
                      "qc_upload_new": "QC - Database Setup",
                      "qc_parse_text_data": "Supplemental Data Collection",
                      "merge_text_db": "Database Setup",
                      "qc_merge_db": "QC - Database Setup", "qc_merge_text_db": "QC - Text Parsing",
                      "withdrawn_processor": "Supplemental Data", "qc_withdrawn_processor": "QC - Supplemental Data"}

    return section_lookup[task_id]


def get_connection_string(config, database='TEMP_UPLOAD_DB'):
    database = '{}'.format(config['DATABASE'][database])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])
    return 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database)


# def get_query_results(config, database_section, query):
#     connect_time = time.time()
#
#     try:
#         with connection.cursor() as query_cursor:
#             query_cursor.execute(query)
#             return query_cursor
#     finally:
#         print(query)
#         connection.close()
#         print("Connection open for {duration} seconds".format(duration=round(time.time() - connect_time, 3)))
#

def get_backup_command(config, project_home):
    command = "mydumper"
    conf_parameter = "{home}/resources/sql.conf".format(home=project_home)
    directory_parameter = "{datahome}/{database}_backup".format(datahome=config["FOLDERS"]["WORKING_FOLDER"],
                                                                database=config["DATABASE"]["OLD_DB"])
    database_parameter = "{database}".format(database=config["DATABASE"]["OLD_DB"])
    verbosity = 3
    thread = 6

    backup_command = "{command} --defaults-file={conf_parameter} -F 50 -s 5000000 -l 1999999999 -v {verbosity} -t {thread} -B {database} -o {directory_parameter} --lock-all-tables".format(
        command=command, conf_parameter=conf_parameter, verbosity=verbosity, thread=thread,
        directory_parameter=directory_parameter, database=database_parameter)

    return backup_command


def get_loader_command(config, project_home):
    command = "bash"
    script = "{home}/lib/loader/index_optimized_loader".format(home=project_home)
    conf_parameter = "{home}/resources/sql.conf".format(home=project_home)
    directory_parameter = "{datahome}/{database}_backup".format(datahome=config["FOLDERS"]["WORKING_FOLDER"],
                                                                database=config["DATABASE"]["OLD_DB"])
    database_parameter = "{database}".format(database=config["DATABASE"]["OLD_DB"])
    verbosity = 3
    thread = 6

    loader_command = "{command} {script} {conf_parameter} -d {directory_parameter} -s {database_parameter} -v {verbosity} -t {thread}".format(
        command=command, script=script, conf_parameter=conf_parameter, directory_parameter=directory_parameter,
        database_parameter=database_parameter, verbosity=verbosity, thread=thread)

    return loader_command


def get_text_table_load_command(config, project_home):
    command = 'mysql'
    defaults_parameter = config['DATABASE']['CONFIG_FILE']
    script_to_load = "{home}/resources/text_tables.sql".format(home=project_home)
    database = config["DATABASE"]['TEMP_UPLOAD_DB']
    create_command = "{command} --defaults-file={default_param} {database} < {script_to_load}".format(command=command,
                                                                                                      default_param=defaults_parameter,
                                                                                                      database=database,
                                                                                                      script_to_load=script_to_load)
    return create_command


def get_scp_copy_command(config):
    # scp -i "$KEYFILE" "$FOLDER"/*.tsv disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/disambiguation/data
    command = 'scp'
    keyfile_parameter = config['DISAMBIGUATION_CREDENTIALS']['KEY_FILE']
    disambig_input_folder = '{}/disambig_inputs'.format(config['FOLDERS']['WORKING_FOLDER'])
    source_blob = "{folder}/*.tsv".format(folder=disambig_input_folder)
    disambig_user = 'disambiguser'
    disambig_host = "ec2-52-21-62-204.compute-1.amazonaws.com"
    destination = "/data/disambiguation/data"

    command = "{command} -i {keyfile} {source_blob} {user}@{host}:{destination}".format(command=command,
                                                                                        keyfile=keyfile_parameter,
                                                                                        source_blob=source_blob,
                                                                                        user=disambig_user,
                                                                                        host=disambig_host,
                                                                                        destination=destination)
    return command


def get_scp_download_command(config):
    command = 'scp'
    keyfile_parameter = config['DISAMBIGUATION_CREDENTIALS']['KEY_FILE']
    disambig_output_folder = '{}/disambig_output/'.format(config['FOLDERS']['WORKING_FOLDER'])
    disambig_user = 'disambiguser'
    disambig_host = "ec2-52-21-62-204.compute-1.amazonaws.com"
    source_files = {
        'inventor_disambiguation.tsv': '/data/disambiguation/exp_out/inventor/disambiguation.postprocessed.tsv',
        'assignee_disambiguation.tsv': '/data/disambiguation/exp_out/assignee/assignee_disambiguation.tsv',
        'location_disambiguation.tsv': '/data/disambiguation/exp_out/location/location_post_processed.tsv'}
    command_strings = ["mkdir -[ {disambig_output_folder}".format(disambig_output_folder=disambig_output_folder)]
    for dest_file in source_files:
        command = "{command} -i {keyfile} {user}@{host}:{source_file} {disambig_output_folder}/{dest_file} ".format(
            command=command, keyfile=keyfile_parameter, dest_file=dest_file, source_file=source_files[dest_file],
            user=disambig_user, host=disambig_host, disambig_output_folder=disambig_output_folder)
        command_strings.append(command)
    return " && ".join(command_strings)
