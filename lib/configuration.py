def get_config(type='granted_patent'):
    import os
    project_home = os.environ['PACKAGE_HOME']
    import configparser

    config = configparser.ConfigParser()
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
    if type == 'granted_patent':
        filename = 'config.ini'
    elif type == 'application':
        filename = 'app_config.ini'
    config_file = "{home}/{filename}".format(home=project_home, filename=filename)
    with open(config_file, "w") as f:
        config.write(f)


def get_section(task_id):
    section_lookup = {'download_xml': "XML Processing", 'process_xml': "XML Processing", 'parse_xml': "XML Processing",
                      "backup_olddb": "Database Setup", "rename_db": "Database Setup"}
    return section_lookup[task_id]


def get_connection_string(config, database='TEMP_UPLOAD_DB'):
    database = '{}'.format(config['DATABASE'][database])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])
    return 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database)


def get_backup_command(config, project_home):
    command = "mydumper"
    conf_parameter = "{home}/resources/sql.conf".format(home=project_home)
    directory_parameter = "{datahome}/{database}_backup".format(datahome=config["FOLDERS"]["WORKING_FOLDER"],
                                                                database=config["DATABASE"]["TEMP_UPLOAD_DB"])
    database_parameter = "{database}".format(database=config["DATABASE"]["OLD_DB"])
    verbosity = 3
    thread = 6

    backup_command = "{command} --defaults-file={conf_parameter} -F 50 -s 5000000 -l 1999999999 -v {verbosity} -t {thread} -B {database} -o {directory_parameter}".format(
        command=command, conf_parameter=conf_parameter, verbosity=verbosity, thread=thread,
        directory_parameter=directory_parameter, database=database_parameter)

    return backup_command


def get_loader_command(config, project_home):
    command = "bash"
    script = "{home}/lib/loader/index_optimized_loader".format(home=project_home)
    conf_parameter = "{home}/resources/sql.conf".format(home=project_home)
    directory_parameter = "{datahome}/{database}_backup".format(datahome=config["FOLDERS"]["WORKING_FOLDER"],
                                                                database=config["DATABASE"]["TEMP_UPLOAD_DB"])
    database_parameter = "{database}".format(database=config["DATABASE"]["OLD_DB"])
    verbosity = 3
    thread = 6

    loader_command = "{command} {script} {conf_parameter} -d {directory_parameter} -s {database_parameter} -v {verbosity} -t {thread}".format(
        command=command, script=script, conf_parameter=conf_parameter, directory_parameter=directory_parameter,
        database_parameter=database_parameter, verbosity=verbosity, thread=thread)

    return loader_command
