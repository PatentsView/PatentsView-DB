def get_config():
    import os
    project_home = os.environ['PACKAGE_HOME']
    import configparser

    config = configparser.ConfigParser()
    config.read(project_home + '/config.ini')
    return config


def get_section(task_id):
    section_lookup = {'download_xml': "XML Processing"}
    return section_lookup[task_id]
