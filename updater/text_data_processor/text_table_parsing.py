import os
import sys

from QA.create_databases.TextTest import TextUploadTest
from lib.configuration import get_today_dict


def add_text_table_suffx(config, execution_date):
    parsing_config_file = config["FILES"]["parsing_config_file"]
    import json
    parsing_config = json.load(open(parsing_config_file))
    for table_config in parsing_config['table_xml_map']:
        table_config['table_name'] = "{tname}_{yr_suffix}".format(
                tname=table_config['table_prefix'],
                yr_suffix=int(execution_date.strftime('%Y')))
    json.dump(parsing_config, open(parsing_config_file, "w"))


def begin_text_parsing(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    add_text_table_suffx(config, execution_date=kwargs['execution_date'])
    project_home = os.environ['PACKAGE_HOME']
    sys.path.append(project_home + '/updater/text_parser/')
    from updater.xml_to_sql.parser import queue_parsers
    queue_parsers(config)


def post_text_parsing(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    tpt = TextUploadTest(config)
    tpt.runTests()


if __name__ == '__main__':
    begin_text_parsing(**get_today_dict('granted_patent'))
