import datetime
import os
import sys

from QA.create_databases.TextTest import TextUploadTest
from lib.configuration import get_realized_text_json_file


def add_text_table_suffx(config, database_date):
    parsing_file_setting = "{prefix}_parsing_config_template_file".format(prefix='long_text')
    parsing_config_file = config["XML_PARSING"][parsing_file_setting]
    parsing_realized_file_setting = "{prefix}_parsing_config_file".format(prefix='long_text')
    parsing_config_realized_file = config["XML_PARSING"][parsing_realized_file_setting]

    import json
    parsing_config = json.load(open(parsing_config_file))
    for table_config in parsing_config['table_xml_map']:
        table_config['table_name'] = "{tname}_{yr_suffix}".format(
                tname=table_config['table_prefix'],
                yr_suffix=int(database_date.strftime('%Y')))
    json.dump(parsing_config, open(parsing_config_realized_file, "w"))


def begin_text_parsing(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    add_text_table_suffx(config,
                         database_date=datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d'))
    project_home = os.environ['PACKAGE_HOME']
    sys.path.append(project_home + '/updater/text_parser/')
    from updater.xml_to_sql.parser import queue_parsers
    queue_parsers(config, type='long_text')


def post_text_parsing(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    tpt = TextUploadTest(config)
    tpt.runTests()


if __name__ == '__main__':
    begin_text_parsing(**{
            "execution_date": datetime.date(2020, 12, 29)
            })
