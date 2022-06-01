import datetime

from lib.configuration import get_current_config
from updater.xml_to_sql.parser import queue_parsers
from updater.xml_to_sql.post_processing import consolidate_granted_cpc


def patent_sql_parser(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    queue_parsers(config, type='granted_patent')
    consolidate_granted_cpc(config)


if __name__ == '__main__':

    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })

    config['DATES'] = {
            "START_DATE": '20201006',
            "END_DATE":   '20201229'
            }
    queue_parsers(config, type='granted_patent')
