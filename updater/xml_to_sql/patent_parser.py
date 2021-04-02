import datetime

from updater.xml_to_sql.parser import queue_parsers




if __name__ == '__main__':
    from lib.configuration import get_current_config

    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })

    config['DATES'] = {
            "START_DATE": '20201006',
            "END_DATE":   '20201229'
            }
    queue_parsers(config, type='granted_patent')
