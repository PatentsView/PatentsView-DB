import datetime
# import os
# import json

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config


class MergeTestQuarterly(DatabaseTester):

    def __init__(self, config, run_id):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)


if __name__ == '__main__':
    # config = get_current_config('granted_patent', **{
    #     "execution_date": datetime.date(2021, 10, 5)
    # })
    config = get_current_config('pgpubs', **{
        "execution_date": datetime.date(2021, 12, 2)
    })
    # fill with correct run_id
    run_id = "scheduled__2021-12-11T09:00:00+00:00"
    mcq = MergeTestQuarterly(config, run_id)
    mcq.runTests()