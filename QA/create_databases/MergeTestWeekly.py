import datetime
import json
import os

import logging

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config
from lib.utilities import class_db_specific_config, load_table_config

logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)

class MergeTestWeekly(DatabaseTester):
    # This test tests new weekly data imported into the production databases
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2021, 10, 5)
    })
    # config = get_current_config('pgpubs', **{
    #     "execution_date": datetime.date(2021, 12, 2)
    # })
    run_id = "scheduled__2021-12-11T09:00:00+00:00"
    mc = MergeTestWeekly(config, run_id)
    mc.runStandardTests()
