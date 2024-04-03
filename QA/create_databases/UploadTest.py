import datetime

from QA.DatabaseTester import DatabaseTester
from lib.configuration import get_current_config
from lib.utilities import class_db_specific_config, load_table_config

import logging
logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)

class UploadTest(DatabaseTester):
    # This test tests the newly parsed database with all of our full test suite
    def __init__(self, config):
        start_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"], start_date, end_date)

if __name__ == '__main__':
    # config = get_current_config('granted_patent', **{
    #     "execution_date": datetime.date(2021, 10, 5)
    # })
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2024, 1, 2)
    })
    upt = UploadTest(config)
    upt.runStandardTests()