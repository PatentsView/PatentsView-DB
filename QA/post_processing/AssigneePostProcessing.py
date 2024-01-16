import datetime
import pandas as pd
from QA.post_processing.DisambiguationTester import DisambiguationTester
from lib.configuration import get_current_config
import logging
from QA.DatabaseTester import DatabaseTester

logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)

class AssigneePostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

    def run_assignee_disambig_tests(self):
        super(AssigneePostProcessingQC, self).runTests()
        self.runDisambiguationTests()

if __name__ == '__main__':
    for d in [datetime.date(2022, 1, 1), datetime.date(2022, 4, 1), datetime.date(2022, 7, 1), datetime.date(2022, 10, 1)]:
        config = get_current_config('granted_patent', schedule='quarterly', **{
                "execution_date": d
                })
        # print({section: dict(config[section]) for section in config.sections()})
        qc = AssigneePostProcessingQC(config)
        qc.run_assignee_disambig_tests()