import datetime

import pandas as pd

from QA.post_processing.DisambiguationTester import DisambiguationTester
from lib.configuration import get_current_config


class AssigneePostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)


    def runTests(self):
        super(AssigneePostProcessingQC, self).runTests()
        super(DisambiguationTester, self).runTests()


if __name__ == '__main__':
    config = get_current_config('granted_patent', schedule='quarterly', **{
            "execution_date": datetime.date(2023, 1, 1)
            })
    # print({section: dict(config[section]) for section in config.sections()})
    qc = AssigneePostProcessingQC(config)
    qc.runTests()