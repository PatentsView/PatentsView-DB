import datetime

from QA.post_processing.DisambiguationTester import DisambiguationTester
from lib.configuration import get_current_config


class LawyerPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

    def runTests(self):
        super(LawyerPostProcessingQC, self).runTests()
        super(DisambiguationTester, self).runDisambiguationTests()

if __name__ == '__main__':
    config = get_current_config('granted_patent',schedule='quarterly', **{
            "execution_date": datetime.date(2021, 10, 1)
            })
    qc = LawyerPostProcessingQC(config)
    qc.runTests()