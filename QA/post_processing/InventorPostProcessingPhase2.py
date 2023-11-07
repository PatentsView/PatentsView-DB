import datetime
from lib.configuration import get_current_config

from QA.post_processing.DisambiguationTester import DisambiguationTester


class InventorPostProcessingQCPhase2(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

    def runTests(self):
        super(InventorPostProcessingQCPhase2, self).runTests()
        super(DisambiguationTester, self).runTests()

if __name__ == '__main__':
    config = get_current_config('granted_patent', schedule="quarterly", **{
                    "execution_date": datetime.date(2023, 1, 1)
                                })
    qc = InventorPostProcessingQCPhase2(config)
    qc.runTests()
