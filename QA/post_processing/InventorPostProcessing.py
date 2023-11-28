import datetime
from lib.configuration import get_current_config
from QA.post_processing.DisambiguationTester import DisambiguationTester
import logging

logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)

class InventorPostProcessingQC(DisambiguationTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["PROD_DB"], datetime.date(year=1976, month=1, day=1), end_date)

    def run_inventor_disambig_tests(self):
        super(InventorPostProcessingQC, self).runTests()
        self.runInventorAssigneeTests()


if __name__ == '__main__':
    config = get_current_config('granted_patent', schedule="quarterly", **{
                    "execution_date": datetime.date(2023, 1, 1)
                                })
    qc = InventorPostProcessingQC(config)
    qc.runTests()
