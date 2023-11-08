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
        self.run_assignee_disambig_tests_pgpubs()

    def run_assignee_disambig_tests_pgpubs(self):
        counter = 0
        total_tables = len(self.table_config.keys())
        self.init_qa_dict()
        for table in self.table_config:
            print(table)
            self.load_table_row_count(table, where_vi=False)
            self.load_nulls(table, self.table_config[table], where_vi=False)
            self.test_blank_count(table, self.table_config[table], where_vi=False)
            self.save_qa_data()
            self.init_qa_dict()
            logger.info(f"FINISHED WITH TABLE: {table}")
            counter += 1
            logger.info(f"Currently Done With {counter} of {total_tables} | {(counter/total_tables)*100} %")

if __name__ == '__main__':
    for d in [datetime.date(2022, 1, 1), datetime.date(2022, 4, 1), datetime.date(2022, 7, 1), datetime.date(2022, 10, 1)]:
        config = get_current_config('granted_patent', schedule='quarterly', **{
                "execution_date": d
                })
        # print({section: dict(config[section]) for section in config.sections()})
        qc = AssigneePostProcessingQC(config)
        qc.run_assignee_disambig_tests()