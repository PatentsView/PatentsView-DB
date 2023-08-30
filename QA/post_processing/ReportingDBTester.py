from lib.download_check_delete_databases import query_for_all_tables_in_db, get_count_for_all_tables
from lib.configuration import get_current_config, get_unique_connection_string
import datetime
from QA.DatabaseTester import DatabaseTester
import logging

logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)


class ReportingDBTester(DatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["REPORTING_DATABASE"], datetime.date(year=1976, month=1, day=1),end_date)

    def run_reporting_db_tests(self):
        counter = 0
        total_tables = len(self.table_config.keys())
        self.init_qa_dict()
        for table in self.table_config:
            self.load_table_row_count(table, where_vi=False)
            self.check_for_indexes(table)
            self.load_nulls(table, self.table_config[table], where_vi=False)
            self.test_blank_count(table, self.table_config[table], where_vi=False)
            self.save_qa_data()
            self.init_qa_dict()
            logger.info(f"FINISHED WITH TABLE: {table}")
            counter += 1
            logger.info(f"Currently Done With {counter} of {total_tables} | {counter/total_tables} %")


def run_reporting_db_qa():
    config = get_current_config('granted_patent', **{
                    "execution_date": datetime.date(2022, 6, 30)
                                })
    qc = ReportingDBTester(config)
    qc.run_reporting_db_tests()


if __name__ == '__main__':
    # check_reporting_db_row_count()
    config = get_current_config('granted_patent', **{"execution_date": datetime.date(2023, 6, 29)})
    qc = ReportingDBTester(config)
    qc.run_reporting_db_tests()
