from lib.download_check_delete_databases import query_for_all_tables_in_db, get_count_for_all_tables
from lib.configuration import get_current_config, get_unique_connection_string
import datetime
from QA.DatabaseTester import DatabaseTester
import logging
import pymysql.cursors

logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)


class ProdDBTester(DatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        database_name = config['PATENTSVIEW_DATABASES']["REPORTING_DATABASE"]
        super().__init__(config, database_name, datetime.date(year=1976, month=1, day=1),end_date)
        self.connection = pymysql.connect(host=config['PROD_DATABASE_SETUP']['HOST'],
                                          user=config['PROD_DATABASE_SETUP']['USERNAME'],
                                          password=config['PROD_DATABASE_SETUP']['PASSWORD'],
                                          db=database_name,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
        self.database_type = "PROD_" + "PatentsView"

    def run_prod_db_tests(self):
        counter = 0
        total_tables = len(self.table_config.keys())
        self.init_qa_dict()
        for table in self.table_config:
            self.load_table_row_count(table, where_vi=False)
            self.check_for_indexes(table)
            self.save_qa_data()
            self.init_qa_dict()
            logger.info(f"FINISHED WITH TABLE: {table}")
            counter += 1
            logger.info(f"Currently Done With {counter} of {total_tables} | {counter/total_tables} %")

def run_prod_db_qa(**kwargs):
    config = get_current_config('granted_patent', schedule="quarterly", **kwargs)
    qc = ProdDBTester(config)
    qc.run_prod_db_tests()


if __name__ == '__main__':
    # check_reporting_db_row_count()
    config = get_current_config('granted_patent', schedule='quarterly', **{"execution_date": datetime.date(2023, 4, 1)})
    qc = ProdDBTester(config)
    qc.run_prod_db_tests()
