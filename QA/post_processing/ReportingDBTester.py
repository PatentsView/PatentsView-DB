from lib.download_check_delete_databases import query_for_all_tables_in_db, get_count_for_all_tables
from lib.configuration import get_current_config, get_unique_connection_string, get_connection_string
import datetime
from QA.DatabaseTester import DatabaseTester
import logging
from sqlalchemy import create_engine
import pandas as pd

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
            # self.load_nulls(table, self.table_config[table], where_vi=False)
            # self.test_blank_count(table, self.table_config[table], where_vi=False)
            self.save_qa_data()
            self.init_qa_dict()
            logger.info(f"FINISHED WITH TABLE: {table}")
            counter += 1
            logger.info(f"Currently Done With {counter} of {total_tables} | {counter/total_tables} %")

    #write new function here
    def datamonitor_table_scatterplot(self, config):
        qa_connection_string = get_connection_string(config, database='QA_DATABASE',
                                                          connection='APP_DATABASE_SETUP')
        qa_engine = create_engine(self.qa_connection_string)
        e_date = config["DATES"]["END_DATE"]
        s_date = config["DATES"]["START_DATE"]
        query = f"""
SELECT * 
FROM patent_QA.DataMonitor_count
WHERE database_type = 'PatentsView'
AND update_version = '{e_date}';
        """
        print(query)
        data_1 = pd.read_sql_query(query, qa_engine)
        print(data_1)

        query = f"""
        SELECT * 
        FROM patent_QA.DataMonitor_count
        WHERE database_type = 'PatentsView'
        AND update_version = '{e_date}';
                """
        print(query)
        data_2 = pd.read_sql_query(query, qa_engine)
        print(data_2)

        #         self.qa_data['DataMonitor_regioncount'].append(
        #             {
        #                 "database_type": self.database_type,
        #                 "update_version": self.version,
        #                 'table_name': table_name,
        #                 "region_type": region_type,
        #                 'region': top_n_data_row[0],
        #                 'count': top_n_data_row[1],
        #                 'quarter': self.quarter
        #             })


def run_reporting_db_qa(**kwargs):
    config = get_current_config('granted_patent', schedule="quarterly", **kwargs)
    qc = ReportingDBTester(config)
    qc.run_reporting_db_tests()


if __name__ == '__main__':
    # check_reporting_db_row_count()
    config = get_current_config('granted_patent', schedule='quarterly', **{"execution_date": datetime.date(2023, 4, 1)})
    qc = ReportingDBTester(config)
    qc.datamonitor_table_scatterplot(config)
    #qc.run_reporting_db_tests() #call name of new function instead of all functions. table will be created in datamonitor
