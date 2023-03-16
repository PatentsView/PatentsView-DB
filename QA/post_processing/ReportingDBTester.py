from lib.download_check_delete_databases import query_for_all_tables_in_db, get_count_for_all_tables
from lib.configuration import get_current_config, get_unique_connection_string
import datetime
from QA.DatabaseTester import DatabaseTester


class ReportingDBTester(DatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["REPORTING_DATABASE"], datetime.date(year=1976, month=1, day=1),end_date)


# def check_reporting_db_row_count():
#     config = get_current_config('patent', **{"execution_date": datetime.date(2022, 1, 1)})
#     reporting_db = config['PATENTSVIEW_DATABASES']['REPORTING_DATABASE']
#     connection_string = get_unique_connection_string(config, database=reporting_db, connection='DATABASE_SETUP')
#     table_list = query_for_all_tables_in_db(connection_string, reporting_db)
#     get_count_for_all_tables(connection_string, table_list, raise_exception=True)

def run_reporting_db_qa():
    config = get_current_config('granted_patent', **{
                    "execution_date": datetime.date(2022, 6, 30)
                                })
    qc = ReportingDBTester(config)
    qc.runTests()


if __name__ == '__main__':
    # check_reporting_db_row_count()
    config = get_current_config('granted_patent', **{"execution_date": datetime.date(2022, 6, 30)})
    qc = ReportingDBTester(config)
    qc.runTests()
