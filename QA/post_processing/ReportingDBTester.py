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


def run_reporting_db_qa(**kwargs):
    config = get_current_config('granted_patent', schedule="quarterly", **kwargs)
    qc = ReportingDBTester(config)
    qc.runReportingTests()


if __name__ == '__main__':
    config = get_current_config('granted_patent', schedule='quarterly', **{"execution_date": datetime.date(2023, 1, 1)})
    qc = ReportingDBTester(config)
    qc.runReportingTests()
