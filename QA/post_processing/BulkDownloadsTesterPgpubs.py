# from lib.download_check_delete_databases import query_for_all_tables_in_db, get_count_for_all_tables
from lib.configuration import get_current_config#, get_unique_connection_string
import datetime
from QA.DatabaseTester import DatabaseTester


class BulkDownloadsTesterPgpubs(DatabaseTester):
    def __init__(self, config):
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, config['PATENTSVIEW_DATABASES']["bulk_export_pgpubs"], datetime.date(year=1976, month=1, day=1),end_date)

def run_bulk_downloads_qa(**kwargs):
    config = get_current_config('pgpubs', schedule='quarterly', **kwargs)
    qc = BulkDownloadsTesterPgpubs(config)
    qc.runStandardTests()

if __name__ == '__main__':
    # check_reporting_db_row_count()
    config = get_current_config('pgpubs', **{"execution_date": datetime.date(2022, 6, 30)})
    qc = BulkDownloadsTesterPgpubs(config)
    qc.runTests()
