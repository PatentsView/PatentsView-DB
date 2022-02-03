from QA.create_databases.RenameTest import DatabaseSetupTest
from lib.configuration import get_current_config, get_today_dict
import datetime

def qc_database_granted(**kwargs):
    config = get_current_config('granted_patent', **kwargs)
    print(config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"])
    print(config['PATENTSVIEW_DATABASES']["PROD_DB"])
    print(config['PATENTSVIEW_DATABASES']["TEXT_DB"])
    qc = DatabaseSetupTest(config).runTests()

def qc_database_pgpubs(**kwargs):
    config = get_current_config('pgpubs', **kwargs)
    qc = DatabaseSetupTest(config).runTests()


if __name__ == '__main__':
    # qc_database_granted(**get_today_dict('granted_patent'))
    qc_database_granted(**{
        "execution_date": datetime.date(2021, 12, 7)
    })