from QA.create_databases.RenameTest import DatabaseSetupTest
from lib.configuration import get_today_dict


def qc_database_granted(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    print(config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"])
    print(config['PATENTSVIEW_DATABASES']["PROD_DB"])
    print(config['PATENTSVIEW_DATABASES']["TEXT_DB"])
    qc = DatabaseSetupTest(config).runTests()

def qc_database_pgpubs(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('pgpubs', **kwargs)
    qc = DatabaseSetupTest(config).runTests()


if __name__ == '__main__':
    qc_database_granted(**get_today_dict('granted_patent'))
