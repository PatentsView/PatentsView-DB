from QA.create_databases.RenameTest import DatabaseSetupTest
from lib.configuration import get_today_dict


def qc_database(**kwargs):
    from lib.configuration import get_current_config
    config = get_current_config('granted_patent', **kwargs)
    qc = DatabaseSetupTest(config).runTests()


if __name__ == '__main__':
    qc_database(**get_today_dict('granted_patent'))
