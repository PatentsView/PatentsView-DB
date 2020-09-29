from sqlalchemy import create_engine

from QA.create_databases.RenameTest import RenameTest
from lib.configuration import get_connection_string


def create_new_database(config):
    new_database = config["DATABASE"]["NEW_DB"]
    connection_string = get_connection_string(config, "OLD_DB")
    engine = create_engine(connection_string)
    connection = engine.connect()
    connection.execute(
        'create schema {}  default character set=utf8mb4 default collate=utf8mb4_unicode_ci'.format(new_database))
    connection.close()


def rename_old_db(update_config):
    old_database = update_config["DATABASE"]["OLD_DB"]
    new_database = update_config["DATABASE"]["NEW_DB"]
    connection_string = get_connection_string(update_config, "OLD_DB")
    engine = create_engine(connection_string)
    tables = [t[0] for t in engine.execute('show tables from {}'.format(old_database)) if not t[0].startswith('temp')]
    tables_to_truncate = ['inventor_disambiguation_mapping', 'assignee_disambiguation_mapping', 'assignee',
                          'cpc_current', 'cpc_group', 'cpc_subgroup', 'cpc_subsection', 'inventor', 'location',
                          'location_assignee', 'location_inventor', 'patent_assignee', 'patent_inventor',
                          'patent_lawyer']

    for table in tables:
        con = engine.connect()
        con.execute('alter table {0}.{2} rename {1}.{2}'.format(old_database, new_database, table))
        if table in tables_to_truncate:
            con.execute('truncate table {}.{}'.format(new_database, table))
        con.close()


def begin_rename(config):
    create_new_database(config)
    rename_old_db(config)


def post_rename(config):
    qc = RenameTest(config)
    qc.runTests()
