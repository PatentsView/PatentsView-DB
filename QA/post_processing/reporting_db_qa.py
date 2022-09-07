from lib.download_check_delete_databases import query_for_all_tables_in_db, get_count_for_all_tables
from lib.configuration import get_current_config, get_unique_connection_string

import datetime


def check_reporting_db_row_count():
    config = get_current_config('patent', **{"execution_date": datetime.date(2022, 1, 1)})
    reporting_db = config['PATENTSVIEW_DATABASES']['REPORTING_DATABASE']
    connection_string = get_unique_connection_string(config, database=reporting_db, connection='DATABASE_SETUP')
    table_list = query_for_all_tables_in_db(connection_string, reporting_db)
    get_count_for_all_tables(connection_string, table_list, raise_exception=True)


if __name__ == '__main__':
    check_reporting_db_row_count()