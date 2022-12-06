import json
import datetime
import os
from sqlalchemy import create_engine

from lib.configuration import get_connection_string, get_current_config
from lib import utilities


class DatabaseSetupTest:
    def __init__(self, config, database):
        self.config = config
        self.database_for_tests = database
        print(self.database_for_tests)
        if self.database_for_tests == 'patent' or self.database_for_tests[:6] == 'upload':
            resources_file = "{root}/{resources}/table_config_granted.json".format(root=config["FOLDERS"]["project_root"],
                                                                                   resources=self.config["FOLDERS"][
                                                                                       "resources_folder"])
        else:
            resources_file = "{root}/{resources}/table_config_pgpubs.json".format(root=config["FOLDERS"]["project_root"],
                                                                                  resources=self.config["FOLDERS"][
                                                                                      "resources_folder"])
        raw_db_table_settings = json.load(open(resources_file))
        utilities.class_db_specific_config(self, raw_db_table_settings, "UploadTest")
        self.required_tables = self.table_config.keys()

    def runTests(self):
        self.test_database_encoding()
        self.test_table_encoding()
        self.test_column_encoding()
        self.test_table_exists()
        self.test_tmp_tables()

    def test_table_exists(self):
        print(f"Checking Table Counts for in database:  {self.database_for_tests}")
        connection_string = get_connection_string(self.config, database="PROD_DB")
        engine = create_engine(connection_string)
        for table in self.required_tables:
            table_exists_query = f"SELECT count(*) from {self.database_for_tests}.{table}"
            table_cursor = engine.execute(table_exists_query)
            count_value = table_cursor.fetchall()[0][0]
            print(f"There are currently {count_value} rows in the {table} table")

    def test_tmp_tables(self):
        print("Checking database for temporary tables for {db}".format(db=self.database_for_tests))
        connection_string = get_connection_string(self.config, database="PROD_DB")
        engine = create_engine(connection_string)
        table_query = f"SELECT  count(*) from information_schema.tables where TABLE_SCHEMA='{self.database_for_tests}' and (TABLE_NAME like 'tmp%%' or TABLE_NAME like 'temp%%' or TABLE_NAME like '\_%%') and (TABLE_NAME not in ('temp_mainclass','temp_subclass'))"
        table_cursor = engine.execute(table_query)
        table_count = table_cursor.fetchall()[0][0]
        if table_count > 0:
            print(table_query)
            raise AssertionError("There are {x} temporary tables in the database".format(x=table_count))

    def test_database_encoding(self):
        print("Checking database encoding for {db}".format(db=self.database_for_tests))
        connection_string = get_connection_string(self.config, database="PROD_DB")
        engine = create_engine(connection_string)

        collation_query = f"SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME from information_schema.schemata where SCHEMA_NAME='{self.database_for_tests}'"
        collation_cursor = engine.execute(collation_query)
        cset, collation = collation_cursor.fetchall()[0]
        if cset != 'utf8mb4':
            raise AssertionError("Database character set should be utf8mb4 instead found {cset}".format(cset=cset))
        if collation != 'utf8mb4_unicode_ci':
            raise AssertionError(
                "Database collation should be utf8mb4_unicode_ci instead found {cset}".format(cset=collation))

    def test_table_encoding(self):
        print("Checking table encoding for {db}".format(db=self.database_for_tests))
        connection_string = get_connection_string(self.config, database="PROD_DB")
        engine = create_engine(connection_string)
        collation_query_table = f"SELECT  TABLE_NAME, TABLE_COLLATION from information_schema.tables where TABLE_SCHEMA='{self.database_for_tests}' and TABLE_COLLATION is not null"
        # VIEW COLLATIONS ARE utf8mb4_general_ci but no way to fix on the VIEW level
        # collation_query_view = f"SELECT  TABLE_NAME, COLLATION_CONNECTION from information_schema.views where TABLE_SCHEMA='{self.database_for_tests}'"
        # for i in [collation_query_table, collation_query_view]:
        for i in [collation_query_table]:
            collation_cursor = engine.execute(i)
            for table_collation_row in collation_cursor:
                print(f"\tChecking Table {table_collation_row[0]}")
                if table_collation_row[1] != 'utf8mb4_unicode_ci':
                    raise AssertionError(
                        "Table  collation should be utf8mb4_unicode_ci instead found {collation} for table {tbl}".format(
                            collation=table_collation_row[1], tbl=table_collation_row[0]))

    def test_column_encoding(self):
        connection_string = get_connection_string(self.config, database="PROD_DB")
        engine = create_engine(connection_string)
        collation_query = f'''
                        SELECT TABLE_NAME, COLUMN_NAME, character_set_name,
                            collation_name
                        FROM   information_schema.columns
                        WHERE  table_schema='{self.database_for_tests}'
                        AND    data_type IN ('varchar', 'longtext', 'mediumtext', 'text', 'enum', 'char', 'set')
        '''
        collation_cursor = engine.execute(collation_query)
        for column_collation_name in collation_cursor:
            print("Checking column encoding for {tbl}.{cl}".format(cl=column_collation_name[1],
                                                                             tbl=column_collation_name[0]))
            if column_collation_name[2] != 'utf8mb4':
                raise AssertionError(
                    "Table character set should be utf8mb4 instead found {cset} for table {tbl}. column name {col}".format(
                        cset=column_collation_name[2], tbl=column_collation_name[0],
                        col=column_collation_name[1]))
            if column_collation_name[3] != 'utf8mb4_unicode_ci':
                raise AssertionError(
                    "Table  collation should be utf8mb4_unicode_ci instead found {collation} for table {tbl}".format(
                        collation=column_collation_name[3], tbl=column_collation_name[0],
                        col=column_collation_name[1]))


if __name__ == '__main__':
    # pgpubs, granted_patent
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2021, 10, 19)
    })
    # config = get_current_config('pgpubs', **{
    #     "execution_date": datetime.date(2021, 12, 2)
    # })
    print(config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"])
    print(config['PATENTSVIEW_DATABASES']["PROD_DB"])
    print(config['PATENTSVIEW_DATABASES']["TEXT_DB"])
    # DBSU = DatabaseSetupTest(config, 'upload_20211026')
    DBSU = DatabaseSetupTest(config, 'pgpubs_20211216')
    DBSU.runTests()
