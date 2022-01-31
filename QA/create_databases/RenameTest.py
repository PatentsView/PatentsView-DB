import json
import datetime
import os
from sqlalchemy import create_engine

from lib.configuration import get_connection_string, get_current_config


class DatabaseSetupTest:
    def __init__(self, config):
        self.project_home = os.environ['PACKAGE_HOME']
        self.raw_database = config['PATENTSVIEW_DATABASES']["PROD_DB"]
        if self.raw_database == 'patent':
            resources_file = "{root}/{resources}/raw_db_tables.json".format(root=self.project_home,
                                                                            resources=config["FOLDERS"]["resources_folder"])
        else:
            resources_file = "{root}/{resources}/pregrant_db_tables.json".format(root=self.project_home,
                                                                            resources=config["FOLDERS"]["resources_folder"])
        raw_db_table_settings = json.load(open(resources_file))
        self.required_tables = {x: False for x in raw_db_table_settings["table_list"].keys()}
        self.empty_tables = [x for x in raw_db_table_settings["table_list"] if
                             not raw_db_table_settings["table_list"][x]["raw_data"]]
        self.config = config


    def runTests(self):
        # self.test_database_encoding()
        # self.test_table_encoding()
        # self.test_column_encoding()
        self.test_table_count()
        # self.test_all_tables()
        # self.test_tmp_tables()

    def test_table_count(self):
        print("Checking database encoding for {db}".format(db=self.raw_database))
        connection_string = get_connection_string(self.config, database="PROD_DB")
        engine = create_engine(connection_string)
        table_query = f"SELECT  TABLE_NAME from information_schema.tables where TABLE_SCHEMA='{self.raw_database}'"
        table_cursor = engine.execute(table_query)
        for table_name in table_cursor:
            # print(table_name[0])
            if table_name[0] in self.required_tables:
                self.required_tables[table_name[0]] = True
                count_query = f"SELECT count(*) from {table_name[0]}"
                count_cursor = engine.execute(count_query)
                count_value = count_cursor.fetchall()[0][0]
                if not count_value > 0 and table_name[0] not in self.empty_tables:
                   raise AssertionError(f"Table {table_name} should not be empty")

    def test_all_tables(self):
        missing = [x for x in self.required_tables if not self.required_tables[x]]
        if len(missing) > 0:
            raise AssertionError("Required tables are missing: {table_list}".format(table_list=", ".join(missing)))

    def test_tmp_tables(self):
        print("Checking database for temporary tables for {db}".format(db=self.raw_database))
        connection_string = get_connection_string(self.config, database="PROD_DB")
        engine = create_engine(connection_string)
        table_query = f"SELECT  count(*) from information_schema.tables where TABLE_SCHEMA='{self.raw_database}' and (TABLE_NAME like 'tmp%%' or TABLE_NAME like 'temp%%' or TABLE_NAME like '\_%%')"
        table_cursor = engine.execute(table_query)
        table_count = table_cursor.fetchall()[0][0]
        if table_count > 0:
            print(table_query)
            raise AssertionError("There are {x} temporary tables in the database".format(x=table_count))

    def test_database_encoding(self):
        print("Checking database encoding for {db}".format(db=self.raw_database))
        connection_string = get_connection_string(self.config, database="PROD_DB")
        engine = create_engine(connection_string)

        collation_query = f"SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME from information_schema.schemata where SCHEMA_NAME='{self.raw_database}'"
        collation_cursor = engine.execute(collation_query)
        cset, collation = collation_cursor.fetchall()[0]
        if cset != 'utf8mb4':
            raise AssertionError("Database character set should be utf8mb4 instead found {cset}".format(cset=cset))
        if collation != 'utf8mb4_unicode_ci':
            raise AssertionError(
                    "Database collation should be utf8mb4_unicode_ci instead found {cset}".format(cset=collation))

    def test_table_encoding(self):
        print("Checking table encoding for {db}".format(db=self.raw_database))
        connection_string = get_connection_string(self.config, database="PROD_DB")
        engine = create_engine(connection_string)
        collation_query_table = f"SELECT  TABLE_NAME, TABLE_COLLATION from information_schema.tables where TABLE_SCHEMA='{self.raw_database}'"
        # VIEW COLLATIONS ARE utf8mb4_general_ci but no way to fix on the VIEW level
        # collation_query_view = f"SELECT  TABLE_NAME, COLLATION_CONNECTION from information_schema.views where TABLE_SCHEMA='{self.raw_database}'"
        # for i in [collation_query_table, collation_query_view]:
        for i in [collation_query_table]:
            collation_cursor = engine.execute(i)
            for table_collation_row in collation_cursor:
                print(f"\tChecking Table {table_collation_row[0]}")
                if table_collation_row[1] != 'utf8mb4_unicode_ci' and table_collation_row[1] is not None:
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
                        WHERE  table_schema='{self.raw_database}'
                        AND    data_type IN ('varchar', 'longtext', 'mediumtext', 'text', 'enum', 'char', 'set')
        '''
        collation_cursor = engine.execute(collation_query)
        for column_collation_name in collation_cursor:
            print("Checking encoding for column {cl} in table: {tbl}".format(cl=column_collation_name[1],
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
    # config = get_current_config('granted_patent', **{
    #     "execution_date": datetime.date(2021, 10, 5)
    # })
    config = get_current_config('pgpubs', **{
        "execution_date": datetime.date(2021, 12, 2)
    })
    print(config['PATENTSVIEW_DATABASES']["TEMP_UPLOAD_DB"])
    print(config['PATENTSVIEW_DATABASES']["PROD_DB"])
    print(config['PATENTSVIEW_DATABASES']["TEXT"])
    DBSU = DatabaseSetupTest(config)
    # DBSU.test_table_count()
    DBSU.runTests()