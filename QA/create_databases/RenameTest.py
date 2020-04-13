from sqlalchemy import create_engine

from lib.configuration import get_connection_string


class RenameTest:
    def __init__(self, config):
        self.empty_tables = ['assignee', 'cpc_current', 'cpc_group', 'cpc_subgroup', 'cpc_subsection', 'claim',
                             'brf_sum_text', 'detail_desc_text', 'draw_desc_text','inventor', 'location',
                             'location_assignee', 'location_inventor', 'patent_assignee', 'patent_inventor',
                             'patent_lawyer']
        self.config = config
        pass

    def runTests(self):
        self.test_database_encoding()
        self.test_table_encoding()
        self.test_column_encoding()
        self.test_table_count()

    def test_table_count(self):
        new_database = self.config['DATABASE']["NEW_DB"]
        connection_string = get_connection_string(self.config, database='NEW_DB')
        engine = create_engine(connection_string)
        table_query = "SELECT  TABLE_NAME from information_schema.tables where TABLE_SCHEMA='{new_db}'".format(
            new_db=new_database)
        table_cursor = engine.execute(table_query)
        for table_name in table_cursor:
            count_query = "SELECT count(*) from {tbl}".format(tbl=table_name[0])
            count_cursor = engine.execute(count_query)
            count_value = count_cursor.fetchall()[0][0]
            if table_name[0] in self.empty_tables:
                if count_value != 0:
                    raise AssertionError("Table {table_name} should be empty".format(table_name=table_name))
            else:
                if not count_value > 0:
                    raise AssertionError("Table {table_name} should not be empty".format(table_name=table_name))

    def test_database_encoding(self):
        new_database = self.config['DATABASE']["NEW_DB"]
        connection_string = get_connection_string(self.config, database='NEW_DB')
        engine = create_engine(connection_string)

        collation_query = "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME from information_schema.schemata where SCHEMA_NAME='{new_db}'".format(
            new_db=new_database)
        collation_cursor = engine.execute(collation_query)
        cset, collation = collation_cursor.fetchall()[0]
        if cset != 'utf8mb4':
            raise AssertionError("Database character set should be utf8mb4 instead found {cset}".format(cset=cset))
        if collation != 'utf8mb4_unicode_ci':
            raise AssertionError(
                "Database collationshould be utf8mb4_unicode_ci instead found {collation}".format(cset=collation))

    def test_table_encoding(self):
        new_database = self.config['DATABASE']["NEW_DB"]
        connection_string = get_connection_string(self.config, database='NEW_DB')
        engine = create_engine(connection_string)
        collation_query = "SELECT  TABLE_NAME, TABLE_COLLATION from information_schema.tables where TABLE_SCHEMA='{new_db}'".format(
            new_db=new_database)
        collation_cursor = engine.execute(collation_query)
        for table_collation_row in collation_cursor:
            if table_collation_row[1] != 'utf8mb4_unicode_ci':
                raise AssertionError(
                    "Table  collation should be utf8mb4_unicode_ci instead found {collation} for table {tbl}".format(
                        cset=table_collation_row[1], tbl=table_collation_row[0]))

    def test_column_encoding(self):
        new_database = self.config['DATABASE']["NEW_DB"]
        connection_string = get_connection_string(self.config, database='NEW_DB')
        engine = create_engine(connection_string)
        collation_query = '''
                        SELECT TABLE_NAME, COLUMN_NAME, character_set_name,
                            collation_name
                        FROM   information_schema.columns
                        WHERE  table_schema='{new_db}'
                        AND    data_type IN ('varchar',
                                            'longtext',
                                            'mediumtext',
                                            'text',
                                            'enum',
                                            'char',
                                            'set')
        '''.format(
            new_db=new_database)
        collation_cursor = engine.execute(collation_query)
        for column_collation_name in collation_cursor:
            if column_collation_name[2] != 'utf8mb4':
                raise AssertionError(
                    "Table character set should be utf8mb4 instead found {cset} for table {tbl}. column name {col}".format(
                        cset=column_collation_name[2], tbl=column_collation_name[0], col=column_collation_name[1]))
            if column_collation_name[3] != 'utf8mb4_unicode_ci':
                raise AssertionError(
                    "Table  collation should be utf8mb4_unicode_ci instead found {collation} for table {tbl}".format(
                        collation=column_collation_name[3], tbl=column_collation_name[0], col=column_collation_name[1]))
