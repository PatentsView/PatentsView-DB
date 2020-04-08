from sqlalchemy import create_engine

from lib.configuration import get_connection_string


class RenameTest:
    def __init__(self, config):
        self.empty_tables = ['assignee', 'cpc_current', 'cpc_group', 'cpc_subgroup', 'cpc_subsection', 'inventor',
                             'location', 'location_assignee', 'location_inventor',
                             'patent_assignee', 'patent_inventor', 'patent_lawyer']
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
            count_query = "SELECT count(*) from {tbl}".format(tbl=table_name)
            count_cursor = engine.execute(count_query)
            count_value = count_cursor.fetchall()[0][0]
            if table_name in self.empty_tables:
                assert count_value == 0
            else:
                assert count_value > 0

    def test_database_encoding(self):
        new_database = self.config['DATABASE']["NEW_DB"]
        connection_string = get_connection_string(self.config, database='NEW_DB')
        engine = create_engine(connection_string)

        collation_query = "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME from information_schema.schemata where SCHEMA_NAME='{new_db}'".format(
            new_db=new_database)
        collation_cursor = engine.execute(collation_query)
        cset, collation = collation_cursor.fetchall()[0]
        assert cset == 'utf8mb4'
        assert collation == 'utf8mb4_unicode_ci'

    def test_table_encoding(self):
        new_database = self.config['DATABASE']["NEW_DB"]
        connection_string = get_connection_string(self.config, database='NEW_DB')
        engine = create_engine(connection_string)
        collation_query = "SELECT  TABLE_COLLATION from information_schema.tables where TABLE_SCHEMA='{new_db}'".format(
            new_db=new_database)
        collation_cursor = engine.execute(collation_query)
        for table_collation_row in collation_cursor:
            assert table_collation_row[0] == 'utf8mb4_unicode_ci'

    def test_column_encoding(self):
        new_database = self.config['DATABASE']["NEW_DB"]
        connection_string = get_connection_string(self.config, database='NEW_DB')
        engine = create_engine(connection_string)
        collation_query = '''
                        SELECT character_set_name,
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
            assert column_collation_name[0] == 'utf8mb4'
            assert column_collation_name[1] == 'utf8mb4_unicode_ci'
