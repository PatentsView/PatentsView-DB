from abc import ABC, abstractmethod

from sqlalchemy import create_engine
import pandas as pd

from lib.configuration import get_connection_string


class PatentDatabaseTester(ABC):
    def __init__(self, config, database_section):
        self.database_section = database_section
        self.qa_connection_string = get_connection_string(config, 'QA_DATABASE')
        self.database_connection_string = get_connection_string(config, database_section)
        self.config = config
        self.table_config = {}
        self.qa_data = {"counts": [], 'null_counts': []}
        self.floating_entities = []
        self.floating_patent = []

    def test_table_row_count(self, table_name):
        engine = create_engine(self.database_connection_string)
        count_query = "SELECT count(*) from {tbl}".format(tbl=table_name)
        count_cursor = engine.execute(count_query)
        count_value = count_cursor.fetchall()[0][0]
        engine.dispose()
        if count_value < 1:
            raise Exception("Empty table found:{table}".format(table=table_name))
        database_type, version = self.config["DATABASE"][self.database_section].split("_")
        self.qa_data['counts'].append({"db": database_type, 'table_name': table_name,
                                       'update_version': version, 'count': count_value})

    def test_blank_count(self, table, table_config):
        engine = create_engine(self.database_connection_string)
        for field in table_config:
            if table_config[field]["data_type"] in ['varchar', 'mediumtext', 'text']:
                count_query = "SELECT count(*) from {tbl} where {field} =''".format(tbl=table, field=field)
                count_cursor = engine.execute(count_query)
                count_value = count_cursor.fetchall()[0][0]
                if count_value != 0:
                    raise Exception(
                        "Blanks encountered in  table found:{database}.{table} column {col}. Count: {count}".format(
                            database=self.database_connection_string, table=table,
                            col=field,
                            count=count_value))
        engine.dispose()

    def test_nulls(self, table, table_config):
        engine = create_engine(self.database_connection_string)
        for field in table_config:
            count_query = "SELECT count(*) from {tbl} where {field} is null".format(tbl=table, field=field)
            count_cursor = engine.execute(count_query)
            count_value = count_cursor.fetchall()[0][0]
            if not table_config[field]['null_allowed']:
                if count_value != 0:
                    raise Exception(
                        "NULLs encountered in table found:{database}.{table} column {col}. Count: {count}".format(
                            database=self.database_connection_string, table=table,
                            col=field,
                            count=count_value))
            database_type, version = self.config["DATABASE"][self.database_section].split("_")
            self.qa_data['null_counts'].append({"db": database_type, 'table_name': table, "column": field,
                                                'update_version': version, 'count': count_value})
        engine.dispose()

    def assert_zero_dates(self, table, field):
        engine = create_engine(self.database_connection_string)
        zero_query = "SELECT count(*) from {tbl} where {field} ='0000-00-00'".format(tbl=table, field=field)
        count_cursor = engine.execute(zero_query)
        count_value = count_cursor.fetchall()[0][0]
        engine.dispose()
        if count_value != 0:
            raise Exception(
                "0000-00-00 date encountered in table found:{database}.{table} column {col}. Count: {count}".format(
                    database=self.database_connection_string, table=table, col=field,
                    count=count_value))

    def save_qa_data(self):
        qa_engine = create_engine(self.qa_connection_string)
        for qa_table in self.qa_data:
            qa_table_data = self.qa_data[qa_table]
            table_frame = pd.DataFrame(qa_table_data)
            table_frame.to_sql(name=qa_table, if_exists='append', con=qa_engine, index=False)



    def runTests(self):
        for table in self.table_config:
            self.test_table_row_count(table)
            self.test_blank_count(table, self.table_config[table])
            self.test_nulls(table, self.table_config[table])

            for field in self.table_config[table]:
                if "date_field" in self.table_config[table][field] and self.table_config[table][field]["date_field"]:
                    self.assert_zero_dates(table, field)

        self.save_qa_data()
