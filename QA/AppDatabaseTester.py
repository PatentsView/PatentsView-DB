from abc import ABC, abstractmethod
import time
from sqlalchemy import create_engine
import pandas as pd
import pymysql.cursors

from lib.configuration import get_connection_string


class AppDatabaseTester(ABC):
    def __init__(self, config, database_section, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.database_section = database_section
        self.qa_connection_string = get_connection_string(config, 'QA_DATABASE')
        self.connection = pymysql.connect(host=config['DATABASE']['HOST'],
                                          user=config['DATABASE']['USERNAME'],
                                          password=config['DATABASE']['PASSWORD'],
                                          db=config['DATABASE'][database_section],
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
        # self.database_connection_string = get_connection_string(config, database_section)
        self.config = config
        self.table_config = {}
        self.qa_data = {"DataMonitor_count": [], 'DataMonitor_nullcount': [], 'DataMonitor_patentyearlycount': []}
        self.floating_entities = []
        self.floating_application = []
application
    def test_table_row_count(self, table_name):
        try:
            if not self.connection.open:
                self.connection.connect()
            count_query = "SELECT count(*) as table_count from {tbl}".format(tbl=table_name)
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(count_query)
                count_value = count_cursor.fetchall()[0][0]
                if count_value < 1:
                    raise Exception("Empty table found:{table}".format(table=table_name))
                database_type, version = self.config["DATABASE"][self.database_section].split("_")
                self.qa_data['DataMonitor_count'].append(
                    {"database_type": database_type, 'table_name': table_name, 'update_version': version,
                     'table_row_count': count_value})
        finally:
            if self.connection.open:
                self.connection.close()

    def test_blank_count(self, table, table_config):
        for field in table_config:
            if table_config[field]["data_type"] in ['varchar', 'mediumtext', 'text']:
                try:
                    if not self.connection.open:
                        self.connection.connect()
                    count_query = "SELECT count(*) as blank_count from {tbl} where {field} =''".format(tbl=table,
                                                                                                       field=field)
                    with self.connection.cursor() as count_cursor:
                        count_cursor.execute(count_query)
                        count_value = count_cursor.fetchall()[0][0]
                        if count_value != 0:
                            raise Exception(
                                "Blanks encountered in  table found:{database}.{table} column {col}. Count: {count}".format(
                                    database=self.config['DATABASE'][self.database_section], table=table,
                                    col=field,
                                    count=count_value))
                finally:

                    if self.connection.open:
                        self.connection.close()

    def test_nulls(self, table, table_config):
        for field in table_config:
            try:
                if not self.connection.open:
                    self.connection.connect()
                count_query = "SELECT count(*) as null_count from {tbl} where {field} is null".format(tbl=table,
                                                                                                      field=field)
                with self.connection.cursor() as count_cursor:
                    count_cursor.execute(count_query)
                    count_value = count_cursor.fetchall()[0][0]
                    if not table_config[field]['null_allowed']:
                        if count_value != 0:
                            raise Exception(
                                "NULLs encountered in table found:{database}.{table} column {col}. Count: {count}".format(
                                    database=self.database_section, table=table,
                                    col=field,
                                    count=count_value))
                    database_type, version = self.config["DATABASE"][self.database_section].split("_")
                    self.qa_data['DataMonitor_nullcount'].append(
                        {"database_type": database_type, 'table_name': table, "column_name": field,
                         'update_version': version, 'null_count': count_value})
            finally:
                if self.connection.open:
                    self.connection.close()

    def assert_zero_dates(self, table, field):
        try:
            if not self.connection.open:
                self.connection.connect()
            zero_query = "SELECT count(*) zero_count from {tbl} where {field} ='0000-00-00'".format(tbl=table,
                                                                                                    field=field)
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(zero_query)
                count_value = count_cursor.fetchall()[0][0]
                if count_value != 0:
                    raise Exception(
                        "0000-00-00 date encountered in table found:{database}.{table} column {col}. Count: {count}".format(
                            database=self.database_section, table=table, col=field,
                            count=count_value))
        finally:
            if self.connection.open:
                self.connection.close()

    def test_yearly_count(self):
        try:
            if not self.connection.open:
                self.connection.connect()
            count_query = "SELECT year(`date`) as `yr`, count(1) as `year_count` from publication group by year(`date`)"
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(count_query)
                database_type, version = self.config["DATABASE"][self.database_section].split("_")
                for count_row in count_cursor.fetchall():
                    self.qa_data['DataMonitor_patentyearlycount'].append(
                        {"database_type": database_type,
                         'update_version': version, 'year': count_row[0],
                         'application_count': count_row[1]})
        finally:
            print(self.qa_data['DataMonitor_patentyearlycount'])
            if self.connection.open:
                self.connection.close()

        self.assert_yearly_counts()

    def assert_yearly_counts(self):
        for year in range(self.start_date.year, self.end_date.year + 1):
            found = False
            for row in self.qa_data['DataMonitor_patentyearlycount']:
                if row['year'] == year:
                    found = True
                    if row['application_count'] < 1:
                        raise Exception("Year {yr} has 0 applications in the database {db}".format(yr=year, db=
                        self.config['DATABASE'][self.database_section]))
            if not found:
                raise Exception("There are no applications for the Year {yr} in the database {db}".format(yr=year, db=
                self.config['DATABASE'][self.database_section]))

    def save_qa_data(self):
        qa_engine = create_engine(self.qa_connection_string)
        for qa_table in self.qa_data:
            qa_table_data = self.qa_data[qa_table]
            table_frame = pd.DataFrame(qa_table_data)
            table_frame.to_sql(name=qa_table, if_exists='append', con=qa_engine, index=False)

    def runTests(self):
        self.test_yearly_count()
        for table in self.table_config:
            self.test_table_row_count(table)
            self.test_blank_count(table, self.table_config[table])
            self.test_nulls(table, self.table_config[table])

            for field in self.table_config[table]:
                if "date_field" in self.table_config[table][field] and self.table_config[table][field]["date_field"]:
                    self.assert_zero_dates(table, field)

        self.save_qa_data()
