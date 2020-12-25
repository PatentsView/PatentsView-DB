from abc import ABC

import pandas as pd
import pymysql.cursors
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from lib.configuration import get_connection_string


class PatentDatabaseTester(ABC):
    def __init__(self, config, database_section, start_date, end_date):
        """
        Do not instantiate. Overriden class constructor
        :param config: Configparser object containing update parameters
        :param database_section: Section in config that indicates database to use. RAW_DB or TEMP_UPLOAD_DB
        :param start_date: Database Update start date
        :param end_date: Database Update end date
        """
        # Tables that do not directly link to patent table
        self.patent_exclusion_list = ['mainclass', 'mainclass_current', 'subclass', 'subclass_current', 'patent',
                                      'rawlocation']
        # Update start and end date
        self.start_date = start_date
        self.end_date = end_date
        # Indicator for Upload/Patents database
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
        # Place Holder for saving QA counts - keys map to table names in patent_QA
        self.qa_data = {
                "DataMonitor_count":               [],
                'DataMonitor_nullcount':           [],
                'DataMonitor_patentyearlycount':   [],
                'DataMonitor_categorycount':       [],
                'DataMonitor_floatingpatentcount': [],
                'DataMonitor_maxtextlength':       [],
                'DataMonitor_prefixedentitycount': []
                }

        database_type = self.config["DATABASE"][self.database_section].split("_")[0]
        self.version = self.end_date.strftime("%Y%m%d")
        self.database_type = database_type

        # Following variables are overridden by inherited classes
        # Dict of tables involved on QC checks
        self.table_config = {}
        # Prefix indicates database where patent table is available
        # Current databaseif prefix is None
        # Used for Text databases
        self.patent_db_prefix = None

    def test_table_row_count(self, table_name):
        """
        Test and collect row counts. Raises exception if any of the tables are empty
        :param table_name: table name to collect row count for
        """
        print("\tTesting Table counts for {table_name} in {db}".format(table_name=table_name,
                                                                       db=self.config["DATABASE"][
                                                                           self.database_section]))
        try:
            if not self.connection.open:
                self.connection.connect()
            count_query = """
SELECT count(*) as table_count
from {tbl}
            """.format(tbl=table_name)
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(count_query)
                count_value = count_cursor.fetchall()[0][0]
                # if count_value < 1:
                #     raise Exception("Empty table found:{table}".format(table=table_name))

                write_table_name = table_name
                prefix = "temp_"
                if table_name.startswith(prefix):
                    write_table_name = table_name[len(prefix):]
                self.qa_data['DataMonitor_count'].append(
                        {
                                "database_type":   self.database_type,
                                'table_name':      write_table_name,
                                'update_version':  self.version,
                                'table_row_count': count_value
                                })
        finally:
            if self.connection.open:
                self.connection.close()

    def test_blank_count(self, table, table_config):
        """
        Verify there are no blank values in any of the text columns
        :param table: Table to verify
        :param table_config: Column configuration for table
        """
        print("\tTesting blanks for {table_name}".format(table_name=table))
        for field in table_config["fields"]:
            if table_config["fields"][field]["data_type"] in ['varchar', 'mediumtext', 'text']:
                try:
                    if not self.connection.open:
                        self.connection.connect()
                    count_query = """
SELECT count(*) as blank_count
from `{tbl}`
where `{field}` = ''
                    """.format(tbl=table, field=field)
                    with self.connection.cursor() as count_cursor:
                        count_cursor.execute(count_query)
                        count_value = count_cursor.fetchall()[0][0]
                        if count_value != 0:
                            exception_message = """
Blanks encountered in  table found:{database}.{table} column {col}. Count: {count}
                            """.format(database=self.config['DATABASE'][self.database_section],
                                       table=table, col=field,
                                       count=count_value)
                            raise Exception(exception_message)
                finally:
                    if self.connection.open:
                        self.connection.close()

    def test_null_byte(self, table, field):
        """
        Verify there are no null bytes in the table fields
        :param table: Table to verify
        :param field: Field to verify
        """
        print("\t\tTesting for NUL bytes for {field} in {table_name}".format(field=field, table_name=table))
        try:
            if not self.connection.open:
                self.connection.connect()
            nul_byte_query = """
SELECT count(*) as count
from `{tbl}`
where INSTR(`{field}`, CHAR(0x00)) > 0
            """.format(
                    tbl=table,
                    field=field)
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(nul_byte_query)
                nul_byte_count = count_cursor.fetchall()[0][0]
                if nul_byte_count > 1:
                    exception_message = """
{count} rows with NUL Byte found in {field} of {table_name} for {db}                    
                    """.format(count=nul_byte_count, field=field, table_name=table,
                               db=self.config["DATABASE"][self.database_section])
                    raise Exception(exception_message)
        finally:
            if self.connection.open:
                self.connection.close()

    def load_category_counts(self, table, field):
        """
        Load counts of rows by each value for a given "categorical variable" field
        :param table: Table to gather counts for
        :param field: Categorical variable field
        """
        print("\t\tLoading category counts for {field} in {table_name}".format(field=field, table_name=table))
        try:
            if not self.connection.open:
                self.connection.connect()
            category_count_query = """
SELECT `{field}` as value, count(*) as count
from `{tbl}`
group by `{field}`
            """.format(tbl=table, field=field)
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(category_count_query)
                for count_row in count_cursor:
                    value = count_row[0]
                    if value is None:
                        value = 'NULL'
                    write_table_name = table
                    prefix = "temp_"
                    if table.startswith(prefix):
                        write_table_name = table[len(prefix):]
                    self.qa_data['DataMonitor_categorycount'].append(
                            {
                                    "database_type":  self.database_type,
                                    'table_name':     write_table_name,
                                    "column_name":    field,
                                    'update_version': self.version,
                                    'value':          value,
                                    'count':          count_row[1]
                                    })
        finally:
            if self.connection.open:
                self.connection.close()

    def test_nulls(self, table, table_config):
        """
        Test and collect count NULL values in different fields
        :param table:
        :param table_config:
        """
        print("\tTesting NULLs for {table_name}".format(table_name=table))
        for field in table_config["fields"]:
            try:
                if not self.connection.open:
                    self.connection.connect()
                count_query = "SELECT count(*) as null_count from `{tbl}` where `{field}` is null".format(tbl=table,
                                                                                                          field=field)
                with self.connection.cursor() as count_cursor:
                    count_cursor.execute(count_query)
                    count_value = count_cursor.fetchall()[0][0]
                    if not table_config["fields"][field]['null_allowed']:
                        if count_value != 0:
                            raise Exception(
                                    "NULLs encountered in table found:{database}.{table} column {col}. Count: {count}".format(
                                            database=self.database_section, table=table,
                                            col=field,
                                            count=count_value))
                    write_table_name = table
                    prefix = "temp_"
                    if table.startswith(prefix):
                        write_table_name = table[len(prefix):]
                    self.qa_data['DataMonitor_nullcount'].append(
                            {
                                    "database_type":  self.database_type,
                                    'table_name':     write_table_name,
                                    "column_name":    field,
                                    'update_version': self.version,
                                    'null_count':     count_value
                                    })
            finally:
                if self.connection.open:
                    self.connection.close()

    def assert_zero_dates(self, table, field):
        print("\t\tTesting 00-00-0000 for {field} in {table_name}".format(field=field, table_name=table))
        try:
            if not self.connection.open:
                self.connection.connect()
            zero_query = "SELECT count(*) zero_count from `{tbl}` where `{field}` ='0000-00-00'".format(tbl=table,
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

    def test_yearly_count(self, table_name, strict=True):
        if table_name not in self.patent_exclusion_list:
            print("\tTesting yearly entities counts for {table_name}".format(table_name=table_name))
            try:
                if not self.connection.open:
                    self.connection.connect()
                patent_table = 'patent' if self.patent_db_prefix is None else '{db}.patent'.format(
                    db=self.patent_db_prefix)
                if table_name == 'patent':
                    count_query = "SELECT year(`date`) as `yr`, count(1) as `year_count` from patent group by year(`date`)"
                else:
                    count_query = "SELECT year(p.`date`) as `yr`, count(1) as `year_count` from {patent_table} p join {entity} et on et.patent_id = p.id group by year(`date`)".format(
                            patent_table=patent_table,
                            entity=table_name)
                with self.connection.cursor() as count_cursor:
                    count_cursor.execute(count_query)
                    for count_row in count_cursor.fetchall():
                        write_table_name = table_name
                        prefix = "temp_"
                        if table_name.startswith(prefix):
                            write_table_name = table_name[len(prefix):]
                        self.qa_data['DataMonitor_patentyearlycount'].append(
                                {
                                        "database_type":  self.database_type,
                                        'update_version': self.version,
                                        'year':           count_row[0],
                                        'table_name':     write_table_name,
                                        'patent_count':   count_row[1]
                                        })
            except pymysql.err.InternalError as e:
                return
            finally:
                if self.connection.open:
                    self.connection.close()
            if strict:
                self.assert_yearly_counts()

    def assert_yearly_counts(self):
        for year in range(self.start_date.year, self.end_date.year + 1):
            found = False
            for row in self.qa_data['DataMonitor_patentyearlycount']:
                if row['year'] == year:
                    found = True
                    if row['patent_count'] < 1:
                        raise Exception("Year {yr} has 0 patents in the database {db}".format(yr=year, db=
                        self.config['DATABASE'][self.database_section]))
            if not found:
                raise Exception("There are no patents for the Year {yr} in the database {db}".format(yr=year, db=
                self.config['DATABASE'][self.database_section]))

    def test_related_floating_entities(self, table_name, table_config):
        if 'related_entities' in table_config:
            for related_entity_config in table_config['related_entities']:
                self.assert_related_floating_entities(table_name, related_entity_config)

    def assert_related_floating_entities(self, table_name, related_config):
        related_query = "SELECT count(1) from {related_table} destination left join {source_table} source on source.{source_id}= destination.{destination_id} where source.{source_id} is null".format(
                source_table=table_name,
                related_table=related_config['table'], source_id=related_config['source_id'],
                destination_id=related_config['destination_id'])
        if not self.connection.open:
            self.connection.connect()
        try:
            with self.connection.cursor() as float_cursor:
                float_cursor.execute(related_query)
                related_count = float_cursor.fetchall()[0][0]
                if related_count > 0:
                    raise Exception(
                            "There are rows in {destination_id} in {related_table} that do not have corresponding {source_id} in {source_table} for {db}".format(
                                    source_table=table_name,
                                    related_table=related_config['table'], source_id=related_config['source_id'],
                                    destination_id=related_config['destination_id'], db=
                                    self.config['DATABASE'][self.database_section]))

        finally:
            self.connection.close()

    def assert_null_string(self, table, field):
        print("\t\tTesting NULL string for {field} in {table_name}".format(field=field, table_name=table))
        try:
            if not self.connection.open:
                self.connection.connect()
            zero_query = "SELECT count(*) NULL_count from `{tbl}` where `{field}` ='NULL'".format(tbl=table,
                                                                                                  field=field)
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(zero_query)
                count_value = count_cursor.fetchall()[0][0]
                if count_value != 0:
                    raise Exception(
                            "NULL strings encountered in table found:{database}.{table} column {col}. Count: {""count}".format(
                                    database=self.database_section, table=table, col=field,
                                    count=count_value))
        finally:
            if self.connection.open:
                self.connection.close()

    def test_floating_entities(self, table_name):
        print("\tTesting floating entities for {table_name}".format(table_name=table_name))
        patent_table = 'patent' if self.patent_db_prefix is None else '{db}.patent'.format(db=self.patent_db_prefix)
        float_query = "SELECT count(1) from {table_name} et left join {patent_table} p on p.id =et.patent_id where " \
                      "p.id is null".format(
                table_name=table_name, patent_table=patent_table)
        if not self.connection.open:
            self.connection.connect()
        print(float_query)
        try:
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(float_query)
                float_count = count_cursor.fetchall()[0][0]
                if float_count > 0:
                    raise Exception("There are patents in {table_name} that are not in patent table for {db}".format(
                            table_name=table_name, db=
                            self.config['DATABASE'][self.database_section]))

        except pymysql.Error as e:
            if table_name not in self.patent_exclusion_list:
                raise e
        finally:
            self.connection.close()

    def load_floating_patent_count(self, table, table_config):
        print("\tTesting floating patent counts for {table_name}".format(table_name=table))
        if not self.connection.open:
            self.connection.connect()
        additional_where = ""
        if 'custom_float_condition' in table_config and table_config['custom_float_condition'] is not None:
            additional_where = "and " + table_config['custom_float_condition']
        patent_table = 'patent' if self.patent_db_prefix is None else '{db}.patent'.format(db=self.patent_db_prefix)
        float_count_query = "SELECT count(1) as count from {patent_table} p left join {entity_table} et on " \
                            "et.patent_id = p.id where et.patent_id is null {additional_where}".format(
                patent_table=patent_table, entity_table=table, additional_where=additional_where)
        print(float_count_query)
        try:
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(float_count_query)
                float_count = count_cursor.fetchall()[0][0]
                write_table_name = table
                prefix = "temp_"
                if table.startswith(prefix):
                    write_table_name = table[len(prefix):]
                self.qa_data['DataMonitor_floatingpatentcount'].append({
                        "database_type":
                                                 self.database_type,
                        'update_version':        self.version,
                        'table_name':
                                                 write_table_name,
                        'floating_patent_count': float_count
                        })
        except pymysql.Error as e:
            if table not in self.patent_exclusion_list:
                raise e
        finally:
            self.connection.close()

    def load_prefix_counts(self, table_name):
        if table_name not in self.patent_exclusion_list:
            print("\tTesting entity counts by patent type for {table_name}".format(table_name=table_name))
            try:
                if not self.connection.open:
                    self.connection.connect()
                patent_table = 'patent' if self.patent_db_prefix is None else '{db}.patent'.format(
                        db=self.patent_db_prefix)

                count_query = "SELECT p.`type` as `type`, count(1) as `type_count` from {patent_table} p join {entity} et on et.patent_id = p.id group by  p.`type`".format(
                        patent_table=patent_table,
                        entity=table_name)
                with self.connection.cursor() as count_cursor:
                    count_cursor.execute(count_query)
                    for count_row in count_cursor.fetchall():
                        write_table_name = table_name
                        prefix = "temp_"
                        if table_name.startswith(prefix):
                            write_table_name = table_name[len(prefix):]
                        self.qa_data['DataMonitor_prefixedentitycount'].append(
                                {
                                        "database_type":  self.database_type,
                                        'update_version': self.version,
                                        'patent_type':    count_row[0],
                                        'table_name':     write_table_name,
                                        'patent_count':   count_row[1]
                                        })
            except pymysql.err.InternalError as e:
                if table_name not in self.patent_exclusion_list:
                    raise e

            finally:
                if self.connection.open:
                    self.connection.close()

    def save_qa_data(self):
        qa_engine = create_engine(self.qa_connection_string)
        for qa_table in self.qa_data:
            qa_table_data = self.qa_data[qa_table]
            table_frame = pd.DataFrame(qa_table_data)
            try:
                table_frame.to_sql(name=qa_table, if_exists='append', con=qa_engine, index=False)
            except SQLAlchemyError as e:
                table_frame.to_csv("errored_qa_data" + qa_table, index=False)
                raise e

    def test_text_length(self, table_name, field_name):
        print(
                "\t\tLoading Text field max Length {field} in {table_name}".format(field=field_name,
                                                                                   table_name=table_name))
        text_length_query = "SELECT max(char_length(`{field}`)) from `{table}`;".format(field=field_name,
                                                                                        table=table_name)
        if not self.connection.open:
            self.connection.connect()

        with self.connection.cursor() as text_cursor:
            text_cursor.execute(text_length_query)
            text_length = text_cursor.fetchall()[0][0]
            write_table_name = table_name
            prefix = "temp_"
            if table_name.startswith(prefix):
                write_table_name = table_name[len(prefix):]
            self.qa_data['DataMonitor_maxtextlength'].append({
                    "database_type":   self.database_type,
                    'update_version':  self.version,
                    'table_name':      write_table_name,
                    'column_name':     field_name,
                    'max_text_length': text_length
                    })

    def runTests(self):
        skiplist = []
        for table in self.table_config:
            if table in skiplist:
                continue
            print("Beginning Test for {table_name} in {db}".format(table_name=table,
                                                                   db=self.config["DATABASE"][self.database_section]))
            self.test_floating_entities(table)
            self.test_yearly_count(table, strict=False)
            self.test_table_row_count(table)
            self.test_blank_count(table, self.table_config[table])
            self.test_nulls(table, self.table_config[table])
            self.load_floating_patent_count(table, self.table_config[table])
            self.load_prefix_counts(table)
            for field in self.table_config[table]["fields"]:
                print("\tBeginning tests for {field} in {table_name}".format(field=field, table_name=table))
                if "date_field" in self.table_config[table]["fields"][field] and \
                        self.table_config[table]["fields"][field]["date_field"]:
                    self.assert_zero_dates(table, field)
                if self.table_config[table]["fields"][field]['category']:
                    self.load_category_counts(table, field)
                if self.table_config[table]["fields"][field]['data_type'] in ['mediumtext', 'longtext', 'text']:
                    self.test_text_length(table, field)
                self.test_null_byte(table, field)

        self.save_qa_data()
