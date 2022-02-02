from abc import ABC

import pandas as pd
import pymysql.cursors
import json
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import datetime
import os

from lib.configuration import get_connection_string
from lib.configuration import get_current_config
from lib import utilities

class DatabaseTester(ABC):
    def __init__(self, config, database_section, start_date, end_date):
        # super().__init__(config, database_section, start_date, end_date)
        self.project_home = os.environ['PACKAGE_HOME']
        class_called = self.__class__.__name__
        utilities.get_relevant_attributes(self, class_called, database_section, config)


            # Update start and end date
        self.start_date = start_date
        self.end_date = end_date

        # Indicator for Upload/Patents database
        self.qa_connection_string = get_connection_string(config, 'QA_DATABASE', connection='QA_DATABASE_SETUP')
        self.connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                          user=config['DATABASE_SETUP']['USERNAME'],
                                          password=config['DATABASE_SETUP']['PASSWORD'],
                                          db=database_section,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
        # self.database_connection_string = get_connection_string(config, database_section)
        self.config = config
        # Place Holder for saving QA counts - keys map to table names in patent_QA
        self.init_qa_dict()
        self.database_section = database_section

        try:
            database_type = self.database_section.split("_")[0]
        except IndexError:
            database_type = self.database_section

        self.version = self.end_date.strftime("%Y%m%d")
        self.database_type = database_type

        utilities.class_db_specific_config(self, self.table_config, class_called)

    def init_qa_dict(self):
        # Place Holder for saving QA counts - keys map to table names in patent_QA
        self.qa_data = {
            'DataMonitor_count': [],
            'DataMonitor_nullcount': [],
            'DataMonitor_patentyearlycount': [],
            'DataMonitor_categorycount': [],
            'DataMonitor_floatingentitycount': [],
            'DataMonitor_maxtextlength': [],
            'DataMonitor_prefixedentitycount': [],
            'DataMonitor_locationcount': [],
        }

    def load_table_row_count(self, table_name):
        print(f"\tTesting Table counts for {table_name} in {self.database_section}")
        try:
            if not self.connection.open:
                self.connection.connect()
            count_query = f"""
SELECT count(*) as table_count
from {table_name}
            """
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
                        "database_type": self.database_type,
                        'table_name': write_table_name,
                        'update_version': self.version,
                        'table_row_count': count_value
                    })
        finally:
            if self.connection.open:
                self.connection.close()

    def test_blank_count(self, table, table_config):
        print("\tTesting blanks for {table_name}".format(table_name=table))
        for field in table_config["fields"]:
            if table_config["fields"][field]["data_type"] in ['varchar', 'mediumtext', 'text']:
                try:
                    if not self.connection.open:
                        self.connection.connect()
                    count_query = f"""
SELECT count(*) as blank_count
from `{table}`
where `{field}` = ''
                    """
                    with self.connection.cursor() as count_cursor:
                        count_cursor.execute(count_query)
                        count_value = count_cursor.fetchall()[0][0]
                        if count_value != 0:
                            exception_message = """
Blanks encountered in  table found:{database}.{table} column {col}. Count: {count}
                            """.format(database=self.database_section,
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
            nul_byte_query = f"""
SELECT count(*) as count
from `{table}`
where INSTR(`{field}`, CHAR(0x00)) > 0
            """
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(nul_byte_query)
                nul_byte_count = count_cursor.fetchall()[0][0]
                if nul_byte_count > 1:
                    exception_message = """
{count} rows with NUL Byte found in {field} of {table_name} for {db}                    
                    """.format(count=nul_byte_count, field=field, table_name=table,
                               db=self.database_section)
                    raise Exception(exception_message)
        finally:
            if self.connection.open:
                self.connection.close()

    def load_category_counts(self, table, field):
        print("\t\tLoading category counts for {field} in {table_name}".format(field=field, table_name=table))
        try:
            if not self.connection.open:
                self.connection.connect()
            category_count_query = f"""
SELECT `{field}` as value
    , count(*) as count
from `{table}`
group by 1
            """
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
                            "database_type": self.database_type,
                            'table_name': write_table_name,
                            "column_name": field,
                            'update_version': self.version,
                            'value': value,
                            'count': count_row[1]
                        })
        finally:
            if self.connection.open:
                self.connection.close()

    def load_nulls(self, table, table_config):
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
                count_query = f"SELECT count(*) as null_count from `{table}` where `{field}` is null"
                with self.connection.cursor() as count_cursor:
                    count_cursor.execute(count_query)
                    count_value = count_cursor.fetchall()[0][0]
                    if not table_config["fields"][field]['null_allowed']:
                        if count_value != 0:
                            raise Exception(
                                "NULLs encountered in table found:{database}.{table} column {col}. Count: {"
                                "count}".format(
                                    database=self.database_section, table=table,
                                    col=field,
                                    count=count_value))
                    write_table_name = table
                    prefix = "temp_"
                    if table.startswith(prefix):
                        write_table_name = table[len(prefix):]
                    self.qa_data['DataMonitor_nullcount'].append(
                        {
                            "database_type": self.database_type,
                            'table_name': write_table_name,
                            "column_name": field,
                            'update_version': self.version,
                            'null_count': count_value
                        })
            finally:
                if self.connection.open:
                    self.connection.close()

    def test_zero_dates(self, table, field):
        print("\t\tTesting 00-00-0000 for {field} in {table_name}".format(field=field, table_name=table))
        try:
            if not self.connection.open:
                self.connection.connect()
            zero_query = f"SELECT count(*) zero_count from `{table}` where `{field}` ='0000-00-00'"
            print(zero_query)
            with self.connection.cursor() as count_cursor:
                count_cursor.execute(zero_query)
                count_value = count_cursor.fetchall()[0][0]
                if count_value != 0:
                    raise Exception(
                        "0000-00-00 date encountered in table found:{database}.{table} column {col}. Count: {"
                        "count}".format(
                            database=self.database_section, table=table, col=field,
                            count=count_value))
        finally:
            if self.connection.open:
                self.connection.close()

    def load_yearly_count(self, table_name, strict=True):
        if table_name not in self.exclusion_list and self.p_key != '':
            print(f"\tTesting yearly entities counts for {table_name}")
            try:
                if not self.connection.open:
                    self.connection.connect()

                if table_name == self.central_entity:
                    count_query = f"""
                    SELECT year(`date`) as `yr`, 
                            count(1) as `year_count` 
                    from {self.central_entity} 
                    group by year(`date`)
                    """
                else:
                    count_query = f"""
                    SELECT year(p.`date`) as `yr`
                        , count(1) as `year_count` 
                    from {self.central_entity} p join {table_name} et 
                        on et.{self.f_key} = p.{self.p_key} 
                    group by year(`date`)
                    """
                print(count_query)

                with self.connection.cursor() as count_cursor:
                    count_cursor.execute(count_query)
                    for count_row in count_cursor.fetchall():
                        write_table_name = table_name
                        prefix = "temp_"
                        if table_name.startswith(prefix):
                            write_table_name = table_name[len(prefix):]
                        self.qa_data['DataMonitor_patentyearlycount'].append(
                            {
                                "database_type": self.database_type,
                                'update_version': self.version,
                                'year': count_row[0],
                                'table_name': write_table_name,
                                'patent_count': count_row[1]
                            })
            except pymysql.err.InternalError as e:
                return
            for year in range(self.start_date.year, self.end_date.year + 1):
                found = False
                for row in self.qa_data['DataMonitor_patentyearlycount']:
                    if row['year'] == year:
                        found = True
                        if row['patent_count'] < 1:
                            raise Exception(
                                f"Year {year} has 0 {self.f_key} in the database {self.database_section}"
                            )
            if self.connection.open:
                    self.connection.close()
            # if strict:
            #     self.load_yearly_count()

    def test_related_floating_entities(self, table_name, table_config):
        if table_name not in self.exclusion_list and 'related_entities' in table_config:
            for related_entity_config in table_config['related_entities']:
                if not self.connection.open:
                    self.connection.connect()
                related_table_exists_query = """
                                SELECT count( distinct {related_table_id}) as count 
                                from {related_table}  
                            """.format(
                    related_table=related_entity_config["related_table"],
                    related_table_id=related_entity_config["related_table_id"])
                try:
                    with self.connection.cursor() as check_table_cursor:
                        check_table_cursor.execute(related_table_exists_query)
                        related_table_count = check_table_cursor.fetchall()[0][0]
                        if related_table_count == 0:
                            print(
                                "\tSkipping check for {related_table} to {main_table} because {related_table} is empty".format(
                                    related_table=related_entity_config["related_table"], main_table=table_name))
                        else:
                            related_query = """
                            SELECT count(1) 
                            from {related_table} related_table 
                                left join {main_table} main_table on main_table.{main_table_id}= related_table.{related_table_id} 
                            where main_table.{main_table_id} is null and related_table.{related_table_id} is not null
                            """.format(
                                main_table=table_name,
                                related_table=related_entity_config['related_table'],
                                main_table_id=related_entity_config['main_table_id'],
                                related_table_id=related_entity_config['related_table_id'])
                            # if not self.connection.open:
                            #     self.connection.connect()
                            try:
                                with self.connection.cursor() as float_cursor:
                                    float_cursor.execute(related_query)
                                    related_count = float_cursor.fetchall()[0][0]
                                    if related_count > 0:
                                        print(related_query)
                                        raise Exception(
                                            "There are rows for the id: {related_table_id} in {related_table} that do not have corresponding rows for the id: {"
                                            "main_table_id} in {main_table} for {db}".format(
                                                main_table=table_name,
                                                related_table=related_entity_config['related_table'],
                                                main_table_id=related_entity_config['main_table_id'],
                                                related_table_id=related_entity_config['related_table_id'],
                                                db=self.database_section)
                                        )
                            except pymysql.Error as e:
                                if table_name not in self.exclusion_list:
                                    raise e
                except pymysql.err.ProgrammingError as e:
                    print(
                        "\tSkipping check for {related_table} to {main_table} because {related_table} is does not exist".format(
                            related_table=related_entity_config["related_table"], main_table=table_name))
                    return
                finally:
                    self.connection.close()


    def load_main_floating_entity_count(self, table_name, table_config):
        if table_name not in self.exclusion_list and 'related_entities' in table_config:
            for related_entity_config in table_config['related_entities']:
                print(" ")
                print("\t\tTesting floating {f_key} counts for main_table: {table_name} and related_table: {related_table}".format(table_name=table_name, f_key=self.f_key, related_table=related_entity_config["related_table"]))
                ###### CHECKING IF THE RELATED TABLE HAS DATA
                if not self.connection.open:
                    self.connection.connect()
                related_table_exists_query = """
                                SELECT count( distinct {related_table_id}) as count from {related_table} 
                            """.format(
                    related_table=related_entity_config["related_table"],
                    related_table_id=related_entity_config["related_table_id"])
                try:
                    with self.connection.cursor() as check_table_cursor:
                        check_table_cursor.execute(related_table_exists_query)
                        related_table_count = check_table_cursor.fetchall()[0][0]
                        if related_table_count==0:
                            print("\tSkipping check for {related_table} to {main_table} because {related_table} is empty".format(related_table=related_entity_config["related_table"], main_table=table_name))
                        else:
                            ###### DYNAMICALLY PICKING THE LASTEST COLUMN FOR CHECKING FLOATING ENTITY COUNT
                            year_columns = []
                            if (table_name == 'persistent_assignee_disambig' and related_entity_config['related_table'] =='assignee') or (table_name == 'persistent_inventor_disambig' and related_entity_config['related_table'] =='inventor'):
                                columns = table_config['fields'].keys()
                                for i in columns:
                                    words = i.split("_")
                                    for w in words:
                                        if w[0] == '2':
                                            year_columns.append(w)
                                last_year = max(year_columns)
                                for k in columns:
                                    if last_year in k:
                                        winner_column = k
                                related_entity_config["main_table_id"] = winner_column
                                print(related_entity_config["main_table_id"])
                            additional_where = ""
                            if 'custom_float_condition' in table_config and table_config['custom_float_condition'] is not None:
                                additional_where = "and " + table_config['custom_float_condition']
                            float_count_query = """
                                            SELECT count(1) as count
                                            from {main_table} main 
                                            left join {related_table} related on main.{main_table_id}=related.{related_table_id} 
                                            where related.{related_table_id} is null {additional_where}
                                        """.format(
                                main_table=table_name,
                                related_table=related_entity_config["related_table"],
                                additional_where=additional_where,
                                related_table_id=related_entity_config["related_table_id"],
                                main_table_id=related_entity_config["main_table_id"])
                            print(float_count_query)
                            try:
                                with self.connection.cursor() as count_cursor:
                                    count_cursor.execute(float_count_query)
                                    float_count = count_cursor.fetchall()[0][0]
                                    prefix = "temp_"
                                    if table_name.startswith(prefix):
                                        write_table_name = table_name[len(prefix):]
                                    self.qa_data['DataMonitor_floatingentitycount'].append({
                                        "database_type": self.database_type,
                                        'update_version': self.version,
                                        'main_table': table_name,
                                        'related_table': related_entity_config["related_table"],
                                        'floating_count': float_count
                                    })
                            except pymysql.Error as e:
                                if table_name not in self.exclusion_list:
                                    raise e
                except pymysql.err.ProgrammingError as e:
                    print(
                        "\tSkipping check for {related_table} to {main_table} because {related_table} is does not exist".format(
                            related_table=related_entity_config["related_table"], main_table=table_name))
                finally:
                    self.connection.close()

    def load_entity_category_counts(self, table_name):
        if table_name not in self.exclusion_list and self.category != '':
            print(f"\t Loading entity counts by {self.category} type for {table_name}")
            try:
                if not self.connection.open:
                    self.connection.connect()
                if table_name == self.central_entity:
                    count_query = f"""
                        select {self.category}, count(1)
                        from {self.central_entity}
                        group by 1            
                    """
                else:
                    count_query = f"""
                        SELECT main.{self.category}, count(1)
                        from {self.central_entity} main join {table_name} related
                        on related.{self.f_key} = main.{self.p_key}
                        group by 1               
                    """
                print(count_query)
                with self.connection.cursor() as count_cursor:
                    count_cursor.execute(count_query)
                    for count_row in count_cursor.fetchall():
                        write_table_name = table_name
                        prefix = "temp_"
                        if table_name.startswith(prefix):
                            write_table_name = table_name[len(prefix):]
                        self.qa_data['DataMonitor_prefixedentitycount'].append(
                            {
                                "database_type": self.database_type,
                                'update_version': self.version,
                                'patent_type': count_row[0],
                                'table_name': write_table_name,
                                'patent_count': count_row[1]
                            })
            except pymysql.err.InternalError as e:
                if table_name not in self.exclusion_list:
                    raise e
            finally:
                if self.connection.open:
                    self.connection.close()

    def load_counts_by_location(self, table, field):
        print("\tTesting {f_key} by location for {table_name}".format(table_name=table, f_key=self.f_key))
        try:
            if not self.connection.open:
                self.connection.connect()
            row_query = "select count(1) from {tbl}".format(tbl=table)
            if table == 'patent':
                location_query = f"""
                    SELECT t.`{field}`, count(*) 
                    from {table} t join patent.country_codes cc
                    on t.country = cc.`alpha-2`
                    group by t.`{field}`             
                """
            else:
                location_query = f"""
                    SELECT t.`{field}`, count(*) 
                    from {table} t join patent.country_codes cc
                    on t.country = cc.`alpha-2`
                    group by t.`{field}`               
                """
            print(location_query)

            with self.connection.cursor() as count_cursor:
                count_cursor.execute(row_query)
                row_count = count_cursor.fetchall()
                count_cursor.execute(location_query)
                for count_row in count_cursor.fetchall():
                    write_table_name = table
                    prefix = "temp_"
                    if table.startswith(prefix):
                        write_table_name = table[len(prefix):]
                    self.qa_data['DataMonitor_locationcount'].append(
                        {
                            "database_type": self.database_type,
                            'update_version': self.version,
                            'table_name': write_table_name,
                            'table_row_count': row_count[0][0],
                            'patent_id_count': count_row[1],
                            'location': count_row[0]
                        })

        except pymysql.err.InternalError as e:
            if table not in self.exclusion_list:
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

    def load_text_length(self, table_name, field_name):
        print(f"\tLoading Text field max Length {field_name} in {table_name}")
        text_length_query = f"SELECT max(char_length(`{field_name}`)) from `{table_name}`;"
        print(text_length_query)
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
                "database_type": self.database_type,
                'update_version': self.version,
                'table_name': write_table_name,
                'column_name': field_name,
                'max_text_length': text_length
            })

    def test_patent_abstract_null(self, table):
        if not self.connection.open:
            self.connection.connect()
        if self.central_entity == 'patent':
            count_query = f"""
            SELECT count(*) as null_abstract_count 
            from {self.central_entity} 
            where abstract is null and type!='design' and type!='reissue' 
                and id not in ('4820515', '4885173', '6095757', '6363330', '6571026', '6601394', '6602488', '6602501', '6602630', '6602899', '6603179', '6615064', '6744569', 'H002199', 'H002200', 'H002203', 'H002204', 'H002217', 'H002235')
            """
        elif self.central_entity == 'publication':
            count_query = f"""
            SELECT count(*) as null_abstract_count 
            from {self.central_entity} p
                left join application a on p.document_number=a.document_number 
            where invention_abstract is null """
        with self.connection.cursor() as count_cursor:
            count_cursor.execute(count_query)
            count_value = count_cursor.fetchall()[0][0]
            if count_value != 0:
                raise Exception(
                        f"NULLs (Non-design patents) encountered in table found:{self.database_section}.{table} column abstract. Count: {count_value}")

    def runTests(self):
        # Skiplist is Used for Testing ONLY, Should remain blank
        # skiplist = []
        for table in self.table_config:
            print(f"Beginning Test for {table} in {self.database_section}")
            self.load_yearly_count(table, strict=False)
            self.load_table_row_count(table)
            self.test_blank_count(table, self.table_config[table])
            self.load_nulls(table, self.table_config[table])
            self.test_related_floating_entities(table_name=table, table_config=self.table_config[table])
            self.load_main_floating_entity_count(table, self.table_config[table])
            self.load_entity_category_counts(table)
            if table == self.central_entity:
                self.test_patent_abstract_null(table)
            for field in self.table_config[table]["fields"]:
                print(f"\tBeginning tests for {field} in {table}")
                if "date_field" in self.table_config[table]["fields"][field] and \
                        self.table_config[table]["fields"][field]["date_field"]:
                    self.test_zero_dates(table, field)
                if "category" in self.table_config[table]["fields"][field] and \
                        self.table_config[table]["fields"][field]["category"]:
                    self.load_category_counts(table, field)
                if self.table_config[table]["fields"][field]['data_type'] in ['mediumtext', 'longtext', 'text']:
                    self.load_text_length(table, field)
                if self.table_config[table]["fields"][field]['location_field'] and \
                        self.table_config[table]["fields"][field]["location_field"]:
                    self.load_counts_by_location(table, field)
                self.test_null_byte(table, field)
            self.save_qa_data()
            self.init_qa_dict()
            print(f"Finished With Table: {table}")


if __name__ == '__main__':
    # config = get_config()
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2020, 12, 29)
    })
    # fill with correct run_id
    run_id = "backfill__2020-12-29T00:00:00+00:00"
    pt = DatabaseTester(config, 'patent', datetime.date(2021, 12, 7), datetime.date(2022, 1, 19))
    pt.runTests()
