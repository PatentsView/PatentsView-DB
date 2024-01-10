from abc import ABC

import pandas as pd
import pymysql.cursors
import json
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import datetime
from time import time
import os
import re

import logging

logging.basicConfig(level=logging.INFO)  # Set the logging level
logger = logging.getLogger(__name__)

from lib.configuration import get_connection_string
from lib.configuration import get_current_config
from lib import utilities


class DatabaseTester(ABC):
    def __init__(self, config, database_section, start_date, end_date):
        # super().__init__(config, database_section, start_date, end_date)

        class_called = self.__class__.__name__
        utilities.get_relevant_attributes(self, class_called, database_section, config)
        # Update start and end date
        self.start_date = start_date
        self.end_date = end_date

        # Indicator for Upload/Patents database
        self.qa_connection_string = get_connection_string(config, database='QA_DATABASE', connection='APP_DATABASE_SETUP')
        self.connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                          user=config['DATABASE_SETUP']['USERNAME'],
                                          password=config['DATABASE_SETUP']['PASSWORD'],
                                          db=database_section,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
        # self.database_connection_string = get_connection_string(config, database_section)
        self.config = config
        self.database_section = database_section
        self.class_called = class_called

        if self.class_called == 'TextQuarterlyMergeTest' and database_section == 'pgpubs_text':
            database_type = 'pregrant'
        else:
            try:
                database_type = self.database_section.split("_")[0]
            except IndexError:
                database_type = self.database_section

        self.version = self.end_date.strftime("%Y-%m-%d")

        # Add Quarter Variable
        df = pd.DataFrame(columns=['date'])
        df.loc[0] = [self.version]
        df['quarter'] = pd.to_datetime(df.date).dt.to_period('Q')
        quarter = str(df['quarter'][0])
        self.quarter = quarter[:4] + "-" + quarter[5]
        #####

        self.database_type = database_type
        utilities.class_db_specific_config(self, self.table_config, class_called)

        #set a break point to check stuff


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
            'DataMonitor_indexcount': []
        }

    def query_runner(self, query, single_value_return=True, where_vi=False, vi_comparison = '='):
        vi_comparison = vi_comparison.strip()
        assert vi_comparison in ['=', '<', '>', '<=', '>=', '<>', '!=']
        if where_vi:
            vi_date = self.end_date.strftime('%Y-%m-%d')
            if 'where' and 'main_table' in query:
                where_statement = f" and main_table.version_indicator {vi_comparison} '{vi_date}'"
            elif 'where' in query:
                where_statement = f" and version_indicator {vi_comparison} '{vi_date}'"
            else:
                where_statement = f" where version_indicator {vi_comparison} '{vi_date}'"
            q = query+where_statement
        else:
            q = query
        logger.info(q)
        try:
            if not self.connection.open:
                self.connection.connect()
            with self.connection as connection:
                with connection.cursor() as generic_cursor:
                    query_start_time = time()
                    generic_cursor.execute(q)
                    query_end_time = time()
                    logger.info(f"\t\tThis query took {query_end_time - query_start_time:.3f} seconds")
                    if single_value_return:
                        count_value = generic_cursor.fetchall()[0][0]
                    else:
                        count_value = generic_cursor.fetchall()
                connection.commit()
        finally:
            if self.connection.open:
                self.connection.close()
        return count_value



    def load_table_row_count(self, table_name, where_vi):
        query = f"""
SELECT count(*) as table_count
from {table_name}"""
        count_value = self.query_runner(query, single_value_return=True, where_vi=where_vi)
        if count_value < 1 and table_name not in ['rawuspc', 'uspc', 'government_organization']:
            raise Exception("Empty table found:{table}".format(table=table_name))
        self.qa_data['DataMonitor_count'].append(
            {
                "database_type": self.database_type,
                'table_name': table_name,
                'update_version': self.version,
                'table_row_count': count_value,
                'quarter': self.quarter
            })


    def test_blank_count(self, table, table_config, where_vi):
        for field in table_config["fields"]:
            if table_config["fields"][field]["data_type"] in ['varchar', 'mediumtext', 'text']:
                count_query = f"""
SELECT count(*) as blank_count
from `{table}`
where `{field}` = ''"""
                count_value = self.query_runner(count_query, single_value_return=True, where_vi=where_vi)
                if count_value != 0:
                    exception_message = """
Blanks encountered in  table found:{database}.{table} column {col}. Count: {count}
                    """.format(database=self.database_section,
                               table=table, col=field,
                               count=count_value)
                    raise Exception(exception_message)


    def test_null_byte(self, table, field, where_vi):
        nul_byte_query = f"""
SELECT count(*) as count
from `{table}`
where INSTR(`{field}`, CHAR(0x00)) > 0"""
        count_value = self.query_runner(nul_byte_query, single_value_return=True, where_vi=where_vi)
        if count_value > 0:
        # attempt automatic correction
            bad_char_fix_query = f"""
            UPDATE `{table}`
            SET `{field}` = REPLACE(REPLACE(REPLACE(`{field}`, CHAR(0x00), ''), CHAR(0x08), ' b'), CHAR(0x1A), 'Z')
            WHERE INSTR(`{field}`, CHAR(0x00)) > 0
            """
            try:
                if not self.connection.open:
                    self.connection.connect()
                with self.connection as connection:
                    with connection.cursor() as generic_cursor:
                        logger.info(bad_char_fix_query)
                        generic_cursor.execute(bad_char_fix_query)
                    connection.commit()
            finally:
                if self.connection.open:
                    self.connection.close()
            logger.info(f"attempted to correct newlines in {table}.{field}. re-performing newline detection query:")
            logger.info(nul_byte_query)
            count_value = self.query_runner(nul_byte_query, single_value_return=True, where_vi=where_vi)
            if count_value > 0:
                exception_message = f"{count_value} rows with NUL Byte found in `{field}` of `{self.database_section}`.`{table}` after attempted correction."
                raise Exception(exception_message)
        


    def test_newlines(self, table, field, where_vi):
        skip = False
        allowables = { # set of tables and fields where newlines are allowable in the field content
            'brf_sum_text' : ['summary_text'], 
            'detail_desc_text' : ['description_text'],
            'claims' : ['claim_text'],
            'rel_app_text' : ['text']
        }
        # autofixes = {
        #     'draw_desc_text' : ['draw_desc_text'],
        #     'rawassignee': ['orgnaization']
        # }
        if table in allowables: #non-text tables
            if field in allowables[table]:
                skip = True
        elif re.match(".*_[0-9]{4}", table) and table[:-5] in allowables: #text-tables
            if field in allowables[table[:-5]]:
                skip = True

        if skip:
            logger.info('newlines marked as permitted for this field. skipping newline test')
        else: 
            newline_query = f"""
            SELECT count(*) as count
            from `{table}`
            where INSTR(`{field}`, '\n') > 0"""
            count_value = self.query_runner(newline_query, single_value_return=True, where_vi=where_vi)
            if count_value > 0:
                logger.info(f"{count_value} rows with unwanted newlines found in {field} of {table} for {self.database_section}. Correcting records ...")
                makelogquery = f"CREATE TABLE IF NOT EXISTS `{table}_newline_log` LIKE {table}"
                filllogquery = f"INSERT INTO `{table}_newline_log` SELECT * FROM `{table}` WHERE `{field}` LIKE '%\n%'"
                fixquery = f"""
                UPDATE `{table}`
                SET {field} = REPLACE(REPLACE({field}, '\n', ' '), '  ', ' ')
                WHERE `{field}` LIKE '%\n%';
                """
                try:
                    if not self.connection.open:
                        self.connection.connect()
                    with self.connection as connection:
                        with connection.cursor() as generic_cursor:
                            for query in [makelogquery, filllogquery, fixquery]:
                                logger.info(query)
                                generic_cursor.execute(query)
                        connection.commit()
                finally:
                    if self.connection.open:
                        self.connection.close()
                logger.info(f"attempted to correct newlines in {table}.{field}. re-performing newline detection query:")
                count_value = self.query_runner(newline_query, single_value_return=True, where_vi=where_vi)
                if count_value > 0:
                    exception_message = f"{count_value} rows with unwanted and unfixed newlines found in {field} of {table} for {self.database_section}"
                    raise Exception(exception_message)


    def load_category_counts(self, table, field):
        category_count_query = f"""
SELECT `{field}` as value
    , count(*) as count
from `{table}`
group by 1"""
        count_value = self.query_runner(category_count_query, single_value_return=False, where_vi=False)
        for count_row in count_value:
            value = count_row[0]
            if value is None:
                value = 'NULL'
            self.qa_data['DataMonitor_categorycount'].append(
                {
                    "database_type": self.database_type,
                    'table_name': table,
                    "column_name": field,
                    'update_version': self.version,
                    'value': value,
                    'count': count_row[1],
                    'quarter': self.quarter
                })


    def load_nulls(self, table, table_config, where_vi):
        for field in table_config["fields"]:
            count_query = f"""
SELECT count(*) as null_count 
from `{table}` where `{field}` is null
            """
            count_value = self.query_runner(count_query, single_value_return=True, where_vi=where_vi)
            if not table_config["fields"][field]['null_allowed']:
                if count_value != 0:
                    raise Exception(
                        "NULLs encountered in table found:{database}.{table} column {col}. Count: {"
                        "count}".format(
                            database=self.database_section, table=table,
                            col=field,
                            count=count_value))
            self.qa_data['DataMonitor_nullcount'].append(
                {
                    "database_type": self.database_type,
                    'table_name': table,
                    "column_name": field,
                    'update_version': self.version,
                    'null_count': count_value,
                    'quarter': self.quarter
                })


    def test_zero_dates(self, table, field, where_vi):
        zero_query = f"""
SELECT count(*) zero_count 
FROM `{table}` 
WHERE `{field}`  LIKE '0000-__-__'
OR `{field}`  LIKE '____-00-__'
OR `{field}`  LIKE '____-__-00'
"""
        count_value = self.query_runner(zero_query, single_value_return=True, where_vi=where_vi)
        if count_value != 0:
            raise Exception(
                "zero date encountered in table found:{database}.{table} column {col}. Count: {"
                "count}".format(
                    database=self.database_section, table=table, col=field,
                    count=count_value))


    def test_null_version_indicator(self, table):
        null_vi_query = \
f"SELECT count(*) null_count " \
f"from {table} " \
f"where version_indicator is null"
        count_value = self.query_runner(null_vi_query, single_value_return=True)
        if count_value != 0:
            raise Exception(
                "Table {database}.{table} Has {count} Nulls in Version Indicator".format(
                    database=self.database_section, table=table, count=count_value))


    def test_white_space(self, table, field):
        white_space_query = \
f"SELECT count(*) " \
f"from {table} W" \
f"HERE CHAR_LENGTH(`{field}`) != CHAR_LENGTH(TRIM(`{field}`))"
        count_value = self.query_runner(white_space_query, single_value_return=True)
        if count_value != 0:
            logger.info("THE FOLLOWING QUERY NEEDS ADDRESSING")
            logger.info(white_space_query)
            raise Exception(f"{self.database_section}.{table}.{field} needs trimming")

    def check_for_indexes(self, table):
        if "webtool" not in table and table not in ["patent_lawyer_unique"]:
            index_query = \
    f"""select count(*) from information_schema.statistics where table_name = '{table}' and table_schema = '{self.database_section}' """
            count_value = self.query_runner(index_query, single_value_return=True)
            if count_value == 0:
                logger.info(index_query)
                raise Exception(f"{self.database_section}.{table} has no indexes")
            self.qa_data['DataMonitor_indexcount'].append(
                {
                    "database_type": self.database_type,
                    'table_name': table,
                    'update_version': self.version,
                    'index_count': count_value,
                    'quarter': self.quarter
                })

    def test_rawassignee_org(self, table, where_vi=False):
        rawassignee_q = """
SELECT count(*) 
FROM rawassignee 
where name_first is not null and name_last is null"""
        count_value = self.query_runner(rawassignee_q, single_value_return=True, where_vi=where_vi)
        if count_value != 0:
            logger.info("THE FOLLOWING QUERY NEEDS ADDRESSING")
            logger.info(rawassignee_q)
            raise Exception(f"{self.database_section}.{table} Has Wrong Organization values")


    def test_related_floating_entities(self, table_name, table_config, where_vi=False, vi_comparison = '='):
        if table_name not in self.exclusion_list and 'related_entities' in table_config:
            for related_entity_config in table_config['related_entities']:
                exists_query = f"""SHOW TABLES LIKE '{related_entity_config["related_table"]}'; """
                exists_table_count = self.query_runner(exists_query, single_value_return=False, where_vi=False)
                if not exists_table_count:
                    continue
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
                    related_count = self.query_runner(related_query, single_value_return=True, where_vi=where_vi, vi_comparison=vi_comparison)
                    if related_count > 0:
                        raise Exception(
                            "There are rows for the id: {related_table_id} in {related_table} that do not have corresponding rows for the id: {"
                            "main_table_id} in {main_table} for {db}".format(
                                main_table=table_name,
                                related_table=related_entity_config['related_table'],
                                main_table_id=related_entity_config['main_table_id'],
                                related_table_id=related_entity_config['related_table_id'],
                                db=self.database_section)
                        )

    def load_main_floating_entity_count(self, table_name, table_config):
        if table_name not in self.exclusion_list and 'related_entities' in table_config:
            for related_entity_config in table_config['related_entities']:
                ###### CHECKING IF THE RELATED TABLE HAS DATA
                exists_query = f"""SHOW TABLES LIKE '{related_entity_config["related_table"]}'; """
                exists_table_count = self.query_runner(exists_query, single_value_return=False, where_vi=False)
                if not exists_table_count:
                    continue
                else:
                    ###### DYNAMICALLY PICKING THE LASTEST COLUMN FOR CHECKING FLOATING ENTITY COUNT
                    year_columns = []
                    if (table_name == 'persistent_assignee_disambig' and related_entity_config[
                        'related_table'] == 'assignee') or (
                            table_name == 'persistent_inventor_disambig' and related_entity_config[
                        'related_table'] == 'inventor'):
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
                    additional_where = ""
                    if 'custom_float_condition' in table_config and table_config[
                        'custom_float_condition'] is not None:
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
                    related_table_count = self.query_runner(float_count_query, single_value_return=True, where_vi=False)
                    self.qa_data['DataMonitor_floatingentitycount'].append({
                        "database_type": self.database_type,
                        'update_version': self.version,
                        'main_table': table_name,
                        'related_table': related_entity_config["related_table"],
                        'floating_count': related_table_count,
                        'quarter': self.quarter
                    })

    def load_entity_category_counts(self, table_name):
        if table_name not in self.exclusion_list and self.category != '':
            if table_name == self.central_entity:
                count_query = f"""
select {self.category}, count(1)
from {self.central_entity}
group by 1"""
            else:
                count_query = f"""
SELECT main.{self.category}, count(1)
from {self.central_entity} main join {table_name} related
on related.{self.f_key} = main.{self.p_key}
group by 1               
                    """
            count_value = self.query_runner(count_query, single_value_return=False, where_vi=False)
            for count_row in count_value:
                self.qa_data['DataMonitor_prefixedentitycount'].append(
                    {
                        "database_type": self.database_type,
                        'update_version': self.version,
                        'patent_type': count_row[0],
                        'table_name': table_name,
                        'patent_count': count_row[1],
                        'quarter': self.quarter
                    })

    def load_counts_by_location(self, table, field):
        row_query = "select count(1) from {tbl}".format(tbl=table)
        if table == 'patent':
            location_query = \
f"""
SELECT t.`{field}`, count(*) 
from {table} t join patent.country_codes cc
on t.country = cc.`alpha-2`
group by t.`{field}`"""
        else:
            location_query = \
f"""
SELECT t.`{field}`, count(*) 
from {table} t join patent.country_codes cc
on t.country = cc.`alpha-2`
group by t.`{field}`"""
        row_count = self.query_runner(row_query, single_value_return=True, where_vi=False)
        count_value = self.query_runner(location_query, single_value_return=False, where_vi=False)
        for count_row in count_value:
            self.qa_data['DataMonitor_locationcount'].append(
                {
                    "database_type": self.database_type,
                    'update_version': self.version,
                    'table_name': table,
                    'table_row_count': row_count,
                    'patent_id_count': count_row[1],
                    'location': count_row[0],
                    'quarter': self.quarter
                })

    def save_qa_data(self):
        qa_engine = create_engine(self.qa_connection_string)
        for qa_table in self.qa_data:
            qa_table_data = self.qa_data[qa_table]
            if len(qa_table_data) == 0: 
                continue
            table_frame = pd.DataFrame(qa_table_data)
            if qa_table == 'DataMonitor_topnentities': 
                entity_set = f"""('{"', '".join(table_frame.entity_name.unique())}')"""
                table_col = 'entity_name'
                addl_condition = f"AND `{table_col}` IN {entity_set}"
                print_condition = f"for {entity_set} "
            elif qa_table in ['DataMonitor_govtinterestsampler']: # table-specific QA tables that just identify records by update_version and db_type
                addl_condition = ""
                print_condition = ""
            elif 'table_name' in table_frame.columns:
                table_set = f"""('{"', '".join(table_frame.table_name.unique())}')"""
                table_col = "table_name"
                addl_condition = f"AND `{table_col}` IN {table_set}"
                print_condition = f"for {table_set} "
            elif 'main_table' in table_frame.columns: # for floating entity table
                table_set = f"""('{"', '".join(table_frame.main_table.unique())}')"""
                table_col = "main_table"
                addl_condition = f"AND `{table_col}` IN {table_set}"
                print_condition = f"for {table_set} "
            else:
                raise NotImplementedError(f"specification of existing rows to remove not implemented for {qa_table}.\ncolumns available: `{'`,`'.join(table_frame.columns)}`")
            try:
                logger.info(f'removing prior {qa_table} {self.database_type} records {print_condition}on {self.version}')
                clean_prior = f"DELETE FROM {qa_table} WHERE `update_version` = '{self.version}' AND `database_type` = '{self.database_type}' {addl_condition}"
                logger.info(clean_prior)
                qa_engine.execute(clean_prior)
                logger.info(f'inserting new {qa_table} records for {self.version} and {self.database_type}')
                table_frame.to_sql(name=qa_table, if_exists='append', con=qa_engine, index=False)
            except SQLAlchemyError as e:
                table_frame.to_csv("errored_qa_data" + qa_table, index=False)
                raise e

    def load_text_length(self, table_name, field_name):
        text_length_query = \
f"SELECT max(char_length(`{field_name}`)) " \
f"from `{table_name}`;"
        text_length = self.query_runner(text_length_query, single_value_return=True)
        self.qa_data['DataMonitor_maxtextlength'].append({
            "database_type": self.database_type,
            'update_version': self.version,
            'table_name': table_name,
            'column_name': field_name,
            'max_text_length': text_length,
            'quarter': self.quarter
        })

    def test_patent_abstract_null(self, table, where_vi=False):
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
            count_value = self.query_runner(count_query, single_value_return=True)
            if count_value != 0:
                raise Exception(
                    f"NULLs (Non-design patents) encountered in table found:{self.database_section}.{table} column abstract. Count: {count_value}")

    def runStandardTests(self):
        self.init_qa_dict()
        counter = 0
        total_tables = len(self.table_config.keys())
        for table in self.table_config:
            logger.info(" -------------------------------------------------- ")
            logger.info(f"BEGINNING TESTS FOR {self.database_section}.{table}")
            logger.info(" -------------------------------------------------- ")
            self.test_blank_count(table, self.table_config[table], where_vi=True)
            self.test_related_floating_entities(table, table_config=self.table_config[table], where_vi=True)
            self.load_nulls(table, self.table_config[table], where_vi=True)
            self.test_null_version_indicator(table)
            self.load_table_row_count(table, where_vi=True)
            self.test_related_floating_entities(table_name=table, table_config=self.table_config[table], where_vi=False, vi_comparison='=')
            self.load_main_floating_entity_count(table, self.table_config[table])
            self.load_entity_category_counts(table)
            if table == 'rawassignee':
                self.test_rawassignee_org(table, where_vi=True)
            if table == self.central_entity:
                self.test_patent_abstract_null(table)
            for field in self.table_config[table]["fields"]:
                logger.info("==============================================================================")
                logger.info(f"\tBEGINNING TESTS FOR COLUMN: {table}.{field}")
                logger.info("==============================================================================")
                if self.table_config[table]["fields"][field]["data_type"] == 'date':
                    self.test_zero_dates(table, field, where_vi=False)
                if self.table_config[table]["fields"][field]["category"]:
                    self.load_category_counts(table, field)
                if self.table_config[table]["fields"][field]['data_type'] in ['mediumtext', 'longtext', 'text']:
                    self.load_text_length(table, field)
                if self.table_config[table]["fields"][field]['data_type'] in ['mediumtext', 'longtext', 'text', 'varchar']:
                    self.test_newlines(table, field, where_vi=False)
                if self.table_config[table]["fields"][field]["location_field"]:
                    self.load_counts_by_location(table, field)
                if self.table_config[table]["fields"][field]['data_type'] == 'varchar' and 'id' not in field:
                    self.test_white_space(table, field)
                self.test_null_byte(table, field, where_vi=False)
            logger.info(f"FINISHED WITH TABLE: {table}")
            counter += 1
            logger.info("==============================================================================")
            logger.info(f"Currently Done With {counter} of {total_tables} | {counter/total_tables:.2%}")
            logger.info("==============================================================================")

    def runDisambiguationTests(self):
        counter = 0
        total_tables = len(self.table_config.keys())
        self.init_qa_dict()
        for table in self.table_config:
            logger.info(table)
            self.check_for_indexes(table)
            self.load_table_row_count(table, where_vi=False)
            self.load_nulls(table, self.table_config[table], where_vi=False)
            self.test_blank_count(table, self.table_config[table], where_vi=False)
            self.save_qa_data()
            self.init_qa_dict()
            logger.info(f"FINISHED WITH TABLE: {table}")
            counter += 1
            logger.info("==============================================================================")
            logger.info(f"Currently Done With {counter} of {total_tables} | {counter/total_tables:.2%}")
            logger.info("==============================================================================")

    def runReportingTests(self):
        counter = 0
        total_tables = len(self.table_config.keys())
        self.init_qa_dict()
        for table in self.table_config:
            self.check_for_indexes(table)
            self.load_table_row_count(table, where_vi=False)
            self.save_qa_data()
            self.init_qa_dict()
            logger.info(f"FINISHED WITH TABLE: {table}")
            counter += 1
            logger.info("==============================================================================")
            logger.info(f"Currently Done With {counter} of {total_tables} | {counter/total_tables:.2%}")
            logger.info("==============================================================================")

    ####### WANT TO DEPRECATE THIS FUNCTION !!!!!
    def runTests(self):
        raise Exception("runTests() has been retired. One of the newer test collections should be used instead.")
    #     counter = 0
    #     total_tables = len(self.table_config.keys())
    #     self.init_qa_dict()
    #     for table in self.table_config:
    #         self.check_for_indexes(table)
    #         logger.info(f"==============================================================================")
    #         logger.info(f"BEGINNING TESTS FOR TABLE: {self.database_section}.{table} %")
    #         logger.info(f"==============================================================================")
    #         if self.class_called != "ReportingDBTester" and "PostProcessingQC" not in self.class_called:
    #             self.test_null_version_indicator(table)
    #         self.load_table_row_count(table, where_vi=False)
    #         if table == 'rawassignee':
    #             self.test_rawassignee_org(table, where_vi=False)
    #         self.test_blank_count(table, self.table_config[table], where_vi=False)
    #         self.load_nulls(table, self.table_config[table], where_vi=False)
    #         vi_cutoff_classes = ['DisambiguationTester', 'LawyerPostProcessingQC']
    #         if "PostProcessingQC" not in self.class_called:
    #             self.test_related_floating_entities(table_name=table, table_config=self.table_config[table],
    #                         where_vi=(True if self.class_called in vi_cutoff_classes else False),
    #                         vi_comparison=('<=' if self.class_called in vi_cutoff_classes else '='))
    #             self.load_main_floating_entity_count(table, self.table_config[table])
    #         self.load_entity_category_counts(table)
    #         if table == self.central_entity:
    #             self.test_patent_abstract_null(table)
    #         for field in self.table_config[table]["fields"]:
    #             logger.info(f"==============================================================================")
    #             logger.info(f"\tBEGINNING TESTS FOR COLUMN: {table}.{field}")
    #             logger.info(f"==============================================================================")
    #             if self.table_config[table]["fields"][field]["data_type"] == 'date':
    #                 self.test_zero_dates(table, field, where_vi=False)
    #             if self.table_config[table]["fields"][field]["category"]:
    #                 self.load_category_counts(table, field)
    #             if self.table_config[table]["fields"][field]['data_type'] in ['mediumtext', 'longtext', 'text']:
    #                 self.load_text_length(table, field)
    #             if self.table_config[table]["fields"][field]['data_type'] in ['mediumtext', 'longtext', 'text', 'varchar']:
    #                 self.test_newlines(table,field, where_vi=False)
    #             if self.table_config[table]["fields"][field]["location_field"]:
    #                 self.load_counts_by_location(table, field)
    #             if self.table_config[table]["fields"][field]['data_type'] == 'varchar' and 'id' not in field and (self.class_called == 'UploadTest' or self.class_called == 'TextUploadTest'):
    #                 self.test_white_space(table, field)
    #             self.test_null_byte(table, field, where_vi=False)
    #         if self.class_called == "TextMergeTest":
    #             continue
    #         else:
    #             self.save_qa_data()
    #             self.init_qa_dict()
    #         logger.info(f"FINISHED WITH TABLE: {table}")
    #         counter += 1
    #         logger.info(f"==============================================================================")
    #         logger.info(f"Currently Done With {counter} of {total_tables} | {counter/total_tables} %")
    #         logger.info(f"==============================================================================")

if __name__ == '__main__':
    # config = get_config()
    config = get_current_config('granted_patent', **{
        "execution_date": datetime.date(2022, 5, 31)
    })
    # fill with correct run_id
    run_id = "backfill__2020-12-29T00:00:00+00:00"
    pt = DatabaseTester(config, 'PatentsView_20230330', datetime.date(2023, 1, 1), datetime.date(2023, 3, 30))
    pt.runTests()

