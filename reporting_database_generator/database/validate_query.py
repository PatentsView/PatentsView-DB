import configparser
import os
import re
import datetime

import sqlparse
from sqlalchemy import create_engine

from lib import database_helpers
from lib.notifications import send_slack_notification
from lib.configuration import get_connection_string
from lib.configuration import get_section
from lib.configuration import get_current_config, get_today_dict
import pymysql.cursors

def parse_and_format_sql(parsed_statement):
    query_lines = []
    # Remove non standard comments and merge multi line SQL to single line sql
    for sql_line in parsed_statement.value.split("\n"):
        if not sql_line.startswith("#"):
            query_lines.append(" ".join(sql_line.split()))
    if len(query_lines) < 1:
        return ""
    single_line_query = " ".join(query_lines).replace("[\s]+", " ")
    return single_line_query

def nextword(target, source):
   for i, w in enumerate(source):
    if w == target:
      return source[i+1]


def db_and_table_as_array(single_line_query):
    # Identify all tables used in the query for collation check
    single_line_query = single_line_query.lower()
    single_line_query = single_line_query.replace('`', '')
    print(f"examining table schema for query: {single_line_query}")
    single_line_query_words = single_line_query.split(" ")
    # print(single_line_query_words)
    after_into = nextword('into', single_line_query_words)
    after_from = nextword('from', single_line_query_words)
    if '(' in after_from:
        after_from = nextword('join', single_line_query_words)
    if after_into is None or after_from is None:
        print("Not set up for schema checks")
        tables_schema = []
    else:
        table_schema_list = "('" + after_into.split(".")[0] + "', " + "'" + after_from.split(".")[0] + "')"
        table_list = "('" + after_into.split(".")[1] + "', " + "'" + after_from.split(".")[1] + "')"

        db_type = 'patent'
        if 'publication' in single_line_query_words:
            db_type = 'pgpubs'
        config = get_current_config(db_type, **{"execution_date": datetime.date(2022, 1, 1)})
        connection = pymysql.connect(host=config['DATABASE_SETUP']['HOST'],
                                          user=config['DATABASE_SETUP']['USERNAME'],
                                          password=config['DATABASE_SETUP']['PASSWORD'],
                                          # db=config['PATENTSVIEW_DATABASES']["PROD_DB"],
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.SSCursor, defer_connect=True)
        if not connection.open:
            connection.connect()
        tables_query = f"""
    select table_schema, table_name 
    from information_schema.tables
    where table_schema in {table_schema_list} and table_name in {table_list}
    group by 1,2
                    """
        print(tables_query)
        with connection.cursor() as cursor:
            cursor.execute(tables_query)
            tables_schema = []
            for item in cursor.fetchall():
                tables_schema.append((item[0], item[1]))
            # row = [item[0] for item in cursor.fetchall()]
    return tables_schema


def validate_and_execute(filename=None, schema_only=False, drop_existing=True,fk_check=True, section=None, host = 'DATABASE_SETUP', **context):
    print(f'filename: {filename}')
    print(f'schema_only: {schema_only}')
    project_home = os.environ['PACKAGE_HOME']
    config = configparser.ConfigParser()
    config.read(project_home + '/config.ini')
    if section is None:
        section = "*SQL Executor (" + filename + ") *"
    # Set up database connection
    cstr = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(
            config[host]['USERNAME'],
            config[host]['PASSWORD'],
            config[host]['HOST'],
            config[host]['PORT'],
            "information_schema")
    db_con = create_engine(cstr)
    if not fk_check:
        db_con.execute("SET FOREIGN_KEY_CHECKS=0")
    # Send start message
    # send_slack_notification(
    #         "Executing Query File: `" + filename + "`", config, section,
    #         "info")
    # when needed, clear out any existing entries in the prod db to make room for inserts - mostly duplicated from insert section below
    if 'delete_sql' in context['templates_dict'] and drop_existing:
        rm_sql_content = context['templates_dict']['delete_sql']
        rm_statements = sqlparse.split(rm_sql_content)
        for sql_statement in rm_statements:
            print(f"preparing query: {sql_statement}")
            single_line_query = sql_statement
            # if the parse is successful we do some sanity checks
            if len(sqlparse.parse(sql_statement)) > 0:
                # Parse SQL
                parsed_statement = sqlparse.parse(sql_statement)[0]
                single_line_query = parse_and_format_sql(parsed_statement)
                ## Based on parameter ignore DROP table commands
                if not drop_existing and parsed_statement.get_type().lower() == 'unknown':
                    if single_line_query.lower().lstrip().startswith("drop"):
                        continue
                    
                if parsed_statement.get_type().lower() == 'insert':
                    # Check if query plan includes full table scan, if it does send an alert
                    query_plan_check = database_helpers.check_query_plan(db_con, single_line_query)
                    if not query_plan_check:
                        message = """
                            Query execution plan involves full table scan: ```{single_line_query} ```
                            """.format(single_line_query=single_line_query)
                        # send_slack_notification(message, config,
                        #                         section,
                        #                         "warning")
                        print(message)
                        # raise Exception(message)

            if not single_line_query.strip():
                continue
            try:
                print(f"executing query: {single_line_query}")
                db_con.execute(single_line_query)
            except Exception as e:
                # send_slack_notification(
                #         """
                # Execution of Query failed: ```{single_line_query} ```
                #     """.format(single_line_query=single_line_query),
                #         config,
                #         section,
                #         "error")
                raise e

            
    # insert portion of merge
    # Get processed template file content
    if 'source_sql' in context['templates_dict']:
        sql_content = context['templates_dict']['source_sql']
        # Extract individual statements from sql file
        sql_statements = sqlparse.split(sql_content)
        for sql_statement in sql_statements:
            print(f"preparing query: {sql_statement}")
            # Certain type of sql are not parsed properly by sqlparse,
            # this is the implicit else to forthcoming if
            single_line_query = sql_statement
            # if the parse is successful we do some sanity checks
            if len(sqlparse.parse(sql_statement)) > 0:
                # Parse SQL
                parsed_statement = sqlparse.parse(sql_statement)[0]
                single_line_query = parse_and_format_sql(parsed_statement)
                ## Based on parameter ignore DROP table commands
                if not drop_existing and parsed_statement.get_type().lower() == 'unknown':
                    if single_line_query.lower().lstrip().startswith("drop"):
                        continue

                # Insert statements are usually insert as select
                # We perform sanity checks on those queries
                if parsed_statement.get_type().lower() == 'insert':
                    # Check if query plan includes full table scan, if it does send an alert
                    query_plan_check = database_helpers.check_query_plan(db_con, single_line_query)
                    if not query_plan_check:
                        message = """
                            Query execution plan involves full table scan: ```{single_line_query} ```
                            """.format(single_line_query=single_line_query)
                        # send_slack_notification(message, config,
                        #                         section,
                        #                         "warning")
                        print(message)
                        # raise Exception(message)

                    collation_check_parameters = db_and_table_as_array(single_line_query)
                    # Check if all text fields in all supplied tables have consistent character set & collation
                    # Stops the process if the collation check fails
                    # This is because joins involving tables with inconsistent collation run forever
                    if not database_helpers.check_encoding_and_collation(db_con, collation_check_parameters):
                        message = """
                            Character set and/or collation mismatch between tables involved in query :
                                {single_line_query}
                                """.format(single_line_query=single_line_query)
                        # send_slack_notification(message, config,
                        #                         section,
                        #                         "warning")
                        raise Exception(message)

                    # Do not run insert statements if it is schema only run
                    if schema_only:
                        continue

            # If empty line move on to next sql
            if not single_line_query.strip():
                continue
            if 'location' in single_line_query or 'assignee' in single_line_query or "FROM (" in single_line_query:
                pass
            else:
                try:
                    print(f"executing query: {single_line_query}")
                    db_con.execute(single_line_query)
                except Exception as e:
                    # send_slack_notification(
                    #         """
                    # Execution of Query failed: ```{single_line_query} ```
                    #     """.format(single_line_query=single_line_query),
                    #         config,
                    #         section,
                    #         "error")
                    raise e
    if not fk_check:
        print(" ")
        db_con.execute("SET FOREIGN_KEY_CHECKS=1")
    db_con.dispose()
    completion_message = """
    Execution for Query file `{filename}` is complete
    """.format(filename=filename)
    # send_slack_notification(completion_message,
    #                         config, "*SQL Executor (" + filename + ")*",
    #                         "success")
#
if __name__ == '__main__':
    # q = "insert into `PatentsView_20220630`.`application` (`application_id`, `patent_id`, `type`, `number`, `country`, `date`) select `id_transformed`, `patent_id`, nullif(trim(`type`), ''), nullif(trim(`number_transformed`), ''), nullif(trim(`country`), ''), case when `date` > date('1899-12-31') and `date` < date_add(current_date, interval 10 year) then `date` else null end from `patent`.`application` where version_indicator<='2021-12-30';"
    q = """create table `PatentsView_20230330`.webtool_comparison_countryI SELECT l.country , p.year , COUNT(DISTINCT inventor_id) AS invCount FROM (SELECT location_id, IF(country = 'AN', 'CW', country) AS country FROM location) l LEFT JOIN patent_inventor pi ON l.location_id = pi.location_id LEFT JOIN patent p ON pi.patent_id = p.patent_id WHERE p.year IS NOT NULL AND l.country IS NOT NULL AND l.country REGEXP '^[A-Z]{2}$' AND l.country NOT IN ('US', 'YU', 'SU') GROUP BY l.country , p.year;"""
    # db_and_table_as_array("INSERT INTO pregrant_publications.publication SELECT * FROM pgpubs_20050101.publication")
    # db_and_table_as_array(q)
    # validate_and_execute(filename='01_04_Application', fk_check=False, source_sql='01_04_Application.sql', **{
    #     "source_sql": "01_04_Application.sql"
    # })
    # from lib.configuration import get_current_config, get_today_dict
    config = get_current_config("granted_patent", **{
        "execution_date": datetime.date(2022, 6, 30)
    })
    # database_name_config = {
    #     'raw_database': config['REPORTING_DATABASE_OPTIONS']['RAW_DATABASE_NAME'],
    #     'reporting_database': config['REPORTING_DATABASE_OPTIONS']['REPORTING_DATABASE_NAME'],
    #     'version_indicator': config['REPORTING_DATABASE_OPTIONS']['VERSION_INDICATOR']
    # }
    # schema_only = config["REPORTING_DATABASE_OPTIONS"]["SCHEMA_ONLY"]
    # if schema_only == "TRUE":
    #     schema_only = True
    # else:
    #     schema_only = False
    # validate_and_execute(filename='01_04_Application', fk_check=False, source_sql='01_04_Application.sql',templates_exts=[".sql"],
    # params=database_name_config, schema_only=schema_only, templates_dict={
    #     'source_sql': '01_04_Application.sql'
    # })

    collation_check_parameters = db_and_table_as_array(q)
    cstr = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(
            config['APP_DATABASE_SETUP']['USERNAME'],
            config['APP_DATABASE_SETUP']['PASSWORD'],
            config['APP_DATABASE_SETUP']['HOST'],
            config['APP_DATABASE_SETUP']['PORT'],
            "information_schema")
    # db_con = create_engine(cstr)
    # database_helpers.check_encoding_and_collation(db_con, collation_check_parameters)

