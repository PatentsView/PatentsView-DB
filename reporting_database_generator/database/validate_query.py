import sqlparse
import re
import os
import configparser

from sqlalchemy import create_engine
from lib.notifications import send_slack_notification
from lib import database_helpers


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


def db_and_table_as_array(single_line_query):
    # Identify all tables used in the query for collation check
    # PatentsView tables
    table_finder_1 = re.compile("`PatentsView_[0-9]{8}[^`]*`.`[^`]+`")
    tables_1 = table_finder_1.findall(single_line_query)
    # patent tables
    table_finder_2 = re.compile("`patent_[0-9]{8}`.`[^`]+`")
    tables_2 = table_finder_2.findall(single_line_query)
    tables = tables_1 + tables_2
    collation_check_parameters = []
    # Split dbname.table_name into parameters array
    for table in tables:
        collation_check_parameters += [x.replace("`", "") for x in table.split(".")]
    return collation_check_parameters


def validate_and_execute(filename=None, schema_only=False, drop_existing=True,
                         fk_check=True, **context):
    print(filename)
    print(schema_only)
    print(context)
    ## Schema only run setting
    # schema_only=context["schema_only"]
    ## Initialization from config files
    project_home = os.environ['PACKAGE_HOME']
    config = configparser.ConfigParser()
    config.read(project_home + '/config.ini')

    # Set up database connection
    cstr = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(config['DATABASE_SETUP']['USERNAME'],
                                                                        config['DATABASE_SETUP']['PASSWORD'],
                                                                        config['DATABASE_SETUP']['HOST'],
                                                                        config['DATABASE_SETUP']['PORT'],
                                                                        "information_schema")
    db_con = create_engine(cstr)
    if not fk_check:
        db_con.execute("SET FOREIGN_KEY_CHECKS=0")
    # Send start message
    send_slack_notification(
        "Executing Query File: `" + filename + "`", config, "*Reporting DB (" + filename + ") *",
        "info")
    # Get processed template file content
    sql_content = context['templates_dict']['source_sql']
    # Extract individual statements from sql file
    sql_statements = sqlparse.split(sql_content)
    for sql_statement in sql_statements:
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

            # Log file output
            print(single_line_query)

            # Insert statements are usually insert as select
            # We perform sanity checks on those queries
            if parsed_statement.get_type().lower() == 'insert':
                # Check if query plan includes full table scan, if it does send an alert
                query_plan_check = database_helpers.check_query_plan(db_con, single_line_query)
                if not query_plan_check:
                    message = "Query execution plan involves full table scan: ```" + single_line_query + "```"
                    send_slack_notification(message, config,
                                            "*Reporting DB (" + filename + ")* ",
                                            "warning")
                    print(message)
                    # raise Exception(message)
                #
                collation_check_parameters = db_and_table_as_array(single_line_query)
                # Check if all text fields in all supplied tables have consistent character set & collation
                # Stops the process if the collation check fails
                # This is because joins involving tables with inconsistent collation run forever
                if not database_helpers.check_encoding_and_collation(db_con, collation_check_parameters):
                    message = "Character set and/or collation mismatch between tables involved in query :```" + single_line_query + "```"
                    send_slack_notification(message, config,
                                            "*Reporting DB (" + filename + ") *",
                                            "warning")
                    raise Exception(message)

                # # Do not run insert statements if it is schema only run
                if schema_only:
                    continue

        # If empty line move on to next sql
        if not single_line_query.strip():
            continue
        try:
            db_con.execute(single_line_query)
        except Exception as e:
            send_slack_notification("Execution of Query failed: ```" + single_line_query + "```", config,
                                    "*Reporting DB (" + filename + ")* ", "error")
            raise e
    if not fk_check:
        db_con.execute("SET FOREIGN_KEY_CHECKS=1")
    db_con.dispose()
    send_slack_notification(
        "Execution for Query File: `" + filename + "` is complete", config, "*Reporting DB (" + filename + ")*",
        "success")
