import sqlparse
import re
import os
import configparser
from Development.helpers import database_helpers
from Development.helpers import general_helpers
import pprint
from slackclient import SlackClient


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


def validate_and_execute(filename, **context):
    ## Initialization from config files
    project_home = os.environ['PACKAGE_HOME']
    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')
    # Create slack client
    slack_token = config["SLACK"]["API_TOKEN"]
    slack_client = SlackClient(slack_token)
    slack_channel = config["SLACK"]["CHANNEL"]
    # Set up database connection
    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'],
                                           config['DATABASE']['PASSWORD'], "information_schema").connect()
    # Send start message
    general_helpers.send_slack_notification(
        "Executing Query File: `" + filename + "`", slack_client, slack_channel, "*Reporting DB (" + filename + ") *",
        "info")
    # Get processed template file content
    sql_content = context['templates_dict']['source_sql']
    # Extract individual statements from sql file
    sql_statements = sqlparse.split(sql_content)
    for sql_statement in sql_statements:
        # Certain type of sql are not parsed properly by sqlparse,
        # this is the implict else to forthcoming if
        single_line_query = sql_statement
        # if the parse is successful we do some sanity checks
        if len(sqlparse.parse(sql_statement)) > 0:
            # Parse SQL
            parsed_statement = sqlparse.parse(sql_statement)[0]
            single_line_query = parse_and_format_sql(parsed_statement)
            ## Uncomment this section to ignore DROP table commands
            # if parsed_statement.get_type().lower() == 'unknown':
            #     if single_line_query.lower().lstrip().startswith("drop"):
            #         continue

            # Log file output
            print(single_line_query)

            # Insert statements are usually insert as select
            # We perform sanity checks on those queries
            if parsed_statement.get_type().lower() == 'insert':
                # Check if query plan includes full table scan, if it does send an alert
                if not database_helpers.check_query_plan(db_con, single_line_query):
                    message = "Query execution plan involves full table scan: ```" + single_line_query + "```"
                    general_helpers.send_slack_notification(message, slack_client, slack_channel,
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
                    general_helpers.send_slack_notification(message, slack_client, slack_channel,
                                                            "*Reporting DB (" + filename + ") *",
                                                            "warning")
                    raise Exception(message)

                # Uncomment this below section to run data less table creation process as test run
                continue

        # If empty line move on to next sql
        if not single_line_query.strip():
            continue
        try:
            db_con.execute(single_line_query)
        except Exception as e:
            general_helpers.send_slack_notification(
                "Execution of Query failed: ```" + single_line_query + "```", slack_client, slack_channel,
                "*Reporting DB (" + filename + ")* ",
                "error")
            raise e
    db_con.close()
    general_helpers.send_slack_notification(
        "Execution for Query File: `" + filename + "` is complete", slack_client, slack_channel,
        "*Reporting DB (" + filename + ")*",
        "success")
