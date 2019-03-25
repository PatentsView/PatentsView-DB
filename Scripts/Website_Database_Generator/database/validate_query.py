import sqlparse
import re
from . import db_con
from Development.helpers import database_helpers
from Development.helpers import general_helpers
import pprint
import configparser
import os



def validate_and_execute(filename, slack_client, slack_channel, **context):
    general_helpers.send_slack_notification(
        "Executing Query File: `" + filename + "`", slack_client, slack_channel, section="Reporting DB Update", level="info")
    sql_content = context['templates_dict']['source_sql']
    sql_statements = sqlparse.split(sql_content)
    for sql_statement in sql_statements:
        query_lines = []
        single_line_query = sql_statement
        if len(sqlparse.parse(sql_statement)) > 0:
            parsed_statement = sqlparse.parse(sql_statement)[0]
            for sql_line in parsed_statement.value.split("\n"):
                if not sql_line.startswith("#"):
                    query_lines.append(" ".join(sql_line.split()))
            if len(query_lines) < 1:
                continue
            single_line_query = " ".join(query_lines).replace("[\s]+", " ")
            if not single_line_query.strip():
                continue
            if parsed_statement.get_type().lower() == 'unknown':
                if single_line_query.lower().lstrip().startswith("drop"):
                    continue
            if parsed_statement.get_type().lower() == 'insert':
                if not database_helpers.check_query_plan(db_con, single_line_query):
                    message = "Query execution plan involves full table scan: ```" + single_line_query + "```"
                    general_helpers.send_slack_notification(message, slack_client, slack_channel, section="Reporting DB Update", level="warning")
                    # raise Exception(message)
                table_finder_1 = re.compile("`PatentsView_[0-9]{8}[^`]*`.`[^`]+`")
                tables_1 = table_finder_1.findall(single_line_query)
                table_finder_2 = re.compile("`patent_[0-9]{8}`.`[^`]+`")
                tables_2 = table_finder_2.findall(single_line_query)
                tables = tables_1 + tables_2
                collation_check_parameters = []
                for table in tables:
                    collation_check_parameters += [x.replace("`", "") for x in table.split(".")]
                if not database_helpers.check_encoding_and_collation(db_con, collation_check_parameters):
                    message = "Character set and/or collation mismatch between tables involved in query :```" + single_line_query + "```"
                    general_helpers.send_slack_notification(message, slack_client, slack_channel, section="Reporting DB Update", level="warning")
                    raise Exception(message)
            # general_helpers.send_slack_notification(
            #     "Executing Query : ```" + single_line_query + "```", "info")
            else:
                try:
                    db_con.execute(single_line_query)
                except Exception as e:
                    general_helpers.send_slack_notification(
                        "Execution of Query failed: ```" + single_line_query + "```", slack_client, slack_channel, section="Reporting DB Update", level="error")
                    raise e
        print(single_line_query)
        db_con.execute(single_line_query)
    general_helpers.send_slack_notification(
        "Execution for Query File: `" + filename + "` is complete", slack_client, slack_channel, section="Reporting DB Update", level="success")
