import sqlparse
import re
import regex
from . import db_con
from Development.helpers import database_helpers
from Development.helpers import general_helpers
def validate_and_execute(filename,**context):
    sql=context['templates_dict']['source_sql']
    general_helpers.send_slack_notification(
        "Executing Query File: `" + filename+ "`", "info")
    sql_content=open(sql).read()
    sql_statements=sqlparse.split(sql_content)
    for sql_statement in sql_statements:
        query_lines = []
        for sql_line in sql_statement.value.split("\n"):
            if not sql_line.startswith("#"):
                query_lines.append(" ".join(sql_line.split()))
        single_line_query = " ".join(query_lines).replace("[\s]+", " ")
        if sql_statement.get_type().lower() == 'insert':
            if not database_helpers.check_query_plan(single_line_query):
                message="Query execution plan involves full table scan: `"+single_line_query+"`"
                general_helpers.send_slack_notification(message, "warning")
                raise AirflowException(message)
        table_finder= re.compile("`[^`]+`.`[^`]+`")
        tables=table_finder.findall(single_line_query)
        collation_check_parameters=[]
        for table in tables:
            collation_check_parameters+=[x.replace("`", "") for x in table.split("/")]
        if not database_helpers.check_encoding_and_collation(db_con, collation_check_parameters):
            message= "Character set and/or collation mismatch between tables involved in query :`" + single_line_query + "`"
            general_helpers.send_slack_notification(message, "warning")
            raise AirflowException(message)
        general_helpers.send_slack_notification(
                "Executing Query : `" + single_line_query + "`", "warning")
        db_con.execute(single_line_query)







