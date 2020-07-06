def check_metadata_lock(db_con):
    col_data = db_con.execute('SELECT * from information_schema.PROCESSLIST')
    for process in col_data:
        if "lock" in process[4].lower():
            return False
    return True


## Check if query plan includes full table scan
def check_query_plan(db_con, query):
    plan_data = db_con.execute("EXPLAIN EXTENDED " + query)
    for plan_row in plan_data:
        if plan_row[3].lower() == "all":
            return False
    return True


## Check if given tables in given schema all have same character set and collation
def check_encoding_and_collation(db_con, tables_list):
    where_part = "(TABLE_SCHEMA = %s AND TABLE_NAME = %s )"
    count = int(len(tables_list) / 2)
    table_where_string = " OR ".join(count * [where_part])
    collation_information = db_con.execute(
        "SELECT DISTINCT CHARACTER_SET_NAME, COLLATION_NAME from information_schema.COLUMNS where DATA_TYPE in ('varchar') AND ("
        + table_where_string + ") AND CHARACTER_SET_NAME is not null AND COLLATION_NAME is not null", tables_list)
    collation_data = collation_information.fetchall()
    if len(collation_data) > 1:
        return False
    return True
