import os
from sqlalchemy import create_engine
import configparser


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
    print(tables_list)
    table_schema_list = []
    table_name_list = []
    for i, j in tables_list:
        table_schema_list.append(i)
        table_name_list.append(j)
    table_schema_list = set(table_schema_list)
    table_name_list = set(table_name_list)
    table_schema_list_str = str(table_schema_list).replace("{", "(").replace("}", ")")
    table_name_list_str = str(table_name_list).replace("{", "(").replace("}", ")")
    if tables_list == "":
        return True
    else:
        query = f"""
                SELECT DISTINCT CHARACTER_SET_NAME, COLLATION_NAME 
                from information_schema.COLUMNS where DATA_TYPE in ('varchar') 
                    AND TABLE_SCHEMA in {table_schema_list_str} 
                    AND TABLE_NAME in {table_name_list_str} 
                    AND CHARACTER_SET_NAME is not null 
                    AND COLLATION_NAME is not null
                    """
        print(query)
        collation_information = db_con.execute(query)
    collation_data = collation_information.fetchall()
    print(collation_data)
    if len(collation_data) > 1:
        if collation_data[0][0] == collation_data[1][0]:
            return True
        else:
            return False
    else:
        return True



def get_dataframe_from_pymysql_cursor(connection, query):
    import pandas as pd
    if not connection.open:
        connection.connect()
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = pd.DataFrame(cursor.fetchall(),
                               columns=[i[0] for i in cursor.description])
    return results

if __name__ == '__main__':
    project_home = os.environ['PACKAGE_HOME']
    config = configparser.ConfigParser()
    config.read(project_home + '/config.ini')
    # Set up database connection
    cstr = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(
            config['DATABASE_SETUP']['USERNAME'],
            config['DATABASE_SETUP']['PASSWORD'],
            config['DATABASE_SETUP']['HOST'],
            config['DATABASE_SETUP']['PORT'],
            "information_schema")
    db_con = create_engine(cstr)
    check_encoding_and_collation(db_con, [('patent', 'location'), ('PatentsView_20230330', 'temp_id_mapping_location')] )
