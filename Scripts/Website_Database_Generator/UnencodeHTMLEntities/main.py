import argparse
import mysql.connector
from htmlentities import HTMLEntities


def unescape_column(read_cursor, write_cursor, table_name, column_name, id_column_name):

    html_entities = HTMLEntities()
    record_count = 0

    print("Processing " + table_name + "." + column_name)

    sql = "select " + id_column_name + ", " + column_name + " from " + table_name + " where " + column_name +\
          " regexp '&[[:alnum:]]+;';"

    read_cursor.execute(sql)

    for (id_value, text_value) in read_cursor:
        unescaped_text_value = html_entities.unescape(text_value)
        updatesql = "update " + table_name + " set " + column_name + " = %s where " + id_column_name + " = %s;"
        write_cursor.execute(updatesql, (unescaped_text_value, id_value))
        record_count += 1
        if record_count % 100 == 0:
            print(record_count)

    if record_count % 100 > 0:
        print(record_count)


def process_columns(host_name, port_number, username, password, database_name, table_name, column_name, id_column_name):
    read_connection = mysql.connector.connect(
        host=host_name,
        port=port_number,
        user=username,
        password=password,
        database=database_name,
        autocommit=True
    )
    read_cursor = read_connection.cursor()

    write_connection = mysql.connector.connect(
        host=host_name,
        port=port_number,
        user=username,
        password=password,
        database=database_name,
        autocommit=True
    )
    write_cursor = write_connection.cursor()

    unescape_column(read_cursor, write_cursor, table_name, column_name, id_column_name)


parser = argparse.ArgumentParser(description="This script will attempt to remove HTML entities and USPTO custom "
                                             "entities from the table column specified.  All parameters are required.",
                                 epilog="Example:\n  main.py 1.2.3.4 5 myusername mypwd thedatabase patent title "
                                        "patent_id")

parser.add_argument("host", help="MySQL host as host name or ip address.")
parser.add_argument("port", help="MySQL host port.")
parser.add_argument("username", help="MySQL username.")
parser.add_argument("password", help="MySQL password.")
parser.add_argument("database", help="Database that contains the columns to be processed.")
parser.add_argument("table", help="Name of table to be processed.")
parser.add_argument("column", help="Name of column to be processed.")
parser.add_argument("id", help="Name of column that represents the identifier for the table to be processed.")

params = parser.parse_args()

process_columns(params.host, params.port, params.username, params.password, params.database, params.table,
                params.column, params.id)
