import argparse
import MySQLdb
import pandas as pd
import configparser
from sqlalchemy import create_engine
import os
import csv

#host = os.environ['PV_INJ_HOST']
#user = os.environ['PV_INJ_USERNAME']
#password = os.environ['PV_INJ_PASSWORD']
#port = os.environ['PV_INJ_PORT']



def export_table(t, db_connection=None, column_list=None):
    select_columns = ",".join(column_list) if column_list is not None else "*"
    query = "select " + select_columns + " from " + str(t)
    filename = "/data/"+str(t) + ".tsv"
    print(query)
    print(filename)
    table_data = pd.read_sql(query, con=db_connection)
    table_data.to_csv(filename,sep="\t", index=False,quoting=csv.QUOTE_NONNUMERIC)

def main():
    parser = argparse.ArgumentParser(
        description='Process table parameters')
    parser.add_argument(
        '-t',
        type=str,
        nargs=1,
        help='File containing table names to create bulk downloads')
    parser.add_argument(
        '-d',
        type=str,
        nargs=1,
        help='Database name (required)')
    parser.add_argument(
        '-c',
        type=str,
        nargs=1,
        help='File containing database config in INI format')
    args = parser.parse_args()
    default_tables = """application assignee botanic brf_sum_text claim cpc_current cpc_group cpc_subsection cpc_subgroup draw_desc_text figures foreign_priority government_interest foreigncitation government_organization inventor ipcr lawyer location location_assignee location_inventor mainclass mainclass_current nber nber_category nber_subcategory non_inventor_applicant otherreference patent patent_assignee patent_contractawardnumber patent_govintorg patent_inventor patent_lawyer pct_data rawassignee rawexaminer rawinventor rawlawyer rawlocation rel_app_text subclass subclass_current us_term_of_grant usapplicationcitation uspatentcitation uspc uspc_current usreldoc wipo wipo_field""".split(
        " ")
    tables = []
    database = ""
    if not args.d:
        parser.print_help()
        exit(1)
    else:
        database = args.d[0]
    if not args.t:
        tables = default_tables
    else:
        lines = []
        try:
            with open(args.t[0], "r") as tab_fd:
                lines = tab_fd.readlines()
        except:
            tables = default_tables
        tables = [line.rstrip("\n") for line in lines]
    config = configparser.ConfigParser()
    config.read(args.c[0])
    print(config)
    user=config["dev_database"]["mysql_db_user"]
    password=config["dev_database"]["mysql_db_password"]
    host=config["dev_database"]["mysql_db_host"]
    port=config["dev_database"]["mysql_db_port"]
    database=config["dev_database"]["mysql_db_name"]
                                
    connection_string = 'mysql://' + \
        str(user) + ':' + str(password) + '@' + \
        str(host) + ':' + str(port) + '/' + str(database)
    read_engine = create_engine(connection_string,
                                echo=True, encoding='utf-8')
    for table in tables:
            export_table(table, read_engine)
    read_engine.dispose()

if __name__ == "__main__":
    main()
