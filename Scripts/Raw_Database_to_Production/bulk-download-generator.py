import argparse
import MySQLdb
import pandas as pd

from sqlalchemy import create_engine
import os


host = os.environ['PV_INJ_HOST']
user = os.environ['PV_INJ_USERNAME']
password = os.environ['PV_INJ_PASSWORD']
port = os.environ['PV_INJ_PORT']



def export_table(t, db_connection=None, column_list=None):
    select_columns = ",".join(column_list) if column_list is not None else "*"
    query = "select " + select_columns + " from " + str(t)
    filename = str(t) + ".tsv"
    print(query)
    table_data = pd.read_sql(query, con=db_connection)
    table_data.to_csv(filename, sep="\t", index=False)


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
    connection_string = 'mysql://' + \
        str(user) + ':' + str(password) + '@' + \
        str(host) + ':' + str(port) + '/' + str(database)
    read_engine = create_engine(connection_string,
                                echo=True)
    try:
        for table in tables:
            export_table(table, read_engine)
    finally:
        read_engine.dispose()

if __name__ == "__main__":
    main()
