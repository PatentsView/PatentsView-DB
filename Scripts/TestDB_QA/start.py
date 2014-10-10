import argparse
import patents_QA,applications_QA

parser = argparse.ArgumentParser(description='This program is used to construct readable CSV spreadsheets by joining multiple MySQL tables for QA.' )
parser.add_argument('--type',required=True,choices=['1','2'],help="Please choose 1 for applications database and 2 for patent grants database.")
parser.add_argument('--output-dir',required=True,help='Full path to directory where to write all output CSV files.')
parser.add_argument('--mysql-host',required=True,help="Specify MySQL host.")
parser.add_argument('--mysql-username',required=True,help="Specify MySQL username.")
parser.add_argument('--mysql-passwd',required=True,help="Specify MySQL password.")
parser.add_argument('--mysql-dbname',required=True,help="Specify MySQL database name.")
params = parser.parse_args()

if int(params.type) == 1:
    applications_QA.generate_app_qa(params.mysql_host,params.mysql_username,params.mysql_passwd,params.mysql_dbname,params.output_dir)
else:
    patents_QA.generate_grants_qa(params.mysql_host,params.mysql_username,params.mysql_passwd,params.mysql_dbname,params.output_dir)