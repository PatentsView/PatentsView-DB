import argparse
from uspto_parsers import USPTO_Claims_2005_2014

parser = argparse.ArgumentParser(description='This program is used to parse full-text patent grant data claims for 2005+ and upload directly to MySQL.',epilog='(Example syntax for processing 2005+ claims and uploading to MySQL: python parser_wrapper.py --mysql 1 --input-dir "c:/uspto_raw/2005-present/" --mysql-host "localhost" --mysql-username "root" --mysql-passwd "password" --mysql-dbname "uspto")' )
parser.add_argument('--input-dir',required=True,help='Full path to directory where 2005+ patent raw files are located (XML format; as downloaded from Google Patents or ReedTech).')
parser.add_argument('--mysql-host',required=True,help="Specify MySQL host.")
parser.add_argument('--mysql-username',required=True,help="Specify MySQL username.")
parser.add_argument('--mysql-passwd',required=True,help="Specify MySQL password.")
parser.add_argument('--mysql-dbname',required=True,help="Specify MySQL database name.")
params = parser.parse_args()

USPTO_Claims_2005_2014.claims_2005(params.input_dir,params.mysql_host,params.mysql_username,params.mysql_passwd,params.mysql_dbname)    