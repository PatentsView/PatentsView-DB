import argparse
from uspto_parsers import generic_parser_1976_2001,generic_parser_2002_2004,csv_to_mysql,uspc_table

parser = argparse.ArgumentParser(description='This program is used to parse USPTO full-text patent grant data for 1976-2004 and also uploading parsed data to MySQL.',epilog='(Example syntax for parsing raw data: python parser_wrapper.py --input-dir "uspto_raw/1976-2001/" --output-dir "uspto_parsed/1976-2001/" --period 1)\n (Example syntax for uploading to MySQL: python parser_wrapper.py --mysql 1 --mysql-input-dir "c:/uspto_parsed/1976-2001/" --mysql-host "localhost" --mysql-username "root" --mysql-passwd "password" --mysql-dbname "uspto")' )
parser.add_argument('--input-dir',help='Full path to directory where all patent raw files are located (TXT or XML format; as downloaded from Google Patents or ReedTech).')
parser.add_argument('--output-dir',help='Full path to directory where to write all output csv files.')
parser.add_argument('--period',default="5",choices=['1','2'],help='Enter 1 for 1976-2001 or 2 for 2002-2004.')
parser.add_argument('--mysql',default="0",choices=['1','0'],required=False,help='If you want to upload resultant files into MySQL - please specify "1" here and MySQL output-dir and connection data.')
parser.add_argument('--mysql-input-dir',help="Full path to directory with all output csv files to upload to MySQL.")
parser.add_argument('--mysql-host',help="Specify MySQL host.")
parser.add_argument('--mysql-username',help="Specify MySQL username.")
parser.add_argument('--mysql-passwd',help="Specify MySQL password.")
parser.add_argument('--mysql-dbname',help="Specify MySQL database name.")
parser.add_argument('--uspc-create',default='0',choices=['1','0'],help='You have to enter 1 if you want to create USPC tables to upload: uspc, mainclass, subclass')
parser.add_argument('--uspc-input-dir',help="Full path to directory where classification master files sit - these should be ctaf....txt and mcfpat....txt")
params = parser.parse_args()

if int(params.period) == 1:
    generic_parser_1976_2001.parse_patents(params.input_dir,params.output_dir)
if int(params.period) == 2:
    generic_parser_2002_2004.parse_patents(params.input_dir,params.output_dir)

if int(params.mysql) == 1 and int(params.period) not in range(1,3):
    csv_to_mysql.mysql_upload(params.mysql_host,params.mysql_username,params.mysql_passwd,params.mysql_dbname,params.mysql_input_dir)

if int(params.uspc_create) == 1 and int(params.period) not in range(1,3):
    uspc_table.uspc_table(params.uspc_input_dir)

