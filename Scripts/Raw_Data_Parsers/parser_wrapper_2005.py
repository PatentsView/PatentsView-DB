from __future__ import unicode_literals
import argparse
from uspto_parsers import generic_parser_1976_2001_b_examiner, parser_2004_e_examiner,csv_to_mysql,uspc_table,merge_db_script, parser_2005_new_fields_f

parser = argparse.ArgumentParser(description='This program is used to parse USPTO full-text patent grant data for 1976-2004 and also uploading parsed data to MySQL.',epilog='(Example syntax for parsing raw data: python parser_wrapper.py --input-dir "uspto_raw/1976-2001/" --output-dir "uspto_parsed/1976-2001/" --period 1)\n (Example syntax for uploading to MySQL: python parser_wrapper.py --mysql 1 --mysql-input-dir "c:/uspto_parsed/1976-2001/" --mysql-host "localhost" --mysql-username "root" --mysql-passwd "password" --mysql-dbname "uspto")\n (Example syntax to create USPC tables: python parser_wrapper.py --uspc-create 1 --uspc-input-dir "c:/master_classfiles/")\n (Example syntax for USPC upload to MySQL: python parser_wrapper.py --uspc-upload 1 --uspc-upload-dir "c:/master_classfiles" --mysql-host .. --mysql-username .. --mysql-passwd .. --uspc-appdb app_smalltest --uspc-patdb grant_smalltest)' )
parser.add_argument('--input-dir',help='Full path to directory where all patent raw files are located (TXT or XML format; as downloaded from Google Patents or ReedTech).')
parser.add_argument('--output-dir',help='Full path to directory where to write all output csv files.')
parser.add_argument('--period',default="5",choices=['1','2', '3'],help='Enter 1 for 1976-2001 or 2 for 2002-2004 or 3 for 2005+.')
parser.add_argument('--mysql',default="0",choices=['1','0'],required=False,help='If you want to upload resultant files into MySQL - please specify "1" here and MySQL output-dir and connection data.')
parser.add_argument('--mysql-input-dir',help="Full path to directory with all output csv files to upload to MySQL.")
parser.add_argument('--mysql-host',help="Specify MySQL host.")
parser.add_argument('--mysql-username',help="Specify MySQL username.")
parser.add_argument('--mysql-passwd',help="Specify MySQL password.")
parser.add_argument('--mysql-dbname',help="Specify MySQL database name.")
parser.add_argument('--uspc-create',default='0',choices=['1','0'],help='You have to enter 1 if you want to create USPC tables to upload: uspc, mainclass, subclass')
parser.add_argument('--uspc-input-dir',help="Full path to directory where classification master files sit - these should be ctaf....txt and mcfpat....txt. Output directory will be the same as this input one.")
parser.add_argument('--uspc-upload',default='0',choices=['1','0'],help="Please enter 1 if you want to upload classification tables to MySQL DB after processing them")
parser.add_argument('--uspc-upload-dir',help="Full path to directory where processed classification files sit")
parser.add_argument('--appdb', default=None, help = "Applications DB name if used.")
parser.add_argument('--patdb', default=None, help = "Grants DB name if used.")
parser.add_argument('--cpc-upload',default='0',choices=['1','0'],help="Please enter 1 if you want to upload CPC classification tables to MySQL DB after processing them")
parser.add_argument('--cpc-upload-dir',help="Full path to directory where processed CPC classification files would be downloaded and used")
parser.add_argument('--merge-db',default='0',choices=['1','0'],help="Please enter 1 if you want to merge DBs to speed up the PatentsProcessor")
parser.add_argument('--sourcedb',help="Please provide what DBs you want to merge, comma-separate list, e.g. app_smalltest_1,app_smalltest2,app_smalltest3,etc.")
parser.add_argument('--targetdb',help="Please provide name of target DB")



params = parser.parse_args()

if int(params.period) == 1:
    generic_parser_1976_2001_b_examiner.parse_patents(params.input_dir,params.output_dir)

elif int(params.period) == 2:
    parser_2004_e_examiner.parse_patents(params.input_dir,params.output_dir)

elif int(params.period)==3:
	parser_2005_new_fields_f.parse_patents(params.input_dir, params.output_dir)

elif int(params.mysql) == 1 and int(params.period) not in range(1,4):
    csv_to_mysql.mysql_upload(params.mysql_host,params.mysql_username,params.mysql_passwd,params.mysql_dbname,params.mysql_input_dir)

elif int(params.uspc_create) == 1 and int(params.period) not in range(1,4):
    uspc_table.uspc_table(params.uspc_input_dir)

elif int(params.uspc_upload) == 1 and int(params.period) not in range(1,4):
    csv_to_mysql.upload_uspc(params.mysql_host,params.mysql_username,params.mysql_passwd,params.appdb,params.patdb,params.uspc_upload_dir)

elif int(params.cpc_upload) == 1 and int(params.period) not in range(1,4):
    csv_to_mysql.upload_cpc(params.mysql_host,params.mysql_username,params.mysql_passwd,params.appdb,params.patdb,params.cpc_upload_dir)

elif int(params.merge_db) == 1 and int(params.period) not in range(1,4):
    merge_db_script.merge_db_pats(params.mysql_host,params.mysql_username,params.mysql_passwd,params.sourcedb,params.targetdb)
else:
    print "Please check you parameters. Consult python parser_wrapper.py --help if necessary."