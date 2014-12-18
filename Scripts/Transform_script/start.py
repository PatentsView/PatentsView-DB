from __future__ import unicode_literals
import argparse
from script import transform_db

parser = argparse.ArgumentParser(description='This program is used to transform the final USPTO DB' )
parser.add_argument('--mysql-host',help="Specify MySQL host.")
parser.add_argument('--mysql-username',help="Specify MySQL username.")
parser.add_argument('--mysql-passwd',help="Specify MySQL password.")
parser.add_argument('--transform',default='0',choices=['1','0'],help="Specify 1 to ensure transform script is started.")
parser.add_argument('--transform-upload-dir',help="Full path to directory where processed application files for broken records sit.")
parser.add_argument('--appdb', default=None, help = "Applications DB name if used.")
parser.add_argument('--patdb', default=None, help = "Grants DB name if used.")
parser.add_argument('--update-appnums',default='0',choices=['1','0'],help="Specify 1 to ensure script updating application numbers is started.")
parser.add_argument('--appnums-upload-dir',help="Full path to directory where correct application numbers will be uploaded and used.")

params = parser.parse_args()

if int(params.transform) == 1:
    transform_db.transform(params.transform_upload_dir,params.mysql_host,params.mysql_username,params.mysql_passwd,params.appdb,params.patdb)
elif int(params.update_appnums) == 1:
    transform_db.update_appnums(params.appnums_upload_dir,params.mysql_host,params.mysql_username,params.mysql_passwd,params.appdb)    
else:
    print "Please check your parameters. Consult python start.py --help if necessary."