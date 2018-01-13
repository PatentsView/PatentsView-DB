from bs4 import BeautifulSoup as bs
import configparser
import argparse
import MySQLdb
import os
from sqlalchemy import create_engine
<<<<<<< Updated upstream
import time
import pprint
import pandas as pd
import numpy as np
import re
=======
import csv
import pprint
>>>>>>> Stashed changes

parser = argparse.ArgumentParser(
    description='Process table parameters')
parser.add_argument(
'-d',
type=str,
nargs=1,
help='Download files Directory')
parser.add_argument(
'-c',
type=str,
nargs=1,
help='File containing database config in INI format')
parser.add_argument(
'-p',
type=str,
nargs=1,
help='Previous database update datestamp')
parser.add_argument(
'-n',
type=str,
nargs=1,
help='Next database update datestamp')
args = parser.parse_args()

config = configparser.ConfigParser()
config.read(args.c[0])

user=config["dev_database"]["mysql_db_user"]
passwd=config["dev_database"]["mysql_db_password"]
mydb=config["dev_database"]["mysql_db_host"]
port=config["dev_database"]["mysql_db_port"]
db=config["dev_database"]["mysql_db_name"] 

connection_string = 'mysql://' + \
        str(user) + ':' + str(passwd) + '@' + \
        str(mydb) + ':' + str(port) + '/' + str(db)
read_engine = create_engine(connection_string,
                                echo=True, encoding='utf-8')
 
file_name = args.d[0] 
data = {}


with open(file_name) as fp:
	for line in fp:
		splits=line.split()
		div_count=0;
		size_value=int(splits[1]);
		unit_value="bytes"
		while (size_value>1024):
			size_value=(1.0*size_value)/1024
			div_count+=1
		if div_count==1:
			unit_value="KB"
		elif div_count==2:
			unit_value="MB"
		elif div_count==3:
			unit_value="GB"
		elif div_count==4:
			unit_value="TB"
		elif div_count==5:
			unit_value="PB"
		
		data[splits[0].split("\\")[-1].split(".")[0]]=str(round(size_value,3))+" "+unit_value 
 
inp = open('/code/bulk_downloads_update.txt').read()
inp = inp.replace(args.p[0],args.n[0])
soup = bs(inp)
rows = soup.findAll('tr')
for row in rows:
    td = row.findAll('td')
    try:
        name = td[0].findAll('a')[0].text
        print(name)
        sizespan = td[0].findAll('span')[0]
        sizespan.string =data[name] 
        query='select count(*) cnt from '+ name
        count_data = pd.read_sql(query, con=read_engine)
        cc = count_data["cnt"][0]
        cc = str(int(cc))
        td[2].string = "{:,}".format(int(cc))
    except:
        pass
 
Html_file= open("/code/bulk-download-"+str(args.n[0])+".html","w")
Html_file.write(str(soup))
Html_file.close()
