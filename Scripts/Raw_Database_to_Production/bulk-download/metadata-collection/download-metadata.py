from bs4 import BeautifulSoup as bs
import configparser
import argparse
import MySQLdb
from sqlalchemy import create_engine

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

onnection_string = 'mysql://' + \
        str(user) + ':' + str(passwd) + '@' + \
        str(mydb) + ':' + str(port) + '/' + str(db)
read_engine = create_engine(connection_string,
                                echo=True, encoding='utf-8')
 
fd = args.d[0] 
data = {}
diri = os.listdir(fd)
for d in diri:
    if d.endswith('zip'):
        data[d.replace('.zip','')] = os.path.getsize(fd+d)
 
 
 
inp = open('bulk_downloads_update.txt').read()
inp = inp.replace(args.p[0],args.n[0])
soup = bs(inp.decode('utf-8','ignore'))
rows = soup.findAll('tr')
for row in rows:
    td = row.findAll('td')
    try:
        name = td[0].findAll('a')[0].text
        print(name)
        sizespan = td[0].findAll('span')[0]
        if re.search('GB',sizespan.text):
            filesize = str(np.round(data[name] /1073741824,3)) + ' GB'
        else:
            filesize = str(np.round(data[name] /1048576,3))+' MB'
        sizespan.string = filesize
        cursor.execute('select count(*) from '+name)
        cc = [c[0] for c in list(cursor.fetchall())][0]
        cc = str(int(cc))
        td[2].string = "{:,}".format(int(cc))
    except:
        pass
 
print(soup)
 
exit()
