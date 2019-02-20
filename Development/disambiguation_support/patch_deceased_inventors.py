import csv
output_folder = '/project/August_2018/disambig_out'
import configparser
config = configparser.ConfigParser()
config.read('/project/Development/config.ini')
import sys
sys.path.append('/project/Development')
from helpers import general_helpers
db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

cursor = db_con.connect()
col_data = cursor.execute('show columns from rawinventor')
cols = [c[0] for c in col_data]
cursor.close()

outp = csv.writer(open('{}/rawinventor.tsv'.format(output_folder),'w'),delimiter='\t')
outp.writerow(cols + ['deceased'])
print('getting data')
cursor = db_con.connect()
existing_data = cursor.execute("select * from rawinventor;")
cursor.close()

print('here')
for_output = []
for e, row in enumerate(existing_data):
    data = [item for item in row]
    if 'deceased' in data[5]:
        data.append(True)
    else:
        data.append(False)
    if e%500000 ==0:
        print(e)

outp.writerows(for_output)