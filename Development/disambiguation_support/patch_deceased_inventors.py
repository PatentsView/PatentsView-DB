import csv
import os
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')
import sys
from Development.helpers import general_helpers
db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

output_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'disambig_out')


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