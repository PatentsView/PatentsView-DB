import pandas as pd
import os
import sys
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read('Development/config.ini')



parsed_folders = '/project/data/parsed'
#parsed_folders = '{}/parsed'.format(config['FOLDERS']['WORKING_FOLDER'])


gi_folder = '{}/government_interest'.format(config['FOLDERS']['WORKING_FOLDER'])

if not os.path.exists(gi_folder):
    os.mkdir(gi_folder)


gi_data = pd.DataFrame(columns = ['patent_id', 'Twin_Arch', 'Title', 'gi_statement'])
for folder in os.listdir(parsed_folders):
    print(folder)
    data = pd.read_csv('{}/{}/government_interest.csv'.format(parsed_folders, folder), delimiter = '\t')
    gi_data = gi_data.append(data, sort=False)
         


gi_data.to_csv('{}/merged_csvs.csv'.format(gi_folder), index = False, encoding = 'utf-8')


#write the config file that the perl script needs
#this is hacky and I don't like it
with open('/usr/local/airflow/config.txt', 'w') as myfile:
    myfile.write('{}/government_interest'.format(config['FOLDERS']['WORKING_FOLDER']))


