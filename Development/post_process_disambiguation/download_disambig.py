import MySQLdb
import os
import csv
import sys
sys.path.append('/usr/local/airflow/PatentsView-DB/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers

import configparser
config = configparser.ConfigParser()
config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')
disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_out')

if not os.path.exists(disambig_folder):
    os.makedirs(disambig_folder)


key_file = config['DISAMBIGUATION_CREDENTIALS']['KEY_FILE']

os.system('scp -i "{}" centos@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/data/multi-canopy-output/clean_inventor_results.txt {}/inventor_disambiguation.tsv'.format(key_file, disambig_folder))

os.system('scp -i "{}" centos@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/exp_out/assignee/disambiguation.post_processed.tsv {}/assignee_disambiguation.tsv'.format(key_file, disambig_folder))

os.system('scp -i "{}" centos@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/exp_out/location_post_processed_dec_12.tsv {}/location_disambiguation.tsv'.format(key_file, disambig_folder))



