import MySQLdb
import os
import csv
import sys
sys.path.append('/project/Development')
from helpers import general_helpers

import configparser
config = configparser.ConfigParser()
config.read('/project/Development/config.ini')
disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_out')

if not os.path.exists(disambig_folder):
    os.makedirs(disambig_folder)


key_file = config['DISAMBIGUATION_CREDENTIALS']['KEY_FILE']

os.system('scp -i "{}" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/data/multi-canopy-output/clean_inventor_results.txt {}/inventor_disambiguation.tsv'.format(key_file, disambig_folder))

os.system('scp -i "{}" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/exp_out/assignee/disambiguation.post_processed.tsv {}/assignee_disambiguation.tsv'.format(key_file, disambig_folder))

os.system('scp -i "{}" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/exp_out/location_post_processed.tsv {}/location_disambiguation.tsv'.format(key_file, disambig_folder))



