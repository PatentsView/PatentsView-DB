import os
import re
import pandas as pd
import csv
import sys
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')
disambig_folder = '{}/disambig_inputs'.format(config['FOLDERS']['WORKING_FOLDER'])

regex_keys = [r"\,\sdeceased",r"\,\sadministrator", r"\,\sexecutor", 
         r"\,\slegal.+",r"\,\spersonal.+" ]
regex = re.compile("(%s)" % "|".join(regex_keys))

inp = csv.reader(open('{}/rawinventor.tsv'.format(disambig_folder),'r'),delimiter='\t')
outp = csv.writer(open('{}/rawinventor_clean.tsv'.format(disambig_folder),'w'),delimiter='\t')

for e, row in enumerate(inp):
    if e%1000000 == 0:
        print(e)
    row[5] = re.sub(regex, '', row[5])
    outp.writerow(row)

os.system('mv {0}/rawinventor_clean.tsv {0}/rawinventor.tsv'.format(disambig_folder))