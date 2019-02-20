#Ratio of Total to Distinct IDs
import pandas as pd
import os
import configparser
import sys
sys.path.append('/project/Development')
from helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read('/project/Development/config.ini')
host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']
previous_qa_loc = config['FOLDERS']['OLD_QA_LOC']
new_qa_loc = config['FOLDERS']['NEW_QA_LOC']
latest_expected_date = config['CONSTANTS']['LATEST_DATE']


def create_ratio_description(old_db_ratio, new_db_ratio, max_dif = .05 ):
    if abs(old_db_ratio - new_db_ratio)/float(old_db_ratio) > max_dif:
        return "Problem: The ratio of distinct to total ids are very different from last year "
    else:
        return "No Problem"
    
def get_ratios(previous_qa_loc, new_qa_loc, new_database):
    ratios = pd.read_excel('{}/1_distinct_to_total.xlsx'.format(previous_qa_loc))
    new_ratios = []
    table_col = zip(ratios['Table'], ratios['Column'])
    for table, col in table_col:
        conn = engine.connect()
        query = "select count({0}), count(distinct {0}) from {1}.{2}".format(col, new_database, table)
        var = conn.execute(query)
        for row in var:
            count = row[0]
            count_distinct = row[1]
        new_ratios.append(count/count_distinct)  
        conn.close()
    return new_ratios   

def write_distinct_excel(new_ratios, previous_qa_loc, new_qa_loc, new_database):
    ratios = pd.read_excel('{}/1_distinct_to_total.xlsx'.format(previous_qa_loc))
    ratios[new_database] = new_ratios
    del ratios['Description']
    #the last row of the table is now the most recent previous database!
    ratios['Description'] = ratios.apply(lambda row: create_ratio_description(row[ratios.columns[-2]], row[new_database]), axis=1)
    ratios.to_csv('{}/05_ratios.csv'.format(new_qa_loc), index = False)
    

if __name__ == '__main__':
    if not os.path.exists(new_qa_loc):
        os.mkdir(new_qa_loc)
    engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
    data = pd.read_excel('{}/1_distinct_to_total.xlsx'.format(previous_qa_loc))
    ratios = get_ratios(previous_qa_loc, new_qa_loc, new_database)
    write_distinct_excel(ratios, previous_qa_loc, new_qa_loc, new_database)