#Right Joins Overnight
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

data_list_overnight= (['rawlocation', 'location_id_transformed'],['location_assignee', 'location_id']), (['location_inventor', 'location_id'], ['location', 'id']), (['location', 'id'], ['location_inventor', 'location_id']), (['patent', 'id'], ['rawinventor', 'patent_id']), (['temp_patent_list', 'id'], ['rawinventor', 'patent_id']),(['inventor', 'id'], ['rawinventor', 'inventor_id']), 
results = pd.read_excel("{0}/3_right_joins_overnight_1.xlsx".format(previous_qa_loc))
right_joins = []
id_list=[]
def right_joins_overnight(new_database,previous_qa_loc,new_qa_loc):
    for i, j in data_list_overnight:
        conn=engine.connect()
        print ("select count(distinct {0}.{1}) from {2} right join {0} on {0}.{1} = {2}.{3} where {2}.{3} is null".format(i[0], i[1], j[0], j[1]))
        var = conn.execute("select count(distinct {0}.{1}) from {2} right join {0} on {0}.{1} = {2}.{3} where {2}.{3} is null".format(i[0], i[1], j[0], j[1]))
        count = [row[0] for row in var][0]
        right_joins.append("There are {0} rows in {1} not in {2}".format(count, i[0], j[0]))
        if count >0: #ie if there are any rows in the left table not in the right 
            ids_sql = pd.read_sql("select distinct {0}.{1} from {2} right join {0} on {0}.{1} = {2}.{3} where {2}.{3} is null limit 5".format(i[0], i[1], j[0], j[1]), conn)
            ids= ids_sql.values.T.tolist()
            id_list.append(ids[0])
        else:
            id_list.append('none')
    conn.close()
    return right_joins, id_list
    results['Description_{0}'.format(new_database)]= right_joins
    results['Example missing IDS_{0}'.format(new_database)]= id_list
def write_over_night_results(new_qa_loc):
    results.to_csv("{0}/11_right_joins_overnight.csv".format(new_qa_loc), index = False)
    


if __name__ == '__main__':
    if not os.path.exists(new_qa_loc):
        os.mkdir(new_qa_loc)
    engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
    data = pd.read_excel("{0}/3_right_joins_overnight_1.xlsx".format(previous_qa_loc))
    right_joins_overnight(new_database,previous_qa_loc,new_qa_loc)
    write_over_night_results(new_qa_loc)