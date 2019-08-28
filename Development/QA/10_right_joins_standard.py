#Right Joins That Follow Pattern

###GOV INTEREST TABLES
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

data_list = [(['patent', 'id'], ['application', 'patent_id']), (['temp_patent_list', 'id'], ['application', 'patent_id']), (['patent', 'id'], ['brf_sum_text', 'patent_id']), (['temp_patent_list', 'id'], ['brf_sum_text', 'patent_id']), (['patent', 'id'], ['claim', 'patent_id']), (['temp_patent_list', 'id'], ['claim', 'patent_id']), (['patent', 'id'], ['cpc_current', 'patent_id']), (['temp_patent_list', 'id'], ['cpc_current', 'patent_id']), (['patent', 'id'], ['detail_desc_text', 'patent_id']), (['temp_patent_list', 'id'], ['detail_desc_text', 'patent_id']), (['patent', 'id'], ['draw_desc_text', 'patent_id']), (['temp_patent_list', 'id'], ['draw_desc_text', 'patent_id']), (['patent', 'id'], ['figures', 'patent_id']), (['temp_patent_list', 'id'], ['figures', 'patent_id']), (['patent', 'id'], ['foreign_priority', 'patent_id']), (['temp_patent_list', 'id'], ['foreign_priority', 'patent_id']), (['patent', 'id'], ['foreigncitation', 'patent_id']), (['temp_patent_list', 'id'], ['foreigncitation', 'patent_id']), (['patent', 'id'], ['ipcr', 'patent_id']), (['temp_patent_list', 'id'], ['ipcr', 'patent_id']),  (['patent', 'id'], ['nber', 'patent_id']), (['temp_patent_list', 'id'], ['nber', 'patent_id']), (['patent', 'id'], ['non_inventor_applicant', 'patent_id']), (['temp_patent_list', 'id'], ['non_inventor_applicant', 'patent_id']), (['patent', 'id'], ['otherreference', 'patent_id']), (['temp_patent_list', 'id'], ['otherreference', 'patent_id']), (['patent', 'id'], ['patent_assignee', 'patent_id']), (['temp_patent_list', 'id'], ['patent_assignee', 'patent_id']), (['assignee', 'id'], ['patent_assignee', 'assignee_id']), (['patent', 'id'], ['patent_contractawardnumber', 'patent_id']), (['temp_patent_list', 'id'], ['patent_contractawardnumber', 'patent_id']), (['patent', 'id'], ['patent_govintorg', 'patent_id']), (['temp_patent_list', 'id'], ['patent_govintorg', 'patent_id']), (['government_organization', 'organization_id'], ['patent_govintorg', 'organization_id']), (['patent', 'id'], ['patent_inventor', 'patent_id']), (['temp_patent_list', 'id'], ['patent_inventor', 'patent_id']), (['inventor', 'id'], ['patent_inventor', 'inventor_id']), (['patent', 'id'], ['patent_lawyer', 'patent_id']), (['temp_patent_list', 'id'], ['patent_lawyer', 'patent_id']), (['lawyer', 'id'], ['patent_lawyer', 'lawyer_id']), (['patent', 'id'], ['pct_data', 'patent_id']), (['temp_patent_list', 'id'], ['pct_data', 'patent_id']), (['patent', 'id'], ['rawassignee', 'patent_id']), (['temp_patent_list', 'id'], ['rawassignee', 'patent_id']), (['assignee', 'id'], ['rawassignee', 'assignee_id']), (['patent', 'id'], ['rawexaminer', 'patent_id']), (['temp_patent_list', 'id'], ['rawexaminer', 'patent_id']),   (['patent', 'id'], ['rawlawyer', 'patent_id']), (['temp_patent_list', 'id'], ['rawlawyer', 'patent_id']), (['lawyer', 'id'], ['rawlawyer', 'lawyer_id']), (['patent', 'id'], ['rel_app_text', 'patent_id']), (['temp_patent_list', 'id'], ['rel_app_text', 'patent_id']), (['patent', 'id'], ['us_term_of_grant', 'patent_id']), (['temp_patent_list', 'id'], ['us_term_of_grant', 'patent_id']), (['patent', 'id'], ['usapplicationcitation', 'patent_id']), (['temp_patent_list', 'id'], ['usapplicationcitation', 'patent_id']), (['patent', 'id'], ['uspatentcitation', 'patent_id']), (['temp_patent_list', 'id'], ['uspatentcitation', 'patent_id']), (['patent', 'id'], ['uspc_current', 'patent_id']), (['temp_patent_list', 'id'], ['uspc_current', 'patent_id']), (['patent', 'id'], ['uspc', 'patent_id']), (['temp_patent_list', 'id'], ['uspc', 'patent_id']), (['patent', 'id'], ['usreldoc', 'patent_id']), (['temp_patent_list', 'id'], ['usreldoc', 'patent_id']), (['patent', 'id'], ['wipo', 'patent_id']), (['temp_patent_list', 'id'], ['wipo', 'patent_id'])]
def right_joins(new_database, previous_qa_loc, new_qa_loc):
        '''new_database: new database
	   previous_qa_loc: location of previous qa document
	   new_qa_loc: location of new qa document
	   this function does all right joins that follow the same pattern 
	   in the new database and writes output to a csv file'''
        results = pd.read_excel("{0}/3_right_joins_standard.xlsx".format(previous_qa_loc))
        right_joins = []
        id_list=[]
        for i, j in data_list:
            conn = engine.connect()
            var = conn.execute("select count(distinct {0}.{1}) from {2} right join {0} on {0}.{1} = {2}.{3} where {2}.{3} is null".format(i[0], i[1], j[0], j[1]))
            count = [row[0] for row in var][0]
            print (count)
            right_joins.append("There are {0} rows in {1} not in {2}".format(count, i[0], j[0]))
            if count >0: #ie if there are any rows in the left table not in the right 
                ids_sql = pd.read_sql("select distinct {0}.{1} from {2} right join {0} on {0}.{1} = {2}.{3} where {2}.{3} is null limit 5".format(i[0], i[1], j[0], j[1]), conn)
                print (ids_sql)
                ids= ids_sql.values.T.tolist()
                id_list.append(ids[0])
            else:
                id_list.append('none')
        conn.close()
        return right_joins, id_list
        results['Description_{0}'.format(new_database)]= right_joins
        results['Example missing IDS_{0}'.format(new_database)]= id_list

def write_standard(new_qa_loc):
	results.to_csv("{0}/09_right_joins.csv".format(new_qa_loc), index = False)

if __name__ == '__main__':
if not os.path.exists(new_qa_loc):
    os.mkdir(new_qa_loc)
engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
data = pd.read_excel("{}/3_right_joins_standard.xlsx".format(previous_qa_loc))
right_joins(new_database, previous_qa_loc, new_qa_loc)
write_standard(new_qa_loc)
