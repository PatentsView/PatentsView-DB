#Left Joins That Follow Pattern
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

data = pd.read_excel("{}/4_left_joins.xlsx".format(previous_qa_loc))

data_list = [(['patent', 'id'], ['application', 'patent_id']), (['patent', 'id'], ['botanic', 'patent_id']), (['patent', 'id'], ['brf_sum_text', 'patent_id']), (['patent', 'id'], ['claim', 'patent_id']), (['patent', 'id'], ['cpc_current', 'patent_id']), (['cpc_subsection', 'id'], ['cpc_current', 'subsection_id']), (['cpc_group', 'id'], ['cpc_current', 'group_id']), (['cpc_subgroup', 'id'], ['cpc_current', 'subgroup_id']), (['cpc_subgroup', 'id'], ['cpc_current', 'subgroup_id']), (['patent', 'id'], ['detail_desc_text', 'patent_id']), (['patent', 'id'], ['draw_desc_text', 'patent_id']), (['patent', 'id'], ['figures', 'patent_id']), (['patent', 'id'], ['foreign_priority', 'patent_id']), (['patent', 'id'], ['foreigncitation', 'patent_id']), (['patent', 'id'], ['government_interest', 'patent_id']), (['patent', 'id'], ['ipcr', 'patent_id']), (['patent', 'id'], ['nber', 'patent_id']), (['nber_category', 'id'], ['nber', 'category_id']), (['nber_category', 'id'], ['nber', 'category_id']), (['nber_subcategory', 'id'], ['nber', 'subcategory_id']), (['nber_subcategory', 'id'], ['nber', 'subcategory_id']), (['patent', 'id'], ['non_inventor_applicant', 'patent_id']), (['rawlocation', 'id'], ['non_inventor_applicant', 'rawlocation_id']), (['patent', 'id'], ['otherreference', 'patent_id']), (['patent', 'id'], ['patent_assignee', 'patent_id']), (['assignee', 'id'], ['patent_assignee', 'assignee_id']), (['patent', 'id'], ['patent_contractawardnumber', 'patent_id']), (['patent', 'id'], ['patent_govintorg', 'patent_id']), (['government_organization', 'organization_id'], ['patent_govintorg', 'organization_id']), (['patent', 'id'], ['patent_inventor', 'patent_id']), (['inventor', 'id'], ['patent_inventor', 'inventor_id']), (['patent', 'id'], ['pct_data', 'patent_id']), (['assignee', 'id'], ['rawassignee', 'assignee_id']), (['rawlocation', 'id'], ['rawassignee', 'rawlocation_id']), (['patent', 'id'], ['rawexaminer', 'patent_id']), (['rawlocation', 'id'], ['rawinventor', 'rawlocation_id']), (['patent', 'id'], ['rel_app_text', 'patent_id']), (['patent', 'id'], ['us_term_of_grant', 'patent_id']), (['patent', 'id'], ['usapplicationcitation', 'patent_id']), (['patent', 'id'], ['uspatentcitation', 'patent_id']), (['patent', 'id'], ['uspc_current', 'patent_id']), (['mainclass_current', 'id'], ['uspc_current', 'mainclass_id']), (['mainclass_current', 'id'], ['uspc_current', 'mainclass_id']), (['subclass_current', 'id'], ['uspc_current', 'mainclass_id']), (['subclass_current', 'id'], ['uspc_current', 'mainclass_id']), (['patent', 'id'], ['uspc', 'patent_id']), (['mainclass', 'id'], ['uspc', 'mainclass_id']), (['mainclass', 'id'], ['uspc', 'mainclass_id']), (['subclass', 'id'], ['uspc', 'mainclass_id']), (['subclass', 'id'], ['uspc', 'mainclass_id']), (['patent', 'id'], ['usreldoc', 'patent_id']), (['patent', 'id'], ['wipo', 'patent_id']), (['wipo_field', 'id'], ['wipo', 'field_id'])]
def left_joins(new_database, previous_qa_loc, new_qa_loc):
    '''new_database: new database
    previous_qa_loc: location of previous qa document
    new_qa_loc: location of new qa document
    this function does all left joins that follow the same pattern 
    in the new database and writes output to a csv file'''
    results = pd.read_excel("{0}/4_left_joins.xlsx".format(previous_qa_loc))
    left_joins = []
    id_list=[]
    for i, j in data_list:
        conn = engine.connect()
        var = conn.execute("select count(distinct {2}.{3}) from {0} right join {2} on {2}.{3} = {0}.{1} where {0}.{1} is null".format(i[0], i[1], j[0], j[1]))
        count = [row[0] for row in var][0]
        left_joins.append("There are {0} rows in {2} not in {1}".format(count, i[0], j[0]))
        print (left_joins)

        if count >0: #ie if there are any rows in the left table not in the right 
            ids_sql = pd.read_sql("select distinct {2}.{3} from {0} right join {2} on {2}.{3} = {0}.{1} where {0}.{1} is null limit 5".format(i[0], i[1], j[0], j[1]), engine)
            ids= ids_sql.values.T.tolist()
            id_list.append(ids[0])
        else:
            id_list.append('none')
    conn.close()
    results['Description_{0}'.format(new_database)]= left_joins
    results['Example missing IDS_{0}'.format(new_database)]= id_list
    return results
def write_left_joins(results, new_qa_loc):
    results.to_csv("{0}/06_left_joins.csv".format(new_qa_loc), index = False)
    
if __name__ == '__main__':
    if not os.path.exists(new_qa_loc):
        os.mkdir(new_qa_loc)
    engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
    data = pd.read_excel("{}/4_left_joins.xlsx".format(previous_qa_loc))
    left_join_results = left_joins(new_database, previous_qa_loc, new_qa_loc)
    write_left_joins(left_join_results, new_qa_loc)