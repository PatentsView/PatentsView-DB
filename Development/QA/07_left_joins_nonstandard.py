#Left Joins nonstandard
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

query_list=["SELECT count(distinct patent_id) FROM rawassignee LEFT JOIN patent on patent.id = patent_id where patent.id is null;",
            "SELECT count(distinct patent_id) FROM rawassignee LEFT JOIN patent on patent.id = patent_id where patent.id is null;",
            "SELECT count(pct_data.patent_id) FROM pct_data LEFT JOIN patent on patent.id = pct_data.patent_id where patent.id is null;",
            "SELECT count(distinct rawlocation.location_id) FROM location_assignee LEFT JOIN rawlocation on rawlocation.location_id_transformed = location_assignee.location_id LEFT JOIN location on location.id = rawlocation.location_id where assignee_id = '';",
            "SELECT count(*) FROM cpc_current LEFT JOIN cpc_subsection on cpc_subsection.id = cpc_current.subsection_id where id is null limit 100;",
            "SELECT count(distinct location_assignee.location_id) FROM temp_patent_list LEFT JOIN patent_assignee on patent_assignee.patent_id = temp_patent_list.id LEFT JOIN location_assignee on location_assignee.assignee_id = patent_assignee.assignee_id LEFT JOIN rawlocation on rawlocation.location_id_transformed = location_assignee.location_id;","SELECT count(distinct location_inventor.location_id) FROM temp_patent_list LEFT JOIN patent_inventor on patent_inventor.patent_id = temp_patent_list.id LEFT JOIN location_inventor on location_inventor.inventor_id = patent_inventor.inventor_id LEFT JOIN location on location.id = location_inventor.location_id where location.id is null;","SELECT count(distinct patent_id) FROM rawinventor LEFT JOIN rawlocation on rawlocation.id = rawinventor.rawlocation_id where rawlocation.id is null;",
            "SELECT count(distinct patent_id) FROM cpc_current LEFT JOIN cpc_subgroup on cpc_subgroup.id = cpc_current.subgroup_id where id is null;",
            "SELECT count(distinct mainclass_id) FROM uspc_current LEFT JOIN mainclass_current on mainclass_current.id = uspc_current.mainclass_id where title is null AND mainclass_id != 'No longer published';",
            "SELECT count(distinct id) FROM location_inventor LEFT JOIN location on location.id = location_inventor.location_id where location_inventor.location_id is null;","SELECT count(distinct patent_id) FROM nber LEFT JOIN nber_category on nber_category.id = nber.category_id where title is null;","SELECT count(distinct category_id) FROM nber LEFT JOIN nber_category on nber_category.id = nber.category_id where title is null;",
"SELECT count(distinct patent_id) FROM nber LEFT JOIN nber_subcategory on nber_subcategory.id = nber.subcategory_id where title is null;","SELECT count(distinct subcategory_id) FROM nber LEFT JOIN nber_subcategory on nber_subcategory.id = nber.subcategory_id where title is null;","SELECT count(distinct patent_id) FROM uspc LEFT JOIN mainclass on mainclass.id = uspc.mainclass_id where mainclass.id is null;",
            "SELECT count(distinct mainclass_id) FROM uspc LEFT JOIN mainclass on mainclass.id = uspc.mainclass_id where patent_id is null;","SELECT count(distinct patent_id) FROM uspc LEFT JOIN subclass on subclass.id = uspc.mainclass_id where subclass.id is null;",
            "SELECT count(distinct subclass_id) FROM uspc LEFT JOIN subclass on subclass.id = uspc.mainclass_id where patent_id is null;","SELECT count(distinct wipo.patent_id) FROM wipo LEFT JOIN wipo_field on wipo_field.id = wipo.field_id where wipo_field.id is null;",
            "SELECT count(distinct patent_id) FROM cpc_current LEFT JOIN cpc_subgroup on cpc_subgroup.id = cpc_current.subgroup_id where id is null;"]
select_list=["SELECT distinct patent_id FROM rawassignee LEFT JOIN patent on patent.id = patent_id where patent.id is null limit 5;","SELECT distinct patent_id FROM rawassignee LEFT JOIN patent on patent.id = patent_id where patent.id is null limit 5;","SELECT pct_data.patent_id FROM pct_data LEFT JOIN patent on patent.id = pct_data.patent_id where patent.id is null limit 5;",
             "SELECT distinct rawlocation.location_id FROM location_assignee LEFT JOIN rawlocation on rawlocation.location_id_transformed = location_assignee.location_id LEFT JOIN location on location.id = rawlocation.location_id where assignee_id = '' limit 5;","SELECT * FROM cpc_current LEFT JOIN cpc_subsection on cpc_subsection.id = cpc_current.subsection_id where id is null limit 100;","SELECT distinct location_assignee.location_id FROM temp_patent_list LEFT JOIN patent_assignee on patent_assignee.patent_id = temp_patent_list.id LEFT JOIN location_assignee on location_assignee.assignee_id = patent_assignee.assignee_id LEFT JOIN rawlocation on rawlocation.location_id_transformed = location_assignee.location_id limit 5;","SELECT distinct location_inventor.location_id FROM temp_patent_list LEFT JOIN patent_inventor on patent_inventor.patent_id = temp_patent_list.id LEFT JOIN location_inventor on location_inventor.inventor_id = patent_inventor.inventor_id LEFT JOIN location on location.id = location_inventor.location_id where location.id is null limit 5;","SELECT distinct patent_id FROM rawinventor LEFT JOIN rawlocation on rawlocation.id = rawinventor.rawlocation_id where rawlocation.id is null limit 5;","SELECT distinct patent_id FROM cpc_current LEFT JOIN cpc_subgroup on cpc_subgroup.id = cpc_current.subgroup_id where id is null limit 5;"
,"SELECT distinct mainclass_id FROM uspc_current LEFT JOIN mainclass_current on mainclass_current.id = uspc_current.mainclass_id where title is null AND mainclass_id != 'No longer published' limit 5;"
,"SELECT distinct id FROM location_inventor LEFT JOIN location on location.id = location_inventor.location_id where location_inventor.location_id is null limit 5;"
,"SELECT distinct patent_id FROM nber LEFT JOIN nber_category on nber_category.id = nber.category_id where title is null limit 5;"
,"SELECT distinct category_id FROM nber LEFT JOIN nber_category on nber_category.id = nber.category_id where title is null limit 5;"
,"SELECT distinct patent_id FROM nber LEFT JOIN nber_subcategory on nber_subcategory.id = nber.subcategory_id where title is null limit 5;"
,"SELECT distinct subcategory_id FROM nber LEFT JOIN nber_subcategory on nber_subcategory.id = nber.subcategory_id where title is null limit 5;"
,"SELECT distinct patent_id FROM uspc LEFT JOIN mainclass on mainclass.id = uspc.mainclass_id where mainclass.id is null limit 5;"
,"SELECT distinct mainclass_id FROM uspc LEFT JOIN mainclass on mainclass.id = uspc.mainclass_id where patent_id is null limit 5;"
,"SELECT distinct patent_id FROM uspc LEFT JOIN subclass on subclass.id = uspc.mainclass_id where subclass.id is null limit 5;"
,"SELECT distinct subclass_id FROM uspc LEFT JOIN subclass on subclass.id = uspc.mainclass_id where patent_id is null limit 5;"
,"SELECT distinct wipo.patent_id FROM wipo LEFT JOIN wipo_field on wipo_field.id = wipo.field_id where wipo_field.id is null limit 5;"
,"SELECT distinct patent_id FROM cpc_current LEFT JOIN cpc_subgroup on cpc_subgroup.id = cpc_current.subgroup_id where id is null limit 5;"]

def left_joins_weird(new_database, query_list, select_list):
    left_joins = []
    id_list=[]
    for i in range(len(query_list)):
        query = query_list[i]
        select_statement = select_list[i]
        conn = engine.connect()
        print(query)
        var = conn.execute(query)
        count = [row[0] for row in var][0]
        left_joins.append(count)  
        if count >0: #ie if there are any rows in the left table not in the right 
            ids_sql = conn.execute(select_statement)
            temp_ids=[]
            for row in ids_sql:
                for key, value in row.items():
                    temp_ids.append(value)
            id_list.append(temp_ids)
        else:
            id_list.append('none')
    return left_joins, id_list
    conn.close()
    
def write_wierd(left_joins,id_list, new_database, previous_qa_loc, new_qa_loc):
results = pd.read_excel("{0}/4_left_joins_nonstandard.xlsx".format(previous_qa_loc))
results['{}_counts'.format(new_database)] = left_joins
results['{}_example_ids'.format(new_database)] = id_list
results.to_csv("{0}/07_left_joins_nonstandard.csv".format(new_qa_loc), index = False)
    
if __name__ == '__main__':
    if not os.path.exists(new_qa_loc):
        os.mkdir(new_qa_loc)
    engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
    data = pd.read_excel("{0}/4_left_joins_nonstandard.xlsx".format(previous_qa_loc))
    left_join_counts, id_list = left_joins_weird(new_database,query_list, select_list)
    write_wierd(left_joins,id_list, new_database, previous_qa_loc, new_qa_loc)