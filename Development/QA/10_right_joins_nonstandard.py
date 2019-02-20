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

query_list=["SELECT count(distinct patent.id) FROM botanic RIGHT JOIN patent on patent.id = botanic.patent_id where botanic.patent_id is null and patent.id like 'P%%';","SELECT count(distinct botanic.patent_id) FROM botanic LEFT JOIN patent on patent.id = botanic.patent_id where patent.id is null and patent.id like 'P%%';","SELECT count(distinct temp_patent_list.id) FROM botanic RIGHT JOIN temp_patent_list on temp_patent_list.id = botanic.patent_id where botanic.patent_id is null and temp_patent_list.id like 'P%%';","SELECT count(distinct patent_id) FROM uspc_current LEFT JOIN subclass_current on subclass_current.id = uspc_current.mainclass_id where title is null AND mainclass_id != 'No longer published';","SELECT count(distinct subclass_id) FROM uspc_current LEFT JOIN subclass_current on subclass_current.id = uspc_current.mainclass_id where title is null AND mainclass_id != 'No longer published';","SELECT count(distinct location_assignee.location_id) FROM location_assignee RIGHT JOIN rawlocation on rawlocation.location_id_transformed = location_assignee.location_id RIGHT JOIN location on location.id = rawlocation.location_id where location.id is null;","SELECT count(distinct location_assignee.location_id) FROM location_assignee RIGHT JOIN rawlocation on rawlocation.location_id_transformed = location_assignee.location_id RIGHT JOIN location on location.id = rawlocation.location_id where location.id is null;"]
select_list=["SELECT distinct patent.id FROM botanic RIGHT JOIN patent on patent.id = botanic.patent_id where botanic.patent_id is null and patent.id like 'P%%' limit 5;" , "SELECT distinct botanic.patent_id FROM botanic LEFT JOIN patent on patent.id = botanic.patent_id where patent.id is null and patent.id like 'P%%' limit 5;", "SELECT distinct temp_patent_list.id FROM botanic RIGHT JOIN temp_patent_list on temp_patent_list.id = botanic.patent_id where botanic.patent_id is null and temp_patent_list.id like 'P%%' limit 5;", 
"SELECT distinct patent_id FROM uspc_current LEFT JOIN subclass_current on subclass_current.id = uspc_current.mainclass_id where title is null AND mainclass_id != 'No longer published' limit 5;", 
"SELECT distinct subclass_id FROM uspc_current LEFT JOIN subclass_current on subclass_current.id = uspc_current.mainclass_id where title is null AND mainclass_id != 'No longer published' limit 5;", "SELECT count(distinct location_assignee.location_id) FROM location_assignee RIGHT JOIN rawlocation on rawlocation.location_id_transformed = location_assignee.location_id RIGHT JOIN location on location.id = rawlocation.location_id where location.id is null limit 5;", "SELECT count(distinct id) FROM cpc_group where title = '' limit 5;"]

def right_joins_weird(new_database, query_list, select_list):
    '''new_db: new database
    previous_qa_loc: location of previous qa document
    new_qa_loc: location of new qa document
    this function does all right joins that don't follow the same pattern 
    in the new database and writes output to a csv file'''
    right_joins = []
    id_list=[]

    for i in range(len(query_list)):
        query = query_list[i]
        select_statement = select_list[i]
        conn=engine.connect()
        print (query)
        var = conn.execute(query)
        count = [row[0] for row in var][0]
        right_joins.append(count)    
        if count >0: #ie if there are any rows in the left table not in the right 
            ids_sql = conn.execute(select_statement)
            temp_ids=[]
            for row in ids_sql:
                for key, value in row.items():
                    temp_ids.append(value)
            id_list.append(temp_ids)
        else:
            id_list.append('none')
    return right_joins, id_list

        
def write_wierd(right_join_counts,id_list, new_database, previous_qa_loc, new_qa_loc):
    results = pd.read_excel("{0}/3_right_joins_nonstandard.xlsx".format(previous_qa_loc))
    results['Description_{0}'.format(new_database)]= right_join_counts
    results['Example missing IDS_{0}'.format(new_database)]= id_list
    results.to_csv("{0}/10_right_joins_nonstandard.csv".format(new_qa_loc), index = False)
    

if __name__ == '__main__':
    if not os.path.exists(new_qa_loc):
        os.mkdir(new_qa_loc)
    engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
    data = pd.read_excel("{0}/3_right_joins_nonstandard.xlsx".format(previous_qa_loc))
    right_join_counts, id_list = right_joins_weird(new_database,query_list, select_list)
    write_wierd(right_join_counts,id_list, new_database, previous_qa_loc, new_qa_loc)