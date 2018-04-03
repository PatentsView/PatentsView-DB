import MySQLdb
import os
import csv
import sys
import pandas as pd
import string,random 
sys.path.append("D:/DataBaseUpdate/To_clone")
from ConfigFiles import config
sys.path.append("Code/PatentsView-DB/Scripts")

def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def update_long_assignees(mydb, govt_assignee):
    lookup = pd.read_csv(govt_assignee + "/assignee_names_fixed.csv")
    lookup.columns = ['id', 'disambiguated_name', 'raw_name', 'new_name']
    lookup = lookup[~pd.isnull(lookup['new_name'])]
    new_assignees = {}
    #make a new assignee id only for the unique names
    for name in list(set(list(lookup['new_name']))):
        new_assignees[name] = id_generator()
    #insert these new assignees into the assignee table
    mydb = MySQLdb.connect(host= config.host,
        user=config.username,
        passwd=config.password,  db=config.merged_database)
    cursor = mydb.cursor()
    for assignee in new_assignees.keys():
        try:
            query = 'insert into assignee (id, organization, type) values ("' + new_assignees[assignee] + '", "' + assignee + '", 6);'
            cursor.execute(query)
        except:
            print query
    mydb.commit()

    lookup['new_assignee_id'] = lookup['new_name'].map(lambda x : new_assignees[x])
    for_raw_update = zip(list(lookup['id']), list(lookup['raw_name']), list(lookup['new_assignee_id']))
    for i in for_raw_update:
        try:
            query = "update rawassignee set assignee_id = '" + i[2] + "' where assignee_id = '" + i[0] + "' and organization = '" + i[1]+"';" 
            cursor.execute(query)
        except:
            print query
    mydb.commit()             

