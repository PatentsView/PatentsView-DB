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
def connect_to_db():
    mydb = MySQLdb.connect(host= config.host,
    user=config.username,
    passwd=config.password,  db =config.merged_database)
    return mydb
def upload_data(mydb):
    cursor = mydb.cursor()
    cursor.execute("select organization,id from assignee")
    data = cursor.fetchall()
    all_ids = list([item[1] for item in data])
    all_disambiged = list([item[0] for item in data])
    existing_lookup = dict(zip(all_disambiged, all_ids))
    manual = pd.read_csv('fixed_assignees.txt', delimiter = '\t')
    new = list(manual['Correct Disambiguation'].unique())
    id_list = [existing_lookup[item] if item in existing_lookup.keys() else id_generator(size=32) for item in new]
    for_lookup = dict(zip(new, id_list))
    manual["assignee_id"] = manual['Correct Disambiguation'].map(lambda x : for_lookup[x])
    assig_all = list(set(manual["assignee_id"]))
    name_all = list(set(manual['Correct Disambiguation']))
    assign_lookup = dict(zip(assig, name))
    assign = [item for item in assig_all if not item in existing_lookup.keys()]
    name = [assign_lookup[item] for item in assign]
    for i in range(len(assig)):
        cursor.execute("insert into assignee (id, organization) values ('" + assig[i] + "' , '" + name[i] + "');")
    print assig[:5]
    print name[:5]
    for i in range(len(assign)):
        try:
            query = "insert into assignee (id, organization) values ('" + assign[i] + "' , '" + name[i] + "');"
            cursor.execute(query)

        except:
            print query
        mydb.commit()
    del manual['Correct Disambiguation']
    manual.to_csv("lookup.csv", sep = "\t", index = False)
    cursor.execute("create table temp_govt_assg (`id` varchar(20) NOT NULL,`raw_assignee` varchar(200), new_assignee_id varchar(40), PRIMARY KEY (`id`))")
    print "Loading Data"
    cursor.execute("load data local infile 'lookup.csv' into table temp_govt_assg fields terminated by '\t' lines terminated by '\r\n' ignore 1 lines")   
    mydb.commit()
    cursor.execute("create table temp_lookup as select * from  temp_govt_assg t left join rawassignee r on t.id=r.patent_id and t.raw_assignee=r.organization;")
def update_rawassignee(mydb):
    cursor = mydb.cursor()
    cursor.execute("select uuid, assignee_id from temp_lookup")
    data = cursor.fetchall()
    uuids = [item[0] for item in data]
    assignees = [item[1] for item in data]
    for i in range(len(uuids)):
        if i%1000 ==0:
            print i
            mydb.commit()
        try:
            cursor.execute("update rawassignee set assignee_id = '" + assignees[i] + "' where uuid = '" + uuids[i] + "';" )
        except:
            pass
            #these are the ones that didn't get a new disambiguated id cause they aren't govenment orgs
            #so we are just skilling
    
    mydb.commit()

