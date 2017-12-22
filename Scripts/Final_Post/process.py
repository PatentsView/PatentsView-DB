import os
import csv
import sys
import pandas as pd
import MySQLdb
sys.path.append("To_clone")
from ConfigFiles import config
sys.path.append("Code/PatentsView-DB/Scripts")

def assignee_types(host, username, password, database):
    '''
    Updates the assignee table to have a type based on the majority vote from the rawassignee table
    '''
    mydb = MySQLdb.connect(host= host,
    user=username,
    passwd=password, db =database)
    cursor = mydb.cursor()
    query = ("create table temp_assignee_lookup as (select a.assignee_id, max(`type`) as new_type from (select assignee_id, max(type_count) as max_count from" + 
            "(select assignee_id, `type`, count(`type`) as type_count from rawassignee group by assignee_id, `type`) as m group by assignee_id) as a" +
            "left join (select assignee_id, `type`, count(`type`) as type_count from rawassignee group by assignee_id, `type`) as b on a.assignee_id =b.assignee_id" + 
            "and a.max_count=b.type_count group by a.assignee_id);")
    cursor.execute(query)
    mydb.commit()
    cursor.execute("create index assignee_ix on temp_assignee_lookup (assignee_id);")
    raw = pd.read_sql("select * from temp_assignee_lookup", mydb)
    assignee_ids = list(raw['assignee_id'])[1:] #drop the blank id
    types = list(raw['new_type'])[1:]
    for i in range(len(assignee_ids)):
        #try:
        query = "update assignee set type = '" + str(types[i]) + "' where id = '" + assignee_ids[i] + "';"
        cursor.execute(query)
        if i%1000==0:
            print i
            mydb.commit()
#         except:
#             print query
    mydb.commit()
    #deal with duplicates , I am not sure if we should have to deal with it
    cursor.execute('create table temp_assignee_backup as select * from assignee;')
    cursor.execute('drop table assignee;')
    cursor.execute('create table assignee as select id, min(type) as type, min(name_first) as name_first, min(name_last) as name_last, min(organization) as organization from temp_assignee_backup group by id;')
    cursor.execute("drop table temp_assignee_backup;")
    mydb.commit()
    cursor.execute("drop table temp_assignee_lookup;")
    mydb.commit()

def assignee_locs(host, username, password, database):
    query1 = "create table temp_assignee_loc as select assignee_id, rawlocation_id, location_id from rawassignee r left join rawlocation l on r.rawlocation_id = l.id;"
    query = (" insert into location_assignee (select max(location_id) as location_id, a.assignee_id from (select assignee_id, max(location_id_count) as max_count from " + 
    "(select assignee_id, location_id, count(location_id) as location_id_count from temp_assignee_loc group by assignee_id, location_id) as m group by assignee_id) as a " +
    "left join (select assignee_id, location_id, count(location_id) as location_id_count from temp_assignee_loc group by assignee_id, location_id) as b on a.assignee_id =b.assignee_id " +
    "and a.max_count=b.location_id_count group by a.assignee_id);")
    mydb = MySQLdb.connect(host= host,
    user=username,
    passwd=password, db =database)
    cursor = mydb.cursor()
    cursor.execute(query1)
    mydb.commit()
    cursor.execute(query)
    cursor.execute('drop table temp_assignee_loc;')
    mydb.commit()

def assignee_locs(host, username, password, database):
    query1 = "create table temp_assignee_loc as select assignee_id, rawlocation_id, location_id from rawassignee r left join rawlocation l on r.rawlocation_id = l.id;"
    query = (" insert into location_assignee (select max(location_id) as location_id, a.assignee_id from (select assignee_id, max(location_id_count) as max_count from " + 
    "(select assignee_id, location_id, count(location_id) as location_id_count from temp_assignee_loc group by assignee_id, location_id) as m group by assignee_id) as a " +
    "left join (select assignee_id, location_id, count(location_id) as location_id_count from temp_assignee_loc group by assignee_id, location_id) as b on a.assignee_id =b.assignee_id " +
    "and a.max_count=b.location_id_count group by a.assignee_id);")
    mydb = MySQLdb.connect(host= host,
    user=username,
    passwd=password, db =database)
    cursor = mydb.cursor()


