import MySQLdb
import os
import csv
import sys
sys.path.append("To_clone")
from ConfigFiles import config
sys.path.append("Code/PatentsView-DB/Scripts")

def lawyer_patch(host, username, password, database):
    mydb = MySQLdb.connect(host= host,
    user=username,
    passwd=password, db =database)
    cursor = mydb.cursor()
    cursor.execute("create table for_update as select * from temporary_update where pk in (select uuid from rawlawyer where lawyer_id = '');")
    raw = pd.read_sql("select * from  rawlawyer where lawyer_id = ''", mydb)
    print "got raw"
    update  = pd.read_sql("select * from for_update", mydb)
    print "got update"
    print "merging"
    for_input = pd.merge(raw, update, left_on = "uuid", right_on = "pk", how = 'left')
    for_input["lawyer_id"] = for_input['update'].fillna("")
    del for_input['pk']
    del for_input['update']
    for_input.to_csv("for_rawlawyer_update.csv", sep = "\t", index = False)
    print "uploading"
    cursor.execute("create table uploaded_rawlawyer like rawlawyer")
    cursor.execute("load data local infile 'for_rawlawyer_update.csv' into table uploaded_rawlawyer fields terminated by '\t' lines terminated by '\r\n' ignore 1 lines")   
    mydb.commit()
    print "making backup"
    cursor.execute("create table temp_rawlawyerbackup as select * from rawlawyer;")
    print "inserting"
    cursor.execute("delete from rawlawyer where lawyer_id = '';")
    cursor.execute("insert into rawlawyer select * from uploaded_rawlawyer;")
    mydb.commit()

def assignee_patch(host, username, password, database):
    mydb = MySQLdb.connect(host= host,
    user=username,
    passwd=password, db =database)
    cursor = mydb.cursor()
    cursor.execute("create table for_update as select * from temporary_update where pk in (select uuid from rawassignee where assignee_id = '');")
    raw = pd.read_sql("select * from  rawassignee where assignee_id = ''", mydb)
    print "got raw"
    update  = pd.read_sql("select * from for_update", mydb)
    print "got update"
    print "merging"
    for_input = pd.merge(raw, update, left_on = "uuid", right_on = "pk", how = 'left')
    for_input["assignee_id"] = for_input['update'].fillna("")
    del for_input['pk']
    del for_input['update']
    for_input.to_csv("for_rawassignee_update.csv", sep = "\t", index = False)
    print "uploading"
    cursor.execute("create table uploaded_rawassignee like rawassignee")
    cursor.execute("load data local infile 'for_rawassignee_update.csv' into table uploaded_rawassignee fields terminated by '\t' lines terminated by '\r\n' ignore 1 lines")   
    mydb.commit()
    print "making backup"
    cursor.execute("create table temp_rawassigneebackup as select * from rawassignee;")
    print "inserting"
    cursor.execute("delete from rawassigneewhere assignee_id = '';")
    cursor.execute("insert into rawassignee select * from uploaded_rawassignee;")
    mydb.commit()