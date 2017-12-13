import MySQLdb
import os
import csv
import sys
sys.path.append("To_clone")
from ConfigFiles import config
sys.path.append("Code/PatentsView-DB/Scripts")

def assignee_patch(host, username, password, database):
    mydb = MySQLdb.connect(host= host,
    user=username,
    passwd=password, db =database)
    cursor = mydb.cursor()
    cursor.execute("create table for_update as select * from temporary_update where pk in (select uuid from rawassignee where assignee_id = "");")
    cursor.execute("create table rawassignees_to_update as select * from rawassignee where assignee_id = "";")
    raw = pd.read_sql("select * from rawassignees_to_update", mydb)
    update  = pd.read_sql("select * from for_update", mydb)
    for_input = pd.merge(raw, update, left_on = "uuid", right_on = "pk", how = 'left')
    for_input["assignee_id"] = for_input['update'].fillna("")
    del for_input['pk']
    del for_input['update']
    for_input.to_csv("for_rawassignee_update.csv", sep = "\t", index = False)
    cursor.execute("load data local infile 'for_rawassignee_update.csv' into table uploaded_rawassignees fields terminated by '\t' lines terminated by '\r\n' ignore 1 lines")   
    mydb.commit()
    cursor.execute("create table uploaded_rawassignees as select * from rawassignees_to_update limit 1")
    cursor.execute("truncate table uploaded_rawassignees;")
    cursor.execute("create table temp_rawassignee_backup as select * from rawassignee;")
    cursor.execute("delete from rawassignee where assignee_id = "";")
    cursor.execute("insert into rawassignee select * from uploaded_rawassignees;")
    mydb.commit()


assignee_patch(config.host, config.username, config.password, config.merged_database)