import sys
import os
import MySQLdb
sys.path.append("D:/DataBaseUpdate/To_clone")
from ConfigFiles import config
from warnings import filterwarnings
import csv
import re,os,random,string,codecs



def upload_cpc_small_tables(host,username,password,db,folder):
    '''
    host : database host
    username: username
    password : database username
    db: new/updated database
    folder: wher the cpc_group/subsection folders are
    '''
    print"Doing CPC_SUBSECTION"
    #Upload CPC subsection data
    mainclass = csv.reader(file(os.path.join(folder,'cpc_subsection.csv'),'rb'))
    counter =0
    for m in mainclass:
        towrite = [re.sub('"',"'",item) for item in m]
        query = 'insert into {}.cpc_subsection values("{}", "{}")'.format(db,towrite[0], towrite[1])
        cursor.execute(query)
    mydb.commit()

    #Upload CPC group data
    subclass = csv.reader(file(os.path.join(folder,'cpc_group.csv'),'rb'))
    exist = set()
    for m in subclass:
        towrite = [re.sub('"',"'",item) for item in m]
        if not towrite[0] in exist:
            exist.add(towrite[0])
            query = 'insert into {}.cpc_group values("{}", "{}")'.format(db,towrite[0], towrite[1])
            cursor.execute(query2)
    mydb.commit()

 def upload_cpc_subgroup(host,username,password,db,folder);
    '''
    host : database host
    username: username
    password : database username
    db: new/updated database
    folder: where the cpc_group/subsection folders are
    This is a separate function because the one-by-one insert is too slow
    So instead post-process and then upload as a csv
    '''
    subgroup = csv.reader(file(os.path.join(folder,'cpc_subgroup.csv'),'rb'))
    subgroup_out = csv.writer(file(os.path.join(folder,'cpc_subgroup_clean.csv'),'wb'),delimiter = '\t')
    exist = set()
    for m in subgroup:
        towrite = [re.sub('"',"'",item) for item in m]
        if not towrite[0] in exist:
            exist.add(towrite[0])
            clean = [i if not i =="NULL" else "" for i in towrite]
            subgroup_out.writerow(clean)
    cursor.execute("load data local infile '{0}/{1}' into table {2}.cpc_subgroup CHARACTER SET utf8 fields terminated by '\t' lines terminated by '\r\n'".format(folder, 'cpc_subgroup_clean.csv', db))
    mydb.commit()
    
if __name__ == '__main__':
    host = config.host
    username = config.username
    password = config.password