import sys
import os
import MySQLdb
sys.path.append("D:/DataBaseUpdate/To_clone")
from ConfigFiles import config
from warnings import filterwarnings
import csv
import re,os,random,string,codecs
from helpers import general_helpers



def upload_cpc_small_tables(db_con,folder):
    '''
    host : database host
    username: username
    password : database username
    db: new/updated database
    folder: wher the cpc_group/subsection folders are
    '''
    #Upload CPC subsection data
    cursor = db_con.cursor()
    mainclass = csv.reader(file(os.path.join(folder,'cpc_subsection.csv'),'rb'))
    counter =0
    for m in mainclass:
        towrite = [re.sub('"',"'",item) for item in m]
        query = 'insert into {}.cpc_subsection values("{}", "{}")'.format(db,towrite[0], towrite[1])
        cursor.execute(query)
    db_con.commit()

    #Upload CPC group data
    subclass = csv.reader(file(os.path.join(folder,'cpc_group.csv'),'rb'))
    exist = set()
    for m in subclass:
        towrite = [re.sub('"',"'",item) for item in m]
        if not towrite[0] in exist:
            exist.add(towrite[0])
            query = 'insert into {}.cpc_group values("{}", "{}")'.format(db,towrite[0], towrite[1])
            cursor.execute(query2)
    db_con.commit()

 def upload_cpc_subgroup(db_con,folder);
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
    cursor = db_con.cursor()
    exist = set()
    for m in subgroup:
        towrite = [re.sub('"',"'",item) for item in m]
        if not towrite[0] in exist:
            exist.add(towrite[0])
            clean = [i if not i =="NULL" else "" for i in towrite]
            subgroup_out.writerow(clean)
    cursor.execute("load data local infile '{0}/{1}' into table {2}.cpc_subgroup CHARACTER SET utf8 fields terminated by '\t' lines terminated by '\r\n'".format(folder, 'cpc_subgroup_clean.csv', db))
    db_con.commit()
    
if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], 
                                            config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

    cpc_folder = '{}/{}'.format(config['FOLDERS']['WORKING_DIR'],'cpc_output')
    
    upload_cpc_small_tables(db_con,cpc_folder)
    upload_cpc_subgroup(db_con,cpc_folder)
