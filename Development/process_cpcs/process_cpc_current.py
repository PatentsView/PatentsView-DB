import sys
import os
import MySQLdb
sys.path.append("D:/DataBaseUpdate/To_clone")
from ConfigFiles import config
from warnings import filterwarnings
import csv
import re,os,random,string,codecs
import multiprocessing



def get_patent_ids(db_con):
    cursor = db_con.cursor()

    cursor.execute('select id, number from '+patdb+'.patent')
    patnums = {}
    for field in cursor.fetchall():
        patnums[field[1]] = field[0]
    return set(patnums.keys()), patnums


def write_cpc_current(cpc_input, cpc_output, error_log, patent_dict, patent_set):
    #Create CPC_current table off full master classification list
    cpc_data= csv.reader(file(cpc_input,'rb'),delimiter = ',')
    errorlog = open(error_log,'w')
    current_exist = {}
    cpc_out = csv.writer(file(cpc_output,'wb'),delimiter = '\t')
    counter = 0
    for row in cpc_data:
        counter +=1
        if counter %100000==0:
            print(counter)
        if str(row[0]) in patent_set:
            towrite = [re.sub('"',"'",item) for item in row[:3]]
            towrite.insert(0,id_generator())
            towrite[1] = patent_dict[row[0]]
            for t in range(len(towrite)):
                try:
                    gg = int(towrite[t])
                    towrite[t] = str(int(towrite[t]))
                except:
                    pass
            primaries = towrite[2].split("; ")
            cpcnum = 0
            for p in primaries:
                try:
                    needed = [id_generator(),towrite[1]]+[p[0],p[:3],p[:4],p,'primary',str(cpcnum)]
                    clean = [i if not i =="NULL" else "" for i in needed]
                    cpcnum+=1
                    cpc_out.writerow(clean)
                except: #sometimes a row doesn't have all the information
                    print(row)

            additionals = [t for t in towrite[3].split('; ') if t!= '']
            for p in additionals:
                try:
                    needed = [id_generator(),towrite[1]]+[p[0],p[:3],p[:4],p,'additional',str(cpcnum)]
                    clean = [i if not i =="NULL" else "" for i in needed]
                    cpcnum+=1
                    cpc_out.writerow(clean)
                except:
                    print(row)
    errorlog.close()

def upload_cpc_current(db_con, cpc_current_loc):
    cursor = db_con.cursor()
    cursor.execute("load data local infile '{}' into table cpc_current CHARACTER SET utf8 fields terminated by '\t' lines terminated by '\r\n' ignore 1 lines".format(cpc_current_loc))
    mydb.commit()
    cursor.execute('update cpc_current set category = "inventional" where category = "primary"')

if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], 
                                            config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

    patent_set, patent_dict = get_patent_ids(db_con)

    cpc_folder = '{}/{}'.format(config['FOLDERS']['WORKING_DIR'],'cpc_output')

    in_files = ['{0}/grants_piecesa{1}'.format(folder, item) for item in  ['a', 'b', 'c', 'd', 'e', 'f', 'g']]
    out_files = ['{0}/out_file_a{1}.csv'.format(folder, item) for item in  ['a', 'b', 'c', 'd', 'e', 'f', 'g']]
    error_log = ['{0}/error_log_a{1}'.format(folder, item) for item in  ['a', 'b', 'c', 'd', 'e', 'f', 'g']]
    #TODO: see if you can zip with just patent_dict and patent_set
    pat_dicts = [patent_dict for item in in_files]
    pat_sets = [patent_set for item in in_files]
    files = zip(in_files, out_files, error_log, pat_dicts, pat_sets)
    

    print("Processing Files")
    desired_processes = 7 # ussually num cpu - 1
    jobs = []
    for f in files:
        jobs.append(multiprocessing.Process(target = write_cpc_current, args=(f)))

    for segment in chunks(jobs, desired_processes):
        print(segment)
        for job in segment:
            job.start()

    print('rejoining')
    #now rejoin the outfiles into the cpc_current
    cpc_current_file = '{}/{}'.format(cpc_folder, 'cpc_current.csv')
    with open(cpc_current_file, 'wb') as output_cpc_file:
        for file in out_files:
            with open(file, 'rb') as myfile:
                for line in myfile:
                    output_cpc_file.write(line)
    print('uploading')
    upload_cpc_current(db_con, cpc_current_file)
