import sys
import os
import re
import csv
sys.path.append('/project/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers
import pandas as pd
import multiprocessing

def write_uspc_current(in_file, out_file, error_log, patent_dict, patent_set):
    #Create USPC_current table off full master classification list
    uspc_data= csv.reader(open(in_file,'r'), delimiter = ',')
    errorlog = open(error_log,'w')
    uspc_current_file = csv.writer(open(out_file, 'w'), delimiter = '\t')
    uspc_current_file.writerow(['uuid', 'patent_id', 'mainclass_id', 'subclass_id', 'sequence'])
    for row in uspc_data:
        if str(row[0]) in patent_set:
            towrite = [re.sub('"',"'",item) for item in row]
            towrite.insert(0,general_helpers.id_generator())
            towrite[1] = patent_dict[row[0]]
            for t in range(len(towrite)):
                try:
                    gg = int(towrite[t])
                    towrite[t] = str(int(towrite[t]))
                except:
                    pass
            try:
                towrite[3] = towrite[2]+'/'+re.sub('^0+','',towrite[3])
            except:
                errorlog.write(str(towrite)) 

            #change only for patents from after 2015-05-31, based on sequential numbering
            if is_after_2015(towrite[1]):
                if towrite[3] == '1/1':
                    towrite[3] = "No longer published"
                    towrite[2] = "No longer published"
            uspc_current_file.writerow(towrite)

def is_after_2015(patent_id):
    '''determine if a patent is from after 2015 based on sequential numbering, cutoffs determined empirically '''
    try:
        patent_id = int(patent_id)
        return True if patent_id >=9043948  else False
    except:
        first_char = patent_id[0]
        if first_char == 'D':
            return True if int(patent_id[1:])>=730616 else False
        elif first_char =='P':
            return True if int(patent_id[2:])>=25608 else False
        elif first_char=='R':
            return True if int(patent_id[2:])>=45533 else False
        elif first_char=='T' or first_char=='H':
            return False #no patents starting the T or H are past the cut off date
        else:
            print(patent_id)
            return False

def upload_uspc_current(db_con, uspc_current_loc):
    uspc_current_files = [f for f in os.listdir(uspc_current_loc) if f.startswith('out')]
    print(uspc_current_files)
    for outfile in uspc_current_files:
        print(outfile)
        if os.path.getsize('{}/{}'.format(uspc_current_loc,outfile)):
            data = pd.read_csv('{}/{}'.format(uspc_current_loc,outfile),delimiter = '\t', encoding ='utf-8')
            data.to_sql('uspc_current', db_con, if_exists = 'append', index=False)


def find_missed_patents(db_con, patent_set, uspc_output):
    patents_in_uspc = db_con.execute('select patent_id from uspc_current')
    current_exists = set()
    for patent in patents_in_uspc:
        current_exists.add(patent['patent_id'])

    missed_patents = list(patent_set - current_exists)

    if len(missed_patents) > 0:
        main = db_con.execute('select id from mainclass_current')
        mainclass = set([item['id'] for item in main])
        sub = db_con.execute('select id from subclass_current')
        subclass = set([item['id'] for item in sub])

        to_add_main = []
        to_add_sub = []
        to_add_uspc_current = []

        count =0
        print(len(missed_patents))
        for patent in missed_patents:
            uspc_data = db_con.execute('select * from uspc where patent_id ="{}"'.format(patent))
            uspc_data_clean = [item for item in uspc_data]
            count +=1
            if count%1000 ==0:
                print(str(count))
            for d in uspc_data_clean:
                if not d['mainclass_id'] in mainclass:
                    to_add_main.append(d['mainclass_id'])
                if not d['subclass_id'] in subclass:
                    to_add_sub.append(d['subclass_id'])
                sequence = int(d['sequence'])
                to_add_uspc_current.append([d['uuid'], d['patent_id'], d['mainclass_id'], d['subclass_id'], sequence]) 
        print("got final data for update")
        data = pd.DataFrame(to_add_uspc_current)
        data.columns = ['uuid', 'patent_id', 'mainclass_id', 'subclass_id', 'sequence']
        data.to_sql(con=db_con, name='temp_uspc_current_to_insert', index = False, if_exists='replace')
        print('uploaded data, inserting')
        db_con.execute('insert into uspc_current select * from temp_uspc_current_to_insert;')
        print("done")


if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('/project/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], 
                                            config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
    
    patent_set, patent_dict = general_helpers.get_patent_ids(db_con, config['DATABASE']['NEW_DB'])
    uspc_folder = '{}/uspc_output'.format(config['FOLDERS']['WORKING_FOLDER'])
    os.system('split -a 1 -n 7 {0}/USPC_patent_classes_data.csv {0}/uspc_patent_pieces_'.format(uspc_folder))

    in_files = ['{0}/uspc_patent_pieces_{1}'.format(uspc_folder, item) for item in  ['a', 'b', 'c', 'd', 'e', 'f', 'g']]
    out_files = ['{0}/out_file_{1}.csv'.format(uspc_folder, item) for item in  ['a', 'b', 'c', 'd', 'e', 'f', 'g']]
    error_log = ['{0}/error_log_{1}'.format(uspc_folder, item) for item in  ['a', 'b', 'c', 'd', 'e', 'f', 'g']]

    pat_dicts = [patent_dict for item in in_files]
    pat_sets = [patent_set for item in in_files]
    files = zip(in_files, out_files, error_log, pat_dicts, pat_sets)


    desired_processes = 7# ussually num cpu - 1
    jobs = []
    for f in files:
        jobs.append(multiprocessing.Process(target = write_uspc_current, args=(f)))

    for segment in general_helpers.chunks(jobs, desired_processes):
        for job in segment:
            print(segment)
            job.start()
 
    upload_uspc_current(db_con, uspc_folder)
    find_missed_patents(db_con, patent_set, '{}/uspc_current.csv'.format(uspc_folder))
