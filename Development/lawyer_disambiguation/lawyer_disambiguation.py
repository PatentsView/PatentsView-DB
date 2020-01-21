import MySQLdb
import os
import csv
import sys
import pandas as pd
import tqdm
import sqlalchemy
import re
import time
from collections import defaultdict, deque
import uuid
from string import ascii_lowercase as alphabet
from hashlib import md5
import pickle as pickle
import alchemy
from textdistance import jaro_winkler
from collections import Counter
#from alchemy import get_config, match
from alchemy import match
from alchemy.schema import *
from alchemy.match import commit_inserts, commit_updates
from handlers.xml_util import normalize_utf8
from datetime import datetime
from sqlalchemy.sql import or_
from sqlalchemy.sql.expression import bindparam
from unidecode import unidecode
from tasks import bulk_commit_inserts, bulk_commit_updates
import configparser

project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers

import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')
new_db = config['DATABASE']['NEW_DB']
THRESHOLD = config["LAWYER"]["THRESHOLD"]

engine = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])
db_con = engine.connect() 

timestamp = str(int(time.time()))

### Make a copy of rawlawyer table ###
engine.execute("CREATE TABLE rawlawyer_copy_backup_{} LIKE rawlawyer;".format(timestamp))
engine.execute("INSERT INTO rawlawyer_copy_backup_{} SELECT * FROM rawlawyer".format(timestamp))
engine.execute("ALTER TABLE rawlawyer ADD COLUMN alpha_lawyer_id varchar(128) AFTER organization;")
engine.execute("UPDATE rawlawyer rc SET rc.alpha_lawyer_id  = rc.organization WHERE rc.organization IS NOT NULL;")
engine.execute("UPDATE rawlawyer rc SET rc.alpha_lawyer_id  = concat(rc.name_first, '|', rc.name_last) WHERE rc.name_first IS NOT NULL AND rc.name_last IS NOT NULL;")
engine.execute("UPDATE rawlawyer rc SET rc.alpha_lawyer_id  = '' WHERE rc.alpha_lawyer_id IS NULL;")


nodigits = re.compile(r'[^\d]+')


disambig_folder = '{}/{}/'.format(config['FOLDERS']['WORKING_FOLDER'], config['DATABASE']['TEMP_UPLOAD_DB'], 'disambig_output')


######## Formerly clean_alpha_lawyer_ids.ipynb ########
# create outfile
outfile = csv.writer(open(disambig_folder +'/rawlawyer_cleanalphaids.tsv','w'),delimiter='\t')
outfile.writerow(['uuid', 'lawyer_id', 'patent_id', 'name_first', 'name_last', 'organization', 'cleaned_alpha_lawyer_id', 'country', 'sequence'])

stoplist = ['the', 'of', 'and', 'a', 'an', 'at']
    
batch_counter = 0
limit = 300000
offset = 0

# process rawlawyer table in chunks
while True:
    batch_counter += 1
    counter=0

    rawlaw_chunk = db_con.execute('SELECT * from rawlawyer order by uuid limit {} offset {}'.format(limit, offset))

    for lawyer in tqdm.tqdm(rawlaw_chunk, total=limit, desc="rawlawyer processing - batch:" + str(batch_counter)):

        uuid_match = lawyer[0]
        law_id = lawyer[1]
        pat_id = lawyer[2]
        name_f = lawyer[3]
        name_l = lawyer[4]
        org = lawyer[5]
        a_id = lawyer[6]
        ctry = lawyer[7]
        seq = lawyer[8]
        
        # removes stop words, then rejoins the string
        a_id = ' '.join([x for x in a_id.split(' ') if x.lower() not in stoplist])
        cleaned_a_id = ''.join(nodigits.findall(a_id)).strip()

        # update cleaned_alpha_lawyer_id
        outfile.writerow([uuid_match, law_id, pat_id, name_f, name_l, org, cleaned_a_id, ctry, seq])
        counter += 1

    print('lawyers cleaned!', flush=True)

    # means we have no more batches to process
    if counter == 0:
        break


    offset = offset + limit
    print("processed batch: ", str(batch_counter))

###
engine.execute("ALTER TABLE rawlawyer RENAME TO rawlawyer_predisambig;")
engine.execute("CREATE TABLE rawlawyer LIKE rawlawyer_predisambig;")
engine.execute("LOAD DATA LOCAL INFILE {} INTO TABLE rawlawyer FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;".format(outfile))



######## Formerly debug_rawlawyer.ipynb ########

def create_jw_blocks(list_of_lawyers):
    """
    Receives list of blocks, where a block is a list of lawyers
    that all begin with the same letter. Within each block, does
    a pairwise jaro winkler comparison to block lawyers together
    """
    global blocks
    consumed = defaultdict(int)
    print('Doing pairwise Jaro-Winkler...', len(list_of_lawyers), flush=True)
    for i, primary in enumerate(list_of_lawyers):
        if consumed[primary]: continue
        consumed[primary] = 1
        blocks[primary].append(primary)
        for secondary in list_of_lawyers[i:]:
            if consumed[secondary]: continue
            if primary == secondary:
                blocks[primary].append(secondary)
                continue
            if jaro_winkler(primary, secondary, 0.0) >= float(THRESHOLD):
                consumed[secondary] = 1
                blocks[primary].append(secondary)
    pickle.dump(blocks, open('lawyer.pickle', 'wb'))
    print('lawyer blocks created!', flush=True)


lawyer_insert_statements = []
patentlawyer_insert_statements = []
update_statements = []
def create_lawyer_table(session):
    """
    Given a list of lawyers and the redis key-value disambiguation,
    populates the lawyer table in the database
    """
    print('Disambiguating lawyers...', flush=True)
    session.execute('set foreign_key_checks = 0;')
    session.commit()
    i = 0
    for lawyer in blocks.keys():
        ra_ids = (id_map[ra] for ra in blocks[lawyer])
        for block in ra_ids:
            i += 1
            rawlawyers = [lawyer_dict[ra_id] for ra_id in block]
            if i % 20000 == 0:
                print(i, datetime.now(), flush=True)
                lawyer_match(rawlawyers, session, commit=True)
            else:
                lawyer_match(rawlawyers, session, commit=False)
    t1 = bulk_commit_inserts(lawyer_insert_statements, Lawyer.__table__, 20000, 'grant')
    t2 = bulk_commit_inserts(patentlawyer_insert_statements, patentlawyer,  20000)
    t3 = bulk_commit_updates('lawyer_id', update_statements, RawLawyer.__table__, 20000)
    # t1.get()
    # t2.get()
    # t3.get()
    # session.commit()
    print(i, datetime.now(), flush=True)

def lawyer_match(objects, session, commit=False):
    freq = defaultdict(Counter)
    param = {}
    raw_objects = []
    clean_objects = []
    clean_cnt = 0
    clean_main = None
    class_type = None
    class_type = None
    for obj in objects:
        if not obj: continue
        class_type = obj.__related__
        raw_objects.append(obj)
        break

    param = {}
    for obj in raw_objects:
        for k, v in obj.summarize.items():
            freq[k][v] += 1
        if "id" not in param:
            param["id"] = obj.uuid
        param["id"] = min(param["id"], obj.uuid)

    # create parameters based on most frequent
    for k in freq:
        if None in freq[k]:
            freq[k].pop(None)
        if "" in freq[k]:
            freq[k].pop("")
        if freq[k]:
            param[k] = freq[k].most_common(1)[0][0]
    if 'organization' not in param:
        param['organization'] = ''
    if 'type' not in param:
        param['type'] = ''
    if 'name_last' not in param:
        param['name_last'] = ''
    if 'name_first' not in param:
        param['name_first'] = ''
    if 'residence' not in param:
        param['residence'] = ''
    if 'nationality' not in param:
        param['nationality'] = ''
    if 'country' not in param:
        param['country'] = ''

    if param["organization"]:
        param["id"] = md5(unidecode(param["organization"]).encode('utf-8')).hexdigest()
    if param["name_last"]:
        param["id"] = md5(unidecode(param["name_last"]+param["name_first"]).encode('utf-8')).hexdigest()
    
    lawyer_insert_statements.append(param)
    tmpids = [x.uuid for x in objects]
    patents = [x.patent_id for x in objects]
    patentlawyer_insert_statements.extend([{'patent_id': x, 'lawyer_id': param['id']} for x in patents])
    update_statements.extend([{'pk':x,'update':param['id']} for x in tmpids])

def run_disambiguation():
    # get all lawyers in database
    print("running")
    doctype='grant' 
    global blocks
    global lawyer_insert_statements
    global patentlawyer_insert_statements
    global update_statements

   
    session = alchemy.fetch_session(dbtype=doctype) 
    
    with open(disambig_folder + '/rawlawyer_memory_log.txt', 'w') as f:
        
    
        print("going through alphabet")
        for letter in alphabet:

            # track memory usage by letter
            f.write('letter is ' + letter + '\n')

            print("letter is: ", letter, datetime.now(), flush=True)

            # bookkeeping
            id_map = defaultdict(list)
            lawyer_dict = {}
            letterblock = []
            blocks = defaultdict(list)
            lawyer_insert_statements = []
            patentlawyer_insert_statements = []
            update_statements = []

            # query by letter        
            lawyers_object = session.query(RawLawyer).filter(RawLawyer.cleaned_alpha_lawyer_id.like(letter + '%'))

            print("query returned")
            
            f.write('size of lawyers_object for letter is ' + str(sys.getsizeof(lawyers_object)) + '\n')

            for lawyer in lawyers_object:
                lawyer_dict[lawyer.uuid] = lawyer
                id_map[lawyer.cleaned_alpha_lawyer_id].append(lawyer.uuid)
                letterblock.append(lawyer.cleaned_alpha_lawyer_id)

                
                
            f.write('size of lawyer_dict for letter is ' + str(sys.getsizeof(lawyer_dict)) + '\n')
            f.write('size of id_map for letter is ' + str(sys.getsizeof(id_map)) + '\n')
            f.write('size of letterblock for letter is ' + str(sys.getsizeof(letterblock)) + '\n')
            
            print(letterblock[0:5])
            create_jw_blocks(letterblock)
            
            f.write('size of blocks for letter is ' + str(sys.getsizeof(blocks)) + '\n')
            create_lawyer_table(session)
            
            f.write('size of lawyer_insert_statements for letter is ' + str(sys.getsizeof(lawyer_insert_statements)) + '\n')
            f.write('size of patentlawyer_insert_statements for letter is ' + str(sys.getsizeof(patentlawyer_insert_statements)) + '\n')
            f.write('size of update_statements for letter is ' + str(sys.getsizeof(update_statements)) + '\n')
            
            f.write('----------------------------------------------' + '\n')
            f.flush()
            
    f.close()

if __name__ == '__main__':
    run_disambiguation()
    engine.execute("ALTER TABLE rawlawyer DROP alpha_lawyer_id")
    engine.dispose()
