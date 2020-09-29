import csv
import pickle as pickle
import re
import time
from collections import Counter
from collections import defaultdict
from datetime import datetime
from hashlib import md5
from string import ascii_lowercase as alphabet

import tqdm
from sqlalchemy import create_engine
from textdistance import jaro_winkler

# from alchemy import get_config, match
from unidecode import unidecode

from QA.post_processing.LawyerPostProcessing import LawyerPostProcessingQC
from lib.configuration import get_config, get_connection_string



def prepare_tables(config):
    cstr = get_connection_string(config, 'NEW_DB')
    engine = create_engine(cstr + "&local_infile=1")
    timestamp = str(int(time.time()))
    with engine.connect() as connection:
        connection.execute("CREATE TABLE rawlawyer_copy_backup_{} LIKE rawlawyer;".format(timestamp))
        connection.execute("INSERT INTO rawlawyer_copy_backup_{} SELECT * FROM rawlawyer".format(timestamp))

    with engine.connect() as connection:
        connection.execute("ALTER TABLE rawlawyer ADD COLUMN alpha_lawyer_id varchar(128) AFTER organization;")

    with engine.connect() as connection:
        connection.execute(
                "UPDATE rawlawyer rc SET rc.alpha_lawyer_id  = rc.organization WHERE rc.organization IS NOT NULL;")
        connection.execute(
                "UPDATE rawlawyer rc SET rc.alpha_lawyer_id  = concat(rc.name_first, '|', rc.name_last) WHERE "
                "rc.name_first IS NOT NULL AND rc.name_last IS NOT NULL;")
        connection.execute("UPDATE rawlawyer rc SET rc.alpha_lawyer_id  = '' WHERE rc.alpha_lawyer_id IS NULL;")


def clean_rawlawyer(config):
    cstr = get_connection_string(config, 'NEW_DB')
    engine = create_engine(cstr + "&local_infile=1")
    nodigits = re.compile(r'[^\d]+')
    disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'disambig_output')
    outfile = csv.writer(open(disambig_folder + '/rawlawyer_cleanalphaids.tsv', 'w'), delimiter='\t')
    outfile.writerow(
            ['uuid', 'lawyer_id', 'patent_id', 'name_first', 'name_last', 'organization', 'cleaned_alpha_lawyer_id',
             'country',
             'sequence'])

    stoplist = ['the', 'of', 'and', 'a', 'an', 'at']

    batch_counter = 0
    limit = 300000
    offset = 0

    # process rawlawyer table in chunks
    while True:
        batch_counter += 1
        counter = 0
        with engine.connect() as db_con:
            rawlaw_chunk = db_con.execute(
                    'SELECT * from rawlawyer order by uuid limit {} offset {}'.format(limit, offset))

            for lawyer in tqdm.tqdm(rawlaw_chunk, total=limit,
                                    desc="rawlawyer processing - batch:" + str(batch_counter)):
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


def load_clean_rawlawyer(config):
    cstr = get_connection_string(config, 'NEW_DB')
    engine = create_engine(cstr + "&local_infile=1")
    disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'disambig_output')
    engine.execute("ALTER TABLE rawlawyer RENAME TO rawlawyer_predisambig;")
    engine.execute("CREATE TABLE rawlawyer LIKE rawlawyer_predisambig;")
    engine.execute(
            """
LOAD DATA LOCAL INFILE '{}' INTO TABLE rawlawyer FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' IGNORE 1 LINES 
(uuid, @vlawyer_id, @vpatent_id, @vname_first, @vname_last, @vorganization, @valpha_lawyer_id, @vcountry, @vsequence) 
SET lawyer_id = NULLIF(@vlawyer_id,''),patent_id = NULLIF(@vpatent_id,''),name_first = NULLIF(@vname_first,''),
name_last = NULLIF(@vname_last,''),organization = NULLIF(@vorganization,''),alpha_lawyer_id = NULLIF(
@valpha_lawyer_id,''),country = NULLIF(@vcountry,''),sequence= CAST(@vsequence AS UNSIGNED);
            """.format(disambig_folder + '/rawlawyer_cleanalphaids.tsv'))


class LawyerDisambiguator:
    def __init__(self, config, doctype='grant'):
        self.lawyer_insert_statements = []
        self.patentlawyer_insert_statements = []
        self.update_statements = []
        self.blocks = defaultdict(list)
        self.config = config
        self.doctype = doctype
        self.disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'disambig_output')
        self.THRESHOLD = config["LAWYER"]["THRESHOLD"]

    timestamp = str(int(time.time()))

    def lawyer_match(self, objects, session, commit=False):

        freq = defaultdict(Counter)
        param = {}
        raw_objects = []
        clean_objects = []
        clean_cnt = 0
        clean_main = None
        class_type = None
        class_type = None
        for obj in objects:
            if not obj:
                continue
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
            param["id"] = md5(unidecode(param["name_last"] + param["name_first"]).encode('utf-8')).hexdigest()

        self.lawyer_insert_statements.append(param)
        tmpids = [x.uuid for x in objects]
        patents = [x.patent_id for x in objects]
        self.patentlawyer_insert_statements.extend([{
                                                            'patent_id': x,
                                                            'lawyer_id': param['id']
                                                            } for x in patents])
        self.update_statements.extend([{
                                               'pk': x,
                                               'update': param['id']
                                               } for x in tmpids])

    def create_lawyer_table(self, id_map, lawyer_dict):
        """
        Given a list of lawyers and the redis key-value disambiguation,
        populates the lawyer table in the database
        """
        from updater.disambiguation.lawyer_disambiguation import alchemy
        from updater.disambiguation.lawyer_disambiguation.alchemy.schema import RawLawyer,Lawyer, patentlawyer
        from updater.disambiguation.lawyer_disambiguation.tasks import bulk_commit_inserts, bulk_commit_updates
        print('Disambiguating lawyers...', flush=True)
        session = alchemy.fetch_session(dbtype=self.doctype)
        session.execute('set foreign_key_checks = 0;')
        session.commit()
        i = 0
        for lawyer in self.blocks.keys():
            ra_ids = (id_map[ra] for ra in self.blocks[lawyer])
            for block in ra_ids:
                if block == []:
                    continue
                else:
                    i += 1
                    rawlawyers = [lawyer_dict[ra_id] for ra_id in block]
                    if i % 20000 == 0:
                        print(i, datetime.now(), flush=True)
                        self.lawyer_match(rawlawyers, session, commit=True)
                    else:
                        self.lawyer_match(rawlawyers, session, commit=False)
        t1 = bulk_commit_inserts(self.lawyer_insert_statements, Lawyer.__table__, 20000, 'grant')
        t2 = bulk_commit_inserts(self.patentlawyer_insert_statements, patentlawyer, 20000)
        t3 = bulk_commit_updates('lawyer_id', self.update_statements, RawLawyer.__table__, 20000)
        # t1.get()
        # t2.get()
        # t3.get()
        # session.commit()
        print(i, datetime.now(), flush=True)

    def create_jw_blocks(self, list_of_lawyers):
        """
        Receives list of blocks, where a block is a list of lawyers
        that all begin with the same letter. Within each block, does
        a pairwise jaro winkler comparison to block lawyers together
        """
        consumed = defaultdict(int)
        print('Doing pairwise Jaro-Winkler...', len(list_of_lawyers), flush=True)
        for i, primary in enumerate(list_of_lawyers):
            if consumed[primary]:
                continue
            consumed[primary] = 1
            self.blocks[primary].append(primary)
            for secondary in list_of_lawyers[i:]:
                if consumed[secondary]:
                    continue
                if primary == secondary:
                    self.blocks[primary].append(secondary)
                    continue
                if jaro_winkler(primary, secondary, 0.0) >= float(self.THRESHOLD):
                    consumed[secondary] = 1
                    self.blocks[primary].append(secondary)
        pickle.dump(self.blocks, open(self.disambig_folder + '/' + 'lawyer.pickle', 'wb'))
        print('lawyer blocks created!', flush=True)

    def run_disambiguation(self):
        from updater.disambiguation.lawyer_disambiguation import alchemy
        from updater.disambiguation.lawyer_disambiguation.alchemy.schema import RawLawyer,Lawyer, patentlawyer


        # get all lawyers in database
        print("running")
        # global blocks
        # global lawyer_insert_statements
        # global patentlawyer_insert_statements
        # global update_statements
        #
        session = alchemy.fetch_session(dbtype=self.doctype)

        print("going through alphabet")
        for letter in alphabet:
            # track memory usage by letter
            print("letter is: ", letter, datetime.now(), flush=True)
            # bookkeeping
            id_map = defaultdict(list)
            lawyer_dict = {}
            letterblock = []
            # query by letter
            lawyers_object = session.query(RawLawyer).filter(RawLawyer.alpha_lawyer_id.like(letter + '%'))
            print("query returned")
            for lawyer in lawyers_object:
                lawyer_dict[lawyer.uuid] = lawyer
                id_map[lawyer.alpha_lawyer_id].append(lawyer.uuid)
                letterblock.append(lawyer.alpha_lawyer_id)
            print(letterblock[0:5])
            self.create_jw_blocks(letterblock)
            self.create_lawyer_table(id_map, lawyer_dict)


def start_lawyer_disambiguation(config):
    prepare_tables(config)
    clean_rawlawyer(config)
    load_clean_rawlawyer(config)
    disambiguator = LawyerDisambiguator(config)
    disambiguator.run_disambiguation()
    cstr = get_connection_string(config, 'NEW_DB')
    engine = create_engine(cstr + "&local_infile=1")

    engine.execute("ALTER TABLE rawlawyer DROP alpha_lawyer_id")
    engine.dispose()


def post_process_qc(config):
    qc = LawyerPostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
    start_lawyer_disambiguation(config)
