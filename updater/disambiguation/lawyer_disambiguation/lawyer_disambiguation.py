import csv
import logging
import pickle as pickle
import re
import time
from collections import Counter
from collections import defaultdict
from datetime import date, datetime
from hashlib import md5
from pathlib import Path
from string import ascii_lowercase as alphabet
import multiprocessing as mp
from sqlalchemy import create_engine
from textdistance import jaro_winkler
from tqdm import tqdm
# from alchemy import get_config, match
from unidecode import unidecode

from QA.post_processing.LawyerPostProcessing import LawyerPostProcessingQC
from lib.configuration import get_connection_string, get_current_config
from lib.utilities import log_writer, update_version_indicator, update_to_granular_version_indicator


def prepare_tables(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr + "&local_infile=1")
    timestamp = str(int(time.time()))
    with engine.connect() as connection:
        q = f"CREATE TABLE rawlawyer_copy_backup_{timestamp} LIKE rawlawyer;"
        print(q)
        connection.execute(q)
        q = "INSERT INTO rawlawyer_copy_backup_{} SELECT * FROM rawlawyer".format(timestamp)
        print(q)
        connection.execute(q)

    with engine.connect() as connection:
        q = "ALTER TABLE rawlawyer ADD COLUMN alpha_lawyer_id varchar(128) AFTER organization;"
        print(q)
        connection.execute(q)

    with engine.connect() as connection:
        q_list = ["UPDATE rawlawyer rc SET rc.alpha_lawyer_id  = rc.organization WHERE rc.organization IS NOT NULL;"
                , "UPDATE rawlawyer rc SET rc.alpha_lawyer_id  = concat(rc.name_first, '|', rc.name_last) WHERE rc.name_first IS NOT NULL AND rc.name_last IS NOT NULL;"
                , "UPDATE rawlawyer rc SET rc.alpha_lawyer_id  = '' WHERE rc.alpha_lawyer_id IS NULL;"]
        for query in q_list:
            print(query)
            connection.execute(query)


def clean_rawlawyer(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr + "&local_infile=1")
    nodigits = re.compile(r'[^\d]+')
    disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'disambig_output')
    Path(disambig_folder).mkdir(parents=True, exist_ok=True)
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
            q = 'SELECT * from rawlawyer order by uuid limit {} offset {}'.format(limit, offset)
            print(q)
            rawlaw_chunk = db_con.execute(q)

            for lawyer in tqdm(rawlaw_chunk, total=limit, desc="rawlawyer processing - batch:" + str(batch_counter)):
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


def load_clean_rawlawyer(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr + "&local_infile=1")
    disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'disambig_output')
    q1 = "ALTER TABLE rawlawyer RENAME TO temp_rawlawyer_predisambig;"
    print(q1)
    engine.execute(q1)
    q2 = "CREATE TABLE rawlawyer LIKE temp_rawlawyer_predisambig;"
    print(q2)
    engine.execute(q2)
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
                'pk':     x,
                'update': param['id']
                } for x in tmpids])

    def load_lawyer_to_db(self):
        # from os import listdir
        # from os.path import isfile, join
        from updater.disambiguation.lawyer_disambiguation.alchemy.schema import RawLawyer, Lawyer, patentlawyer
        from updater.disambiguation.lawyer_disambiguation.tasks import bulk_commit_inserts, bulk_commit_updates
        from updater.disambiguation.lawyer_disambiguation import alchemy
        session = alchemy.fetch_session(dbtype=self.doctype)
        session.execute('set foreign_key_checks = 0;')
        session.commit()

        # pickle_files = [f for f in listdir(self.disambig_folder) if isfile(join(self.disambig_folder, f))]
        # for letter in ['u','q']:
        for letter in alphabet:
            self.lawyer_insert_statements = pickle.load(open(self.disambig_folder + '/' + f'lawyer_insert_statements_{letter}.pickle', 'rb'))
            t1 = bulk_commit_inserts(self.lawyer_insert_statements, Lawyer.__table__, 20000, 'grant')

            self.patentlawyer_insert_statements = pickle.load(open(self.disambig_folder + '/' + f'patentlawyer_insert_statements_{letter}.pickle', 'rb'))
            t2 = bulk_commit_inserts(self.patentlawyer_insert_statements, patentlawyer, 20000)

            self.update_statements = pickle.load(open(self.disambig_folder + '/' + f'update_statements_{letter}.pickle', 'rb'))
            t3 = bulk_commit_updates('lawyer_id', self.update_statements, RawLawyer.__table__, 20000)
            print(f"Finished uploading data for letter: {letter}!")
            print(" ")

        session.close_all()

    def create_lawyer_table(self, id_map, lawyer_dict, letter):
        """
        Given a list of lawyers and the redis key-value disambiguation,
        populates the lawyer table in the database
        """
        print('Disambiguating lawyers...', flush=True)
        from updater.disambiguation.lawyer_disambiguation import alchemy
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

        pickle.dump(self.lawyer_insert_statements, open(self.disambig_folder + '/' + f'lawyer_insert_statements_{letter}.pickle', 'wb'))
        pickle.dump(self.patentlawyer_insert_statements, open(self.disambig_folder + '/' + f'patentlawyer_insert_statements_{letter}.pickle', 'wb'))
        pickle.dump(self.update_statements, open(self.disambig_folder + '/' + f'update_statements_{letter}.pickle', 'wb'))
        print(f"Finished Exporting Letter {letter} to pickle", i, datetime.now(), flush=True)

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
        print(f'lawyer blocks created for letter: {list_of_lawyers[0][0]}!', flush=True)

    def process_alphabet(self, letter, log_queue):
        from updater.disambiguation.lawyer_disambiguation import alchemy
        from updater.disambiguation.lawyer_disambiguation.alchemy.schema import RawLawyer
        # track memory usage by letter
        log_queue.put({
            "level": logging.INFO,
            "message": "STARTING LETTER: {letter} at {t}".format(letter=letter, t=datetime.now())})
        # bookkeeping
        id_map = defaultdict(list)
        lawyer_dict = {}
        letterblock = []
        # query by letter
        session = alchemy.fetch_session(dbtype=self.doctype)
        lawyers_object = session.query(RawLawyer).filter(RawLawyer.alpha_lawyer_id.like(letter + '%'))
        print("query returned")
        for lawyer in tqdm(lawyers_object):
            lawyer_dict[lawyer.uuid] = lawyer
            id_map[lawyer.alpha_lawyer_id].append(lawyer.uuid)
            letterblock.append(lawyer.alpha_lawyer_id)
        session.close_all()
        print(letterblock[0:3])
        print(" ")
        self.create_jw_blocks(letterblock)
        self.create_lawyer_table(id_map, lawyer_dict, letter)
        print(f"Finished with Letter:{letter}!")

    def run_disambiguation(self):

        # get all lawyers in database
        print("running")
        # global blocks
        # global lawyer_insert_statements
        # global patentlawyer_insert_statements
        # global update_statements
        #
        completed_alphabets = []
        parallelism = int(self.config["PARALLELISM"]["parallelism"])
        manager = mp.Manager()
        log_queue = manager.Queue()

        parser_start = time.time()
        pool = mp.Pool(parallelism)
        watcher = pool.apply_async(log_writer, (log_queue, "lawyer_disambiguation"))
        p_list = []
        # process_cpc_file(cpc_xml_file, list(xml_file_name_generator)[-1], config, log_queue, csv_queue)
        # for letter in ['q','u']:
        for letter in alphabet:
            p = pool.apply_async(self.process_alphabet, (letter, log_queue))
            p_list.append(p)

        for t in p_list:
            t.get()

        log_queue.put({
            "level": logging.INFO,
            "message": "Total disambiguation time {parser_duration}".format(
                parser_duration=round(time.time() - parser_start, 3))
        })
        log_queue.put({
            "level": None,
            "message": "kill"
        })
        watcher.get()
        pool.close()
        pool.join()


def rawlawyer_postprocesing(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    cstr = get_connection_string(config, 'PROD_DB')
    engine = create_engine(cstr + "&local_infile=1")
    q = "ALTER TABLE rawlawyer DROP alpha_lawyer_id"
    print(q)
    engine.execute(q)
    engine.dispose()
    update_version_indicator('lawyer', 'granted_patent', **kwargs)
    # update_to_granular_version_indicator('rawlawyer', 'granted_patent')


def start_lawyer_disambiguation(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    disambiguator = LawyerDisambiguator(config)
    disambiguator.run_disambiguation()
    disambiguator.load_lawyer_to_db()

def post_process_qc(**kwargs):
    config = get_current_config(schedule='quarterly', **kwargs)
    qc = LawyerPostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    start_lawyer_disambiguation(**{
        "execution_date": date(2021, 10, 1)
    })

