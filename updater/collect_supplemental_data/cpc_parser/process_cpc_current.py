import csv
import datetime
import logging
import billiard as mp
import os
import time
import uuid
import zipfile

from lxml import etree
from sqlalchemy import create_engine
from tqdm import tqdm

from lib.configuration import get_connection_string, get_current_config, get_version_indicator
from lib.utilities import generate_index_statements, log_writer, mp_csv_writer, xstr
from updater.create_databases.upload_new import setup_database
from updater.collect_supplemental_data.cpc_parser.pgpubs_cpc_parser import parse_and_write_cpc

def prepare_cpc_table(config, drop_indexes):
    """
    Prepare the CPC Current table by dropping Indexes
    :param config: Config file containing variour runtime paramters
    :param drop_indexes: List of Drop Index Statements
    """
    engine = create_engine(get_connection_string(config, "TEMP_UPLOAD_DB"))
    for drop_statement in drop_indexes:
        print(drop_statement[0])
        engine.execute(drop_statement[0])


def consolidate_cpc_data(config, add_indexes, db='granted_patent', cpc_file=None):
    """
    Finalize CPC Current table by removing patents not in patent database and re-adding indexes
    :param config: Consolidate_cpc_dataonfig file containing variour runtime paramters
    :param add_indexes: List of Add Index statments
    """
    import pandas as pd

    engine = create_engine(get_connection_string(config, "TEMP_UPLOAD_DB"))
    start_date = datetime.datetime.strptime(config['DATES']['START_DATE'], '%Y%m%d')
    suffix = (start_date - datetime.timedelta(days=1)).strftime('%Y%m%d')
    end_date = config['DATES']['END_DATE']

    if db == 'granted_patent':
        cpc_csv_file_chunks = pd.read_csv(cpc_file, sep=",", quoting=csv.QUOTE_NONNUMERIC, chunksize=100_000)
        for cpc_chunk in tqdm(cpc_csv_file_chunks):
            with engine.connect() as conn:
                cpc_chunk.to_sql('cpc_current', conn, if_exists='append', index=False, method="multi")
        delete_query = "DELETE cpc FROM cpc_current cpc Inner JOIN {raw_db}.patent p on p.id = cpc.patent_id WHERE p.id is null or p.version_indicator >'{vind}'"\
                .format(raw_db=config["PATENTSVIEW_DATABASES"]["RAW_DB"], vind=end_date)
        dedup_ind_add =["ALTER TABLE cpc_current ADD INDEX patent_id_ind (patent_id) USING BTREE;",
                        "ALTER TABLE cpc_current ADD INDEX subgroup_id_ind (subgroup_id) USING BTREE;",
                        "ALTER TABLE cpc_current ADD INDEX sequence_ind (sequence) USING BTREE;"]
        dedup_query = """
            DELETE t1 FROM cpc_current t1 
            INNER JOIN cpc_current t2
            ON (t1.patent_id = t2.patent_id 
            AND t1.subgroup_id = t2.`subgroup_id`
            AND t1.sequence = t2.sequence
            AND t1.uuid < t2.uuid);
            """
        dedup_ind_drop = ["ALTER TABLE cpc_current DROP INDEX patent_id_ind;", 
                          "ALTER TABLE cpc_current DROP INDEX subgroup_id_ind;", 
                          "ALTER TABLE cpc_current DROP INDEX sequence_ind;"]
    else:
        delete_query = "DELETE cpc FROM {raw_db}.cpc_current cpc Inner JOIN {raw_db}.publication p on p.document_number = cpc.document_number WHERE p.document_number is null or p.version_indicator >'{vind}'".format(
        raw_db=config["PATENTSVIEW_DATABASES"]["PROD_DB"], vind=end_date)
        dedup_ind_add = ["ALTER TABLE cpc_current ADD INDEX document_number_ind (document_number) USING BTREE;",
                         "ALTER TABLE cpc_current ADD INDEX subgroup_id_ind (subgroup_id) USING BTREE;" ,
                         "ALTER TABLE cpc_current ADD INDEX sequence_ind (sequence) USING BTREE;"]
        dedup_query = """
            DELETE t1 FROM cpc_current t1 
            INNER JOIN cpc_current t2
            ON (t1.document_number = t2.document_number 
            AND t1.subgroup_id = t2.`subgroup_id`
            AND t1.sequence = t2.sequence
            AND t1.uuid < t2.uuid);
            """
        dedup_ind_drop = ["ALTER TABLE cpc_current DROP INDEX document_number_ind;",
                          "ALTER TABLE cpc_current DROP INDEX subgroup_id_ind;",
                          "ALTER TABLE cpc_current DROP INDEX sequence_ind;"]
    print(f"initial size of cpc_current: {engine.execute('SELECT COUNT(*) FROM cpc_current').fetchone()[0]}")
    print("deleting patents absent in patent table:")
    print(delete_query)
    deletion = engine.execute(delete_query)
    print(f"number of records removed: {deletion.rowcount}")
    print("adding temporary indices for deduplication")
    for q in dedup_ind_add:
        print(q)
        engine.execute(q)
    print("deduplicating table on document id, subgroup id, and sequence:")
    print(dedup_query)
    deduplication = engine.execute(dedup_query)
    print(f"number of duplicates removed: {deduplication.rowcount}") # this may slightly overcount due to patents with multiple duplicates
    print("removing temporary indices")
    for q in dedup_ind_drop:
        print(q)
        engine.execute(q)
    print(f"final size of cpc_current: {engine.execute('SELECT COUNT(*) FROM cpc_current').fetchone()[0]}")
    print("adding permanent table indices:")
    for add_statement in add_indexes:
        print(add_statement)
        engine.execute(add_statement[0])
    rename_raw_statement = """
    rename table {raw_db}.cpc_current to {raw_db}.cpc_current_{suffix}
    """.format(raw_db=config["PATENTSVIEW_DATABASES"]["PROD_DB"], suffix=suffix)
    rename_upload_statement = """
    rename table {upload_db}.cpc_current to {raw_db}.cpc_current
    """.format(raw_db=config["PATENTSVIEW_DATABASES"]["PROD_DB"],
               upload_db=config["PATENTSVIEW_DATABASES"]["TEMP_UPLOAD_DB"])
    print("renaming past cpc_current table:")
    print(rename_raw_statement)
    engine.execute(rename_raw_statement)
    print("relocating new cpc_current table to production:")
    print(rename_upload_statement)
    engine.execute(rename_upload_statement)


def generate_file_list(zipfilepath, extension='.xml'):
    """
    Generate a list of files contained in zip archive with given extenstion
    :param zipfilepath: Zip Archive path
    :param extension: Extension of filenames to return
    """
    archive = zipfile.ZipFile(zipfilepath, 'r')
    for filename in archive.filelist:
        fname, ext = os.path.splitext(filename.filename)
        if ext == extension:
            # print(filename.filename) # printed outside of generator as well
            yield filename.filename


def get_cpc_xml_root(zipfilepath, filename):
    """
    Obtain LXML root element from loading XML file content
    :param zipfilepath: Zip Archive path
    :param filename: XML File to parse
    :return: LXML Element object
    """
    archive = zipfile.ZipFile(zipfilepath, 'r')
    cpcxml = archive.read(filename)
    tree = etree.fromstring(cpcxml)
    return tree


def get_cpc_components_from_xml(cpc_xml_element, ns):
    """
    Extract CPC components and format them into cpc_current format
    :param cpc_xml_element: LXML element of CPC element
    :param ns: XML namespaces as obtained by LXML
    :return: A dictionary of cpc classification compenents
    """
    class_code_element = cpc_xml_element.find(
        'pat:CPCClassificationValueCode', namespaces=ns)
    class_code = class_code_element.text if class_code_element is not None else None
    cpc_section_element = cpc_xml_element.find(
        'pat:CPCSection', namespaces=ns)
    cpc_section = cpc_section_element.text if cpc_section_element is not None else None
    cpc_class_element = cpc_xml_element.find('pat:Class',
                                             namespaces=ns)
    cpc_class = cpc_class_element.text if cpc_class_element is not None else None
    cpc_subclass_element = cpc_xml_element.find(
        'pat:Subclass', namespaces=ns)
    cpc_subclass = cpc_subclass_element.text if cpc_subclass_element is not None else None
    cpc_maingroup_element = cpc_xml_element.find(
        'pat:MainGroup', namespaces=ns)
    cpc_maingroup = cpc_maingroup_element.text if cpc_maingroup_element is not None else None
    cpc_subgroup_element = cpc_xml_element.find(
        'pat:Subgroup', namespaces=ns)
    cpc_subgroup = cpc_subgroup_element.text if cpc_subgroup_element is not None else None
    if not cpc_section:
        return []
    return [xstr(uuid.uuid4()), xstr(cpc_section),
            "{cpc_section}{cpc_class}".format(cpc_section=xstr(cpc_section), cpc_class=xstr(cpc_class)),
            "{cpc_section}{cpc_class}{cpc_subclass}".format(cpc_section=xstr(cpc_section), cpc_class=xstr(cpc_class),
                                                            cpc_subclass=xstr(cpc_subclass)),
            "{cpc_section}{cpc_class}{cpc_subclass}{cpc_maingroup}/{cpc_subgroup}".format(cpc_section=xstr(cpc_section),
                                                                                          cpc_class=xstr(cpc_class),
                                                                                          cpc_subclass=xstr(
                                                                                              cpc_subclass),
                                                                                          cpc_maingroup=xstr(
                                                                                              cpc_maingroup),
                                                                                          cpc_subgroup=xstr(
                                                                                              cpc_subgroup)),
            'inventional' if class_code == 'I' else 'additional']


def get_cpc_records(xml_root, version_indicator):
    """
    Extract CPC records from a XML element
    :param xml_root: LXML element root
    """
    pat_path = "pat:PatentGrantIdentification/pat:PatentNumber"
    main_cpc_path = "pat:CPCClassificationBag/pat:MainCPC"
    further_cpc_path = "pat:CPCClassificationBag/pat:FurtherCPC"
    ns = xml_root.nsmap
    for element in xml_root.getchildren():
        pat_number = element.find(pat_path, namespaces=ns).text
        sequence = 0
        main_cpc_element = element.find(main_cpc_path, ns)
        for main_cpc_element in main_cpc_element.getchildren():
            cpc_record = [pat_number, sequence, version_indicator] + get_cpc_components_from_xml(main_cpc_element, ns)
            sequence = sequence + 1
            yield cpc_record

        further_cpc_element = element.find(further_cpc_path, namespaces=ns)
        if further_cpc_element is not None:
            for further_cpc_element in further_cpc_element.getchildren():
                cpc_record = [pat_number, sequence, version_indicator] + get_cpc_components_from_xml(
                    further_cpc_element, ns)
                sequence = sequence + 1
                yield cpc_record


def load_cpc_records(records_generator, log_queue, csv_queue):
    """
    Load Extracter CPC Records into CPC Current Table
    :param records_generator: generator that produces cpc records as dictionary
    """
    # engine = create_engine(get_connection_string(config, "TEMP_UPLOAD_DB"))
    start = time.time()
    # with engine.connect() as conn:
    counter = 0
    save_counter = 0
    for cpc_record in records_generator:
        counter += 1
        if len(cpc_record) > 3:
            save_counter += 1
            csv_queue.put(cpc_record)
    end = time.time()
    duration = str(round(end - start))
    log_queue.put({
        "level": logging.INFO,
        "message": f"Chunk Load Time: {duration}. Saved {save_counter} out of {counter}".format(
            duration=duration, save_counter=save_counter, counter=counter)
    })


def process_cpc_file(cpc_xml_zip_file, cpc_xml_file, config, log_queue, writer):
    start = time.time()
    xml_tree = get_cpc_xml_root(cpc_xml_zip_file, cpc_xml_file)
    cpc_records = get_cpc_records(xml_tree, version_indicator=config['DATES']['END_DATE'])
    load_cpc_records(cpc_records, log_queue, writer)
    end = time.time()
    log_queue.put({
        "level": logging.INFO,
        "message": "XML File {xml_file} Processing Time: {duration}".format(
            duration=round(end - start),
            xml_file=cpc_xml_file)
    })

# process_and_upload_patent_cpc_current
def process_and_upload_cpc_current(db='granted_patent', **kwargs):
    config = get_current_config(db, schedule='quarterly', **kwargs)
    setup_database(config, drop=False, cpc_only=True)
    cpc_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
    cpc_output_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_output')

    if db == 'granted_patent':
        file_name = "CPC_grant_mcf"
    else:
        file_name = "CPC_pgpub_mcf"

    cpc_xml_file = None
    for filename in os.listdir(cpc_folder):
        if (filename.startswith(file_name) and filename.endswith('.zip')):
            cpc_xml_file = "{cpc_folder}/{cpc_file}".format(cpc_folder=cpc_folder, cpc_file=filename)
            print(cpc_xml_file)

    if cpc_xml_file:
        add_index, drop_index = generate_index_statements(config, "PROD_DB", "cpc_current")
        prepare_cpc_table(config, drop_index)

        if db == 'granted_patent':
            xml_file_name_generator = generate_file_list(cpc_xml_file)
            print(xml_file_name_generator)

            parallelism = int(config["PARALLELISM"]["parallelism"])
            manager = mp.Manager()
            log_queue = manager.Queue()
            csv_queue = manager.Queue()

            parser_start = time.time()
            pool = mp.Pool(parallelism)
            cpc_file = "{data_folder}/cpc_current.csv".format(data_folder=cpc_output_folder)
            header = ["patent_id", "sequence", "version_indicator", "uuid", "section_id", "subsection_id",
                      "group_id",
                      "subgroup_id", "category"]
            watcher = pool.apply_async(log_writer, (log_queue, "cpc_parser"))
            writer = pool.apply_async(mp_csv_writer, (
                csv_queue,
                cpc_file, header))
            p_list = []
            # process_cpc_file(cpc_xml_file, list(xml_file_name_generator)[-1], config, log_queue, csv_queue)
            for xml_file_name in xml_file_name_generator:
                print(xml_file_name)
                # process_cpc_file(cpc_xml_file, xml_file_name, config, log_queue, csv_queue)
                p = pool.apply_async(process_cpc_file, (cpc_xml_file, xml_file_name, config, log_queue, csv_queue))
                p_list.append(p)

            for t in p_list:
                t.get()

            log_queue.put({
                "level": logging.INFO,
                "message": "Total parsing time {parser_duration}".format(
                    parser_duration=round(time.time() - parser_start, 3))
            })
            log_queue.put({
                "level": None,
                "message": "kill"
            })
            csv_queue.put(['kill'])
            watcher.get()
            writer.get()
            pool.close()
            pool.join()
            consolidate_cpc_data(config, add_index, db, cpc_file)
        else:
            parse_and_write_cpc(cpc_folder, **kwargs)
            consolidate_cpc_data(config, add_index, db)
    else:
        print("Could not find CPC Zip XML file under {cpc_input}".format(cpc_input=cpc_folder))
        exit(1)

def delete_cpc_currents_pre_1976(db='granted_patent', **kwargs):
    config = get_current_config(db, schedule='quarterly', **kwargs)
    engine = create_engine(get_connection_string(config, "PROD_DB"))
    query = """
delete 
from cpc_current a 
	left join patent b on a.patent_id=b.patent_id 
where b.patent_id is null;
    """
    print(query)
    engine.execute(query)


if __name__ == '__main__':
    process_and_upload_cpc_current(db='pgpubs', **{
        "execution_date": datetime.date(2021, 12, 30)
    })
