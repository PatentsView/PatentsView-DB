import logging
import time
import multiprocessing as mp
from lxml import etree
import zipfile
import os
from io import StringIO, BytesIO
import pandas as pd
import uuid
from sqlalchemy import create_engine

from lib.configuration import get_connection_string, get_config
from lib.utilities import generate_index_statements, log_writer


def prepare_cpc_table(config, drop_indexes):
    """
    Prepare the CPC Current table by dropping Indexes
    :param config: Config file containing variour runtime paramters
    :param drop_indexes: List of Drop Index Statements
    """
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    for drop_statement in drop_indexes:
        engine.execute(drop_statement[0])


def consolidate_cpc_data(config, add_indexes):
    """
    Finalize CPC Current table by removing patents not in patent database and re-adding indexes
    :param config: Config file containing variour runtime paramters
    :param add_indexes: List of Add Index statments
    """
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    delete_query = "DELETE cpc FROM cpc_current cpc LEFT JOIN patent p on p.id = cpc.patent_id WHERE p.id is null"
    engine.execute(delete_query)
    for add_statement in add_indexes:
        engine.execute(add_statement[0])


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

    return {
        'uuid':
            str(uuid.uuid4()),
        'section_id':
            cpc_section,
        'subsection_id':
            "{cpc_section}{cpc_class}".format(cpc_section=cpc_section,
                                              cpc_class=cpc_class),
        'group_id':
            "{cpc_section}{cpc_class}{cpc_subclass}".format(
                cpc_section=cpc_section,
                cpc_class=cpc_class,
                cpc_subclass=cpc_subclass),
        'subgroup_id':
            "{cpc_section}{cpc_class}{cpc_subclass}{cpc_maingroup}/{cpc_subgroup}".format(
                cpc_section=cpc_section,
                cpc_class=cpc_class,
                cpc_subclass=cpc_subclass,
                cpc_maingroup=cpc_maingroup,
                cpc_subgroup=cpc_subgroup),
        'category':
            'inventional' if class_code == 'I' else 'additional'
    }


def get_cpc_records(xml_root):
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
            cpc_record_dict = get_cpc_components_from_xml(main_cpc_element, ns)
            cpc_record_dict['patent_id'] = pat_number
            cpc_record_dict['sequence'] = sequence
            sequence = sequence + 1
            yield cpc_record_dict

        further_cpc_element = element.find(further_cpc_path, namespaces=ns)
        if further_cpc_element is not None:
            for further_cpc_element in further_cpc_element.getchildren():
                cpc_record_dict = get_cpc_components_from_xml(further_cpc_element, ns)
                cpc_record_dict['patent_id'] = pat_number
                cpc_record_dict['sequence'] = sequence
                sequence = sequence + 1
                yield cpc_record_dict


def load_cpc_records(records_generator, config, log_queue):
    """
    Load Extracter CPC Records into CPC Current Table
    :param records_generator: generator that produces cpc records as dictionary
    """
    cpc_records_frame = pd.DataFrame(records_generator)
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    start = time.time()
    with engine.connect() as conn:
        cpc_records_frame.to_sql('cpc_current', conn, if_exists='append', index=False, method="multi")
    end = time.time()
    log_queue.put({"level": logging.INFO, "message": "Chunk Load Time:" + str(round(end - start))})


def process_cpc_file(cpc_xml_zip_file, cpc_xml_file, config, log_queue):
    start = time.time()
    xml_tree = get_cpc_xml_root(cpc_xml_zip_file, cpc_xml_file)
    cpc_records = get_cpc_records(xml_tree)
    load_cpc_records(cpc_records, config, log_queue)
    end = time.time()
    log_queue.put({"level": logging.INFO,
                   "message": "XML File {xml_file} Processing Time: {duration}".format(duration=round(end - start),
                                                                                       xml_file=cpc_xml_file)})


def process_and_upload_cpc_current(config):
    cpc_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
    cpc_xml_file = None
    for filename in os.listdir(cpc_folder):
        if (filename.startswith('CPC_grant_mcf') and
                filename.endswith('.zip')):
            cpc_xml_file = "{cpc_folder}/{cpc_file}".format(cpc_folder=cpc_folder, cpc_file=filename)

    if cpc_xml_file:
        print(cpc_xml_file)
        add_index, drop_index = generate_index_statements(config, "NEW_DB", "cpc_current")

        prepare_cpc_table(config, drop_index)
        xml_file_name_generator = generate_file_list(cpc_xml_file)

        parallelism = int(config["PARALLELISM"]["parallelism"])
        manager = mp.Manager()
        log_queue = manager.Queue()

        parser_start = time.time()
        pool = mp.Pool(parallelism)
        watcher = pool.apply_async(log_writer, (log_queue, "cpc_parser"))
        p_list = []
        for xml_file_name in xml_file_name_generator:
            p = pool.apply_async(process_cpc_file, (cpc_xml_file, xml_file_name, config, log_queue))
            p_list.append(p)

        for t in p_list:
            t.get()

        log_queue.put({"level": logging.INFO,
                       "message": "Total parsing time {parser_duration}".format(
                           parser_duration=round(time.time() - parser_start, 3))})
        log_queue.put({"level": None, "message": "kill"})
        watcher.get()
        pool.close()
        pool.join()

        consolidate_cpc_data(config, add_index)
    else:
        print("Could not find CPC Zip XML file under {cpc_input}".format(cpc_input=cpc_folder))
        exit(1)


if __name__ == '__main__':
    config = get_config()
    process_and_upload_cpc_current(config)
