import json
import re
import time
import pandas as pd
from datetime import date, datetime
from lxml import etree
from typing import Iterator, Dict

from updater.xml_to_sql.parser import process_publication_document
from lib.configuration import get_current_config, dtd_finder, tables_dtd_to_json, long_text_dtd_to_json

def document_text_generator(xml_file: str) -> Iterator[str]:
    """
    generator that sequentially yields each xml document within an xml file as a string
    :param xml_file: the path to the xml file to iteratively extract text from
    """
    xml_marker = '<?xml version="1.0"'
    current_document_lines = []
    with open(xml_file, "r") as freader:
        # Loop through all the lines in the file
        for line in freader:
            # Determine the start of a new document
            if xml_marker in line:
                current_document_lines.append(line.split(xml_marker)[0])
                # Join all lines for a given document
                current_xml = "".join(current_document_lines)
                if current_xml.strip() != '':
                    yield current_xml
                current_document_lines = []
                current_document_lines.append(xml_marker)
                current_document_lines.append(line.split(xml_marker)[-1])
            else:
                current_document_lines.append(line)
        current_xml = "".join(current_document_lines)
        yield current_xml

def extract_specific_doc_text(xml_file:str, doc_number:str) -> str :
    """
    returns an LXML etree containing the document specified by the document number within the provided xml file if it exists
    :param xml_file: the path to the xml to search for the desired document
    :param doc_number: the document number or patent id of the desired document
    """
    for doc in document_text_generator(xml_file):
        if f'{doc_number}' in doc:
            return(doc)
    print(f"""
    document number {doc_number} not found in {xml_file}.
    check that filename and document number are correct.
    note that Patents with a letter prefix may have a leading zero removed from the numeric part of the ID.""")
    return None

def extract_specific_doc_tree(xml_file:str, doc_number:str) -> etree.Element :
    """
    returns an LXML etree containing the document specified by the document number within the provided xml file if it exists
    :param xml_file: the path to the xml to search for the desired document
    :param doc_number: the document number or patent id of the desired document
    """
    for doc in document_text_generator(xml_file):
        if f'{doc_number}' in doc:
            tree = etree.XML(doc.encode('utf-8'), parser=etree.XMLParser(no_network=False))
            return(tree)
    print(f"""
    document number {doc_number} not found in {xml_file}.
    check that filename and document number are correct.
    note that Patents with a letter prefix may have a leading zero removed from the numeric part of the ID.""")
    return None

def tree_generator(xml_file:str) -> Iterator[etree.Element] :
    """
    generator that yields each xml document within an xml file as an lxml etree object, skipping over sequence-cwu documents
    :param xml_file: the path to the xml file to iteratively produce etrees from
    """
    for doc in document_text_generator(xml_file):
        tree = etree.XML(doc.encode('utf-8'), parser=etree.XMLParser(no_network=False))
        if tree.tag == 'sequence-cwu':
            continue
        yield tree


def parse_single_xml_to_dfs(xml_file:str, parse_type:str, logging:bool=False) -> Dict[str, pd.DataFrame]:
    """
    replicates functionality of updater.xml_to_sql.parser.queue_parsers with several modifications for local troubleshooting:
    multiprocessing is disabled, only one file is parsed at a time, logging is optional,
    and the result of the parse is returned as a dict of Data Frames instead of uploaded to MySQL.
    The tables to be parsed are controlled by the table toggle file listed in config.ini
    :param xml_file: the name/path of the xml file to be parsed
    :param parse_type: controls whether to use the parsing configuration for patent tables, application tables, or long text tables
    :param logging: boolean indicating whether to produce a log file
    """
    assert parse_type in ('granted_patent','pgpubs','long_text'), f"invalid parse_type: '{parse_type}'. Allowed values are {{'granted_patent','pgpubs','long_text'}}"
    parser_start = time.time()
    p_list = []
    dtd_file = dtd_finder(xml_file)
    # as a precaution
    if dtd_file is None:
        if parse_type == 'granted_patent':
            dtd_file = config['XML_PARSING']['default_grant_dtd']
        else:
            dtd_file = config['XML_PARSING']['default_pgp_dtd']
    if parse_type in ['pgpubs','granted_patent']:
        parsing_config_file = tables_dtd_to_json[dtd_file]
    if parse_type == 'long_text':
        parsing_config_file = long_text_dtd_to_json[dtd_file]
    if parsing_config_file is None:
        if type == 'granted_patent': 
            parsing_config_file = config['XML_PARSING']['default_grant_parsing_config']
        else:
            parsing_config_file = config['XML_PARSING']['default_pgp_parsing_config']
    parsing_config_file = '/'.join((config['FOLDERS']['json_folder'], parsing_config_file))
    parsing_config = json.load(open(parsing_config_file))
    dtd_file = '/'.join((config['FOLDERS']['dtd_folder'], dtd_file))
    if logging:
        from queue import Queue
        import logging
        log_queue = Queue()
        log_queue.put({
                "level":   logging.INFO,
                "message": "Starting parsing of {xml_file} using {parsing_config}; Validated by {validator}".format(
                        xml_file=file_name,
                        parsing_config=parsing_config_file,
                        validator=dtd_file)
                })
    
    dfs = parse_publication_xml(xml_file, dtd_file, parsing_config, config, log_queue, destination='return')

    if logging:
        log_queue.put({
                "level":   logging.INFO,
                "message": "Total parsing time {parser_duration}".format(
                        parser_duration=round(time.time() - parser_start, 3))
                })
        log_queue.put({
                "level":   None,
                "message": "kill"
                })
        log_writer(log_queue)

    return dfs
