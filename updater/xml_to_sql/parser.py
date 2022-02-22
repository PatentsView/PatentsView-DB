import csv
import json
import logging
import multiprocessing as mp
import os
import pprint
import re
import time
# import datetime
from datetime import datetime, date
from queue import Queue

import pandas as pd
from lxml import etree
from sqlalchemy import create_engine

from lib.configuration import get_current_config
from lib.utilities import download_xml_files
from lib.utilities import log_writer

newline_tags = ["p", "heading", "br"]


def generate_headers(xml_map):
    """
    Generate column names for each dataframe corresponding to each target table
    :param xml_map: Json Table map containing parsing configuration
    :return: dictionary of lists containing header data for each table in the DB
    """
    headers_list = {}
    # Loop through the each table in the mapping configuration
    for table in xml_map["table_xml_map"]:
        # Create list of column name for each table
        field_list = []
        table_name = table['table_name']

        # Add all field_names to the list
        for fields in table['fields']:
            field_name = fields['field_name']
            if field_name not in field_list:
                field_list.append(field_name)
        field_list.append(xml_map['foreign_key_config']['field_name'])
        # field_list.append("document_number")
        # Append to larger list containing all tables
        headers_list[table_name] = field_list
    return headers_list


def generate_dfs(headers):
    """
    Generate empty dataframes for each table in the DB
    :param headers: dictionary of list containing header for each table
    :return: Dictionary of data frames
    """
    df_dict = {}
    # Loop through the list of headers
    for table in headers:
        # Rest of the elements are the column names
        cols = headers[table]

        # Add empty dataframe to the dictionary
        df_dict[table] = pd.DataFrame(columns=cols)
    return df_dict


def text_extractor(root, level=0):
    """
    Extract text from "fulltext" XML tags including nested tags
    :param root: text element root
    :param level: Level of nesting
    :return: list of strings extracted from XML
    """
    # Set Indent Variable based on nesting Level
    # Useful for debugging
    indent = '\t' * level
    partial_strings = []
    # Math tag needs special processing: Essentially appending all the math content on single line
    if root.tag in ["maths", "tables", "figures"]:
        for t in root.itertext():
            if t not in ['\ue89e', '\ue8a0'] and len(t.strip()) > 0:
                partial_strings.append(t.strip())
        # All non Math Elements
    else:
        '''
        Text based XML tags can have the text data in 3 areas
        (1) Starting right after the tag until the first child,
        (2) Between the end of tag and up until start of the next tag
        (3) As part of its children, essentially (1) & (2) for each of its children
        Eg:
            <body> Hello,  <b>Welcome</b>to <b> PatentsView  </b> Website </body>
            
        In the above example 
                Tag          | Text        | Tail
                --------------------------------------
                body         | Hello,      | 
                1st   b      | Welcome     | to
                2nd   b      | PatentsView | Website
        
         Tail Property contains the text that directly follows the element, up to the next element in the XML tree.
         We use the "text" & "tail" property in conjunction with recursively calling the method 
         on each children to extract all the text data from a text tag
        '''
        if str(type(root.tag)) != '<class \'cython_function_or_method\'>' and root.text and len(root.text.strip()) > 0:
            partial_strings.append(root.text.strip())
        for text_children in root.getchildren():
            if text_children.tag in newline_tags:
                partial_strings.append("\n")
            partial_strings += text_extractor(text_children, level=level + 1)
        if root.tail and len(root.tail.strip()) > 0:
            partial_strings.append(root.tail.strip())
    return partial_strings


def extract_text_from_all_children(element):
    """
    Extract text data from each children of current element
    :param element: Element whose children contain the text data
    :return: list of text strings
    """
    partial_strings = []
    if element.getchildren() == []:
        partial_strings += text_extractor(element, level=0)
    else:
        for elem in element.getchildren():
            partial_strings += text_extractor(elem, level=0)
            # If p or heading section add newlines to the list to match formatting
            if elem.tag in newline_tags:
                partial_strings.append("\n\n")
    return partial_strings


def parse_description(patent_doc, text_type):
    """
    Parse and extract data from "description" fields (i.e. brf_sum_text, claim, draw_desc_text)
    :param patent_doc: XML element containing text data
    :param text_type: Data Field for which text is to be extracted
    :return: Text data from each element
    """
    section = None
    partial_strings = []
    # Loop through the children in the input xml element
    for element in patent_doc.getchildren():
        # Check to see if the element is a ProcessingInstruction
        # Processing Instruction tags have "description" attribute
        if 'description' in element.attrib:
            # Get the name of the current text section
            section = element.attrib['description']
            # Determine position of current text section ( start of the text or the end of text)
            position = element.attrib['end']
            # If we are at the end of the current text section & the section corresponds to data field of interest,
            # Stop reading in rest of the tags, we have already reached end of section
            if position == 'tail' and section == text_type:
                break
        else:
            # If the current text section corresponds to data field of interest
            if section == text_type:
                # draw_desc_text is nested within its own tag unlike the other fields
                if section == 'Brief Description of Drawings':
                    # Extract text from current tag
                    partial_strings += extract_text_from_all_children(element)
                else:
                    # Extract text element from current tag
                    partial_strings += text_extractor(element, level=0)
                    # If p or heading section add newlines to the list to match formatting
                    if element.tag in newline_tags:
                        partial_strings.append("\n\n")
    # Return extracted partial strings list as a string
    if any([True if len(x.strip()) > 0 else False for x in partial_strings]):
        return "".join(partial_strings)
    else:
        return None


def extract_field_data(field_element, field, attribute, description, flag, tag):
    # If this is a description field use the parse_description() function to get text data
    if description is not None:
        return parse_description(field_element, description)
    # If the field is a "fulltext" type, extract all the necessary text
    elif field['data-type'] == 'fulltext':
        partial_string = ''.join(field_element.itertext())
        return partial_string.strip()
    # If this field is determined by an attributes, then extract its value
    elif attribute is not None:
        return field_element.attrib[attribute]
    # If this field is a flag determine its truth value
    elif flag is not None:
        if field_element.tag == flag:
            return "TRUE"
        else:
            return "FALSE"
    # If this field is a tag get the element's tag value
    elif tag:
        return field_element.tag
        # For all other fields get the text value
    else:
        return field_element.text


def extract_table_data(tab, patent_doc, doc_number, seq, foreign_key_config):
    """
    Extract a single table's data fields from given single document
    :param tab: Table parsing configuration
    :param patent_doc: XML element for current document
    :param doc_number: Current document number
    :param seq: Current sequence number
    :return: Dictionary containing rows for current table
    """
    # List for data in this field, initialize with document number
    doc_number_field = foreign_key_config["field_name"]
    try:
        data_list = {
                doc_number_field: int(doc_number)
                }
    except ValueError:
        data_list = {
                doc_number_field: doc_number
                }
    # Loop through all the fields in the table from the input
    for field in tab['fields']:
        if field["field_name"] not in data_list or data_list[field["field_name"]] is None:
            # Load flags that indicate the type of field indicating extraction mechanism
            attribute = field['attribute']
            description = field['description']
            flag = field['flag']
            tag = field['tag']
            path = field['xml_path']

            # If the field is sequence, simply add the current tag sequence
            if field['sequence']:
                data_list[field["field_name"]] = seq
            # If we are looking for the text data in the claims table use the text_extractor to get the right data
            # Claims are special cases
            elif tab['friendly_name'] == 'Claim' and field['field_name'] == 'text':
                partial_strings = []
                field_elements = patent_doc.findall(path)
                for elem in field_elements:
                    partial_strings += text_extractor(elem)
                    if elem.tag in newline_tags:
                        partial_strings.append("\n\n")
                    data_list[field["field_name"]] = ' '.join(partial_strings)
            elif tab['friendly_name'] == 'Drawing Description Text' and field['field_name'] == 'text':
                partial_strings = []
                field_elements = patent_doc.findall(path)
                for elem in field_elements:
                    partial_strings += extract_text_from_all_children(elem)
                    data_list[field["field_name"]] = ' '.join(partial_strings)
            else:
                # Find all elements in the xml_path for a current field
                field_elements = patent_doc.findall(path)
                # If there are no elements append None to the data list
                if len(field_elements) < 1:
                    data_list[field["field_name"]] = None
                else:
                    # Get the first element containing data
                    # Both multi valued and single valued tables at this level will have only one element matching the path
                    multi_value_list = []
                    for field_element in field_elements:
                        extracted_data = extract_field_data(field_element, field, attribute, description, flag, tag)
                        multi_value_list.append(extracted_data)
                    if not all([x is None for x in multi_value_list]):
                        data_list[field["field_name"]] = ", ".join(
                            [x if x is not None else '' for x in multi_value_list])
                    else:
                        data_list[field["field_name"]] = None  # Return the extracted data
                    if tab['table_name'] == 'usreldoc_single' and field_element.tag == 'related-publication':
                        data_list = {}
                        break
    return data_list


def process_publication_document(patent_app_document, patent_config):
    """
    Extract various entities from single published document
    :param patent_app_document: XML element representing a single published application document
    :param patent_config: dictionary containing parsing configuration
    :returns tuple containing the table_name and its data
    """
    # Get the document number for the given input
    document_number = patent_app_document.findall(patent_config['foreign_key_config']['xml_path'])[0].text
    # Get the table_xml_map element from the JSON file
    table_xml_map = patent_config['table_xml_map']
    # Loop through the tables in the table_xml_map to extract all data that is present
    for table in table_xml_map:
        # Initialize return list and table name
        table_rows = []
        table_name = table['table_name']
        # If this table has only one value per field extract its data and add to the list
        if table['multi_valued'] == False:
            table_rows.append(
                    extract_table_data(table, patent_app_document, document_number, 0,
                                       patent_config['foreign_key_config']))
        # If this table can have multiple values (i.e. multiple inventors per document) loop through these elements to get the data
        else:
            # This is the start of the path from which the multiple values will exists
            # i.e. /inventors can contain multiple /inventor tags within it
            entity_root_path = table['entity_root_path']
            sequence = 1
            # extract all data necessary
            for entity_element in patent_app_document.findall(entity_root_path):
                table_rows.append(extract_table_data(table, entity_element, document_number, sequence,
                                                     patent_config['foreign_key_config']))
                sequence += 1
        yield table_name, table_rows


def load_df_to_sql(dfs, xml_file_name, config, log_queue, foreign_key_config):
    """
    Add all data to the MySQL database
    :param dfs: dictionary of dataframes for each table
    :param xml_file_name: name of the file
    :param config: credentials to connect to database
    """
    sql_start = time.time()
    database = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    host = '{}'.format(config['DATABASE_SETUP']['HOST'])
    user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
    password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
    port = '{}'.format(config['DATABASE_SETUP']['PORT'])

    text_output_folder = config['FOLDERS']['TEXT_OUTPUT_FOLDER']
    engine = create_engine(
            'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    for df in dfs:
        cols = list(dfs[df].columns)
        cols.remove(foreign_key_config["field_name"])
        dfs[df] = dfs[df].dropna(subset=cols, how='all')
        dfs[df]['version_indicator'] = config['DATES']['END_DATE']
        try:
            dfs[df].to_sql(df, con=engine, if_exists='append', index=False)
        except Exception as e:
            log_queue.put({
                    "level":   logging.ERROR,
                    "message": "{xml_file}: Error when writing to database : {error}".format(
                            xml_file=xml_file_name,
                            error=pprint.pformat(
                                    e))
                    })

            dfs[df].to_csv(
                    "{folder}/{xml_file}_{entity}.csv".format(folder=text_output_folder, xml_file=xml_file_name,
                                                              entity=df), sep=",",
                    quotechar='"', quoting=csv.QUOTE_NONNUMERIC, index=False)
            raise e
    log_queue.put({
            "level":   logging.INFO,
            "message": "XML Document {xml_file} took {duration} seconds to load to SQL".format(
                    xml_file=xml_file_name,

                    duration=time.time() - sql_start)
            })


def extract_document(xml_file):
    xml_marker = '<?xml version="1.0" encoding="UTF-8"?>\n'
    current_document_lines = []
    with open(xml_file, "r") as freader:
        # Loop through all the lines in the file
        for line in freader:
            # Determine the start of a new document
            if line == xml_marker:
                # Join all lines for a given document
                current_xml = "".join(current_document_lines)
                yield current_xml
                current_document_lines = []
            current_document_lines.append(line)
        current_xml = "".join(current_document_lines)
        yield current_xml


def parse_publication_xml(xml_file, dtd_file, table_xml_map, config, log_queue, unlink=False):
    """
    Parse all data from a single XML file into dataframe for each table
    :param xml_file: XML file with pgpubs data
    :param table_xml_map: dictionary of parsing configuration
    :param config: config file
    """
    xml_file_name = os.path.basename(xml_file)
    debug = False
    if config["DEBUG"]["debug"] == "1":
        debug = True
    stats = {
            "total_documents":           0,
            "successful_documents":      -1,
            "total_file_time":           0,
            "average_time_per_document": 0
            }
    xml_marker = '<?xml version="1.0" encoding="UTF-8"?>\n'
    log_queue.put({
            "level":   logging.INFO,
            "message": "{xml_file}: Init: Creating empty data frames".format(xml_file=xml_file_name)
            })
    xml_file_start = time.time()
    # Generate the list of headers and use them to create dataframes for each table
    header_list = generate_headers(table_xml_map)
    dfs = generate_dfs(header_list)
    # Set the dtd
    dtd = etree.DTD(open(dtd_file))

    log_queue.put({
            "level":   logging.INFO,
            "message": "{xml_file}: Begin XML Processing".format(xml_file=xml_file_name)
            })
    # Counter to show progress
    counter = 0
    error = 0
    num_lines = 0
    end = False
    # List of lines in the current document
    current_document_lines = []
    parse_start = time.time()
    xml_doc_start = time.time()
    fsize = len(open(xml_file).readlines())
    # Open the given xml_file
    for current_xml in extract_document(xml_file):
        if len(current_xml) > 0:
            counter += 1
            if debug and counter > 250:
                break
            # Create an etree element for the current document
            parser = etree.XMLParser(load_dtd=True, no_network=False)
            patent_app_document = etree.XML(current_xml.encode('utf-8'), parser=parser)
            if patent_app_document.tag == 'sequence-cwu':
                continue
            else:
                # Extract the data fields
                data = process_publication_document(patent_app_document, table_xml_map)
                # Add the data to the proper dataframe
                try:
                    for table_name, extracted_data in data:
                        if len(table_name) > 0:
                            current_data_frame = pd.DataFrame(extracted_data)
                            dfs[table_name] = dfs[table_name].append(current_data_frame)
                        else:
                            continue
                except IndexError as e:
                    log_queue.put(
                            {
                                    "level":   logging.DEBUG,
                                    "message": "{xml_file}: {document}".format(xml_file=xml_file_name,
                                                                               document=pprint.pformat(
                                                                                       patent_app_document.getchildren()))
                                    })
    log_queue.put({
            "level":   logging.INFO,
            "message": "XML Document {xml_file} took {duration} seconds to parse".format(
                    xml_file=xml_file_name,
                    duration=time.time() - parse_start)
            })
    # Load the generated data frames to database
    load_df_to_sql(dfs, xml_file_name, config, log_queue, table_xml_map["foreign_key_config"])

    xml_file_duration = round(
            time.time() - xml_file_start, 3)
    log_queue.put({
            "level":   logging.INFO,
            "message": "XML Document {xml_file} took {duration} seconds to process.".format(
                    xml_file=xml_file_name,
                    duration=xml_file_duration)
            })
    stats["total_documents"] = counter
    stats["successful_documents"] = counter - error
    stats["total_file_time"] = xml_file_duration
    stats["average_time_per_document"] = (xml_file_duration * 1.0) / (counter - error)
    log_queue.put(
            {
                    "level":   logging.DEBUG,
                    "message": "{xml_file}: {stats}".format(xml_file=xml_file_name, stats=pprint.pformat(stats))
                    })
    if unlink:
        delete_xml_file(xml_file)
    print("Finished parse_publication_xml TASK")


def chunks(l, n):
    """
    Split a list into n length chunks
    :param l: list to be split
    :param n: number of chunks
    """
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i + n]


def get_filenames_to_parse(config, type='granted_patent'):
    xml_directory_setting = "{prefix}_bulk_xml_location".format(prefix=type)
    xml_directory = config['FOLDERS'][xml_directory_setting]

    xml_files = []
    start_date_string = '{}'.format(config['DATES']['START_DATE'])
    start_date = datetime.strptime(start_date_string, '%Y%m%d')
    end_date_string = '{}'.format(config['DATES']['END_DATE'])
    end_date = datetime.strptime(end_date_string, '%Y%m%d')
    for file_name in os.listdir(xml_directory):
        print(file_name)
        if file_name.endswith(".xml"):
            file_date_string = re.match(".*([0-9]{6}).*", file_name).group(1)
            file_date = datetime.strptime(file_date_string, '%y%m%d')

            # file_date = file_name.split("_")[-1].split(".")[0]
            # file_date = file_name[3:-4]
            print(file_date)
            print(start_date)
            print(end_date)
            if start_date <= file_date <= end_date:
                xml_files.append(xml_directory + "/" + file_name)

    return xml_files


def queue_parsers(config, type='granted_patent'):
    """
    Multiprocessing call of the parse_publication_xml function
    :param config: config file
    """
    parsing_file_setting = "{prefix}_parsing_config_file".format(prefix=type)
    dtd_file_setting = "{prefix}_dtd_file".format(prefix=type)
    dtd_file = '{}'.format(config['XML_PARSING'][dtd_file_setting])
    parsing_config_file = config["XML_PARSING"][parsing_file_setting]
    project_home = os.environ['PACKAGE_HOME']
    parsing_config = json.load(open("{}".format(project_home + "/" + config["XML_PARSING"][parsing_file_setting])))
    xml_files = get_filenames_to_parse(config, type=type)
    parser_start = time.time()

    parallelism = int(config["PARALLELISM"]["parallelism"])

    pool = None
    watcher = None
    if parallelism > 1:
        # must use Manager queue here, or will not work
        manager = mp.Manager()
        log_queue = manager.Queue()
        pool = mp.Pool(parallelism)
        watcher = pool.apply_async(log_writer, (log_queue,))
    else:
        log_queue = Queue()
    p_list = []
    for file_name in xml_files:
        log_queue.put({
                "level":   logging.INFO,
                "message": "Starting parsing of {xml_file} using {parsing_config}; Validated by {validator}".format(
                        xml_file=file_name,
                        parsing_config=parsing_config_file,
                        validator=dtd_file)
                })
        # break
        if parallelism > 1:
            p = pool.apply_async(parse_publication_xml, (file_name, dtd_file, parsing_config, config, log_queue))
            p_list.append(p)
        else:
            parse_publication_xml(file_name, dtd_file, parsing_config, config, log_queue)
    if parallelism > 1:
        idx_counter = 0
        for t in p_list:
            # try:
            t.get()

            # except Exception as e:
            #     log_queue.put({"level": logging.INFO,
            #                    "message": "{xml_file}: Error during parsing {error}".format(
            #                        xml_file=file_name,
            #                        error=pprint.pformat(e))})
            idx_counter += 1
    log_queue.put({
            "level":   logging.INFO,
            "message": "Total parsing time {parser_duration}".format(
                    parser_duration=round(time.time() - parser_start, 3))
            })
    log_queue.put({
            "level":   None,
            "message": "kill"
            })
    if parallelism > 1:
        watcher.get()
        pool.close()
        pool.join()
    else:
        log_writer(log_queue)
    print("Finished Queue Parser Task")


def delete_xml_file(filename):
    os.remove(filename)


def begin_parsing(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)
    download_xml_files(config)
    queue_parsers(config, 'pgpubs')


if __name__ == "__main__":
    # begin_parsing(**{
    #         "execution_date": date(2020, 12, 17)
    #         })
    # type = 'pgpubs'
    # config = get_current_config(type=type, **{
    #     "execution_date": date(2021, 7, 8)
    # })
    # parsing_file_setting = "{prefix}_parsing_config_file".format(prefix=type)
    # dtd_file_setting = "{prefix}_dtd_file".format(prefix=type)
    # dtd_file = '{}'.format(config['XML_PARSING'][dtd_file_setting])
    # parsing_config_file = "/opt/project/rawinventor.json"
    # parsing_config = json.load(open(parsing_config_file))
    # log_queue = Queue()
    # parse_publication_xml("/opt/project/ipa210708.xml", dtd_file='{}'.format(config['XML_PARSING'][dtd_file_setting]),
    #                       table_xml_map=parsing_config, config=config, log_queue=log_queue)
    begin_parsing(
        **{
            "execution_date": date(2021, 12, 23)
        }
    )