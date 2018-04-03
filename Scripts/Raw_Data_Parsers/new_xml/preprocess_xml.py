import os
import sys
import re
#update with the path to your config file
sys.path.append("D:/DataBaseUpdate/To_clone")
from ConfigFiles import config
import xml.etree.ElementTree as ET
from lxml import etree
import csv
import unittest

def get_data(data_directory):
    files =os.listdir(data_directory)
    full_addresses = [data_directory + "/" + f for f in files]
    return full_addresses

def clean_files(raw_files):
    '''
    Alters the raw patentsview data files to produce valid XML. 
    Original is invalid because it lacks a root tag and has multiple xml version tags
    :param raw_files: list of raw patentsview files
    :return  null, creates list of files
    '''
    for raw_file in raw_files:
        new_file = raw_file[:-4] +'_clean.xml'
        with open(raw_file, 'r+') as f:
            with open(new_file, 'w') as new_f:
                new_f.write('<?xml version="1.0" encoding="UTF-8"?>' + "\n")
                new_f.write('<root>' + '\n')
                for line in f.readlines():
                    if not line.lstrip().startswith('<?xml') and not line.lstrip().startswith('<!DOCTYPE'):
                        new_f.write(line)
                new_f.write('</root>')

def get_xml(valid_xml_file):
    '''
    Return a xml object with patent data
    :param valid_xml_file: a valid xml object
    :return the root object of the xml file, ready to parse
    '''
    tree = etree.parse(valid_xml_file)
    root = tree.getroot()
    return root

def check_schema(patent_xml, expected_schema):
    '''
    Used to determine if the core data containers have changed (as they sometimes do)
    Uses the patent xml data to generate a list of the highest level fields
    and the fields under 'us-bibliographic-data-grant' (the main category).
    :param patent_xml: the root object that represents the patent xml data
    :param expected_schema: the list of fields previously at those levels for comparison
    :returns null: Raises errors if the schema is unexpected
    '''
    
    high_level = []
    main_fields = []
    without_us_bibliographic =0
    for patent in patent_xml:
        high_level+=[field.tag for field in patent]
        if patent.find('us-bibliographic-data-grant') is not None:
            main_fields+=[field.tag for field in patent.find('us-bibliographic-data-grant')]
        else:
            without_us_bibliographic +=1
    high_level = sorted(list(set(high_level)))
    main_fields = sorted(list(set(main_fields)))
    
    if not high_level == expected_schema[0]:
        print high_level
        raise Exception("The high level fields have changed ...check that it is not the ones we use.")
    if not main_fields == expected_schema[1]:
        print main_fields
        raise Exception("The main fields in the us-bibliographic-grant-data have changed ...check that it is not the ones we use.")
    if without_us_bibliographic > 200:
        raise Exception("There are more patents missing the us-bibliographic-grant-data field than ussual ")

if __name__ == '__main__':
	#produce the clean xml files
	files = get_data(config.data_to_parse)
	clean_files(files)
	clean_example =[file for file in get_data(config.data_to_parse) if file.endswith("clean.xml")][0]
	patents = get_xml(clean_example)
	#check the schema
	expected_high_level = ['abstract', 'claims', 'description', 'drawings', 'number',
	                      'publication-reference', 'sequence-list-new-rules', 'table',
	                      'us-bibliographic-data-grant', 'us-chemistry', 'us-claim-statement',
	                      'us-math', 'us-sequence-list-doc']
	expected_main_fields = ['application-reference', 'assignees', 'classification-locarno', 'classification-national',
	                        'classifications-cpc', 'classifications-ipcr', 'examiners', 'figures', 
	                        'hague-agreement-data', 'invention-title', 'number-of-claims',
	                        'pct-or-regional-filing-data', 'pct-or-regional-publishing-data',
	                        'priority-claims', 'publication-reference', 'rule-47-flag',
	                        'us-application-series-code', 'us-botanic', 'us-exemplary-claim',
	                        'us-field-of-classification-search', 'us-parties', 'us-references-cited',
	                        'us-related-documents', 'us-term-of-grant']
	expected_schema = (expected_high_level, expected_main_fields)
	check_schema(patents, expected_schema)