import os
import sys
import csv
import re
import pandas as pd
import simplejson as json
sys.path.append("D:/DataBaseUpdate/Code/New Code")
from parser_helpers import output, xml_helpers
from lxml import etree
from collections import defaultdict
import string
import random
import multiprocessing



def get_one_field(patents, field_dictionary): 
    '''
    Adapt this to parse one single field as needed
    '''
    field_list = [] #make a list for the table you are reparsing
    error_log = [[None, None]]
    already_seen_patents = []
    for patent in patents:
        #you almost always need the patent_id
        patent_data =  xml_helpers.get_entity(patent, 'publication-reference', attribute_list=None)[0]
        patent_id = xml_helpers.process_patent_numbers(patent_data['document-id-doc-number'])
        #there are a small number (~150 per file) patents that have a second patent document
        #this second one contains DNA sequence information and we should skip it for now
        if patent_id in already_seen_patents:
            continue
        already_seen_patents.append(patent_id)
        #########################################################################################
        #### insert here the code to parse whatever fiedl you need. Copy from parse_patents  ####
        #########################################################################################
        
        
    return {'field': field_list}, error_log

def single_process(data_file, outloc, indiv_field_dictionary):
    '''
    Driver function to process one particular field for patches and partial updates
    '''
    patent_xml = xml_helpers.get_xml(data_file)
    results,error_log = get_one_field(patent_xml, indiv_field_dictionary)
    error_data = pd.DataFrame(error_log)
    error_data.columns = ['patent_id', 'field']
    error_counts = error_data.groupby("field").count()
    os.mkdir(outloc)
    error_data.to_csv('{0}/error_data.csv'.format(outloc))
    error_counts.to_csv('{0}/error_counts.csv'.format(outloc))
    output.write_partial(results, outloc, indiv_field_dictionary)




if __name__ == '__main__':

    with open('D:/DataBaseUpdate/Code/New Code/persistent_files/field_dict.json') as myfile:
        field_dictionary = json.load(myfile)

    #this is the folder with the xml files that we want to reparse
    folder  = 'H:/share/Science Policy Portfolio/PatentsView IV/Raw Data/2005-2017/Clean'
    in_files = ['{0}/{1}'.format(folder, item) for item in os.listdir(folder)]
    out_files= ['H:/share/Science Policy Portfolio/PatentsView IV/ParsedData/Redo/{0}'.format(item[3:9]) 
                   for item in files]
    

    #replace patent with the field you want
    single_dict = {'patent': field_dictionary['patent']}
    fields = [single_dict for item in data_files]

    files = zip(data_files, out_files, fields)

    desired_processes = 7 # ussually num cpu - 1
    jobs = []
    for f in files:
        jobs.append(multiprocessing.Process(target = single_process, args=(f)))
    for segment in helpers.chunks(jobs, desired_processes):
        print segment
        for job in segment:
            job.start()
        for job in segment:
            job.join()