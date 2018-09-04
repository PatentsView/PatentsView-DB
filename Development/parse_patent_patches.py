import os
import sys
import csv
import re
import pandas as pd
import simplejson as json
sys.path.append("D:/Caitlin_PV/PatentsView-DB USE/PatentsView-DB/Development")
from helpers import output, xml_helpers, general_helpers
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
		# PCT Data
		#102_data is ALWAYS null, need to check this
		pct_filing_data = xml_helpers.get_entity(patent, 'pct-or-regional-filing-data')[0]
		if pct_filing_data is not None:
			if pct_filing_data['us-371c124-date-date'] is not None:
				pct_filing_data['us-371c12-date-date'] = pct_filing_data['us-371c124-date-date']
			field_list.append([output.id_generator(), patent_id, pct_filing_data['document-id-doc-number'],
							 pct_filing_data['document-id-date'], pct_filing_data['us-371c12-date-date'],
							 pct_filing_data['document-id-country'], pct_filing_data['document-id-kind'],
							 "pct_application", None])
		pct_pub_data = xml_helpers.get_entity(patent, 'pct-or-regional-publishing-data')[0]
		if pct_pub_data is not None:
			field_list.append([output.id_generator(), patent_id, pct_pub_data['document-id-doc-number'],
							 pct_pub_data['document-id-date'], None,
							 pct_pub_data['document-id-country'], pct_pub_data['document-id-kind'],
							 "wo_grant", None])       
		
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
	folder  = '//dc1fs/DC1EHD/share/Science Policy Portfolio\PatentsView IV/Raw Data/2005-2018/Clean'
	#folder  = 'D:\Caitlin_PV\Test'
	in_files = ['{0}/{1}'.format(folder, item) for item in os.listdir(folder)]
	out_files= ['D:/Caitlin_PV/Outfile/{0}'.format(item[3:9]) 
				   for item in os.listdir(folder)]
	

	#replace patent with the field you want
	single_dict = {'field': field_dictionary['pct_data']}
	fields = [single_dict for item in in_files]

	files = zip(in_files, out_files, fields)
	

	desired_processes = 7 # ussually num cpu - 1
	jobs = []
	for f in files:
		jobs.append(multiprocessing.Process(target = single_process, args=(f)))
	for segment in general_helpers.chunks(jobs, desired_processes):
		print segment
		for job in segment:
			job.start()
		for job in segment:
			job.join()