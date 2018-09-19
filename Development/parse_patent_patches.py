import os
import sys
import csv
import re
import pandas as pd
import simplejson as json
sys.path.append("D:/DataBaseUpdate/Code_V2/PatentsView-DB/Development/helpers")
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


		#############################
		# Citations
		##############################
		#initialize all the sequence variables as 0
		uspatseq = 0
		appseq = 0
		forpatseq= 0
		otherseq = 0
	
		patent_citations, nonpatent_citations = xml_helpers.get_citations(patent)
		if patent_citations is not None:
			#citation list can only be empty for plant patents?
			for citation in patent_citations:
				cited_doc_num = citation['doc-number']

				is_app = True #indicator for whether something being cited is a patent application
				if cited_doc_num:
					if re.match(r'^[A-Z]*\d+$', cited_doc_num): #basically if there is anything other than number and digits its an application
						is_app = False
				# if cited_doc_num:
				# 	print re.match(r'^[A-Z]*\d+$', cited_doc_num)
				# 	if re.match(r'^[A-Z]*\d+$', cited_doc_num): #basically if there is anything other than number and digits its an application
				# 		num = re.findall('\d+', cited_doc_num)
				# 		num = num[0] #turns it from list to string
				# 		if num[0] == '0': #drop leading zeros
				# 			num = num[1:]
				# 		let = re.findall('[a-zA-Z]+', cited_doc_num)
				# 		if let:
				# 			print cited_doc_num
				# 			let = let[0]#list to string
				# 			cited_doc_num = let +num
				# 			print cited_doc_num
				# 		else:
				# 			cited_doc_num = num
				# 		is_app = False



				if citation['country'] == "US":
					if cited_doc_num and not is_app: #citations without document numbers are otherreferences
						cited_doc_num = xml_helpers.process_patent_numbers(cited_doc_num)
						field_list.append([output.id_generator(), patent_id, cited_doc_num, citation['date'], citation['name'],
												   citation['kind'], citation['country'],
												  citation['category'],str(uspatseq), citation['main-classification']])
		# 				uspatseq+=1
		# 			if cited_doc_num  and is_app:
		# 				cit_app_id_transformed = cited_doc_num[:5] + cited_doc_num[:4] + cited_doc_num[5:]
		# 				cit_app_number_transformed = cited_doc_num.replace('/', '')
		# 				usapplicationcitation_list.append([output.id_generator(), patent_id,cited_doc_num, citation['date'], citation['name'],
		# 							  citation['kind'], cited_doc_num, citation['country'], citation['category'],
		# 												str(appseq), cit_app_id_transformed, cit_app_number_transformed])
		# 				appseq +=1
		# 		elif cited_doc_num:
		# 			foreigncitation_list.append([output.id_generator(), patent_id, citation['date'] ,cited_doc_num,
		# 									  citation['country'], citation['category'], str(forpatseq)])
		# 			forpatseq+=1 
		# 		else:
		# 			error_log.append([patent_id, citation])
		# # elif app_type !='plant': #only plant patents are allowed to not have a patent citation
		# # 	error_log.append([patent_id, 'citations'])        
		# if nonpatent_citations is not None:
		# 	for citation in nonpatent_citations:
		# 		otherreference_list.append([output.id_generator(), patent_id, citation['text'].replace("\\", "/"), str(otherseq)])
		# 		otherseq +=1     
		
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
	#folder  = 'D:/PV_Patches/Cites_with_alpha/Data'
	in_files = ['{0}/{1}'.format(folder, item) for item in os.listdir(folder)]
	out_files= ['D:/PV_Patches/Cites_with_alpha/Outfile/{0}'.format(item[3:9]) 
				   for item in os.listdir(folder)]
	

	#replace patent with the field you want
	single_dict = {'field': field_dictionary['uspatentcitation']}
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