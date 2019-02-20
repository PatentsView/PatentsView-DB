##################################################################
#### This file is a re-write of govtInterest_v1.0.pl - with locations
#### input files: merged csvs, NER, omitLocs
#### output files: "NER_output.txt","distinctOrgs.txt"
##################################################################

# run under pl_rewrite virtualenv anaconda
# python G:\PatentsView\cssip\PatentsView-DB\Development\government_interest\govtinterest_v3.0.py

import pandas as pd
import sys
import math
import subprocess
import os
from os import listdir
import re 
from itertools import chain 
import string 


# Requires: mergedcsvs.csv filepath
# Modifies: nothing
# Effects: read in mergedcsvs file, return dataframe
def read_mergedCSV(fp):
	print("Reading in mergedcsvs.csv from: " + fp + 'mergedcsvs.csv')
	try: 
		merged_df = pd.read_csv(fp, header=None, encoding="ISO-8859-1")
	except:
		print("Error reading in file, check specified filepath")
	
	test_dataframe(merged_df, 6127, 4)
	
	# Set column headers
	merged_df.columns = ['patent_num','twin_arch','gi_title', 'gi_stmt']
	
	return merged_df 

# Requires: NER DB filepath, dataframe
# Modifies: nothing
# Effects: Run NER on gi_statements from dataframe
def run_NER(fp, txt_fp_in, txt_fp_out, data, classif, classif_dirs ):
	os.chdir(fp)
	patents = data['patent_num'].tolist()
	
	gi_stmt_full = data['gi_stmt'].tolist()
	
	# Limit to how many lines java call can read for NER
	nerfc = 5000

	# Estimate # of files for all gi_statements to process
	num_files = int(math.ceil(len(gi_stmt_full) / nerfc))
	print("Number of input files needed for NER: " + str(num_files))
	# Store name of input files for NER call
	input_files = []
	# Note: Support more than 2 files
	txt_list = []
	num_file = 0
	idx = 0
	# For each gi statement
	for gi in range(0,len(gi_stmt_full)):
		
		# If < 5000 - add onto list 
		if (idx <= nerfc):
			txt_list.append(gi_stmt_full[gi])
			idx = idx + 1
			# case when not reached 5000, but finished with all gi_statements
			if (gi == len(gi_stmt_full)-1):
				with open(txt_fp_in + str(num_file) + '_file.txt', 'w', encoding='utf-8') as f:

		 			gi_stmt_str = '\n'.join(txt_list)
		 			f.write(gi_stmt_str)

		 		# Save file name
				infile_name = str(num_file) + "_file.txt"
				input_files.append(infile_name)

		# Reached limit, save to file 
		else:
			with open(txt_fp_in + str(num_file) + '_file.txt', 'w', encoding='utf-8') as f:
		 		
		 		gi_stmt_str = '\n'.join(txt_list)
		 		f.write(gi_stmt_str)

		 	# Save file name
			infile_name = str(num_file) + "_file.txt"
			input_files.append(infile_name)
		 	# Move onto next file
			num_file = num_file + 1
			# Empty txt_string for next batch
			txt_list = []
			# Reset idx for next batch 5000
			idx = 0

	
	# Run java call for NER
	for cf in range(0,len(classif)):
		for f in input_files:	    
			cmd_pt1 = 'java -mx500m -classpath stanford-ner.jar;lib/* edu.stanford.nlp.ie.crf.CRFClassifier'
			cmd_pt2 = '-loadClassifier ' + './' + classif[cf]
			cmd_pt3 = '-textFile ./in/' + f + ' -outputFormat inlineXML 2>> error.log'
			cmd_full = cmd_pt1 + ' ' + cmd_pt2 + ' ' + cmd_pt3
			cmdline_params = cmd_full.split()
			print(cmdline_params)
			
			with open(txt_fp_out + classif_dirs[cf] + f, "w") as xml_out: 

				subprocess.run(cmdline_params, stdout=xml_out)
			
	return


# Requires: filepath, merged_df frame
# Modifies: nothing
# Effects: Process NER on merged_csvs, returns orgs list, locs list
def process_NER(txt_fp_out, data):
	os.chdir(txt_fp_out)
	ner_output = listdir(os.getcwd())
	print(ner_output)
	orgs_full_list = []
	locs_full_list = []
	for f in ner_output:
		with open(f, "r") as output:
			content = output.readlines()
			orgs_full_list, locs_full_list = parse_xml_ner(orgs_full_list, locs_full_list, content)	

	# Flatten list of lists 
	flat_orgs = [y for x in orgs_full_list for y in x]
	flat_locs = [y for x in locs_full_list for y in x]
	
	orgs_final = set(flat_orgs)
	locs_final = set(flat_locs)

	return orgs_final, locs_final

# Requires: filepath, merged_df frame
# Modifies: nothing
# Effects: Process NER on merged_csvs, returns orgs list, locs list
def add_cols(data, orgs, locs):

	print("Cleaning and Adding Columns...")
	# Clean organizations
	orgs_final = clean_orgs(orgs)
	
	gi_statements = data['gi_stmt'].tolist()
	
	# Add orgs column
	gi_all_orgs = []
	for gi in gi_statements:
		gi_orgs = []
		for org in orgs_final:
			if org in gi:
				gi_orgs.append(org)

		# Once full org list formed for gi, join
		gi_final = '|'.join(gi_orgs)		
		gi_all_orgs.append(gi_final)

	
	gi_all_orgs = [x.lstrip('|') for x in gi_all_orgs]
	data['orgs'] = pd.Series(gi_all_orgs)


	# Extract and clean Contract Numbers
	contracts = clean_contracts(data, gi_statements)
	
	# Add contracts column for contracts
	data['contracts'] = pd.Series(contracts)

	return data, orgs_final

# Requires: data dict
# Modifies: nothing
# Effects: Writes nerOutput file 
def write_output(output_fp,data, orgs):
	print("Writing Output...")
	# Write out extracted NER Output
	data.to_csv(output_fp + "NER_output.csv", index=False)

	# Write out distinct organizations
	orgs.sort()
	with open(output_fp + "distinct_orgs.txt", "w") as p:
		for item in orgs:
			p.write(str(item) + "\n")
	
	return

#--------Helper Functions-------#
# Requires: organizations list
# Modifies: organizations list
# Effects: clean organizations
def clean_orgs(orgs):
	
	# Strip whitespace
	orgs = [x.lstrip() for x in orgs]
	orgs = [x.rstrip() for x in orgs]

	# Grant-related cleaning
	orgs = [re.sub("Federal\sGrant.+|Grant\sNumber.+|Grant\sNo\.?.+|Grant\s#.+|(and)?\s?Grant", "", x) for x in orgs]

	# Contract/Case related cleaning
	orgs = [re.sub("Government\sContract.+|Case?\s(Number)?|Case|Subcontract.+|and\s(Contract)?|(and)?\s?Contract\sNos?\.?.+|Contract\s[A-Z,\d,\-,a-z].+[\d].+|Case\sNo\.?.+|Contract\sNumber.+|Contract\s#.+|Order\sNo|[C,c]ontract\/"
		, "", x) for x in orgs]
	
	# Award/Agreement related cleaning
	orgs = [re.sub("Award\sNumber.+|Award|Award\sNos?\.?.+|Agreement\sNos?\.?|Agreement\sNumbers.+|Award\s[A-Z].+[\d].+\sand|Award\s[A-Z,\d,\-,/]{10,30}", "", x) for x in orgs]
	
	# Misc. cleaning
	orgs = [re.sub("Cooperative\sAgreement.+", "Cooperative Agreement", x) for x in orgs]
	orgs = [re.sub("Agreement\sNCRID-08-317-00", "Agreement", x) for x in orgs]
	orgs = [re.sub("Energy\sContract.+", "Energy", x) for x in orgs]
	orgs = [re.sub("Development\sInitiative\s.+", "Development Initiative", x) for x in orgs]
	orgs = [re.sub("USEPA.+", "USEPA", x) for x in orgs]
	orgs = [re.sub("YFA.+", "YFA", x) for x in orgs]
	orgs = [re.sub("Work\sUnit.+", "Work Unit", x) for x in orgs]
	orgs = [re.sub("Training\sNumber.+|\?|\)|\(|,\.", "", x) for x in orgs]
	orgs = [re.sub("[A-Z,\d,\-]{10,30}|Agreement\|Number?.+|And Contract|And$","", x) for x in orgs]
	orgs = [re.sub("\sNos?\.?$|\]|\[|no\.|NS.+|&|;$|\s1$|#$|'|[#,A-Z][a-z,\d,A-Z][a-z\d][\d]{3,10}", "", x) for x in orgs]
	orgs = [re.sub("Applications?\sI[dD]?.+|Project\s#|Project\sNumber.+|Prime\sContract|Merit\sReview?.+|Merit\sAward", "", x) for x in orgs]
	orgs = [re.sub("Goverment|Government\sSupport\s?(under)?|(NIH|NHI)\s1|Health 1R43|U01", "", x) for x in orgs]
	orgs = [re.sub("Foundation\sNumber|Foundation\s[\d]{5,15}|U01|R01|P\.O\.|Numbers?", "", x) for x in orgs]
	orgs = [re.sub("NIH[#/]", "NIH", x) for x in orgs]
	orgs = [x.lstrip() for x in orgs]
	orgs = [x.rstrip() for x in orgs]

	# Remove Dups
	orgs = set(orgs)

	# Additional general fields to remove
	to_remove = ["national","National","National Science", "RR","National Institute of","research", "Research","US government", "U.S. Government", "US Government", "United","United States Government", "United States Department","United Stated", "United States", "U.S. Department","U.S.C", "U.S.C", "Defense", "Merit" ,"Government", "U.S.", "USA", "s", "Department"]
	orgs = [x for x in orgs if x not in to_remove]
	
	return orgs

# Requires: data dict
# Modifies: nothing
# Effects: clean giStatement field for certain contract #s, return data with
#          contracts column
# Note: look at this again, right now Bethesda & SD related only
def clean_contracts(data, gi_statements):

	contracts = []
	# STEP 1. Public Law - Don't need contract awards 
	contract_nums = data['gi_stmt'].str.contains("Public Law")
	
	# get index of law ones
	law_stmts = contract_nums[contract_nums].index
	law_stmts = law_stmts.tolist()

	for law in law_stmts:
		gi_statements[law] = ""

	for gi in gi_statements:
	# STEP 2. Extract contract awards
	############################# Expression 1
	# [A-Za-z\d] start with alphanumeric char
	# [A-Za-z\d-] 2nd char alphanumeric or - 
	# [^\s] no spacing
	# [\d] at least one more digit 
	# [A-Za-z\d-]+  finish with alphanumeric char or - 1 or more times
	############################ Expression 2 
	# [A-Z\d]{1,3} - alphanumeric 1-3 times, capital A-Z only
	# \s single space
	# [A-Z\d-]+\d - alphanumeric or - 1 or more times, followed by digit (redundant but stops
	# expression for case like "IN AGREEMENT")

		contract_nums = re.findall("[A-Za-z\d][A-Za-z\d-]+[^\s][\d][A-Za-z\d-]+|[A-Z\d]{1,3}\s[A-Z\d-]+\d", gi)

		contract_nums = '|'.join(contract_nums)
		contracts.append(contract_nums)

	# Clean up calif./bethesda codes
	ca_be = data['gi_stmt'].str.contains("Calif\.|Bethesda", regex=True)
	
	# Get index of calif./bethesda
	idx_cabe = ca_be[ca_be].index
	
	for idx in idx_cabe:

		contracts[idx] = re.sub("619\)553-5118\|?|619\)553-5120\|?|553-5118\|?|(619-)?553-2778\|?|92152\|?|72120\|?|20012\|?|53510\|?|D0012\|?|53560\|?|20014\|?|20892\|?", "", contracts[idx])

	contracts = [x.lstrip() for x in contracts]
	contracts = [x.rstrip('|') for x in contracts]
	
	return contracts


# Requires: organizations list, locations list, content
# Modifies: nothing
# Effects: parses XML file for orgs, locs
def parse_xml_ner(orgs_full, locs_full, content):
	
	for line in content: 
				orgs = re.findall("<ORGANIZATION>[^<]+</ORGANIZATION>", line)
				orgs_clean = [re.sub("<ORGANIZATION>|</ORGANIZATION>", "", x) for x in orgs]
				
				locs = re.findall("<LOCATION>[^<]+</LOCATION>", line)
				locs_clean = [re.sub("<LOCATION>|</LOCATION>", "", x) for x in locs]
				
				orgs_full.append(orgs_clean)
				locs_full.append(locs_clean)

	return orgs_full, locs_full


#--------Test Functions -------#
# Requires: dataframe, # of rows, # of cols
# Modifies: nothing
# Effects: checks if dataframe has been read in correctly
def test_dataframe(df, rw, col):
	if df.shape[0] != rw:
		print('Incorrect # of rows')
	elif df.shape[1] != col:
		print('Incorrect # of cols')
	else:
		print('pass')

	return 


if __name__ == '__main__':

	print("Running file.......")
	
	# Set up vars + directories
	#omitLocs_dir = 'D:/DataBaseUpdate/2018_Nov/contract_award_patch/'
	merged_dir = 'D:/DataBaseUpdate/2018_Nov/contract_award_patch/merged_csvs.csv'
	ner_dir = "G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/NER/stanford-ner-2017-06-09/"
	ner_txt_indir = "G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/NER/stanford-ner-2017-06-09/in/"
	ner_txt_outdir = "G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/NER/stanford-ner-2017-06-09/out_final/"
	classifiers = ['classifiers/english.all.3class.distsim.crf.ser.gz', 'classifiers/english.conll.4class.distsim.crf.ser.gz', 'classifiers/english.muc.7class.distsim.crf.ser.gz']
	ner_classif_dirs = ['out-3class', 'out-4class', 'out-7class']

	final_output_dir = "G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/test_output/"

	# 1. Read in the input file
	merged_df = read_mergedCSV(merged_dir)

	# 2. run NER
	run_NER(ner_dir, ner_txt_indir, ner_txt_outdir,merged_df, classifiers, ner_classif_dirs)
	
	# 3. process NER output
	orgs_list, locs_list = process_NER(ner_txt_outdir, merged_df)
	
	# 4. add extracted organizations and contract numbers
	df_final,orgs_final = add_cols(merged_df, orgs_list, locs_list)
	
	# 5. write output file
	write_output(final_output_dir,df_final,orgs_final)
	
	print("Done!")

