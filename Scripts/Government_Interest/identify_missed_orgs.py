import pandas as pd
import string
import re
import os

def better_title(text):
    title = " ".join([item if item not in ["Of", "The", "For", "And", "On"] else item.lower() for item in text.title().split( )])
    return title.translate(None, string.punctuation)

def govt_match(item, gov_org_long, gov_org_acc_dict, org_dict, how):
	for org in org_dict.keys():
	    if item == org:
	        how.append("Existing")
	        return org_dict[org]
	for org in gov_org_long:
	    if org in item:
	        how.append("Long")
	        return org
	for acc in gov_org_acc_dict.keys():
	    if re.search(r'\b'+ acc + r'\b', item):
	        how.append("Acc")
	        return gov_org_acc_dict[acc]
	how.append("None")
	return "None"


def find_missing(folder, persistent_files, processed_gov):

	lookup = persistent_files + "/existing_orgs_lookup.csv"
	new_data = processed_gov + "/distinctOrgs.txt"
	govt_acro = persistent_files + "/list_of_government_agencies.csv"

	new_orgs = pd.read_csv(new_data, delimiter = "\t")
	new_orgs["Organization"] = new_orgs["Organization"].map(lambda x : better_title(x))
	existing_lookup = pd.read_csv(lookup).dropna()
	government_orgs = pd.read_csv(govt_acro)
	
	#create a look up for already matched organizations
	orgs_to_find = list(new_orgs['Organization'])
	org_dict = dict(zip(list(existing_lookup["raw_org"]),list(existing_lookup["clean_org"])))
	existing = list(existing_lookup["raw_org"])
	#Create an acronym/long form dictionary
	govt_acc_dict = dict(zip([better_title(item.strip()) for item in government_orgs["Acronym"]], [item.strip() for item in government_orgs["Long_form"]]))
	govt_long = [better_title(item.strip()) for item in government_orgs["Long_form"]]
	govt_acc_dict['Usaf'] = "AIR FORCE" #there is an extra 'department' here

	how = []
	gov_match = [govt_match(item, govt_long, govt_acc_dict, org_dict, how) for item in orgs_to_find]
	matches = pd.DataFrame({"Org":orgs_to_find, "Match": gov_match, "How": how})
	#print the unmatched items to a csv for hand-checking
	unmatched = matches[matches['Match']=="None"]
	for_hand_match = folder + "/government_manual"
	os.mkdir(for_hand_match)
	unmatched.to_csv(for_hand_match + "/unmatched.csv")