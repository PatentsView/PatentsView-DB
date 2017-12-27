import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import string
import re
import os
import MySQLdb

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
    org_dict = dict(zip(list(existing_lookup["original"]),list(existing_lookup["clean"])))
    existing = list(existing_lookup["clean"])
    #Create an acronym/long form dictionary
    govt_acc_dict = dict(zip([better_title(item.strip()) for item in government_orgs["Acronym"]], [item.strip() for item in government_orgs["Long_form"]]))
    govt_long = [better_title(item.strip()) for item in government_orgs["Long_form"]]
    govt_acc_dict['Usaf'] = "AIR FORCE" #there is an extra 'department' here

    how = []
    #do the exact matches first, since those don't need to be hand checked
    gov_match = [govt_match(item, govt_long, govt_acc_dict, org_dict, how) for item in orgs_to_find]
    matches = pd.DataFrame({"How": how, "Match": gov_match, "Found_Org":orgs_to_find})
    matches = matches.reindex( columns = matches.columns.tolist() + ["Existing_Org","Confused","NonGovernment","New_Org"])
    
    found_matched = matches[matches['Match']!="None"][["Found_Org", "Match"]]
    unmatched = matches[matches['Match']=="None"]
    del unmatched["How"]
    
    #do fuzzy matching on things that didn't have an exact match
    #this is all the possible things to fuzzy match to
    all_possible_long = [better_title(item.strip()) for item in government_orgs["Long_form"]] + [better_title(item.strip()) for item in existing]
    #also do accronyms
    solid_match = []
    possible_match = []
    for org in list(unmatched["Found_Org"]):
        clean_org, score = process.extractOne(org, all_possible_long, scorer=fuzz.partial_ratio)
        if score > 90 and len(org) >=6: #small accronyms match too many things
            if clean_org in ["Army", "Air Force", "Navy"]: #special procesing to avoid losing information on longer military divisions
                if len(org) <= len(clean_org) + 4:  
                    solid_match.append(clean_org)
                    possible_match.append(np.nan)
                else:
                    solid_match.append(np.nan)
                    possible_match.append(clean_org)
            else:
                solid_match.append(clean_org)
                possible_match.append(np.nan)
        elif score >50:
            solid_match.append(np.nan)
            possible_match.append(clean_org)
        else:
            solid_match.append(np.nan)
            possible_match.append(np.nan)
    unmatched["Match"] = solid_match
    unmatched["Possible_Match"] = possible_match
    
    #lookup ito add to existing orgs lookup
    to_merge = unmatched[~pd.isnull(unmatched['Match'])][["Found_Org", "Match"]]
    all_matched = pd.concat([to_merge, found_matched])
    all_matched.columns = ["original", "clean"]
    all_plus_existing = pd.concat([all_matched, existing_lookup])
    all_plus_existing.to_csv(persistent_files + "/existing_orgs_lookup.csv")

    #write out the unmatched records for hand matching
    for_hand_match = folder + "/government_manual"
    #os.mkdir(for_hand_match)
    unmatched.to_csv(for_hand_match + "/unmatched.csv")




def get_orgs(host, username,password, old_database,folder):
	mydb = MySQLdb.connect(host= host,
    user=username,
    passwd= password, db =old_database)
	raw = pd.read_sql("select * from government_organization", mydb)
	location = folder + "/government_manual"
	raw.to_csv(location + "/government_organization.csv", index = None)