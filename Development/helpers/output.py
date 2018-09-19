import csv 
import string
import random
import sys


def write_titles(data_file, fields_list):
    with open(data_file, 'w', encoding ='utf8') as myfile:
        writer = csv.writer(myfile, delimiter = '\t')
        writer.writerow(fields_list)
def write_data(data_file, results_list):
    with open(data_file, 'a', encoding ='utf8') as myfile:
        writer = csv.writer(myfile, delimiter = '\t')
        for row in results_list:
            #encoded_data = [s.encode('utf8') if not s is None and not isinstance(s, int) else s for s in row]
            writer.writerow(row)
def get_alt_tags(data_dict, tags_list):
    results = None
    for tag in tags_list:
        if data_dict[tag]:
            results = data_dict[tag]
    return results

def mandatory_fields(field_name, patent_id, error_log, any_cols, all_cols = []):
    if any_cols.count(None) == len(any_cols): #this gets if the list is all None
        error_log.append(patent_id, field_name)
    if None in all_cols:
        error_log.append(patent_id, field_name)

def write_partial(results, out_location, field_dictionary):
    
    for field in list(filter(lambda x : x not in ['subclass', 'mainclass'], field_dictionary.keys())):
        write_titles("{0}/{1}.csv".format(out_location, field), field_dictionary[field])
        write_data("{0}/{1}.csv".format(out_location, field), results[field])
    if 'subclass' in field_dictionary.keys(): #allows this to work for partial reruns as well
	    for field in ['subclass', 'mainclass']:
	        #mainclass and subclass both only want a set of results
	        field_data = [[item] for item in list(set(results[field]))]
	        write_titles("{0}/{1}.csv".format(out_location, field), field_dictionary[field])
	        write_data("{0}/{1}.csv".format(out_location, field), field_data)
