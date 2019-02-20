import os.path, sys , datetime , pprint 
import csv , re
from collections import defaultdict
import pandas as pd
import numpy as np
import MySQLdb
sys.path.append('/project/Development')
from helpers import general_helpers

def upload_new_orgs(govt_manual, engine):
	data = pd.read_excel(govt_manual + '/looked_up_orgs.csv')
	data.fillna("", inplace = True)
	ex = data.values.tolist()
	cursor = engine.connect()
	#this is because the auto increment isn't working for some reason
	id_code = cursor.execute("select max(organization_id) from government_organization;")
	max_id = [i for i in id_code][0] 
	for i in range(len(ex)):
		data = cursor.execute("select * from government_organization where name ='" + ex[i][0] + "'")
        results = [i for i in data]
		if len(results) <1 : #so if the organization is really not there
			org_id = max_id + i + 1 #adding one so we start with the next id after the original max
			row = str(tuple([int(org_id)] + [str(item) for item in ex[i]]))
			cursor.execute("insert into government_organization (organization_id, name, level_one, level_two, level_three) values " + row + ";")

def lookup(processed_gov, manual_gov, persistent_files):
	#Get the manually looked up data
	lookup_data = pd.read_csv(manual_gov + '/matched.csv')
	lookup_data['original'] = lookup_data["Found_Org"]
	lookup_data["clean"] = lookup_data .apply(lambda row: row["Existing_Org"] if row['Existing_Org'] is not np.nan else row["New_Org"], axis =1)
	clean_lookup = lookup_data[['original', 'clean']].dropna()

	
	#get the previously cleaned data
	existing_lookup = pd.read_csv(persistent_files + '/existing_orgs_lookup.csv')
	existing_lookup = existing_lookup.rename(columns = {'raw_org': 'original', 'clean_org':'clean'})
	existing_lookup  = existing_lookup[['original', 'clean']].dropna()

	all_lookup = pd.concat([clean_lookup, existing_lookup])
	all_lookup.to_csv(persistent_files + '/existing_orgs_lookup.csv', index = False)

	#Creating a dictionary with key = raw.organization name and value = cleansed.organization
	original = [item.upper() for item in all_lookup['original']]
	dict_clean_org = dict(zip(original, all_lookup['clean']))

	#Final output path  
	# original to check 'cleansed_output_08.08.2017_v2.csv'
	outp = csv.writer(open(manual_gov +'/clean_output.csv','wb') , delimiter='\t' , quoting=csv.QUOTE_NONNUMERIC)  #final output file
	headers = ['Patent number' , 'TwinArch set' , 'GI title' , 'GI statement'  , 'Orgs' , 'Cleansed.Org' , 'Contract/award(s)' , 'Has address'   , 'Has phone' ]
	outp.writerow(headers)

	#output from NER
	#original to check 'output.17.08.08.txt'
	org_df = open(pre_manual + '/NER_output.txt')
	org_df.next()

	num = 0 
	dict_org = defaultdict(int)
	caught = 0
	missed =0
	missed_orgs = []

	i = 0 
	for r in org_df:
		r = r.split('\t')
		data  = [] #Declaring an empty list to store our data 
		#Check if record has an organization 
		if  r[4] != "" :
			org = r[4]
			org = str(org).strip("\"")
			org = org.upper()
			org= org.split('|')
			c_org = set() #Declaring a set to store unique cleansed organizations related to the raw organizations
			set_org = ''
			#Loop through a list of raw organization
			for g in org : 
				#Check for cleaned organization related to the raw organization 
				if g in dict_clean_org  and dict_clean_org[g] != "": 
					# print g, dict_clean_org[g]
					c_org.add(dict_clean_org[g].strip()) #adding cleaned organization into a c_org set
					caught +=1
				else:
					missed +=1
					missed_orgs.append(g)
			#Removing "United States Government" when there are more than one cleansed organization in c_org set 
			if len(c_org)  >  1 and "United States Government" in c_org :
				try :
					c_org.remove("United States Government")
				except ValueError:
					pass 
			#Creating a dictionary of the frequence of cleansed org that are parsed in the final output  
			# print "This is my set:" , c_org
			for c in c_org:
				m = c
				dict_org[m]  +=1 
			# Converting a set of cleanse org into a string separated by pipe 	
			set_org = ("|".join(c_org).replace("||" , "|").strip(' \t\n\r'))
		#If NER failled to pull an organization, we are parsing "United States Government" as a defaul org if there is any instance of "govern" in GI Statement
		else :
			c_org = set() #Declaring a set to store unique cleansed organizations related to the raw organizations
			set_org = ''
			text = r[3].lower()
			text = text.replace("\"" , "")
			# print text 
			if re.search("govern",  text) != None : 
				# print re.search("govern",  text)
				c_org.add("United States Government")
				# print c_org
				# print  dict_org['United States Governmen']
							
				dict_org['United States Government'] +=1 
				
				set_org = ("|".join(c_org).replace("||" , "|").strip(' \t\n\r'))
				# print set_org

			else: 
				pass 
			

						
		#print set_org
		set_org = set_org.lstrip('|').rstrip('|')	 #removing trailing pipe 	
		#Appending column to data 	
		data.append(r[0].strip("\"")) #Appending patent number
		data.append(r[1].strip("\"")) #Apeending Twin Arch set
		data.append(r[2].strip("\"")) #Appedning GI Title
		data.append(r[3].replace("\"" , "")) #Appending  Statement
		data.append(r[4].strip("\"")) #Appedning Raw Organization 

		data.append(set_org) #Appending cleansed organization 
		data.append(r[5].strip("\""))  #Appending Contract number 
		data.append(r[6]) #Appending Has address
		data.append(r[7].strip("\"").strip('\n'))	#Appending Has phone  
		#Writting data to the output file
		outp.writerow(data)	
		set_org = ''


	outp2 = csv.writer(open(manual_gov+ '/Unique_cleansed_org.csv' , 'wb') , delimiter='\t' , quoting=csv.QUOTE_NONNUMERIC)  #final output file
	header = ['Cleanse_org' , 'N']
	outp2.writerow(header)
	for k, v in  dict_org.items():
		outp2.writerow([k, v])
	print "We missed", str(missed)
	print "We caught", str(caught)
	missed_list = list(set(missed_orgs))
	missed_csv = csv.writer(open(manual_gov + "/missed_orgs.csv", 'wb'), delimiter = "\t", quoting=csv.QUOTE_NONNUMERIC)
	for item in missed_list:
		missed_csv.writerow([item])
        
def readOrgs (db_cursor):
    orgh = {}
    sql = "SELECT organization_id, name FROM government_organization";
    db_cursor.execute(sql)
    for row in db_cursor:
        orgh[row[1]] = int(row[0])
    return orgh
    
    
# process and save records.  assumes organizational data have already been loaded 
def process(db_con, govt_manual):
    seenPats = []
    counter = 0
    cursor = db_con.connect()
    orgh = readOrgs(cursor)
    print len(orgh)
    records = csv.reader(open(govt_manual + "/clean_output.csv"), delimiter="\t", quoting=csv.QUOTE_NONNUMERIC)
    next(records)
    missed_orgs = []
    for row in records:
        #print "Working on " + ','.join(row) 
        patno = row[0]
        if patno.startswith("US0"):
            patno = patno[3:]
        elif patno.startswith("US"):
            patno = patno[2:]
        
        if patno in seenPats:
            print "Skipping " + patno + " because it's already been seen" # patents should appear a single time
            continue
        
        gistmt = None
        if row[3]: 
            gistmt = row[3]
        cleansedOrgs = row[5]
        contractAwards = row[6]
        
        # save patno with gistmt 
        sql = """INSERT INTO government_interest (patent_id, gi_statement) VALUES ( %s, %s);"""
        
        seenPats.append(patno)
        
        try:
            cursor.execute(sql, (str(patno), gistmt), )
            mydb.commit()
        except:
            raise Exception('Was not able to save ' + patno + " into government_interest")
            mydb.rollback()
            continue
            
        cleansedOrgList = cleansedOrgs.split('|')
        uncleansedOrgs = row[4]

        seenOrgs = []

        for co in cleansedOrgList:
            if co != "": 
                if co not in orgh:
                    #raise LookupError ("\tCannot find " + co + " in orgh")
                    missed_orgs.append(co)
                    print co
                elif co in seenOrgs: 
                    print "\tAlready encountered " + co + " in list of cleansed organizations" 
                else: # save patno with orgid
                    seenOrgs.append(co)
                    orgid = orgh[co]
                    sql = """INSERT INTO patent_govintorg (patent_id, organization_id) VALUES (%s, %s);"""
                    #print "\t\t" + sql % (str(patno), str(orgid))
                    try:
                        cursor.execute(sql, (str(patno), str(orgid), ))
                        mydb.commit()
                    except:
                        raise Exception('Was not able to save ' + str(patno) + " into patent_govintorg with org id " + str(orgid))
                        mydb.rollback()
        
        #deal with this next
        cleansedContractAwardList = contractAwards.split('|')
        
        for ca in cleansedContractAwardList: 
            if ca != "":
                sql = """INSERT INTO patent_contractawardnumber (patent_id, contract_award_number) VALUES ( %s, %s);"""
                #print "\t\t" + sql % (str(patno), str(ca))
                try:
                    cursor.execute(sql, (str(patno), str(ca), ))
                    mydb.commit()
                except:
                    raise Exception('Was not able to save ' + str(patno) + " into patent_contractawardnumber with contract/award_number " + ca)
                    mydb.rollback()
    print counter
    orgs_to_add = list(set(missed_orgs))
    missed_csv = csv.writer(open(govt_manual + "/missed_orgs_upload.csv", 'wb'), delimiter = "\t", quoting=csv.QUOTE_NONNUMERIC)
    for item in orgs_to_add:
        missed_csv.writerow([item])

         
        
if __name__ == '__main__':
    persistent_files = config['FOLDERS']['PERSISTENT_FILES']
    pre_manual = '{}/government_interest/pre_manual'.format(config['FOLDERS']['WORKING_FOLDER'])
    manual_inputs = '{}/government_interest/manual_inputs'.format(config['FOLDERS']['WORKING_FOLDER'])
    post_manual = '{}/government_interest/post_manual'.format(config['FOLDERS']['WORKING_FOLDER'])
    import configparser
    config = configparser.ConfigParser()
    config.read('/project/Development/config.ini')
    db_con = general_helpers.connect_to_db(host= host, user = username, passwd = password, db = database)
    
    lookup(post_manual, manual_inputs, pre_manual, persistent_files)
    upload_new_orgs(post_manual, db_con)
    process(db_con, post_manual)
    
    
    