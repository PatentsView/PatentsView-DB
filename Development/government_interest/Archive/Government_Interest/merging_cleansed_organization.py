# Packages for Notepad++
import os.path, sys , datetime , pprint 
import csv ,  pprint , re
from collections import defaultdict
import pandas as pd
import numpy as np
import MySQLdb

def upload_new_orgs(govt_manual, host, username, password, database):
	data = pd.read_excel(govt_manual + '/looked_up_orgs.xlsx')
	data.fillna("", inplace = True)
	ex = data.values.tolist()
	mydb = MySQLdb.connect(host= host, user = username, passwd = password, db = database)
	cursor = mydb.cursor()
	#this is because the auto increment isn't working for some reason
	cursor.execute("select max(organization_id) from government_organization;")
	max_id = cursor.fetchone()[0] 
	print max_id
	for i in range(len(ex)):
		cursor.execute("select * from government_organization where name ='" + ex[i][0] + "'")
		if len(cursor.fetchall()) <1 : #so if the organization is really not there
			org_id = max_id + i + 1 #adding one so we start with the next id after the original max
			row = str(tuple([int(org_id)] + [str(item) for item in ex[i]]))
			cursor.execute("insert into government_organization (organization_id, name, level_one, level_two, level_three) values " + row + ";")
	mydb.commit()

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
	org_df = open(processed_gov + '/NER_output.txt')
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
		# data.append(str(list(c_org)).replace('[' , '').replace(']' , '').replace("'" , "").replace(", ," , ",").lstrip(',').rstrip(',').replace(',' , '|') )  #Appending cleansed organization 
		# data.append("|".join(c_org).replace("||" , "|").lstrip('|').rstrip('|').strip(' \t\n\r')) #Appending cleansed organization 
		data.append(set_org) #Appending cleansed organization 
		data.append(r[5].strip("\""))  #Appending Contract number 
		data.append(r[6]) #Appending Has address
		data.append(r[7].strip("\"").strip('\n'))	#Appending Has phone  
		# data.append(r[8])	#Appending Has phone  
		# data.append(r[9])	#Appending Has phone  
		# data.append(r[10].strip('\n'))	#Appending Has phone  
		
		#Writting data to the output file
		outp.writerow(data)	
		set_org = ''



	# outp2 = csv.writer(open(wdata+ '/Unique_cleansed_org.18.2016_v2.csv' , 'wb') , delimiter=',' , quoting=csv.QUOTE_MINIMAL)  #final output file
	outp2 = csv.writer(open(manual_gov+ '/Unique_cleansed_org.csv' , 'wb') , delimiter='\t' , quoting=csv.QUOTE_NONNUMERIC)  #final output file
	header = ['Cleanse_org' , 'N']
	outp2.writerow(header)
	for k, v in  dict_org.iteritems() :
		d = [k, v]
		outp2.writerow(d)
	print "We missed", str(missed)
	print "We caught", str(caught)
	missed_list = list(set(missed_orgs))
	missed_csv = csv.writer(open(manual_gov + "/missed_orgs.csv", 'wb'), delimiter = "\t", quoting=csv.QUOTE_NONNUMERIC)
	for item in missed_list:
		missed_csv.writerow([item])
	print "*** The End ***"
		