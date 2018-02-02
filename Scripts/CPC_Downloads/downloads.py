import urllib
import lxml.html
import zipfile
import os
def download_schema(working_directory):
	#get the most recent CPC Schema
	connection = urllib.urlopen('http://www.cooperativepatentclassification.org/cpcSchemeAndDefinitions/Bulk.html')
	dom =  lxml.html.fromstring(connection.read())

	#look up the exact name of the schema file which changes based on year
	for link in dom.xpath('//a/@href'): 
		#print link
		if link.startswith("/cpc/interleaved/CPCSchemeXML"):
			cpc_schema_files = "http://www.cooperativepatentclassification.org" + link
			print cpc_schema_files
	name, other  = urllib.urlretrieve(cpc_schema_files, working_directory + "/temp.zip")
	z = zipfile.ZipFile(name)
	z.extractall(working_directory + "/CPC_Schema")
	z.close()
	os.remove(working_directory + "/temp.zip")
def download_input(working_directory):
	#get the latest cpc master list
	connection = urllib.urlopen('https://bulkdata.uspto.gov/data/patent/classification/cpc/')
	dom =  lxml.html.fromstring(connection.read())
	#look up the file names as they change based on date
	for link in dom.xpath('//a/@href'): 
	    if link.startswith("US_Grant_CPC_MCF_Text"):
	        grant = link
	        grant_url = "https://bulkdata.uspto.gov/data/patent/classification/cpc/" + link
	    if link.startswith("US_PGPub_CPC_MCF_Text"):
	        ppubs = link
	        ppubs_url = "https://bulkdata.uspto.gov/data/patent/classification/cpc/" + link
	os.mkdir(working_directory + "/CPC_input")
	urllib.urlretrieve(grant_url, working_directory +"/CPC_input/" + grant )
	urllib.urlretrieve(ppubs_url, working_directory +"/CPC_input/" + ppubs)
def download_ipc(working_directory):
	connection = urllib.urlopen('http://www.cooperativepatentclassification.org/cpcConcordances.html')
	dom =  lxml.html.fromstring(connection.read())
	#look up the exact name of the concordance which changes based on montn
	for link in dom.xpath('//a/@href'): 
		if link.startswith("/cpcConcordances/CPCtoIPCtxt"):
			ipc_concordance = "http://www.cooperativepatentclassification.org/" + link
	os.mkdir(working_directory + "/WIPO_input")
	urllib.urlretrieve(ipc_concordance, working_directory + "/WIPO_input/ipc_concordance.txt")
