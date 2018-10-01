import csv
import MySQLdb
import pprint
import sys

# This short script loads gi records into the patents (grants) database, currently four tables 
# Needs to be folded into the parser set of processes 
# Originally authored by: sarora@air.org 
# August 18, 2016
# Heavily updataed by: skelley@air.org
# December 12, 2017 

#This should be new orgs definitely
#orgs = csv.reader(open("H:/share/Science Policy Portfolio/PatentsView IV/Govt Interest/Used_For_Update/looked_up_orgs.csv"))
#records = csv.reader(open("H:/share/Science Policy Portfolio/PatentsView IV/Govt Interest/Used_For_Update/cleansed_output_08.08.2017_v2.csv"), delimiter="\t", quoting=csv.QUOTE_NONNUMERIC)

# assume organizations have already been loaded 
def readOrgs (db_cursor):
    orgh = {}
    sql = "SELECT organization_id, name FROM government_organization";
    db_cursor.execute(sql)
    for row in db_cursor:
        orgh[row[1]] = int(row[0])
    return orgh
    
    
# process and save records.  assumes organizational data have already been loaded 
def process(host, username, password, database, govt_manual):
    seenPats = []
    counter = 0
    mydb = MySQLdb.connect(host= host, user = username, passwd = password, db = database)
    cursor = mydb.cursor()
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

         