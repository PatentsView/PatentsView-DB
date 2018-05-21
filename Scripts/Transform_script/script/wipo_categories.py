import re,csv,os,MySQLdb
from collections import Counter

def wipo_lookups(working_directory, persistent_files, host, username, password, database):
    ### IPC to WIPO lookup ###

    #this input file is a static, it contains IPC technology class definitions
    inp = csv.reader(file(persistent_files+'/ipc_technology.csv','rb'))

    #this creates a dictionary mapping IPC code to field number
    data = {}
    for i in inp:
        data[i[7].replace("%","").replace(' ','')] = i[0]

    ### CPC to IPC mapping ###
    #This grabs the ipc_concordance file from the location it gets downloaded to
    #ipc concordance is downloaded from the website (automatically) every time the update is run
    inp = open(working_directory + "/" + "WIPO_input/ipc_concordance.txt" + f).read().split("\n")

    #takes the first two columns from file
    cpc2ipc = {}
    for i in inp:
        i = i.split("\t\t")
        try:
            cpc2ipc[i[0]] = i[1]
        except:
            pass

    #connect to the database using stored credentials
    mydb = MySQLdb.connect(host=host,
    user=username,
    passwd=password,
    db=database)
    cursor = mydb.cursor()
    
    #edit the cpc_current table to change the 'primary' category to 'inventional'
    cursor.execute('update cpc_current set category = "inventional" where category = "primary"')
    mydb.commit()

    #get a list of patent ids and cpc subgroup ids from the database
    cursor.execute('select distinct patent_id,subgroup_id from cpc_current where category="inventional" order by patent_id asc,sequence asc')
    res = list(cursor.fetchall())

    #creating output file which will have rows of the form [patent_id, cpc, wipo]
    os.mkdir(working_directory + "/WIPO_output/")
    with open(working_directory + "/WIPO_output/" +'WIPO_Cats_assigned_CPC2IPC.csv','wb') as myfile:
        outp = csv.writer(myfile,delimiter='\t')
        outp.writerow(['patent_id','cpc','wipo_cat'])


        pats = {}
        pats_cpc = {}
        missing = set()
        for r in res: #res is the list of patent_id, subgroup_id pairs
            r = list(r) #r will be a list of the form [patent_id, subgroup_id]
            try:
                try:
                    #look up the subgroup_id (r[1]) in the cpc2ipc dictionary and assign this to a new variable, ipcconcord
                    ipcconcord = cpc2ipc[r[1]]
                except: #if the subgroup id is not in the cpc2ipc dictionary then do:
                    #add the subgroup id to the missing list and make 'ipcconcord' equal to the cpc subgroup id
                    missing.add(r[1])
                    ipcconcord = r[1] 
                patent = r[0]
                cpc = r[1]

                
                section = ipcconcord[:4]
                group = ipcconcord.split("/")[0]
                try:
                    wipo = data[section] #data is the mapping of ipc code to field number from ipc_technology file
                except:
                    wipo = data[group] #if section is not in the ipc code mapping, look for group
                
                try: #if the patent is already in the pats dictionary, add this wipo field to it
                    pats[patent].append(wipo)
                except: #otherwise, create a new entry in the dictionary for this patent
                    pats[patent] = [wipo]

                try: #add the cpc subgroup to the pats_cpc dictionary
                    pats_cpc[patent].append(r[1])
                except:
                    pats_cpc[patent] = [r[1]]

                if ipcconcord!="CPCONLY": #only if the ipcconcord is not "CPCONLY" should we write to the file
                    #write the patent number, cpc_subgroup_id and wipo classification to the 'WIPO_Cats_assigned_CPC2IPC.csv' file
                    outp.writerow([patent,r[1],wipo])

            except:
                pass

    #now we will open and write to  'WIPO_Cats_assigned.csv', which gets loaded into wipo_fields table in raw db
    with open(working_directory + "/WIPO_output/" +'WIPO_Cats_assigned.csv','wb') as myfile:
        outp = csv.writer(myfile,delimiter='\t')
        outp.writerow(['patent_id','wipo_cat','sequence'])

        #the pats dictionary has patent ids as keys and a list of wipo field numbers as values:
        for k,v in pats.items():
            #the variable need will have a list of wipo fields sorted by the number of times they appear
            # this will have the format: [(field_id, count),(field_id, count), (field_id, count), .... ]
            need = sorted(Counter(v).items(),key=lambda x:-x[1])
            #if there are more than 2 distinct wipo fields and the top 3 appear equally often
            if len(need)>2 and need[0][1] == need[1][1] and need[0][1] == need[2][1]:
                ind1 = v.index(need[0][0]) #what numerical position did this first appear in the in original values list
                ind2 = v.index(need[1][0])
                ind3 = v.index(need[2][0])

                #write the wipo field that appears first to the output file, 
                #then write the other equally common ones; then all the rest
                if min([ind1,ind2,ind3]) == ind1:
                    outp.writerow([k,need[0][0],'0'])
                    for n in range(1,len(need)):
                        outp.writerow([k,need[n][0],n])
                elif min([ind1,ind2,ind3]) == ind2:
                    outp.writerow([k,need[1][0],'0'])
                    outp.writerow([k,need[0][0],'1'])
                    outp.writerow([k,need[2][0],'2'])
                    for n in range(3,len(need)):
                        outp.writerow([k,need[n][0],n])
            
                elif min([ind1,ind2,ind3]) == ind3:
                    outp.writerow([k,need[2][0],'0'])
                    outp.writerow([k,need[0][0],'1'])
                    outp.writerow([k,need[1][0],'2'])
                    for n in range(3,len(need)):
                        outp.writerow([k,need[n][0],n])
                else:
                    pass

            #if there are two equally common wipo fields, do same as above
            elif len(need)>1 and need[0][1] == need[1][1]:
                ind1 = v.index(need[0][0])
                ind2 = v.index(need[1][0])
                if ind1<ind2:
                    outp.writerow([k,need[0][0],'0'])
                    for n in range(1,len(need)):
                        outp.writerow([k,need[n][0],n])
                else:
                    outp.writerow([k,need[1][0],'0'])
                    outp.writerow([k,need[0][0],'1'])
                    for n in range(2,len(need)):
                        outp.writerow([k,need[n][0],n])
            
            #if there are not two equally common fields
            else:
                #next 3 lines check if something really does have equally common wipo fields and somehow got skipped
                counts = [n[1] for n in need]
                if len(counts) > 3 and len(set(counts[:4])) == 1:
                    print k,v
                #otherwise, write wipo fields to output file in order of appearance
                else:
                    outp.writerow([k,need[0][0],'0'])
                    if len(need)>1:
                        for n in range(1,len(need)):
                            outp.writerow([k,need[n][0],n])
#the 'WIPO_Cats_assigned.csv' then gets loaded into the wipo_fields table
        