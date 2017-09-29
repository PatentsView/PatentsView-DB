import re,csv,os,MySQLdb
from collections import Counter

def wipo_lookups(working_directory, persistent_files, host, username, password, database):
    ### IPC to WIPO lookup ###
    inp = csv.reader(file(persistent_files+'/ipc_technology.csv','rb'))
    data = {}
    for i in inp:
        data[i[7].replace("%","").replace(' ','')] = i[0]
    ### CPC to IPC mapping ###
    #have to download this, in download script
    f = os.listdir(working_directory + "/" + "WIPO_input/")[0]
    inp = open(working_directory + "/" + "WIPO_input/" + f).read().split("\n")
    cpc2ipc = {}
    for i in inp:
        i = i.split("\t\t")
        try:
            cpc2ipc[i[0]] = i[1]
        except:
            pass

    mydb = MySQLdb.connect(host=host,
    user=username,
    passwd=password,
    db=database)
    cursor = mydb.cursor()
    #should fix this step farther back!
    cursor.execute('update cpc_current set category = "inventional" where category = "primary"')
    mydb.commit()
    cursor.execute('select distinct patent_id,subgroup_id from cpc_current where category="inventional" order by patent_id asc,sequence asc')
    res = list(cursor.fetchall())
    os.mkdir(working_directory + "/WIPO_output/")
    with open(working_directory + "/WIPO_output/" +'WIPO_Cats_assigned_CPC2IPC.csv','wb') as myfile:
        outp = csv.writer(myfile,delimiter='\t')
        outp.writerow(['patent_id','cpc','wipo_cat'])

        pats = {}
        pats_cpc = {}
        missing = set()
        for r in res:
            r = list(r)
            try:
                try:
                    ipcconcord = cpc2ipc[r[1]]
                except:
                    missing.add(r[1])
                    ipcconcord = r[1]
                patent = r[0]
                cpc = r[1]
                if ipcconcord!="CPCONLY":
                    section = ipcconcord[:4]
                    group = ipcconcord.split("/")[0]
                    try:
                        wipo = data[section]
                    except:
                        wipo = data[group]
                    try:
                        pats[patent].append(wipo)
                    except:
                        pats[patent] = [wipo]
                    try:
                        pats_cpc[patent].append(r[1])
                    except:
                        pats_cpc[patent] = [r[1]]
                    outp.writerow([patent,r[1],wipo])
                    #print '\t'.join([ patent,cpc,ipcconcord,wipo,r[-2],str(r[-1]) ])
                else:
                    section = cpc[:4]
                    group = cpc.split("/")[0]
                    try:
                        wipo = data[section]
                    except:
                        wipo = data[group]
                    try:
                        pats[patent].append(wipo)
                    except:
                        pats[patent] = [wipo]
                    try:
                        pats_cpc[patent].append(r[1])
                    except:
                        pats_cpc[patent] = [r[1]]
            
                #print '\t'.join([patent,cpc,ipcconcord,'',r[-2],str(r[-1])])
            except:
                pass
                #print r[1]
    with open(working_directory + "/WIPO_output/" +'WIPO_Cats_assigned.csv','wb') as myfile:
        outp = csv.writer(myfile,delimiter='\t')
        outp.writerow(['patent_id','wipo_cat','sequence'])
        for k,v in pats.items():
            need = sorted(Counter(v).items(),key=lambda x:-x[1])
            if len(need)>2 and need[0][1] == need[1][1] and need[0][1] == need[2][1]:
                ind1 = v.index(need[0][0])
                ind2 = v.index(need[1][0])
                ind3 = v.index(need[2][0])
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
            
            else:
                counts = [n[1] for n in need]
                if len(counts) > 3 and len(set(counts[:4])) == 1:
                    print k,v
                else:
                    outp.writerow([k,need[0][0],'0'])
                    if len(need)>1:
                        for n in range(1,len(need)):
                            outp.writerow([k,need[n][0],n])
        