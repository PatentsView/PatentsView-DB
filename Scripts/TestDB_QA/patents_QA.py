def generate_grants_qa(host,username,password,dbname,outputdir):
    import csv,random
    import MySQLdb
    import re,sys,os
    import datetime
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password)
    cursor = mydb.cursor()
    cursor.execute('SELECT id from '+dbname+'.patent where year(date)> 1990 and year(date) < 2000')
    fields = [f[0] for f in cursor.fetchall()]
    select = random.sample(fields,25)
    
    cursor.execute('SELECT id from '+dbname+'.patent where year(date) > 2001 and year(date)<2005')
    fields = [f[0] for f in cursor.fetchall()]
    select.extend(random.sample(fields,25))
    
    cursor.execute('SELECT id from '+dbname+'.patent where year(date)>= 2005')
    fields = [f[0] for f in cursor.fetchall()]
    select.extend(random.sample(fields,25))
    
    quer = open('query_testdb_grant.txt').read().split('\n')
    core = quer[0]+quer[1]
        
    data = {}
    for s in select:
        print s
        for n in range(2,11):
            query = core+re.sub(',$','',quer[n])+''.join(quer[12:])
            query = query.replace('grant_smalltest_20141006',dbname)
            cursor.execute(query+"'"+s+"'")
            outp = cursor.fetchall()
            if len(outp) > 30:
                outp = outp[:30]
            for o in outp:
                try:
                    gg = data[s+'_'+str(n)]
                    o = [str(oo) for oo in list(o)]
                    for nu in range(6,len(gg)):
                        data[s+'_'+str(n)][nu]+='; '+o[nu]
                        #print data[s+'_'+str(n)]
                            
                except:
                    data[s+'_'+str(n)] = [str(oo) for oo in list(o)]
    
    outputfile = csv.writer(open(outputdir+'patents_QA.csv','wb'))
    outputfile.writerow(['patent_id','patent_number','patent_date','patent_title','patent_abstract','num_claims','inventors','assignee_names','assignee_orgs','application_number','app_date','primary_USclass','other_USClass','IPC classes','foreign_patentcitation','other_references','lawyer_name','lawyer_firm','lawyer_country','USpatent_citations'])
    merge = {}
    
    
    
    for s in select:
        for n in range(2,11):
            try:
                gg = merge[s]
                merge[s].extend(data[s+'_'+str(n)][6:])
            except:
                merge[s] = data[s+'_'+str(n)]
    print "GOOD"
    for k,v in merge.items():
        val = [str(va) for va in v]
        invt_first = val[6].split('; ')
        invt_last = val[7].split("; ")
        inventors = []
        for nn in range(len(invt_first)):
            inventors.append(invt_first[nn]+' '+invt_last[nn])
        assg_first = val[8].split('; ')
        assg_last = val[9].split("; ")
        assignees = []
        for nn in range(len(assg_first)):
            assignees.append(assg_first[nn]+' '+assg_last[nn])
        if '; '.join(assignees) == "None None":
            assignees = ['None']
        usclass_order = val[13].split('; ')
        usclass = val[14].split("; ")
        uspc = []
        uspc_primary = 'None'
        for nn in range(len(usclass_order)):
            if int(usclass_order[nn]) == 0:
                uspc_primary = usclass[nn]
            else:
                uspc.append(usclass[nn])
        ipcclass = [vv.split('; ') for vv in val[15:20]]
        ipcr = []
        for nn in range(len(ipcclass[0])):
            classif = ''
            for nnn in range(len(ipcclass)-1):
                classif+=ipcclass[nnn][nn]
            classif+='/'+ipcclass[-1][nn]
            ipcr.append(classif)
        if len(set(ipcr)) < 3:
            ipcr = ['None']
        forcit = [vv.split('; ') for vv in val[20:24]]
        foreign = []
        for nn in range(len(forcit[0])):
            classif = []
            for nnn in range(len(forcit)):
                if forcit[nnn][nn] != "None":
                    classif.append(forcit[nnn][nn])
            foreign.append(' '.join(classif))
        if '; '.join(foreign) == '':
            foreign = ["None"]
        law_first = val[25].split('; ')
        law_last = val[26].split("; ")
        lawyers = []
        for nn in range(len(law_first)):
            lawyers.append(law_first[nn]+' '+law_last[nn])
        if '; '.join(lawyers) == "None None":
            lawyers = ['None']
        patcit = [vv.split('; ') for vv in val[29:]]
        uspat = []
        for nn in range(len(patcit[0])):
            classif = []
            for nnn in range(len(patcit)):
                if patcit[nnn][nn] != "None":
                    classif.append(patcit[nnn][nn])
            uspat.append(' '.join(classif))
        if '; '.join(uspat) == '':
            uspat = ["None"]
        
        outputfile.writerow(val[:6]+['; '.join(inventors),'; '.join(assignees)]+val[10:13]+[uspc_primary,'; '.join(uspc),'; '.join(ipcr),'; '.join(foreign),val[24],'; '.join(lawyers)]+val[27:29]+['; '.join(uspat)])