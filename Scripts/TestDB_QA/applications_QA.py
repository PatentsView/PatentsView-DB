def generate_app_qa(host,username,password,dbname,outputdir):
    import csv,random
    import MySQLdb
    import re,sys,os
    import datetime
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password)
    cursor = mydb.cursor()
    cursor.execute('SELECT id from '+dbname+'.application where year(date)=2001')
    fields = [f[0] for f in cursor.fetchall()]
    select = random.sample(fields,25)
    
    cursor.execute('SELECT id from '+dbname+'.application where year(date)=2005')
    fields = [f[0] for f in cursor.fetchall()]
    select.extend(random.sample(fields,25))
    
    cursor.execute('SELECT id from '+dbname+'.application where year(date)= 2014')
    fields = [f[0] for f in cursor.fetchall()]
    select.extend(random.sample(fields,25))
    
    quer = open('query_testdb_app.txt').read().split('\n')
    core = quer[0]+quer[1]
    data = {}
    for s in select:
        print s
        for n in range(2,6):
            query = core+re.sub(',$',' ',quer[n])+' '+quer[7]+' '+quer[6+n]+' '+quer[-1]
            query = query.replace('app_smalltest_20141008',dbname)
            cursor.execute(query+"'"+s+"'")
            outp = cursor.fetchall()
            if len(outp) > 30:
                outp = outp[:30]
            for o in outp:
                try:
                    gg = data[s+'_'+str(n)]
                    o = [str(oo) for oo in list(o)]
                    for nu in range(7,len(gg)):
                        data[s+'_'+str(n)][nu]+='; '+o[nu]
                            
                except:
                    data[s+'_'+str(n)] = [str(oo) for oo in list(o)]
    
    outputfile = csv.writer(open(os.path.join(outputdir,'applications_QA.csv'),'wb'))
    outputfile.writerow(['application_id','application_number','app_date','app_country','app_title','app_abstract','num_claims','inventors','inventor_location','assignee_names','assignee_orgs','assignee_location','assignee_nationality','assignee_residence','primary_USclass','other_USClass','claims'])
    merge = {}
    
    
    
    for s in select:
        for n in range(2,6):
            try:
                gg = merge[s]
                merge[s].extend(data[s+'_'+str(n)][7:])
            except:
                merge[s] = data[s+'_'+str(n)]
    print "GOOD"
    for k,v in merge.items():
        val = [str(va) for va in v]
        invt_first = val[7].split('; ')
        invt_last = val[8].split("; ")
        inventors = []
        for nn in range(len(invt_first)):
            inventors.append(invt_first[nn]+' '+invt_last[nn])
        invtloc = [vv.split('; ') for vv in val[9:12]]
        invtlocation = []
        for nn in range(len(invtloc[0])):
            classif = []
            for nnn in range(len(invtloc)):
                if invtloc[nnn][nn] != "None":
                    classif.append(invtloc[nnn][nn])
            invtlocation.append(' '.join(classif))
        
        assg_first = val[12].split('; ')
        assg_last = val[13].split("; ")
        assignees = []
        for nn in range(len(assg_first)):
            assignees.append(assg_first[nn]+' '+assg_last[nn])
        if '; '.join(assignees) == "None None":
            assignees = ['None']
        
        assgloc = [vv.split('; ') for vv in val[15:18]]
        assglocation = []
        for nn in range(len(assgloc[0])):
            classif = []
            for nnn in range(len(assgloc)):
                if assgloc[nnn][nn] != "None":
                    classif.append(assgloc[nnn][nn])
            assglocation.append(' '.join(classif))
        usclass_order = val[20].split('; ')
        usclass = val[21].split("; ")
        uspc = []
        uspc_primary = 'None'
        for nn in range(len(usclass_order)):
            if int(usclass_order[nn]) == 0:
                uspc_primary = usclass[nn]
            else:
                uspc.append(usclass[nn])
        outputfile.writerow(val[:7]+['; '.join(inventors),'; '.join(invtlocation),'; '.join(assignees),val[14],'; '.join(assglocation)]+val[18:20]+[uspc_primary,'; '.join(uspc),val[22]])
        