import csv
import MySQLdb
import re,os,random,string,codecs


def mysql_upload(host,username,password,dbname,folder,output_folder):
    inp = open(os.path.join(folder,'patent.csv'),'rb').read().decode('utf-8','ignore').split("\r\n")
    del inp[0]
    del inp[-1]
    duplicates = {}
    allpatents = {}
    mergersid = {}
    seconddupl = {}
    secondmerg = {}
    for n in range(len(inp)-1):
        try:
            gg = allpatents[inp[n].split("\t")[2]]
            try:
                duplicates[gg].append(inp[n].split("\t")[0])
                seconddupl[inp[n].split("\t")[0]] = gg
            except:
                duplicates[gg] = [inp[n].split("\t")[0]]
                seconddupl[inp[n].split("\t")[0]] = gg
        except:
            allpatents[inp[n].split("\t")[2]] = inp[n].split("\t")[0]
        
        if inp[n+1].split('\t')[2] == "NULL":
            try:
                mergersid[runnums].append(inp[n+1].split("\t")[0])
                secondmerg[inp[n+1].split("\t")[0]] = runnums
            except:
                mergersid[runnums] = [inp[n+1].split("\t")[0]]
                secondmerg[inp[n+1].split("\t")[0]] = runnums
        else:
            runnums = inp[n+1].split("\t")[0]
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password,
        db=dbname,
        charset='utf8',
        use_unicode=True)
    cursor = mydb.cursor()
    
    #Empty the allpatents container to free up memory - not needed any more
    allpatents = {}
    
    
    duplicdata = {}
    mergersdata = {}
    diri = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder,f))] # gets only files, not folders
    del diri[diri.index('patent.csv')]
    diri.insert(0,'patent.csv')
    del diri[diri.index('rawlocation.csv')]
    diri.insert(2,'rawlocation.csv')
    
    rawlocchek = {}
    cursor.execute('select id from rawlocation')
    locids = [f[0] for f in cursor.fetchall()]
    for l in locids:
        rawlocchek[l.lower()] = 1
    
    mainclasschek = {}
    cursor.execute('select id from mainclass')
    locids = [f[0] for f in cursor.fetchall()]
    for l in locids:
        mainclasschek[l.lower()] = 1
    
    subclasschek = {}
    cursor.execute('select id from subclass')
    locids = [f[0] for f in cursor.fetchall()]
    for l in locids:
        subclasschek[l.lower()] = 1
    
    #Initiate HTML Parser for unescape characters
    import HTMLParser
    h = HTMLParser.HTMLParser()
    
    for d in diri:
        print d
        infile = h.unescape(codecs.open(os.path.join(folder,d),'rb',encoding='utf-8').read()).split('\r\n')
        outp = csv.writer(open(os.path.join(output_folder,d),'wb'),delimiter='\t')
        #head = infile.next()
        head = infile[0].split('\t')
        del infile[0]
        del infile[-1]
        nullid = None
        duplicdata = {}
        mergersdata = {}
        if d == "patent.csv":
            idelem = 0
        else:
            try:
                idelem = head.index('patent_id')
            except:
                idelem = None
        if d == 'rawassignee.csv' or d == 'rawinventor.csv':
            nullid = 2
        if d == 'rawlawyer.csv' or d == 'rawlocation.csv':
            nullid = 1
        checkifexists = None
        if d == 'rawlocation.csv':
            checkifexists = 1
        if d == 'mainclass.csv':
            checkifexists = 2
        if d == "subclass.csv":
            checkifexists = 3
        tablename = d[0:d.index('.')]
        numRows = 0
        for i in infile:
            i = i.encode('utf-8','ignore')
            i = i.split('\t')
            towrite = [item.replace('"',"'") for item in i]
            if nullid:
                towrite[nullid] = 'NULL'
            try:
                gg = duplicates[i[idelem]]
                duplicdata[i[idelem]] = towrite  
            except:
                try:
                    gg = seconddupl[i[idelem]]
                    for nu in range(len(duplicdata[gg])):
                        if duplicdata[gg][nu] == "NULL":
                            duplicdata[gg][nu] = towrite[nu]
                except:
                    try:
                        gg = mergersid[i[idelem]]
                        mergersdata[i[idelem]] = towrite  
                    except:
                        try:
                            gg = secondmerg[i[idelem]]
                            for nu in range(len(mergersdata[gg])):
                                if mergersdata[gg][nu] == "NULL":
                                    mergersdata[gg][nu] = towrite[nu]
                        except:
                            if checkifexists:
                                try:
                                    if checkifexists == 1:
                                        gg = rawlocchek[towrite[0].lower()]
                                    if checkifexists == 2:
                                        gg = mainclasschek[towrite[0].lower()]
                                    if checkifexists == 3:
                                        gg = subclasschek[towrite[0].lower()]
                                except:
                                    outp.writerow(towrite)
                                    #query = """insert into """+tablename+""" values ("""+'"'+'","'.join(towrite)+'")'
                                    #query = query.replace(',"NULL"',",NULL")
                                    #cursor.execute(query)
                            else:
                                outp.writerow(towrite)
                                #query = """insert into """+tablename+""" values ("""+'"'+'","'.join(towrite)+'")'
                                #query = query.replace(',"NULL"',",NULL")
                                #query = query.replace(',""',',NULL')
                                #query = query.replace('\\','\\\\')
                                #cursor.execute(query)
            numRows += 1
            #if numRows % 10000 == 0:
            #    print d, numRows
            #    mydb.commit()

        for v in mergersdata.values():
            outp.writerow(v)
            #query = """insert into """+tablename+""" values ("""+'"'+'","'.join(v)+'")'
            #query = query.replace(',"NULL"',",NULL")
            #cursor.execute(query)
        
        for v in duplicdata.values():
            outp.writerow(v)
            #query = """insert into """+tablename+""" values ("""+'"'+'","'.join(v)+'")'
            #query = query.replace(',"NULL"',",NULL")
            #cursor.execute(query)

        #print d, numRows
        #mydb.commit()

    print "ALL GOOD"


def upload_csv(host,username,password,dbname,folder):
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password,
        db=dbname,
        charset='utf8',
        use_unicode=True)
    cursor = mydb.cursor()
    diri = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder,f))] # gets only files, not folders
    for d in diri:
        cursor.execute("load data local infile '"+os.path.join(folder,d)+"' into table "+d.replace('.csv','')+" fields terminated by '\t' lines terminated by '\r\n'")

    

def upload_uspc(host,username,password,appdb,patdb,folder):
    
    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password)
    cursor = mydb.cursor()
    
    dbnames = [appdb,patdb]
    for n in range(2):
        d = dbnames[n]
        if d:
            if n == 0:
                idname = 'application'
            else:
                idname = 'patent'
            

            #Dump mainclass_current, subclass_current and uspc_current if they exist - WILL NEED THIS WHEN UPDATING THE CLASSIFICATION SCHEMA
            cursor.execute('SET FOREIGN_KEY_CHECKS=0')
            cursor.execute('truncate table '+d+'.mainclass_current')
            cursor.execute('truncate table '+d+'.subclass_current')
            cursor.execute('truncate table '+d+'.uspc_current')
            cursor.execute('set foreign_key_checks=1')
            cursor.execute('insert into '+d+'.mainclass_current values("1","Unclassified")')
            cursor.execute('insert into '+d+'.subclass_current values("1/1","Unclassified")')
            mydb.commit() 
    
    #Upload mainclass data
    mainclass = csv.reader(file(os.path.join(folder,'mainclass.csv'),'rb'))
    for m in mainclass:
        towrite = [re.sub('"',"'",item) for item in m]
        query = """insert into mainclass_current values ("""+'"'+'","'.join(towrite)+'")'
        query = query.replace(',"NULL"',",NULL")
        try:
            query2 = query.replace("mainclass_current",appdb+'.mainclass_current')
            cursor.execute(query2)
        except:
            pass
        try:
            query2 = query.replace("mainclass_current",patdb+'.mainclass_current')
            cursor.execute(query2)
        except:
            pass
    mydb.commit()

    #Upload subclass data
    subclass = csv.reader(file(os.path.join(folder,'subclass.csv'),'rb'))
    exist = {}
    for m in subclass:
        towrite = [re.sub('"',"'",item) for item in m]
        try:
            gg = exist[towrite[0]]
        except:
            exist[towrite[0]] = 1
            try:
                towrite[1] = re.search('^(.*?)-',towrite[1]).group(1)
            except:
                pass
            query = """insert into subclass_current values ("""+'"'+'","'.join(towrite)+'")'
            query = query.replace(',"NULL"',",NULL")
            try:
                query2 = query.replace("subclass_current",appdb+'.subclass_current')
                cursor.execute(query2)
            except:
                pass
            try:
                query2 = query.replace("subclass_current",patdb+'.subclass_current')
                cursor.execute(query2)
            except:
                pass
            
    mydb.commit()
                
    
    if patdb:
        # Get all patent numbers in the current database not to upload full USPC table going back to 19th century
        cursor.execute('select id,number from '+patdb+'.patent')
        patnums = {}
        for field in cursor.fetchall():
            patnums[field[1]] = field[0]
        
        #Create USPC table off full master classification list
        uspc_full = csv.reader(file(os.path.join(folder,'USPC_patent_classes_data.csv'),'rb'))
        errorlog = open(os.path.join(folder,'upload_error_patents.log'),'w')
        current_exist = {}
        for m in uspc_full:
            try:
                gg = patnums[m[0]]
                current_exist[m[0]] = 1
                towrite = [re.sub('"',"'",item) for item in m]
                towrite.insert(0,id_generator())
                towrite[1] = gg
                for t in range(len(towrite)):
                    try:
                        gg = int(towrite[t])
                        towrite[t] = str(int(towrite[t]))
                    except:
                        pass
                towrite[3] = towrite[2]+'/'+re.sub('^0+','',towrite[3])
                try:
                    query = """insert into """+patdb+""".uspc_current values ("""+'"'+'","'.join(towrite)+'")'
                    query = query.replace(',"NULL"',",NULL")
                    cursor.execute(query)
                except:
                    if int(towrite[-1]) == 0:
                        query = """insert into """+patdb+""".mainclass_current values ("""+'"'+towrite[2]+'",NULL)'
                        cursor.execute(query)
                        query = """insert into """+patdb+""".subclass_current values ("""+'"'+towrite[3]+'",NULL)'
                        cursor.execute(query)
                        query = """insert into """+patdb+""".uspc_current values ("""+'"'+'","'.join(towrite)+'")'
                        query = query.replace(',"NULL"',",NULL")
                        cursor.execute(query)
                    else:
                        print>>errorlog,' '.join(towrite+[m[0]])
            except:
                pass
        
        for k in patnums.keys():
            try:
                gg = current_exist[k]
            except:
                cursor.execute('select * from '+patdb+'.uspc where patent_id ="'+str(k)+'"')
                datum = cursor.fetchall()
                for d in datum:
                    cursor.execute('select * from '+patdb+'.mainclass_current where id = "'+d[2]+'"')
                    if len(cursor.fetchall()) == 0:
                        cursor.execute('insert into '+patdb+'.mainclass_current values ("'+d[2]+'",NULL)')
                    cursor.execute('select * from '+patdb+'.subclass_current where id = "'+d[3]+'"')
                    if len(cursor.fetchall()) == 0:
                        cursor.execute('insert into '+patdb+'.subclass_current values ("'+d[3]+'",NULL)')
                    query = "insert into "+patdb+".uspc_current values ("+'"'+'","'.join([str(dd) for dd in d])+'")'
                    query = query.replace(',"NULL"',",NULL")
                    cursor.execute(query)
                        
                
        errorlog.close()
        mydb.commit()
    
    if appdb:
        # Get all application numbers in the current database not to upload full USPC table going back to 19th century
        cursor.execute('select id,number from '+appdb+'.application')
        patnums = {}
        for field in cursor.fetchall():
            patnums[field[0]] = field[1]
        #Create USPC table off full master classification list
        uspc_full = csv.reader(file(os.path.join(folder,'USPC_application_classes_data.csv'),'rb'))
        errorlog = open(os.path.join(folder,'upload_error_apps.log'),'w')
        current_exist = {}
        for m in uspc_full:
            try:
                gg = patnums[m[0]]
                current_exist[m[0]] = 1
                towrite = [re.sub('"',"'",item) for item in m]
                towrite.insert(0,id_generator())
                towrite[1] = gg
                for t in range(len(towrite)):
                    try:
                        gg = int(towrite[t])
                        towrite[t] = str(int(towrite[t]))
                    except:
                        pass
                towrite[3] = towrite[2]+'/'+re.sub('^0+','',towrite[3])
                towrite[1] = towrite[1][:4]+'/'+towrite[1] 
                try:
                    query = """insert into """+appdb+""".uspc_current values ("""+'"'+'","'.join(towrite)+'")'
                    query = query.replace(',"NULL"',",NULL")
                    cursor.execute(query)
                except:
                    if int(towrite[-1]) == 0:
                        query = """insert into """+appdb+""".mainclass_current values ("""+'"'+towrite[2]+'",NULL)'
                        cursor.execute(query)
                        query = """insert into """+appdb+""".subclass_current values ("""+'"'+towrite[3]+'",NULL)'
                        cursor.execute(query)
                        query = """insert into """+appdb+""".uspc_current values ("""+'"'+'","'.join(towrite)+'")'
                        query = query.replace(',"NULL"',",NULL")
                        cursor.execute(query)
                    else:
                        print>>errorlog,' '.join(towrite+[m[0]])
            except:
                pass
        
        for k in patnums.keys():
            try:
                gg = current_exist[k]
            except:
                cursor.execute('select * from '+appdb+'.uspc where application_id ="'+str(k)+'"')
                datum = cursor.fetchall()
                for d in datum:
                    cursor.execute('select * from '+appdb+'.mainclass_current where id = "'+d[2]+'"')
                    if len(cursor.fetchall()) == 0:
                        cursor.execute('insert into '+appdb+'.mainclass_current values ("'+d[2]+'",NULL)')
                    cursor.execute('select * from '+appdb+'.subclass_current where id = "'+d[3]+'"')
                    if len(cursor.fetchall()) == 0:
                        cursor.execute('insert into '+appdb+'.subclass_current values ("'+d[3]+'",NULL)')
                    query = """insert into """+appdb+""".uspc_current values ("""+'"'+'","'.join([str(dd) for dd in d])+'")'
                    query = query.replace(',"NULL"',",NULL")
                    cursor.execute(query)
                        
                
        errorlog.close()
        mydb.commit()
        

def upload_cpc(host,username,password,appdb,patdb,folder):
    from zipfile import ZipFile
    
    import mechanize,os
    br = mechanize.Browser()
    
    files = ['applications_classes.csv','grants_classes.csv','cpc_subsection.csv','cpc_group.csv','cpc_subgroup.csv']
    for f in files:
        url = 'http://www.dev.patentsview.org/data/'+f
        br.retrieve(url,os.path.join(folder,f))
    
    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password)
    cursor = mydb.cursor()
    
    dbnames = [appdb,patdb]
    for n in range(2):
        d = dbnames[n]
        if d:
            if n == 0:
                idname = 'application'
            else:
                idname = 'patent'
            

            #Dump mainclass_current, subclass_current and uspc_current if they exist - WILL NEED THIS WHEN UPDATING THE CLASSIFICATION SCHEMA
            cursor.execute('SET FOREIGN_KEY_CHECKS=0')
            cursor.execute('truncate table '+d+'.cpc_subsection')
            cursor.execute('truncate table '+d+'.cpc_group')
            cursor.execute('truncate table '+d+'.cpc_subgroup')
            cursor.execute('truncate table '+d+'.cpc_current')
            cursor.execute('set foreign_key_checks=1')
            mydb.commit() 
    
    #Upload CPC subsection data
    mainclass = csv.reader(file(os.path.join(folder,'cpc_subsection.csv'),'rb'))
    for m in mainclass:
        towrite = [re.sub('"',"'",item) for item in m]
        query = """insert into cpc_subsection values ("""+'"'+'","'.join(towrite)+'")'
        query = query.replace(',"NULL"',",NULL")
        try:
            query2 = query.replace("cpc_subsection",appdb+'.cpc_subsection')
            cursor.execute(query2)
        except:
            pass
        try:
            query2 = query.replace("cpc_subsection",patdb+'.cpc_subsection')
            cursor.execute(query2)
        except:
            pass
    mydb.commit()

    #Upload CPC group data
    subclass = csv.reader(file(os.path.join(folder,'cpc_group.csv'),'rb'))
    exist = {}
    for m in subclass:
        towrite = [re.sub('"',"'",item) for item in m]
        try:
            gg = exist[towrite[0]]
        except:
            exist[towrite[0]] = 1
            query = """insert into cpc_group values ("""+'"'+'","'.join(towrite)+'")'
            query = query.replace(',"NULL"',",NULL")
            try:
                query2 = query.replace("cpc_group",appdb+'.cpc_group')
                cursor.execute(query2)
            except:
                pass
            try:
                query2 = query.replace("cpc_group",patdb+'.cpc_group')
                cursor.execute(query2)
            except:
                pass
            
    mydb.commit()
                
    #Upload CPC subgroup data
    subclass = csv.reader(file(os.path.join(folder,'cpc_subgroup.csv'),'rb'))
    exist = {}
    for m in subclass:
        towrite = [re.sub('"',"'",item) for item in m]
        try:
            gg = exist[towrite[0]]
        except:
            exist[towrite[0]] = 1
            query = """insert into cpc_subgroup values ("""+'"'+'","'.join(towrite)+'")'
            query = query.replace(',"NULL"',",NULL")
            try:
                query2 = query.replace("cpc_subgroup",appdb+'.cpc_subgroup')
                cursor.execute(query2)
            except:
                pass
            try:
                query2 = query.replace("cpc_subgroup",patdb+'.cpc_subgroup')
                cursor.execute(query2)
            except:
                pass
            
    mydb.commit()
    
    
    if patdb:
        # Get all patent numbers in the current database not to upload full CPC table going back to 19th century
        cursor.execute('select id,number from '+patdb+'.patent')
        patnums = {}
        for field in cursor.fetchall():
            patnums[field[1]] = field[0]
        
        #Create CPC_current table off full master classification list
        uspc_full = csv.reader(file(os.path.join(folder,'grants_classes.csv'),'rb'),delimiter = '\t')
        uspc_full.next()
        errorlog = open(os.path.join(folder,'upload_error_patents.log'),'w')
        current_exist = {}
        for nnn in range(10000000):
            try:
                m = uspc_full.next()
                good = None
                try:
                    gg = patnums[m[0]]
                    good = 1
                except:
                    pass
                if good:
                    current_exist[m[0]] = 1
                    towrite = [re.sub('"',"'",item) for item in m[:3]]
                    towrite.insert(0,id_generator())
                    towrite[1] = gg
                    for t in range(len(towrite)):
                        try:
                            gg = int(towrite[t])
                            towrite[t] = str(int(towrite[t]))
                        except:
                            pass
                    primaries = towrite[2].split("; ")
                    cpcnum = 0
                    for p in primaries:
                        try:
                            needed = [id_generator(),towrite[1]]+[p[0],p[:3],p[:4],p,'primary',str(cpcnum)]
                            query = """insert into """+patdb+""".cpc_current values ("""+'"'+'","'.join(needed)+'")'
                            query = query.replace(',"NULL"',",NULL")
                            cursor.execute(query)
                            cpcnum+=1
                        except:
                            print>>errorlog,p+'\t'+' '.join(towrite)+'\t'+m[0]
                    additionals = [t for t in towrite[3].split('; ') if t!= '']
                    for p in additionals:
                        try:
                            needed = [id_generator(),towrite[1]]+[p[0],p[:3],p[:4],p,'additional',str(cpcnum)]
                            query = """insert into """+patdb+""".cpc_current values ("""+'"'+'","'.join(needed)+'")'
                            query = query.replace(',"NULL"',",NULL")
                            cursor.execute(query)
                            cpcnum+=1
                        except:
                            print>>errorlog,p+'\t'+' '.join(towrite)+'\t'+m[0]
            
            except:
                pass
            
        
        errorlog.close()
        mydb.commit()
    
    if appdb:
        # Get all application numbers in the current database not to upload full USPC table going back to 19th century
        cursor.execute('select id,number from '+appdb+'.application')
        patnums = {}
        for field in cursor.fetchall():
            patnums[field[0]] = field[1]
        
        #Create USPC table off full master classification list
        uspc_full = csv.reader(file(os.path.join(folder,'applications_classes.csv'),'rb'),delimiter = '\t')
        errorlog = open(os.path.join(folder,'upload_error_apps.log'),'w')
        current_exist = {}
        for m in uspc_full:
            try:
                gg = patnums[m[0]]
                current_exist[m[0]] = 1
                towrite = [re.sub('"',"'",item) for item in m[:3]]
                towrite.insert(0,id_generator())
                towrite[1] = m[0]
                for t in range(len(towrite)):
                    try:
                        gg = int(towrite[t])
                        towrite[t] = str(int(towrite[t]))
                    except:
                        pass
                primaries = towrite[2].split("; ")
                cpcnum = 0
                for p in primaries:
                    try:
                        needed = [id_generator(),towrite[1]]+[p[0],p[:3],p[:4],p,'primary',str(cpcnum)]
                        query = """insert into """+appdb+""".cpc_current values ("""+'"'+'","'.join(needed)+'")'
                        query = query.replace(',"NULL"',",NULL")
                        cursor.execute(query)
                        cpcnum+=1
                    except:
                        print>>errorlog,p+'\t'+' '.join(towrite)+'\t'+m[0]
            
                additionals = [t for t in towrite[3].split('; ') if t!='']
                for p in additionals:
                    try:
                        needed = [id_generator(),towrite[1]]+[p[0],p[:3],p[:4],p,'additional',str(cpcnum)]
                        query = """insert into """+appdb+""".cpc_current values ("""+'"'+'","'.join(needed)+'")'
                        query = query.replace(',"NULL"',",NULL")
                        cursor.execute(query)
                        cpcnum+=1
                    except:
                        print>>errorlog,p+'\t'+' '.join(towrite)+'\t'+m[0]
            
            except:
                pass
            
        errorlog.close()
        mydb.commit()
        
def upload_nber(host,username,password,patdb,folder):
    import mechanize,os
    br = mechanize.Browser()
    
    files = ['nber_classes.csv','patent_nber_all.csv']
    for f in files:
        url = 'http://cssip.org/docs/'+f
        br.retrieve(url,os.path.join(folder,f))
    
    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password,
        db=patdb)
    cursor = mydb.cursor()
    
    
    idname = 'patent'
    cursor.execute('SHOW TABLES')
    tables = [f[0] for f in cursor.fetchall()]
    #Dump NBER tables data if populated - WILL NEED THIS WHEN UPDATING THE CLASSIFICATION SCHEMA
    cursor.execute('SET FOREIGN_KEY_CHECKS=0')
    cursor.execute('truncate table '+patdb+'.nber_category')
    cursor.execute('truncate table '+patdb+'.nber_subcategory')
    cursor.execute('truncate table '+patdb+'.nber')
    cursor.execute('set foreign_key_checks=1')
    mydb.commit() 

    #Upload NBER category and subcategory data
    mainclass = csv.reader(file(os.path.join(folder,'nber_classes.csv'),'rb'))
    mainclass.next()
    for m in mainclass:
        towrite = [re.sub('"',"'",item) for item in m]
        #category
        try:
            query = """insert into nber_category values ("""+'"'+'","'.join(towrite[:2])+'")'
            query = query.replace(',"NULL"',",NULL")
            query2 = query.replace("nber_category",patdb+'.nber_category')
            cursor.execute(query2)
        except:
            pass
    
        #subcategory
        query = """insert into nber_subcategory values ("""+'"'+'","'.join(towrite[2:])+'")'
        query = query.replace(',"NULL"',",NULL")
        query2 = query.replace("nber_subcategory",patdb+'.nber_subcategory')
        cursor.execute(query2)
    
    mydb.commit()
    
    # Get all patent numbers in the current database not to upload full NBER table going back to 19th century
    cursor.execute('select id,number from '+patdb+'.patent')
    patnums = {}
    for field in cursor.fetchall():
        patnums[field[1]] = field[0]
    
    #Create NBER table off full master classification list
    uspc_full = csv.reader(file(os.path.join(folder,'patent_nber_all.csv'),'rb'))
    uspc_full.next()
    errorlog = open(os.path.join(folder,'upload_error_patents.log'),'w')
    num = 0
    check = range(1000,10000000,1000)
    for u in uspc_full:
        try:
            gg = patnums[u[1]]
            num+=1
            try:
                query = """insert into """+patdb+""".nber values ("""+'"'+'","'.join(u)+'")'
                cursor.execute(query)
            except:
                cursor.execute('insert into '+patdb+'.nber_category values ("'+u[2]+'","Unclassified")')
                cursor.execute('insert into '+patdb+'.nber_subcategory values ("'+u[3]+'","Unclassified")')
                query = """insert into """+patdb+""".nber values ("""+'"'+'","'.join(u)+'")'
                cursor.execute(query)
            if num in check:
                mydb.commit()
        except:
            pass
    
    errorlog.close()
    mydb.commit()
