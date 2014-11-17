import csv
import MySQLdb
import re,os,random,string,codecs


def mysql_upload(host,username,password,dbname,folder):
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
    diri = os.listdir(folder)
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
                                    query = """insert into """+d.replace('.csv','')+""" values ("""+'"'+'","'.join(towrite)+'")'
                                    query = query.replace(',"NULL"',",NULL")
                                    cursor.execute(query)
                            else:
                                query = """insert into """+d.replace('.csv','')+""" values ("""+'"'+'","'.join(towrite)+'")'
                                query = query.replace(',"NULL"',",NULL")
                                cursor.execute(query)
    
        for v in mergersdata.values():
            query = """insert into """+d.replace('.csv','')+""" values ("""+'"'+'","'.join(v)+'")'
            query = query.replace(',"NULL"',",NULL")
            cursor.execute(query)
        
        for v in duplicdata.values():
            query = """insert into """+d.replace('.csv','')+""" values ("""+'"'+'","'.join(v)+'")'
            query = query.replace(',"NULL"',",NULL")
            cursor.execute(query)

    mydb.commit()

    print "ALL GOOD"


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
                
    #mydb.commit()
    
    if patdb:
        # Get all patent numbers in the current database not to upload full USPC table going back to 19th century
        cursor.execute('select id,number from '+patdb+'.patent')
        patnums = {}
        for field in cursor.fetchall():
            patnums[field[1]] = field[0]
        
        #Create USPC table off full master classification list
        uspc_full = csv.reader(file(os.path.join(folder,'USPC_patent_classes_data.csv'),'rb'))
        errorlog = open(os.path.join(folder,'upload_error.log'),'w')
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
        errorlog = open(os.path.join(folder,'upload_error.log'),'w')
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
                        cursor.execute('insert into '+appdb+'.mainclass_current values ("'+d[2]+'",NULL,NULL)')
                    cursor.execute('select * from '+appdb+'.subclass_current where id = "'+d[3]+'"')
                    if len(cursor.fetchall()) == 0:
                        cursor.execute('insert into '+appdb+'.subclass_current values ("'+d[3]+'",NULL,NULL)')
                    query = """insert into """+appdb+""".uspc_current values ("""+'"'+'","'.join([str(dd) for dd in d])+'")'
                    query = query.replace(',"NULL"',",NULL")
                    cursor.execute(query)
                        
                
        errorlog.close()
        mydb.commit()
        
            
