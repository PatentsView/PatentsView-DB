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
    
    #Add filename column to patent table
    try:
        cursor.execute('ALTER TABLE patent ADD filename varchar(120) DEFAULT NULL')
        mydb.commit()
    except:
        pass
    
    #Change column length for type in patent table
    try:
        cursor.execute('ALTER TABLE patent MODIFY type varchar(100)')
        mydb.commit()
    except:
        pass
    
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
    
    diri = ['patent.csv','rawlocation.csv','rawinventor.csv']
    for d in diri:
        print d
        infile = codecs.open(os.path.join(folder,d),'rb',encoding='utf-8').read().split('\r\n')
        #head = infile.next()
        head = infile[0].split('\t')
        del infile[0]
        del infile[-1]
        nullid = None
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
            #i = i.encode('utf-8','ignore')
            i = i.split('\t')
            towrite = [item.replace('"','') for item in i]
            towrite = [w.replace("'",'') for w in towrite]
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
                                    query = "insert into "+d.replace('.csv','')+" values ('"+"','".join(towrite)+"')"
                                    query = query.replace(",'NULL'",",NULL")
                                    cursor.execute(query)
                            else:
                                query = "insert into "+d.replace('.csv','')+" values ('"+"','".join(towrite)+"')"
                                query = query.replace(",'NULL'",",NULL")
                                cursor.execute(query)
    
        for v in mergersdata.values():
            query = "insert into "+d.replace('.csv','')+" values ('"+"','".join(towrite)+"')"
            query = query.replace(",'NULL'",",NULL")
            cursor.execute(query)
        
        for v in duplicdata.values():
            query = "insert into "+d.replace('.csv','')+" values ('"+"','".join(towrite)+"')"
            query = query.replace(",'NULL'",",NULL")
            cursor.execute(query)

    mydb.commit()

    print "ALL GOOD"


def upload_uspc(host,username,password,dbname,folder):
    
    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password,
        db=dbname)
    cursor = mydb.cursor()
    
    #Create tables for current classification if they do not exist - mainclass_current, subclass_current and uspc_current
    cursor.execute("""
    -- Dumping structure for table PatentsProcessorGrant.mainclass_current
        CREATE TABLE IF NOT EXISTS `mainclass_current` (
          `id` varchar(20) NOT NULL,
          `title` varchar(256) DEFAULT NULL,
          `text` varchar(256) DEFAULT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;""")
    mydb.commit()
    cursor.execute("""
    -- Dumping structure for table PatentsProcessorGrant.subclass_current
        CREATE TABLE IF NOT EXISTS `subclass_current` (
          `id` varchar(20) NOT NULL,
          `title` varchar(256) DEFAULT NULL,
          `text` varchar(256) DEFAULT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """)
    mydb.commit()
    cursor.execute("""
    -- Dumping structure for table PatentsProcessorGrant.uspc_current
        CREATE TABLE IF NOT EXISTS `uspc_current` (
          `uuid` varchar(36) NOT NULL,
          `patent_id` varchar(20) DEFAULT NULL,
          `mainclass_id` varchar(20) DEFAULT NULL,
          `subclass_id` varchar(20) DEFAULT NULL,
          `sequence` int(11) DEFAULT NULL,
          PRIMARY KEY (`uuid`),
          KEY `patent_id` (`patent_id`),
          KEY `mainclass_id` (`mainclass_id`),
          KEY `subclass_id` (`subclass_id`),
          KEY `ix_uspc_sequence` (`sequence`),
          CONSTRAINT `uspc_current_ibfk_1` FOREIGN KEY (`patent_id`) REFERENCES `patent` (`id`),
          CONSTRAINT `uspc_current_ibfk_2` FOREIGN KEY (`mainclass_id`) REFERENCES `mainclass_current` (`id`),
          CONSTRAINT `uspc_current_ibfk_3` FOREIGN KEY (`subclass_id`) REFERENCES `subclass_current` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """)
    
    mydb.commit()
    
    #Dump mainclass_current, subclass_current and uspc_current if they exist - WILL NEED THIS WHEN UPDATING THE CLASSIFICATION SCHEMA
    cursor.execute('select id from mainclass_current')
    ids = [f[0] for f in cursor.fetchall()]
    for i in ids:
        cursor.execute('DELETE FROM mainclass_current where id="'+i+'"')
    cursor.execute('select id from subclass_current')
    ids = [f[0] for f in cursor.fetchall()]
    for i in ids:
        cursor.execute('DELETE FROM subclass_current where id="'+i+'"')
    cursor.execute('select uuid from uspc_current')
    ids = [f[0] for f in cursor.fetchall()]
    for i in ids:
        cursor.execute('DELETE FROM uspc_current where uuid="'+i+'"')
    mydb.commit()
    
    #Upload mainclass data
    mainclass = csv.reader(file(os.path.join(folder,'mainclass.csv'),'rb'))
    for m in mainclass:
        towrite = [re.sub('"','',item) for item in m]
        towrite = [re.sub("'",'',w) for w in towrite]
        query = "insert into mainclass_current values ('"+"','".join(towrite)+"')"
        query = query.replace(",'NULL'",",NULL")
        cursor.execute(query)
 
    #Upload subclass data
    subclass = csv.reader(file(os.path.join(folder,'subclass.csv'),'rb'))
    exist = {}
    for m in subclass:
        towrite = [re.sub('"','',item) for item in m]
        towrite = [re.sub("'",'',w) for w in towrite]
        try:
            gg = exist[towrite[0]]
        except:
            exist[towrite[0]] = 1
            query = "insert into subclass_current values ('"+"','".join(towrite)+"')"
            query = query.replace(",'NULL'",",NULL")
            cursor.execute(query)
        
    mydb.commit()
    
    # Get all patent numbers in the current database not to upload full USPC table going back to 19th century
    cursor.execute('select id,number from patent')
    patnums = {}
    for field in cursor.fetchall():
        patnums[field[1]] = field[0]
    
    #Create USPC table off full master classification list
    uspc_full = csv.reader(file(os.path.join(folder,'USPC_patent_classes_data.csv'),'rb'))
    errorlog = open(os.path.join(folder,'upload_error.log'),'w')
    for m in uspc_full:
        try:
            gg = patnums[m[0]]
            towrite = [re.sub('"','',item) for item in m]
            towrite = [re.sub("'",'',w) for w in towrite]
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
                query = "insert into uspc_current values ('"+"','".join(towrite)+"')"
                query = query.replace(",'NULL'",",NULL")
                cursor.execute(query)
            except:
                print>>errorlog,' '.join(towrite+[m[0]])
        except:
            pass
    
    errorlog.close()
    mydb.commit()    