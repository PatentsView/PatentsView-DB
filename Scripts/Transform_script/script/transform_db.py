def transform(folder,host,username,password,appdb,patdb):
    import MySQLdb,re
    
    import mechanize,os,csv
    br = mechanize.Browser()
    files = ['application_correct.csv']
    for f in files:
        url = 'http://www.dev.patentsview.org/data/'+f
        br.retrieve(url,os.path.join(folder,f))
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password)
    cursor = mydb.cursor()
    
    if patdb:
        ### Correct lawyer tables ###
        tables = [patdb+'.lawyer',patdb+'.rawlawyer']
        for t in tables:
            # Get columns
            cursor.execute('SHOW columns from '+t)
            raw = [f[:2] for f in cursor.fetchall() if f[0]!='sequence']
            #Change blanks and UNKNOWN to NULL
            for ro in raw:
                r = ro[0]
                cursor.execute('select country from '+t+' where '+r+' = "" or '+r+'="UNKNOWN" limit 10')
                if len(cursor.fetchall()) > 0:
                    try:
                        cursor.execute('alter table '+t+' add column '+r+'_transformed '+ro[1])
                    except:
                        pass
                    cursor.execute('update '+t+' set '+r+'_transformed = '+r)
                    cursor.execute('update '+t+' set '+r+'_transformed = NULL where '+r+'="" OR '+r+'="UNKNOWN"')
            mydb.commit()
        
        ### Change SIR to statutory invention registration in patent type ###
        cursor.execute('update '+patdb+'.patent set type = "statutory invention registration" where type = "SIR"')
        mydb.commit()
        
        ### Change country codes with X in rawlocation ###
        try:
            cursor.execute('alter table '+patdb+'.rawlocation add column country_transformed varchar(10)')
        except:
            pass
        cursor.execute('update '+patdb+'.rawlocation set country_transformed = country')
        cursor.execute('update '+patdb+'.rawlocation set country_transformed = NULL where country = "" OR country = "unknown"')
        cursor.execute('update '+patdb+'.rawlocation set country_transformed = left(country,2) where length(country) >= 3')
        mydb.commit()
        
        ### Update application numbers for broken records ###
        inp = csv.reader(file(os.path.join(folder,'application_correct.csv'),'rb'))
        inp.next()
        try:
            cursor.execute('alter table '+patdb+'.application add column id_transformed varchar(36), add column number_transformed varchar(64), add column type_transformed varchar(20)')
            mydb.commit()
        except:
            pass
        cursor.execute('update '+patdb+'.application set id_transformed = id,number_transformed = number,type_transformed=type')
        mydb.commit()
        for i in inp:
            if i[2] == 'D':
                cursor.execute('update '+patdb+'.application set id_transformed="'+i[1]+'",number_transformed="'+i[1]+'",type_transformed="D" where patent_id = "'+i[0]+'"')
            else:
                cursor.execute('update '+patdb+'.application set id_transformed="'+i[1][:2]+'/'+i[1][2:]+'",number_transformed="'+i[1]+'",type_transformed="'+i[2]+'" where patent_id = "'+i[0]+'"')
        mydb.commit()
        
        ### Transform application_id and number in usapplicationcitation table ###
        try:
            cursor.execute('alter table '+patdb+'.usapplicationcitation add column application_id_transformed varchar(36), add column number_transformed varchar(64)')
            mydb.commit()
        except:
            pass
        cursor.execute('select application_id,number from '+patdb+'.usapplicationcitation')
        fields = list(cursor.fetchall())
        for f in fields:
            idnum = f[0][:5]+f[0][:4]+f[0][5:]
            numb = f[1].replace('/','')
            cursor.execute('update '+patdb+'.usapplicationcitation set application_id_transformed = "'+idnum+'",number_transformed = "'+numb+'"')
        mydb.commit()
    if appdb:
        ### Change country codes with X in rawlocation ###
        try:
            cursor.execute('alter table '+appdb+'.rawlocation add column country_transformed varchar(10)')
        except:
            pass
        cursor.execute('update '+appdb+'.rawlocation set country_transformed = country')
        cursor.execute('update '+appdb+'.rawlocation set country_transformed = NULL where country = "" OR country = "unknown"')
        cursor.execute('select id,country from '+appdb+'.rawlocation where length(country)>=3')
        fields = cursor.fetchall()
        for f in fields:
            countries  = set([re.sub('\s','',i) for i in f[1].split(" ") if re.sub('\s','',i) != ''])
            cursor.execute('update '+appdb+'.rawlocation set country_transformed = "'+list(countries)[0]+'" where id = "'+f[0]+'"')
        mydb.commit()

def update_appnums(folder,host,username,password,appdb): 
    import MySQLdb,re,datetime
    
    import mechanize,os,csv
    br = mechanize.Browser()
    files = ['appnums_correct_grant.csv']
    
    for f in files:
        url = 'http://www.dev.patentsview.org/data/'+f
        br.retrieve(url,os.path.join(folder,f))
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password)
    cursor = mydb.cursor()
    
    cursor.execute('select * from '+appdb+'.application')
    nums = [list(f) for f in cursor.fetchall()]
    inp = csv.reader(file(os.path.join(folder,files[0]),'rb'))
    outp = open(os.path.join(folder,'appnums_tmp.csv'),'wb')
    tmp = csv.writer(outp,delimiter='\t')
    data = {}
    for i in inp:
        data[i[0]] = i
    for n in nums:
        gg = data[n[2]]
        tmp.writerow(n[:7]+[gg[-1]]+n[8:]+[gg[-2]])
    outp.close()
    cursor.execute('load data local infile "'+folder+'/appnums_tmp.csv'+'" into table '+appdb+'.application_update fields optionally enclosed by '+"'"+'"'+"'"+' terminated by "\t" lines terminated by "\n"')
    mydb.commit()
    
