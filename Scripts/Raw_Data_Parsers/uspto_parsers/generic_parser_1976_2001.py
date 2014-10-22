def parse_patents(fd,fd2):
    import re,csv,os
    import string,random
    
    
    
    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    
    type_kind = {'1': ["A","utility"],
                 '2': ["E","reissue"],
                 '3': ["I5","TVPP"],
                 '4': ["S","design"],
                 '5': ["I4","defensive publication"],
                 '6': ["P","plant"],
                 '7': ["H","statutory invention registration"]
                 }    
    
    
    fd+='/'
    fd2+='/'
    diri = os.listdir(fd)
    diri = [d for d in diri if d.endswith('txt')]

    #Remove all files from output dir before writing
    outdir = os.listdir(fd2)
    for oo in outdir:
        os.remove(os.path.join(fd2,oo))

    #Rewrite files and write headers to them
    appfile = open(os.path.join(fd2,'application.csv'),'wb')
    app = csv.writer(appfile)
    app.writerow(['id','patent_id','type','number','country','date'])
    
    claimsfile = open(os.path.join(fd2,'claim.csv'),'wb')
    clms = csv.writer(claimsfile)
    clms.writerow(['uuid','patent_id','text','dependent','sequence'])
    
    rawlocfile = open(os.path.join(fd2,'rawlocation.csv'),'wb')
    rawloc = csv.writer(rawlocfile)
    rawloc.writerow(['id','location_id','city','state','country'])
    
    rawinvfile = open(os.path.join(fd2,'rawinventor.csv'),'wb')
    rawinv = csv.writer(rawinvfile)
    rawinv.writerow(['uuid','patent_id','inventor_id','rawlocation_id','name_first','name_last','sequence'])
    
    rawassgfile = open(os.path.join(fd2,'rawassignee.csv'),'wb')
    rawassg = csv.writer(rawassgfile)
    rawassg.writerow(['uuid','patent_id','assignee_id','rawlocation_id','type','name_first','name_last','organization','residence','nationality','sequence'])
    
    ipcrfile = open(os.path.join(fd2,'ipcr.csv'),'wb')
    ipcr = csv.writer(ipcrfile)
    ipcr.writerow(['uuid','patent_id','classification_level','section','subclass','main_group','subgroup','symbol_position','classification_value','classification_status','classification_data_source','action_date','ipc_version_indicator','sequence'])
    
    patfile = open(os.path.join(fd2,'patent.csv'),'wb')
    pat = csv.writer(patfile)
    pat.writerow(['id','type','number','country','date','abstract','title','kind','num_claims'])
    
    uspatentcitfile = open(os.path.join(fd2,'uspatentcitation.csv'),'wb')
    uspatcit = csv.writer(uspatentcitfile)
    uspatcit.writerow(['uuid','patent_id','citation_id','date','name','kind','number','country','category','sequence'])
    
    foreigncitfile = open(os.path.join(fd2,'foreigncitation.csv'),'wb')
    foreigncit = csv.writer(foreigncitfile)
    foreigncit.writerow(['uuid','patent_id','date','kind','number','country','category','sequence'])
    
    otherreffile = open(os.path.join(fd2,'otherreference.csv'),'wb')
    otherref = csv.writer(otherreffile)
    otherref.writerow(['uuid','patent_id','text','sequence'])
    
    rawlawyerfile = open(os.path.join(fd2,'rawlawyer.csv'),'wb')
    rawlawyer = csv.writer(rawlawyerfile)
    rawlawyer.writerow(['uuid','lawyer_id','patent_id','name_first','name_last','organization','country','sequence'])
    
    uspcfile = open(os.path.join(fd2,'uspc.csv'),'wb')
    uspcc = csv.writer(uspcfile)
    uspcc.writerow(['uuid','patent_id','mainclass_id','subclass_id','sequence'])

    mainclassfile = open(os.path.join(fd2,'mainclass.csv'),'wb')
    mainclass = csv.writer(mainclassfile)
    mainclass.writerow(['id','title','text'])

    subclassfile = open(os.path.join(fd2,'subclass.csv'),'wb')
    subclass = csv.writer(subclassfile)
    subclass.writerow(['id','title','text'])
    
    mainclassfile.close()
    subclassfile.close()
    appfile.close()
    rawlocfile.close()
    rawinvfile.close()
    rawassgfile.close()
    ipcrfile.close()
    otherreffile.close()
    foreigncitfile.close()
    patfile.close()
    rawlawyerfile.close()
    uspatentcitfile.close()
    uspcfile.close()
    claimsfile.close()
    
    loggroups = ['PATN','INVT','ASSG','PRIR','REIS','RLAP','CLAS','UREF','FREF','OREF','LREP','PCTA','ABST','GOVT','PARN','BSUM','DRWD','DETD','CLMS','DCLM']
    
    numii = 0
    rawlocation = {}
    mainclassdata = {}
    subclassdata = {}

    for d in diri:
        print d
        infile = open(os.path.join(fd,d)).read().split('PATN')
        del infile[0]
        for i in infile:
          numii+=1
          try:    
            # Get relevant logical groups from patent records according to documentation
            # Some patents can contain several INVT, ASSG and other logical groups - so, is important to retain all
            avail_fields = {}
            num = 1
            avail_fields['PATN'] = i.split('INVT')[0]
            runnums = []
            for n in range(1,len(loggroups)):
                try:
                    gg = re.search('\n'+loggroups[n],i).group()
                    if num-n == 0:
                        runnums.append(n)
                        num+=1
                        go = list(re.finditer('\n'+loggroups[n-1],i))
                        if len(go) == 1:
                            needed = i.split(loggroups[n-1])[1]
                            avail_fields[loggroups[n-1]] = needed.split(loggroups[n])[0]
                        elif len(go) > 1:
                            needed = '\n\n\n\n\n'.join(i.split(loggroups[n-1])[1:])
                            avail_fields[loggroups[n-1]] = needed.split(loggroups[n])[0]
                        else:
                            pass
                    else:
                        go = list(re.finditer('\n'+loggroups[runnums[-1]],i))
                        if len(go) == 1:
                            needed = i.split(loggroups[runnums[-1]])[1]
                            avail_fields[loggroups[runnums[-1]]] = needed.split(loggroups[n])[0]
                        elif len(go) > 1:
                            needed = '\n\n\n\n\n'.join(i.split(loggroups[runnums[-1]])[1:])
                            avail_fields[loggroups[runnums[-1]]] = needed.split(loggroups[n])[0]
                        else:
                            pass
                        runnums.append(n)
                        num = n+1
                    
                except:
                    pass
            
            # Create containers based on existing Berkeley DB schema (not all are currently used - possible compatibility issues)
            application = {}
            claimsdata = {}
            foreigncitation = {}
            ipcr = {}
            otherreference = {}
            patentdata = {}
            rawassignee = {}
            rawinventor = {}
            rawlawyer = {}
            usappcitation = {}
            uspatentcitation = {}
            uspc = {}
            usreldoc = {}
            
            
            ###                PARSERS FOR LOGICAL GROUPS                  ###
            #PATN
            updnum = 'NULL'
            appnum = 'NULL'
            apptype = 'NULL'
            appdate = 'NULL'
            title = 'NULL'
            issdate = 'NULL'
            numclaims = 'NULL'
            
            try:
                patent = avail_fields['PATN'].split('\n')
                for line in patent:
                    if line.startswith("WKU"):
                        patnum = re.search('WKU\s+(.*?)$',line).group(1)
                        updnum = re.sub('^H0','H',patnum)[:8]
                        updnum = re.sub('^RE0','RE',updnum)[:8]
                        updnum = re.sub('^PP0','PP',updnum)[:8]
                        updnum = re.sub('^PP0','PP',updnum)[:8]
                        updnum = re.sub('^D0', 'D', updnum)[:8]
                        updnum = re.sub('^T0', 'T', updnum)[:8]
                        if len(patnum) > 7 and patnum.startswith('0'):
                            updnum = patnum[1:8]
                        #data['patnum'] = updnum
                        #print updnum
                    if line.startswith('SRC'):
                        seriescode = re.search('SRC\s+(.*?)$',line).group(1)
                        try:
                            gg = int(seriescode)
                            if len(seriescode) == 1:
                                seriescode = '0'+seriescode
                        except:
                            pass
                    if line.startswith('APN'):
                        appnum = re.search('APN\s+(.*?)$',line).group(1)[:6]
                        if len(appnum) != 6:
                            appnum = 'NULL'
                            #data['appnum'] = appnum
                    if line.startswith('APT'):
                        apptype = re.search('APT\s+(.*?)$',line).group(1)
                        apptype = re.search('\d',apptype).group()
                    if line.startswith('APD'):
                        appdate = re.search('APD\s+(.*?)$',line).group(1)
                        appdate = appdate[:4]+'-'+appdate[4:6]+'-'+appdate[6:]
                        #print appdate
                    if line.startswith('TTL'):
                        title = re.search('TTL\s+(.*?)ISD',avail_fields['PATN'],re.DOTALL).group(1)
                        title = re.sub('[\n\t\r\f]+','',title)
                        title = re.sub('\s+$','',title)
                        title = re.sub('\s+',' ',title)
                    if line.startswith('ISD'):
                        issdate = re.search('ISD\s+(.*?)$',line).group(1)
                        if issdate[6:] == "00":
                            day = '01'
                        else:
                            day = issdate[6:]
                        if issdate[4:6] == "00":
                            month = '01'
                        else:
                            month = issdate[4:6]
                        year = issdate[:4]
                        issdate = year+'-'+month+'-'+day
                        #print issdate
                    if line.startswith("NCL"):
                        numclaims = re.search('NCL\s+(.*?)$',line).group(1)
            except:
                pass
            
            patent_id = updnum
            
            if int(appdate[:4]) >= 1992 and seriescode == "D":
                seriescode = "29"
            application[seriescode+'/'+appnum] = [patent_id,seriescode,seriescode+appnum,'US',appdate]
            
            #INVT - can be several
            try:
                inv_info = avail_fields['INVT'].split("\n\n\n\n\n")
                for n in range(len(inv_info)):
                    fname = 'NULL'
                    lname = 'NULL'
                    invtcity = 'NULL'
                    invtstate = 'NULL'
                    invtcountry = 'NULL'
                    invtzip = 'NULL'
                    for line in inv_info[n].split("\n"):
                        if line.startswith("NAM"):
                            invtname = re.search('NAM\s+(.*?)$',line).group(1).split("; ")
                            fname = invtname[1]
                            lname = invtname[0]
                            
                        if line.startswith("CTY"):
                            invtcity = re.search('CTY\s+(.*?)$',line).group(1)
                            
                        if line.startswith("STA"):
                            invtstate = re.search('STA\s+(.*?)$',line).group(1)
                        
                        if line.startswith("CNT"):
                            invtcountry = re.search('CNT\s+(.*?)$',line).group(1)
                            if len(invtcountry) == 3 and invtcountry.endswith('X'):
                                invtcountry = invtcountry[:-1]
                                
                        if line.startswith("ZIP"):
                            invtzip = re.search('ZIP\s+(.*?)$',line).group(1)
                    
                    if invtcountry == "NULL":
                        invtcountry = 'US'
                    if (invtcity+'|'+invtstate+'|'+invtcountry).lower() in rawlocation:
                        loc_idd = rawlocation[(invtcity+'|'+invtstate+'|'+invtcountry).lower()][0]
                    else:
                        loc_idd = id_generator()
                        rawlocation[(invtcity+'|'+invtstate+'|'+invtcountry).lower()] = [loc_idd,"NULL",invtcity,invtstate,invtcountry]
                    rawinventor[id_generator()] = [patent_id,"NULL",loc_idd,fname,lname,str(n)]
            except:
                pass
            
            #ASSG - can be several
            try:
                assg_info = avail_fields['ASSG'].split('\n\n\n\n\n')
                for n in range(len(assg_info)):    
                    assorg = ''
                    assgfname = ''
                    assglname = ''
                    assgcity = 'NULL'
                    assgstate = 'NULL'
                    assgcountry = 'NULL'
                    assgzip = 'NULL'
                    assgtype = 'NULL'
                    for line in assg_info[n].split("\n"):
                        if line.startswith("NAM"):
                            assgname = re.search('NAM\s+(.*?)$',line).group(1).split("; ")
                            if len(assgname) == 1:
                                assgorg = assgname[0]
                                assgfname = ''
                                assglname = ''
                            else:
                                assgfname = assgname[1]
                                assglname = assgname[0]
                                assgorg = ''
                            
                        if line.startswith("CTY"):
                            assgcity = re.search('CTY\s+(.*?)$',line).group(1)
                            
                        if line.startswith("STA"):
                            assgstate = re.search('STA\s+(.*?)$',line).group(1)
                        
                        if line.startswith("CNT"):
                            assgcountry = re.search('CNT\s+(.*?)$',line).group(1)
                            if len(assgcountry) == 3 and assgcountry.endswith('X'):
                                assgcountry = assgcountry[:-1]
                        if line.startswith("ZIP"):
                            assgzip = re.search('ZIP\s+(.*?)$',line).group(1)
                        
                        if line.startswith("COD"):
                            assgtype = re.search("COD\s+(.*?)$",line).group(1)
                
                    if assgcountry == 'NULL':
                        assgcountry = 'US'
                    if (assgcity+'|'+assgstate+'|'+assgcountry).lower() in rawlocation:
                        loc_idd = rawlocation[(assgcity+'|'+assgstate+'|'+assgcountry).lower()][0]
                    else:
                        loc_idd = id_generator()
                        rawlocation[(assgcity+'|'+assgstate+'|'+assgcountry).lower()] = [loc_idd,"NULL",assgcity,assgstate,assgcountry]
                    rawassignee[id_generator()] = [patent_id,"NULL",loc_idd,re.sub('^0+','',assgtype),assgfname,assglname,assgorg,assgcountry,'NULL',str(n)]
            except:
                pass
            
            
            crossclass = 1
            #CLAS - should be several
            try:
                num = 0
                for line in avail_fields['CLAS'].split('\n'):
                    if line.startswith('ICL'):
                        intsec = 'NULL'
                        mainclass = 'NULL'
                        subclass = 'NULL'
                        group = 'NULL'
                        subgroup = 'NULL'
                        
                        intclass = re.search('ICL\s+(.*?)$',line).group(1)
                        if int(year) <= 1984:
                            intsec = intclass[0]
                            mainclass = intclass[1:3]
                            if intsec == "D":
                                subclass = intclass[3:5]
                                group = "NULL"
                                subgroup = "NULL"
                            else:
                                subclass = intclass[3]
                                group = re.sub('^\s+','',intclass[4:7])
                                subgroup = re.sub('^\s+','',intclass[7:])
                        elif int(year) >= 1997 and updnum.startswith("D"):
                            intsec = "D"
                            try:
                                mainclass = intclass[0:2]
                                subclass = intclass[2:]
                                group = 'NULL'
                                subgroup = "NULL"
                            except:
                                mainclass = "NULL"
                                subclass = "NULL"
                                group = "NULL"
                                subgroup = "NULL"
                        else:
                            intsec = intclass[0]
                            mainclass = intclass[1:3]
                            subclass = intclass[3]
                            group = re.sub('^\s+','',intclass[4:7])
                            subgroup = re.sub('^\s+','',intclass[7:])
                   
                        ipcr[id_generator()] = [patent_id,mainclass,intsec,subclass, group,subgroup,"NULL","NULL","NULL","NULL","NULL","NULL",str(num)]
                        num+=1     
                        
                    if line.startswith("OCL"):
                        origmainclass = 'NULL'
                        origsubclass = 'NULL'
                        origclass = re.search('OCL\s+(.*?)$',line).group(1).upper()
                        origmainclass = re.sub('\s+','',origclass[0:3])
                        origsubclass = re.sub('\s+','',origclass[3:])
                        if len(origsubclass) > 3 and re.search('^[A-Z]',origsubclass[3:]) is None:
                            origsubclass = origsubclass[:3]+'.'+origsubclass[3:]
                        origsubclass = re.sub('^0+','',origsubclass)
                        if re.search('[A-Z]{3}',origsubclass[:3]):
                            origsubclass = origsubclass.replace('.','')
                        if origsubclass != "":
                            mainclassdata[origmainclass] = [origmainclass,'NULL','NULL']
                            uspc[id_generator()] = [patent_id,origmainclass,origmainclass+'/'+origsubclass,'0']
                            subclassdata[origmainclass+'/'+origsubclass] = [origmainclass+'/'+origsubclass,'NULL','NULL']
                        
                    if line.startswith("XCL"):
                        crossrefmain = "NULL"
                        crossrefsub = "NULL"
                        crossrefclass = re.search('XCL\s+(.*?)$',line).group(1).upper()
                        crossrefmain = re.sub('\s+','',crossrefclass[:3])
                        crossrefsub = re.sub('\s+','',crossrefclass[3:])
                        if len(crossrefsub) > 3 and re.search('^[A-Z]',crossrefsub[3:]) is None:
                            crossrefsub = crossrefsub[:3]+'.'+crossrefsub[3:]
                        crossrefsub = re.sub('^0+','',crossrefsub)
                        if re.search('[A-Z]{3}',crossrefsub[:3]):
                            crossrefsub = crossrefsub.replace(".","")
                        if crossrefsub != "":
                            mainclassdata[crossrefmain] = [crossrefmain,'NULL','NULL']
                            uspc[id_generator()] = [patent_id,crossrefmain,crossrefmain+'/'+crossrefsub,str(n)]
                            subclassdata[crossrefmain+'/'+crossrefsub] = [crossrefmain+'/'+crossrefsub,'NULL','NULL']
                        crossclass+=1
                        
            except:
                pass
                           
            # U.S. Patent Reference - can be several
            try:
                uspatref = avail_fields['UREF'].split("\n\n\n\n\n")
                for n in range(len(uspatref)):
                    refpatnum = 'NULL'
                    refpatname = 'NULL'
                    refpatdate = 'NULL'
                    refpatclass = 'NULL'
                    for line in uspatref[n].split("\n"):
                        if line.startswith('PNO'):
                            refpatnum = re.search('PNO\s+(.*?)$',line).group(1)
                        
                        if line.startswith('ISD'):
                            refpatdate = re.search('ISD\s+(.*?)$',line).group(1)
                            if refpatdate[6:] == "00":
                                day = '01'
                            else:
                                day = refpatdate[6:]
                            if refpatdate[4:6] == "00":
                                month = '01'
                            else:
                                month = refpatdate[4:6]
                            year = refpatdate[:4]
                            refpatdate = year+'-'+month+'-'+day
                            
                        if line.startswith('NAM'):
                            refpatname = re.search('NAM\s+(.*?)$',line).group(1)
                            
                        if line.startswith('OCL'):
                            refpatclass = re.search('OCL\s+(.*?)$',line).group(1) 
                    uspatentcitation[id_generator()] = [patent_id,refpatnum,refpatdate,"NULL","NULL",refpatnum,'US',"CITATION SOURCE",str(n)]
            except:
                pass
            
            #Foreign reference - can be several
            try:
                foreignref = avail_fields['FREF'].split('\n\n\n\n\n')
                for n in range(len(foreignref)):
                    forrefpatnum = 'NULL'
                    forrefpatdate = 'NULL'
                    forrefpatcountry = 'NULL'
                    forrefpatclass = 'NULL'
                    for line in foreignref[n].split("\n"):
                        if line.startswith('PNO'):
                            forrefpatnum = re.search('PNO\s+(.*?)$',line).group(1)
                        
                        if line.startswith('ISD'):
                            forrefpatdate = re.search('ISD\s+(.*?)$',line).group(1)
                            if forrefpatdate[6:] == "00":
                                day = '01'
                            else:
                                day = forrefpatdate[6:]
                            if forrefpatdate[4:6] == "00":
                                month = '01'
                            else:
                                month = forrefpatdate[4:6]
                            year = forrefpatdate[:4]
                            forrefpatdate = year+'-'+month+'-'+day
                            
                        if line.startswith('CNT'):
                            forrefpatcountry = re.search('CNT\s+(.*?)$',line).group(1)
                            if len(forrefpatcountry) == 3 and forrefpatcountry.endswith('X'):
                                forrefpatcountry = forrefpatcountry[:-1]
                            
                        if line.startswith('ICL'):
                            forrefpatclass = re.search('ICL\s+(.*?)$',line).group(1) 
                    foreigncitation[id_generator()] = [patent_id,forrefpatdate,"NULL",forrefpatnum,forrefpatcountry,"NULL",str(n)] 
            except:
                pass
            
            
            #Other reference - can be several
            try:
                otherreflist = avail_fields['OREF'].split('\n\n\n\n\n')
                for n in range(len(otherreflist)):
                    if re.search('PAL ',otherreflist[n]):
                        allrefs = otherreflist[n].split('PAL ')
                        del allrefs[0]
                        for a in range(len(allrefs)):
                            otherref = 'NULL'
                            otherref = re.sub('\s+',' ',allrefs[a])
                            otherref = re.sub('^\s+[A-Z]+\s+','',otherref)
                            #print otherref
                            otherreference[id_generator()] = [patent_id,otherref,str(a)]
                    else:
                        otherref = 'NULL'
                        otherref = re.sub('\s+',' ',otherreflist[n])
                        otherref = re.sub('^\s+[A-Z]+\s+','',otherref)
                        #print otherref
                        otherreference[id_generator()] = [patent_id,otherref,str(n)]
            except:
                pass
            
            #Legal information - can be several
            try:
                legal_info = avail_fields['LREP'].split("\n\n\n\n\n")
                for n in range(len(legal_info)):
                    legalcountry = 'NULL'
                    legalfirm = 'NULL'
                    attfname = 'NULL'
                    attlname = 'NULL'
                    for line in legal_info[n].split('\n'):
                        if line.startswith("FRM"):
                            legalfirm = re.search('FRM\s+(.*?)$',line).group(1)
                            #print legalfirm
                        if line.startswith("FR2"):
                            attorney = re.search('FR2\s+(.*?)$',line).group(1).split('; ')
                            attfname = attorney[1]
                            attlname = attorney[0]
                            #print attlname
                        if line.startswith('CNT'):
                            legalcountry = re.search('CNT\s+(.*?)$',line).group('; ')
                            if len(legalcountry) == 3 and legalcountry.endswith('X'):
                                legalcountry = legalcountry[:-1]
                                    
                    
                    rawlawyer[id_generator()] = ["NULL",patent_id,attfname,attlname,legalfirm,legalcountry,str(n)]
                        
            except:
                pass
            
            
            # Abstract - can be several
            abst = 'NULL'
            try:
                abst = re.sub('PAL\s+','',avail_fields['ABST'])
                abst = re.sub('PAR\s+','',abst)
                abst = re.sub('TBL\s+','',abst)
                abst = re.sub('\s+',' ',abst)
                
            except:
                pass
            
            patentdata[patent_id] = [type_kind[apptype][1],updnum,'US',issdate,abst,title,type_kind[apptype][0],numclaims,d]
            
            # Claims data parser
            datum = {}
            check = re.search('\nCLMS',i)
            if check:
                try:
                    claims = re.search('CLMS(.*)\nDCLM',i,re.DOTALL).group(1)
                except:    
                    try:
                        claims = re.search('CLMS(.*)\nEOV',i,re.DOTALL).group(1)
                    except:
                        try:
                            claims = re.search('CLMS(.*)\nEOF',i,re.DOTALL).group(1)
                        except:
                            try:
                                claims = re.search('CLMS(.*)',i,re.DOTALL).group(1)
                            except:
                                claims = ''
                clnums = claims.split('NUM  ')
                del clnums[0]
                nnn = []
                if len(clnums) > 1:
                    for c in range(len(clnums)):
                        needed = re.search('(\d+)\.\s(.*)',clnums[c],re.DOTALL)
                        try:
                            number = needed.group(1)
                            text = needed.group(2)
                            text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',text)
                            text = re.sub('[\n\t\r\f]+','',text)
                            text = re.sub('\s+',' ',text)
                            text = re.sub('_+','',text)
                            datum[int(number)] = text
                            nnn.append(int(number))
                        except:
                            text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',clnums[c])
                            text = re.sub('[\n\t\r\f]+','',text)
                            text = re.sub('\s+',' ',text)
                            text = re.sub('_+','',text)
                            try:
                                datum[nnn[-1]]+=text
                            except:
                                pass
                else:
                    try:
                        needed = re.findall('\d+\.',claims)
                        for ne in range(len(needed)-1):
                            number = needed[ne]
                            text = re.search(needed[ne]+'(.*)'+needed[ne+1],claims,re.DOTALL).group(1)
                            text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',text)
                            text = re.sub('[\n\t\r\f]+','',text)
                            text = re.sub('\s+',' ',text)
                            text = re.sub('_+','',text)
                            text = re.sub('^\s+','',text)
                            number = re.sub('\.','',number)
                            datum[int(number)] = text
                            #print text
                        number = needed[-1]
                        text = re.search(number+'(.*)',claims,re.DOTALL).group(1)
                        text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',text)
                        text = re.sub('[\n\t\r\f]+','',text)
                        text = re.sub('\s+',' ',text)
                        text = re.sub('_+','',text)
                        number = re.sub('\.','',number)
                        text = re.sub('^\s+','',text)
                        datum[int(number)] = text
                    except:
                        try:
                            number = '1'
                            text = re.search('STM  (.*)',claims,re.DOTALL).group(1)
                            text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',text)
                            text = re.sub('[\n\t\r\f]+','',text)
                            text = re.sub('\s+',' ',text)
                            text = re.sub('_+','',text)
                            number = re.sub('\.','',number)
                            text = re.sub('^\s+','',text)
                            datum[int(number)] = text
                        except:
                            pass
                
                datum = sorted(datum.items())
                for k,v in datum:
                    claimsdata[id_generator()] = [updnum,re.sub('^\s\d+\.\s','',v),"NULL",str(k)]
                if len(datum) == 0:
                    pass
            else:
                check = re.search('\nDCLM',i)
                if check:
                    try:
                        claims = re.search('DCLM(.*)\nEOV',i,re.DOTALL).group(1)
                    except:    
                        try:
                            claims = re.search('DCLM(.*)\nEOF',i,re.DOTALL).group(1)
                        except:
                            try:
                                claims = re.search('DCLM(.*)',i,re.DOTALL).group(1)
                            except:
                                claims = ''
                        
                    try:
                        text = re.sub('[\n\t\r\f]+','',claims)
                        text = re.sub('^\W+[A-Z]+\s+','',text)
                        text = re.sub('\s+',' ',text)
                    except:
                        pass
                    
                    claimsdata[id_generator()]=[updnum,re.sub('^PAR\s+','',text),"NULL",'1']
                else:
                    pass

            
            patfile = csv.writer(open(os.path.join(fd2,'patent.csv'),'ab'))
            for k,v in patentdata.items():
                patfile.writerow([k]+v)

            claimsfile = csv.writer(open(os.path.join(fd2,'claim.csv'),'ab'))
            for k,v in claimsdata.items():
                claimsfile.writerow([k]+v)            
            
            appfile = csv.writer(open(os.path.join(fd2,'application.csv'),'ab'))
            for k,v in application.items():
                appfile.writerow([k]+v)
            
            rawinvfile = csv.writer(open(os.path.join(fd2,'rawinventor.csv'),'ab'))
            for k,v in rawinventor.items():
                rawinvfile.writerow([k]+v)
            
            rawassgfile = csv.writer(open(os.path.join(fd2,'rawassignee.csv'),'ab'))
            for k,v in rawassignee.items():
                rawassgfile.writerow([k]+v)
            
            ipcrfile = csv.writer(open(os.path.join(fd2,'ipcr.csv'),'ab'))
            for k,v in ipcr.items():
                ipcrfile.writerow([k]+v)
            
            uspcfile = csv.writer(open(os.path.join(fd2,'uspc.csv'),'ab'))
            for k,v in uspc.items():
                uspcfile.writerow([k]+v)
            
            uspatentcitfile = csv.writer(open(os.path.join(fd2,'uspatentcitation.csv'),'ab'))
            for k,v in uspatentcitation.items():
                uspatentcitfile.writerow([k]+v)

            foreigncitfile = csv.writer(open(os.path.join(fd2,'foreigncitation.csv'),'ab'))
            for k,v in foreigncitation.items():
                foreigncitfile.writerow([k]+v)

            otherreffile = csv.writer(open(os.path.join(fd2,'otherreference.csv'),'ab'))
            for k,v in otherreference.items():
                otherreffile.writerow([k]+v)
            
            rawlawyerfile = csv.writer(open(os.path.join(fd2,'rawlawyer.csv'),'ab'))
            for k,v in rawlawyer.items():
                rawlawyerfile.writerow([k]+v)
          except:
              pass
              
    rawlocfile = csv.writer(open(os.path.join(fd2,'rawlocation.csv'),'ab'))
    for k,v in rawlocation.items():
        rawlocfile.writerow(v)
            
    mainclassfile = csv.writer(open(os.path.join(fd2,'mainclass.csv'),'ab'))
    for k,v in mainclassdata.items():
        mainclassfile.writerow(v)
    
    subclassfile = csv.writer(open(os.path.join(fd2,'subclass.csv'),'ab'))
    for k,v in subclassdata.items():
        subclassfile.writerow(v)
            
    print numii

