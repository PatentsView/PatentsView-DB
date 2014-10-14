def parse_patents(fd,fd2):
    import re,csv,os
    import string,random
    from bs4 import BeautifulSoup as bs
    
    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    fd+='/'
    fd2+='/'
    diri = os.listdir(fd)
    diri = [d for d in diri if re.search('XML',d,re.I)]
    
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
    
    foreigncitfile = open(os.path.join(fd2,'foreigncitation.csv'),'wb')
    foreigncit = csv.writer(foreigncitfile)
    foreigncit.writerow(['uuid','patent_id','date','kind','number','country','category','sequence'])
    
    uspatentcitfile = open(os.path.join(fd2,'uspatentcitation.csv'),'wb')
    uspatcit = csv.writer(uspatentcitfile)
    uspatcit.writerow(['uuid','patent_id','citation_id','date','name','kind','number','country','category','sequence'])
    """
    usappcitfile = open(os.path.join(fd2,'usapplicationcitation.csv'),'wb')
    usappcit = csv.writer(usappcitfile)
    usappcit.writerow(['uuid','patent_id','application_id','date','name','kind','number','country','category','sequence'])
    """
    uspcfile = open(os.path.join(fd2,'uspc.csv'),'wb')
    uspcc = csv.writer(uspcfile)
    uspcc.writerow(['uuid','patent_id','mainclass_id','subclass_id','sequence'])

    otherreffile = open(os.path.join(fd2,'otherreference.csv'),'wb')
    otherref = csv.writer(otherreffile)
    otherref.writerow(['uuid','patent_id','text','sequence'])
    
    rawlawyerfile = open(os.path.join(fd2,'rawlawyer.csv'),'wb')
    rawlawyer = csv.writer(rawlawyerfile)
    rawlawyer.writerow(['uuid','lawyer_id','patent_id','name_first','name_last','organization','country','sequence'])

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
    patfile.close()
    rawlawyerfile.close()
    uspatentcitfile.close()
    uspcfile.close()
    foreigncitfile.close()
    claimsfile.close()
    #usappcitfile.close()
    
    
    #Type kind crosswalk - lookup table
    type_kind = {
                "A": 'utility',   #Utility Patent issued prior to January 2, 2001. 
                "A1": 'utility', #Utility Patent Application published on or after January 2, 2001. 
                "A2": 'utility', #Second or subsequent publication of a Utility Patent Application. 
                'A9': 'utility', #Corrected published Utility Patent Application. 
                'Bn': 'reexamination certificate', #Reexamination Certificate issued prior to January 2, 2001. NOTE: "n" represents a value 1 through 9. 
                'B1': 'utility', #Utility Patent (no pre-grant publication) issued on or after January 2, 2001. 
                'B2': 'utility', #Utility Patent (with pre-grant publication) issued on or after January 2, 2001. 
                'Cn': 'utility', #Reexamination Certificate issued on or after January 2, 2001. NOTE: "n" represents a value 1 through 9 denoting the publication level. 
                'E1': 'reissue', #Reissue Patent. 
                'Fn': 'reexamination certificate', #Reexamination Certificate of a Reissue Patent NOTE: "n" represents a value 1 through 9 denoting the publication level. 
                'H1': 'statutoty invention registration', #Statutory Invention Registration (SIR) Patent Documents. SIR documents began with the December 3, 1985 issue. 
                'I1': 'reissue', #"X" Patents issued from July 31, 1790 to July 13, 1836. 
                'I2': 'reissue', #"X" Reissue Patents issued from July 31, 1790 to July 13, 1836. 
                'I3': 'additional improvements', #Additional Improvements - Patents issued between 1838 and 1861. 
                'I4': 'defensive publication', #Defensive Publication - Documents issued from November 5, 1968 through May 5, 1987. 
                'I5': 'TVPP', #Trial Voluntary Protest Program (TVPP) Patent Documents. 
                'NP': 'non-patent literature', #Non-Patent Literature. 
                'P': 'plant', #Plant Patent issued prior to January 2, 2001. 
                'P1': 'plant', #Plant Patent Application published on or after January 2, 2001. 
                'P2': 'plant', #Plant Patent (no pre-grant publication) issued on or after January 2, 2001. 
                'P3': 'plant', #Plant Patent (with pre-grant publication) issued on or after January 2, 2001. 
                'P4': 'plant', #Second or subsequent publication of a Plant Patent Application. 
                'P9': 'plant', #Correction publication of a Plant Patent Application. 
                'S1': 'design' #Design Patent.  
                 }
    
    
    ### !For loggroups the last one will never be parsed but needs to be valid and required for parsing everything before it!
    loggroups = ['B100','B200', 'B510','B521','B522','B540','B561','B562','B570','B580','B721','B731','B732US','B741','SDOAB','CL']
    numi = 0
    num = 0
    
    #Rawlocation, mainclass and subclass should write after all else is done to prevent duplicate values
    rawlocation = {}
    mainclassdata = {}
    subclassdata = {}
    
    for d in diri:
        print d
        infile = open(fd+d,'rb').read().split('<!DOCTYPE')
        del infile[0]
        numi+=len(infile)
        for i in infile:
            # Get relevant logical groups from patent records according to documentation
            # Some patents can contain several INVT, ASSG and other logical groups - so, is important to retain all
            avail_fields = {}
            num = 1
            avail_fields['B100'] = i.split('B200')[0]
            runnums = []
            for n in range(1,len(loggroups)):
                try:
                    gg = re.search('\n<'+loggroups[n],i).group()
                    if num-n == 0:
                        runnums.append(n)
                        num+=1
                        go = list(re.finditer('\n<'+loggroups[n-1],i))
                        if len(go) == 1:
                            needed = i.split(loggroups[n-1])[1]
                            avail_fields[loggroups[n-1]] = needed.split(loggroups[n])[0]
                        elif len(go) > 1:
                            needed = '\n\n\n\n\n'.join(i.split(loggroups[n-1])[1:])
                            avail_fields[loggroups[n-1]] = needed.split(loggroups[n])[0]
                        else:
                            pass
                    else:
                        go = list(re.finditer('\n<'+loggroups[runnums[-1]],i))
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
            assignee = {}
            claims = {}
            foreigncitation = {}
            inventor = {}
            ipcr = {}
            lawyer = {}
            location = {}
            location_assignee = {}
            location_inventor = {}
            otherreference = {}
            patentdata = {}
            patent_assignee = {}
            patent_inventor = {}
            patent_lawyer = {}
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
            issdate = 'NULL'
            patkind = 'NULL'
            patcountry = 'NULL'
            

            try:
                patent = avail_fields['B100'].split('\n')
                for line in patent:
                    if line.startswith("<B110>"):
                        patnum = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                        updnum = re.sub('^H0','H',patnum)[:8]
                        updnum = re.sub('^RE0','RE',updnum)[:8]
                        updnum = re.sub('^PP0','PP',updnum)[:8]
                        updnum = re.sub('^PP0','PP',updnum)[:8]
                        updnum = re.sub('^D0', 'D', updnum)[:8]
                        updnum = re.sub('^T0', 'T', updnum)[:8]
                        if len(patnum) > 7 and patnum.startswith('0'):
                            updnum = patnum[1:8]
                        #print updnum
                        #data['patnum'] = updnum
                    if line.startswith('<B122US>'):
                        patkind = 'H1'
                    if line.startswith('<B130>'):
                        patkind = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                    if line.startswith('<B190>'):
                        patcountry = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                    if line.startswith('<B140>'):
                        issdate = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                        if issdate[6:] != "00":
                            issdate = issdate[:4]+'-'+issdate[4:6]+'-'+issdate[6:]
                        else:
                            issdate = issdate[:4]+'-'+issdate[4:6]+'-'+'01'
                        year = issdate[:4]
                        #print issdate
            except:
                pass
            
            #Application
            appnum = 'NULL'
            apptype = 'NULL'
            appdate = 'NULL'
            
            try:
                patent = avail_fields['B200'].split('\n')
                for line in patent:
                    if line.startswith('<B210>'):
                        appnum = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                        #print appnum
                    if line.startswith('<B211US>'):
                        apptype = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                        #print apptype
                    if line.startswith('<B220>'):
                        appdate = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                        appdate = appdate[:4]+'-'+appdate[4:6]+'-'+appdate[6:]
                        #print appdate
            except:
                pass
            
            
            #Patent title
            title = 'NULL'
            try:
                patent = avail_fields['B540']
                title = re.search('<PDAT>(.*?)</PDAT>',patent).group(1)
                #print title
            except:
                pass
            
            
            #Number of claims
            numclaims = 'NULL'
            try:
                patent = avail_fields['B570'].split('\n')
                for line in patent:
                    if line.startswith('<B577>'):    
                        numclaims = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                        #print numclaims
            except:
                pass
            
            patent_id = updnum
            
            application[apptype+'/'+appnum[2:]] = [patent_id,apptype,appnum,patcountry,appdate]
            
            # Claims data
            text = re.search('<CL(.*)</CL>',i,re.DOTALL).group()
            soup = bs(text)
            claimsdata = soup.findAll('clm')
            
            for so in claimsdata:
                clid = so['id']
                clnum = int(clid.replace('CLM-',''))
                try:
                    dependent = re.search('<clref id="CLM-(\d+)',str(so),re.DOTALL).group(1)
                    dependent = str(int(dependent))
                except:
                    dependent = "NULL"
                need = re.sub('<.*?>|</.*?>','',str(so))
                need = re.sub('[\n\t\r\f]+','',need)
                need = re.sub('^\d+\. ','',need)
                claims[id_generator()] = [patent_id,need,dependent,str(clnum)]
            
            #INVT - can be several
            try:
                inv_info = avail_fields['B721'].split("\n\n\n\n\n")
                inv_info = [a for a in inv_info if a != ">\r\n<"]
                for n in range(len(inv_info)):
                    fname = 'NULL'
                    lname = 'NULL'
                    invtcity = 'NULL'
                    invtstate = 'NULL'
                    invtcountry = 'NULL'
                    invtzip = 'NULL'
                    for line in inv_info[n].split("\n"):
                        #print line
                        if line.startswith("<NAM>"):
                            try:
                                fname = re.search('<FNM><PDAT>(.*?)</PDAT>',line).group(1)
                                lname = re.search('<SNM><STEXT><PDAT>(.*?)</PDAT>',line).group(1)
                            except:
                                try:
                                    lname = re.search('<SNM><STEXT><PDAT>(.*?)</PDAT>',line).group(1)
                                    fname = 'NULL'
                                except:
                                    try:
                                        fname = re.search('<FNM><PDAT>(.*?)</PDAT>',line).group(1)
                                        lname = 'NULL'
                                    except:
                                        print line
                            
                        if line.startswith("<CITY>"):
                            invtcity = re.search('<CITY><PDAT>(.*?)</PDAT>',line).group(1)
                            
                        if line.startswith("<STATE>"):
                            invtstate = re.search('<STATE><PDAT>(.*?)</PDAT>',line).group(1)
                        
                        if line.startswith("<CTRY>"):
                            invtcountry = re.search('<CTRY><PDAT>(.*?)</PDAT>',line).group(1)
                        
                        if line.startswith("<PCODE>"):
                            invtzip = re.search('<PCODE><PDAT>(.*?)</PDAT>',line).group(1)
                            #print invtzip
                
                    loc_idd = id_generator()
                    if invtcountry == 'NULL':
                        invtcountry = 'US'
                    rawlocation[(invtcity+'|'+invtstate+'|'+invtcountry).lower()] = [loc_idd,"NULL",invtcity,invtstate,invtcountry]
                    
                    if fname == "NULL" and lname == "NULL":
                        pass
                    else:
                        rawinventor[id_generator()] = [patent_id,"NULL",loc_idd,fname,lname,str(n)]
            except:
                pass
            
            #ASSG - can be several
            try:
                assg_info = avail_fields['B731'].split('\n\n\n\n\n')
                assg_type = avail_fields['B732US'].split("\n\n\n\n\n")
                assg_info = [a for a in assg_info if a != ">\r\n<"]
                assg_type = [a for a in assg_type if a != ">\r\n<"]
                for n in range(len(assg_info)):    
                    assorg = 'NULL'
                    assgfname = 'NULL'
                    assglname = 'NULL'
                    assgcity = 'NULL'
                    assgstate = 'NULL'
                    assgcountry = 'NULL'
                    assgzip = 'NULL'
                    assgtype = re.search('<PDAT>(.*?)</PDAT>',assg_type[n]).group(1)
                    for line in assg_info[n].split("\n"):
                        if line.startswith("<NAM>"):
                            try:
                                assgorg = re.search('<ONM><STEXT><PDAT>(.*?)</PDAT>',line).group(1)
                                assgfname = 'NULL'
                                assglname = 'NULL'
                                #print assgorg
                            except:
                                assgfname = re.search('<FNM><PDAT>(.*?)</PDAT>',line).group(1)
                                assglname = re.search('<SNM><STEXT><PDAT>(.*?)</PDAT>',line).group(1)
                                assgorg = 'NULL'
                                
                        if line.startswith('<ADR>'):
                            try:
                                assgcity = re.search('<CITY><PDAT>(.*?)</PDAT>',line).group(1)
                            except:
                                pass
                            try:    
                                assgstate = re.search('<STATE><PDAT>(.*?)</PDAT>',line).group(1)
                            except:
                                pass
                            try:    
                                assgcountry = re.search('<CTRY><PDAT>(.*?)</PDAT>',line).group(1)
                            except:
                                pass
                            try:
                                assgzip = re.search('<PCODE><PDAT>(.*?)</PDAT>',line).group(1)
                            except:
                                pass
                        
                    loc_idd = id_generator()
                    if assgcountry == 'NULL':
                        assgcountry = 'US'
                    rawlocation[(assgcity+'|'+assgstate+'|'+assgcountry).lower()] = [loc_idd,"NULL",assgcity,assgstate,assgcountry]
                    rawassignee[id_generator()] = [patent_id,"NULL",loc_idd,assgtype,assgfname,assglname,assgorg,assgcountry,'NULL',str(n)]
            except:
                pass
            
            #CLAS - should be several
            try:
                num = 0
                classes = avail_fields['B510'].split('\r\n')
                del classes[0]
                del classes[-1]
                for line in classes:
                    if line.startswith('<B511>'):
                        intsec = 'NULL'
                        mainclass = 'NULL'
                        subclass = 'NULL'
                        group = 'NULL'
                        subgroup = 'NULL'
                        intclass = re.search('<B511><PDAT>(.*?)</PDAT>',line).group(1)
                        intsec = intclass[0]
                        mainclass = intclass[1:3]
                        if updnum.startswith("D"):
                            intsec = 'D'
                            mainclass = intclass[0]
                            subclass = intclass[1:5]
                            group = "NULL"
                            subgroup = "NULL"
                        else:
                            subclass = intclass[3]
                            group = re.sub('^\s+','',intclass[4:7])
                            subgroup = re.sub('^\s+','',intclass[7:])
                    
                    if line.startswith('<B516>'):
                        ipcrversion = re.search('<B516><PDAT>(.*?)</PDAT>',line).group(1)
                            
                ipcr[id_generator()] = [patent_id,mainclass,intsec,subclass, group,subgroup,"NULL","NULL","NULL","NULL","NULL",ipcrversion,str(num)]
                num+=1     
                    
                        
            except:
                pass
            
            #Original classification
            try:
                num = 0
                classes = avail_fields['B521']
                origclass = re.search('<PDAT>(.*?)</PDAT>',line).group(1).upper()
                origmainclass = re.sub("\s+",'',origclass[0:3])
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
                
            except:
                pass
                           
            # Cross-reference - official to U.S. classification
            try:
                num = 0
                classes = avail_fields['B522'].split('\n\n\n\n\n')
                classes = [c for c in classes if c != ">\r\n<"]
                for n in range(len(classes)):
                    crossrefmain = "NULL"
                    crossrefsub = "NULL"
                    crossrefclass = re.search('<PDAT>(.*?)</PDAT>',classes[n]).group(1).upper()
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
            except:
                pass
    
            # U.S. Patent Reference - can be several
            refpatnum = 'NULL'
            refpatname = 'NULL'
            refpatdate = 'NULL'
            refpatclass = 'NULL'
            refpatcountry = 'US'
                    
            try:
                uspatref = avail_fields['B561'].split("\n\n\n\n\n")
                uspatref = [a for a in uspatref if a != ">\r\n<"]
                uspatseq = 0
                forpatseq = 0
                for n in range(len(uspatref)):
                    for line in uspatref[n].split("\n"):
                        if line.startswith('<DOC>'):
                            refpatnum = re.search('<DNUM><PDAT>(.*?)</PDAT>',line).group(1)
                        
                        if line.startswith('<DATE>'):
                            refpatdate = re.search('<DATE><PDAT>(.*?)</PDAT>',line).group(1)
                            if refpatdate[6:] != '00':
                                refpatdate = refpatdate[:4]+'-'+refpatdate[4:6]+'-'+refpatdate[6:]
                            else:
                                refpatdate = refpatdate[:4]+'-'+refpatdate[4:6]+'-01'
                        
                        if line.startswith('<KIND>'):
                            refpatkind = re.search('<KIND><PDAT>(.*?)</PDAT>',line).group(1)
                        
                        if line.startswith('<CTRY>'):
                            refpatcountry = re.search('<CTRY><PDAT>(.*?)</PDAT>',line).group(1)
                        
                        if line.startswith('<NAM>'):
                            refpatname = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                            
                        if line.startswith('<PNC>'):
                            refpatclass = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                            citedby = re.search('<CITED-BY-(.*?)/>',line).group(1)
                            citedby = 'cited by '+citedby.lower()
                        
                    if refpatcountry != 'US':
                        foreigncitation[id_generator()] = [patent_id,refpatdate,'NULL',refpatnum,refpatcountry,citedby,str(forpatseq)]
                        forpatseq+=1
                    else:
                        uspatentcitation[id_generator()] = [patent_id,refpatnum,refpatdate,"NULL",refpatkind,refpatnum,refpatcountry,citedby,str(uspatseq)]
                        uspatseq+=1
                        
            except:
                pass
            
            #Other reference - can be several
            try:
                otherreflist = avail_fields['B562'].split('\n\n\n\n\n')
                otherreflist = [a for a in otherreflist if a != ">\r\n<"]
                otherrefseq = 0
                appcitseq = 0
                for n in range(len(otherreflist)):
                    otherref = 'NULL'
                    otherref = re.search('<PDAT>(.*?)</PDAT>',otherreflist[n]).group(1)
                    appcit = re.search('applicationgggg',otherref)
                    if appcit:
                        usappcitation[id_generator()] = [patent_id,appcit.group(2).replace(' ',''),appcit.group(4),appcit.group(1),appcit.group(3),appcit.group(2).replace('US ',''),'US','NULL',str(appcitseq)]
                        appcitseq+=1
                    else:
                        otherreference[id_generator()] = [patent_id,otherref,str(otherrefseq)]
                        otherrefseq+=1
                    
            except:
                pass
            
            #Legal information - can be several
            try:
                legal_info = avail_fields['B741'].split("\n\n\n\n\n")
                legal_info = [a for a in legal_info if a != ">\r\n<"]
                for n in range(len(legal_info)):
                    legalcountry = 'NULL'
                    legalfirm = 'NULL'
                    attfname = 'NULL'
                    attlname = 'NULL'
                    for line in legal_info[n].split('\n'):
                        if line.startswith("<NAM>"):
                            try:
                                attfname = re.search('<FNM><PDAT>(.*?)</PDAT>',line).group(1)
                                attlname = re.search('<SNM><STEXT><PDAT>(.*?)</PDAT>',line).group(1)
                                legalfirm = 'NULL'
                            except:
                                legalfirm = re.search('<ONM><STEXT><PDAT>(.*?)</PDAT>',line).group(1)
                                attfname = 'NULL'
                                attlname = 'NULL'
                            
                        legalcountry = 'US'        
                    
                    rawlawyer[id_generator()] = ["NULL",patent_id,attfname,attlname,legalfirm,legalcountry,str(n)]
                        
            except:
                pass
            
            # Abstract - can be several lines
            try:
                abstfield = avail_fields['SDOAB'].split("<PDAT>")
                del abstfield[0]
                abst = ''
                for a in abstfield:
                    abst+=re.search('(.*?)</PDAT>',a).group(1)
                #print abst
            except:
                abst = 'NULL'
            
            patentdata[patent_id] = [type_kind[patkind],updnum,'US',issdate,abst,title,patkind,numclaims]
            
            patfile = csv.writer(open(os.path.join(fd2,'patent.csv'),'ab'))
            for k,v in patentdata.items():
                patfile.writerow([k]+v)
            
            appfile = csv.writer(open(os.path.join(fd2,'application.csv'),'ab'))
            for k,v in application.items():
                appfile.writerow([k]+v)
            
            claimsfile = csv.writer(open(os.path.join(fd2,'claim.csv'),'ab'))
            for k,v in claims.items():
                claimsfile.writerow([k]+v)
            
            rawinvfile = csv.writer(open(os.path.join(fd2,'rawinventor.csv'),'ab'))
            for k,v in rawinventor.items():
                rawinvfile.writerow([k]+v)
            
            rawassgfile = csv.writer(open(os.path.join(fd2,'rawassignee.csv'),'ab'))
            for k,v in rawassignee.items():
                rawassgfile.writerow([k]+v)
            """
            usappcitfile = csv.writer(open(os.path.join(fd2,'usapplicationcitation.csv'),'ab'))
            for k,v in usappcitation.items():
                usappcitfile.writerow([k]+v)
            """
            
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
            
    
    rawlocfile = csv.writer(open(os.path.join(fd2,'rawlocation.csv'),'ab'))
    for k,v in rawlocation.items():
        rawlocfile.writerow(v)

    mainclassfile = csv.writer(open(os.path.join(fd2,'mainclass.csv'),'ab'))
    for k,v in mainclassdata.items():
        mainclassfile.writerow(v)
    
    subclassfile = csv.writer(open(os.path.join(fd2,'subclass.csv'),'ab'))
    for k,v in subclassdata.items():
        subclassfile.writerow(v)
            

            
    print numi
