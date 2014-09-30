def parse_patents(fd,fd2):
    import re,csv,os
    import string,random
    
    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    fd+='/'
    fd2+='/'
    diri = os.listdir(fd)
    
    #Rewrite files and write headers to them
    appfile = open(os.path.join(fd2,'application.csv'),'wb')
    app = csv.writer(appfile)
    app.writerow(['id','patent_id','type','number','country','date'])
    
    rawlocfile = open(os.path.join(fd2,'rawlocation.csv'),'wb')
    rawloc = csv.writer(rawlocfile)
    rawloc.writerow(['id','location_id','city','state','country'])
    
    rawinvfile = open(os.path.join(fd2,'rawinventor.csv'),'wb')
    rawinv = csv.writer(rawinvfile)
    rawinv.writerow(['uuid','patent_id','inventor_id','rawlocation_id','name_first','name_last','sequence'])
    
    rawassgfile = open(os.path.join(fd2,'rawassignee.csv'),'wb')
    rawassg = csv.writer(rawassgfile)
    rawassg.writerow(['uuid','patent_id','assignee_id','rawlocation_id','type','name_first','name_last','organization','residence','nationality','sequence'])
    
    ipcrfile = open(os.path.join(fd2,'ipc.csv'),'wb')
    ipcr = csv.writer(ipcrfile)
    ipcr.writerow(['uuid','patent_id','classification_level','section','subclass','main_group','subgroup','symbol_position','classification_value','classification_status','classification_data_source','action_date','ipc_version_indicator','sequence'])
    
    patfile = open(os.path.join(fd2,'patent.csv'),'ab')
    pat = csv.writer(patfile)
    pat.writerow(['id','type','number','country','date','abstract','title','kind','num_claims'])
    
    uspatentcitfile = open(os.path.join(fd2,'uspatentcitation.csv'),'ab')
    uspatcit = csv.writer(uspatentcitfile)
    uspatcit.writerow(['uuid','patent_id','citation_id','date','name','kind','number','country','category','sequence'])
    
    otherreffile = open(os.path.join(fd2,'otherreference.csv'),'ab')
    otherref = csv.writer(otherreffile)
    otherref.writerow(['uuid','patent_id','text','sequence'])
    
    rawlawyerfile = open(os.path.join(fd2,'rawlawyer.csv'),'ab')
    rawlawyer = csv.writer(rawlawyerfile)
    rawlawyer.writerow(['uuid','lawyer_id','patent_id','name_first','name_last','organization','country','sequence'])
    
    
    appfile.close()
    rawlocfile.close()
    rawinvfile.close()
    rawassgfile.close()
    ipcrfile.close()
    otherreffile.close()
    patfile.close()
    rawlawyerfile.close()
    uspatentcitfile.close()
    
    
    ### !For loggroups the last one will never be parsed but needs to be valid and required for parsing everything before it!
    loggroups = ['B100','B200', 'B510','B521','B522','B540','B561','B562','B570','B580','B721','B731','B732US','B741','SDOAB','CL']
    numi = 0
    num = 0
    for d in diri:
        print d
        infile = open(fd+d,'rb').read().split('<!DOCTYPE')
        del infile[0]
        numi+=len(infile)
        for i in infile:
          try:
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
            mainclassdata = {}
            otherreference = {}
            patentdata = {}
            patent_assignee = {}
            patent_inventor = {}
            patent_lawyer = {}
            rawassignee = {}
            rawinventor = {}
            rawlawyer = {}
            rawlocation = {}
            subclassdata = {}
            usappcitation = {}
            uspatentcitation = {}
            uspc = {}
            usreldoc = {}
            crossrefclassdata = {}
            
            
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
                        updnum = re.sub('^D0', 'D', updnum)[:8]
                        updnum = re.sub('^T0', 'T', updnum)[:8]
                        if len(patnum) > 7 and patnum.startswith('0'):
                            updnum = patnum[1:8]
                        #print updnum
                        #data['patnum'] = updnum
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
                        appnum = re.search('(?<=<PDAT>)\w+',line).group()[:6]
                        if len(appnum) != 6:
                            appnum = 'NULL'
                            #data['appnum'] = appnum
                        #print appnum
                    if line.startswith('<B211US>'):
                        apptype = re.search('(?<=\<PDAT>)\w+',line).group()
                        #print apptype
                    if line.startswith('<B220>'):
                        appdate = re.search('(?<=\<PDAT>)\w+',line).group()
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
            
            patent_id = id_generator()
            
            application[appnum[:2]+'/'+appnum[2:]] = [patent_id,apptype,appnum,patcountry,appdate]
            
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
                    try:
                        gg = rawlocation[invtcity+'|'+invtstate+'|'+invtcountry]
                    except:
                        rawlocation[invtcity+'|'+invtstate+'|'+invtcountry] = [invtcity,invtstate,invtcountry]
                    
                    rawinventor[id_generator()] = [patent_id,patent_id+'-'+str(n),invtcity+'|'+invtstate+'|'+invtcountry,fname,lname,str(n)]
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
                        
                    try:
                        gg = rawlocation[assgcity+'|'+assgstate+'|'+assgcountry]
                        
                    except:
                        rawlocation[assgcity+'|'+assgstate+'|'+assgcountry] = [assgcity,assgstate,assgcountry]
                    
                    rawassignee[id_generator()] = [patent_id,'ASSG'+patent_id+'-'+str(n),assgcity+'|'+assgstate+'|'+assgcountry,assgtype,assgfname,assglname,assgorg,assgcountry,'NULL',str(n)]
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
                origclass = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                origmainclass = origclass[0:3]
                origsubclass = origclass[3:]
                mainclassdata[id_generator()] = [patent_id,origmainclass]
                subclassdata[id_generator()] = [patent_id,origsubclass]    
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
                    crossrefclass = re.search('<PDAT>(.*?)</PDAT>',classes[n]).group(1)
                    crossrefmain = crossrefclass[:3]
                    crossrefsub = crossrefclass[3:]
                    crossrefclassdata[id_generator()] = [patent_id,crossrefmain,crossrefsub]
                        
            except:
                pass
    
            # U.S. Patent Reference - can be several
            refpatnum = 'NULL'
            refpatname = 'NULL'
            refpatdate = 'NULL'
            refpatclass = 'NULL'
                    
            try:
                uspatref = avail_fields['B561'].split("\n\n\n\n\n")
                uspatref = [a for a in uspatref if a != ">\r\n<"]
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
                        
                        if line.startswith('<NAM>'):
                            refpatname = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                            
                        if line.startswith('<PNC>'):
                            refpatclass = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                            citedby = re.search('<CITED-BY-(.*?)/>',line).group(1)
                            citedby = 'cited by '+citedby.lower()
                            
                    uspatentcitation[id_generator()] = [patent_id,refpatnum,refpatdate,refpatname,re.sub('\d$','',refpatnum[:2]),refpatnum,'US',citedby,str(n)]
            except:
                pass
            
            #Other reference - can be several
            try:
                otherreflist = avail_fields['B562'].split('\n\n\n\n\n')
                otherreflist = [a for a in otherreflist if a != ">\r\n<"]
                for n in range(len(otherreflist)):
                    otherref = 'NULL'
                    otherref = re.search('<PDAT>(.*?)</PDAT>',otherreflist[n]).group(1)
                    otherreference[id_generator()] = [patent_id,otherref,str(n)]
            except:
                pass
            
            """
            #Foreign reference - can be several
            try:
                foreignref = avail_fields[''].split('\n\n\n\n\n')
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
                            if forrefpatdate[6:]!='00':
                                forrefpatdate = refpatdate[:4]+'-'+refpatdate[4:6]+'-'+refpatdate[6:]
                            else:
                                forrefpatdate = refpatdate[:4]+'-'+refpatdate[4:6]+'-01'
                            
                        if line.startswith('CNT'):
                            forrefpatcountry = re.search('CNT\s+(.*?)$',line).group(1)
                            
                        if line.startswith('ICL'):
                            forrefpatclass = re.search('ICL\s+(.*?)$',line).group(1) 
                    foreigncitation[id_generator()] = [patent_id,forrefpatdate,"TAKE KIND FROM PATENT NUMBER?",forrefpatnum,forrefpatcountry,forrefpatclass,str(n)] 
            except:
                pass
            """
            
            #Legal information - can be several
            try:
                legal_info = avail_fields['B741'].split("\n\n\n\n\n")
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
                    
                    rawlawyer[id_generator()] = ['LAWYER'+patent_id+'-'+str(n),patent_id,attfname,attlname,legalfirm,legalcountry,str(n)]
                        
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
            
            patentdata[patent_id] = ['TAKE FROM KIND',updnum,'US',issdate,abst,title,re.sub('\d$','',updnum[0:2]),numclaims]
            
            patfile = csv.writer(open(os.path.join(fd2,'patent.csv'),'ab'))
            for k,v in patentdata.items():
                patfile.writerow([k]+v)
            
            appfile = csv.writer(open(os.path.join(fd2,'application.csv'),'ab'))
            for k,v in application.items():
                appfile.writerow([k]+v)
            
            rawlocfile = csv.writer(open(os.path.join(fd2,'rawlocation.csv'),'ab'))
            for k,v in rawlocation.items():
                rawlocfile.writerow([k]+v)
            
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
            
            """
            mainclassfile = csv.writer(open(os.path.join(fd2,'original_mainclass.csv'),'ab'))
            for k,v in mainclassdata.items():
                mainclassfile.writerow([k]+v)
            
            subclassfile = csv.writer(open(os.path.join(fd2,'original_subclass.csv'),'ab'))
            for k,v in subclassdata.items():
                subclassfile.writerow([k]+v)
            """
            uspatentcitfile = csv.writer(open(os.path.join(fd2,'uspatentcitation.csv'),'ab'))
            for k,v in uspatentcitation.items():
                uspatentcitfile.writerow([k]+v)
            
            """
            foreigncitfile = csv.writer(open(os.path.join(fd2,'foreigncitation.csv'),'ab'))
            for k,v in foreigncitation.items():
                foreigncitfile.writerow([k]+v)
            """
            otherreffile = csv.writer(open(os.path.join(fd2,'otherreference.csv'),'ab'))
            for k,v in otherreference.items():
                otherreffile.writerow([k]+v)
            
            rawlawyerfile = csv.writer(open(os.path.join(fd2,'rawlawyer.csv'),'ab'))
            for k,v in rawlawyer.items():
                rawlawyerfile.writerow([k]+v)
            """
            crossreffile = csv.writer(open(os.path.join(fd2,'crossreferenceclass_XCL.csv'),'ab'))
            for k,v in crossrefclassdata.items():
                crossreffile.writerow([k]+v)
            """
          except:
             print i
    
    print numi
