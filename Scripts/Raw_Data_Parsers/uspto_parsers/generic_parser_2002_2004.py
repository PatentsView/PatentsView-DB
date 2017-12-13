def parse_patents(fd,fd2):
    import re,csv,os,codecs,traceback
    import string,random,zipfile
    from bs4 import BeautifulSoup as bs
    from unidecode import unidecode
    import HTMLParser
    import htmlentitydefs

    _char = re.compile(r'&(\w+?);')
    print ("this is coppied from Github!")
    
    # Generate some extra HTML entities
    defs=htmlentitydefs.entitydefs
    defs['apos'] = "'"
    entities = open('uspto_parsers/htmlentities').read().split('\n')
    for e in entities:
        try:
            first = re.sub('\s+|\"|;|&','',e[3:15])
            second = re.sub('\s+|\"|;|&','',e[15:24])
            define = re.search("(?<=\s\s\').*?$",e).group()
            defs[first] = define[:-1].encode('utf-8')
            defs[second] = define[:-1].encode('utf-8')
        except:
            pass
    
    def _char_unescape(m, defs=defs):
        try:
            return defs[m.group(1)].encode('utf-8','ignore')
        except:
            return m.group()
            
    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    fd+='/'
    fd2+='/'
    diri = os.listdir(fd)
    diri = [d for d in diri if re.search('zip',d,re.I)]
    
    #Initiate HTML Parser for unescape characters
    h = HTMLParser.HTMLParser()
    
    #Remove all files from output dir before writing
    outdir = os.listdir(fd2)
    for oo in outdir:
        os.remove(os.path.join(fd2,oo))
    
    #Rewrite files and write headers to them
    appfile = open(os.path.join(fd2,'application.csv'),'wb')
    appfile.write(codecs.BOM_UTF8)
    app = csv.writer(appfile,delimiter='\t')
    app.writerow(['id','patent_id','type','number','country','date'])
    
    claimsfile = open(os.path.join(fd2,'claim.csv'),'wb')
    claimsfile.write(codecs.BOM_UTF8)
    clms = csv.writer(claimsfile,delimiter='\t')
    clms.writerow(['uuid','patent_id','text','dependent','sequence', 'exemplary'])

    rawlocfile = open(os.path.join(fd2,'rawlocation.csv'),'wb')
    rawlocfile.write(codecs.BOM_UTF8)
    rawloc = csv.writer(rawlocfile,delimiter='\t')
    rawloc.writerow(['id','location_id','city','state','country'])

    rawinvfile = open(os.path.join(fd2,'rawinventor.csv'),'wb')
    rawinvfile.write(codecs.BOM_UTF8)
    rawinv = csv.writer(rawinvfile,delimiter='\t')
    rawinv.writerow(['uuid','patent_id','inventor_id','rawlocation_id','name_first','name_last','sequence','rule_47'])
    
    examinerfile = open(os.path.join(fd2,'examiner.csv'),'wb')
    examinerfile.write(codecs.BOM_UTF8)
    exam = csv.writer(examinerfile,delimiter='\t')
    exam.writerow(['id','patent_id','fname','lname','role','group'])

    rawassgfile = open(os.path.join(fd2,'rawassignee.csv'),'wb')
    rawassgfile.write(codecs.BOM_UTF8)
    rawassg = csv.writer(rawassgfile,delimiter='\t')
    rawassg.writerow(['uuid','patent_id','assignee_id','rawlocation_id','type','name_first','name_last','organization','sequence'])

    ipcrfile = open(os.path.join(fd2,'ipcr.csv'),'wb')
    ipcrfile.write(codecs.BOM_UTF8)
    ipcr = csv.writer(ipcrfile,delimiter='\t')
    ipcr.writerow(['uuid','patent_id','classification_level','section','mainclass','subclass','main_group','subgroup','symbol_position','classification_value','classification_status','classification_data_source','action_date','ipc_version_indicator','sequence'])
    
    patfile = open(os.path.join(fd2,'patent.csv'),'wb')
    patfile.write(codecs.BOM_UTF8)
    pat = csv.writer(patfile,delimiter='\t')
    pat.writerow(['id','type','number','country','date','abstract','title','kind','num_claims', 'filename'])
    
    foreigncitfile = open(os.path.join(fd2,'foreigncitation.csv'),'wb')
    foreigncitfile.write(codecs.BOM_UTF8)
    foreigncit = csv.writer(foreigncitfile,delimiter='\t')
    foreigncit.writerow(['uuid','patent_id','date','number','country','category','sequence'])
    
    uspatentcitfile = open(os.path.join(fd2,'uspatentcitation.csv'),'wb')
    uspatentcitfile.write(codecs.BOM_UTF8)
    uspatcit = csv.writer(uspatentcitfile,delimiter='\t')
    uspatcit.writerow(['uuid','patent_id','citation_id','date','name','kind','country','category','sequence'])
    """
    usappcitfile = open(os.path.join(fd2,'usapplicationcitation.csv'),'wb')
    usappcitfile.write(codecs.BOM_UTF8)
    usappcit = csv.writer(usappcitfile)
    usappcit.writerow(['uuid','patent_id','application_id','date','name','kind','number','country','category','sequence'])
    """
    uspcfile = open(os.path.join(fd2,'uspc.csv'),'wb')
    uspcfile.write(codecs.BOM_UTF8)
    uspcc = csv.writer(uspcfile,delimiter='\t')
    uspcc.writerow(['uuid','patent_id','mainclass_id','subclass_id','sequence'])

    otherreffile = open(os.path.join(fd2,'otherreference.csv'),'wb')
    otherreffile.write(codecs.BOM_UTF8)
    otherref = csv.writer(otherreffile,delimiter='\t')
    otherref.writerow(['uuid','patent_id','text','sequence'])
    
    rawlawyerfile = open(os.path.join(fd2,'rawlawyer.csv'),'wb')
    rawlawyerfile.write(codecs.BOM_UTF8)
    rawlawyer = csv.writer(rawlawyerfile,delimiter='\t')
    rawlawyer.writerow(['uuid','lawyer_id','patent_id','name_first','name_last','organization','country','sequence'])

    mainclassfile = open(os.path.join(fd2,'mainclass.csv'),'wb')
    mainclassfile.write(codecs.BOM_UTF8)
    mainclass = csv.writer(mainclassfile,delimiter='\t')
    mainclass.writerow(['id'])

    subclassfile = open(os.path.join(fd2,'subclass.csv'),'wb')
    subclassfile.write(codecs.BOM_UTF8)
    subclass = csv.writer(subclassfile,delimiter='\t')
    subclass.writerow(['id'])
    
    ### New fields ###
    forpriorityfile = open(os.path.join(fd2,'foreign_priority.csv'),'wb')
    forpriorityfile.write(codecs.BOM_UTF8)
    forpriority = csv.writer(forpriorityfile,delimiter='\t')
    forpriority.writerow(['uuid', 'patent_id', "sequence", "kind", "app_num", "app_date", "country"])

    us_term_of_grantfile = open(os.path.join(fd2,'us_term_of_grant.csv'), 'wb')
    us_term_of_grantfile.write(codecs.BOM_UTF8)
    us_term_of_grant = csv.writer(us_term_of_grantfile, delimiter='\t')
    us_term_of_grant.writerow(['uuid','patent_id','lapse_of_patent', 'disclaimer_date' 'term_disclaimer', 'term_grant', 'term_ext'])

    usreldocfile = open(os.path.join(fd2,'usreldoc.csv'), 'wb')
    usreldocfile.write(codecs.BOM_UTF8)
    usrel = csv.writer(usreldocfile, delimiter='\t')
    usrel.writerow(['uuid', 'patent_id', 'doc_type',  'relkind', 'reldocno', 'relcountry', 'reldate',  'parent_status', 'rel_seq','kind'])

    draw_desc_textfile = open(os.path.join(fd2,'draw_desc_text.csv'), 'wb')
    draw_desc_textfile.write(codecs.BOM_UTF8)
    drawdesc = csv.writer(draw_desc_textfile, delimiter='\t')
    drawdesc.writerow(['uuid', 'patent_id', 'text', "seq"])

    brf_sum_textfile = open(os.path.join(fd2,'brf_sum_text.csv'), 'wb')
    brf_sum_textfile.write(codecs.BOM_UTF8)
    brf_sum = csv.writer(brf_sum_textfile, delimiter='\t')
    brf_sum.writerow(['uuid', 'patent_id', 'text'])

    det_desc_textfile = open(os.path.join(fd2,'detail_desc_text.csv'), 'wb')
    det_desc_textfile.write(codecs.BOM_UTF8)
    det_desc = csv.writer(det_desc_textfile, delimiter='\t')
    det_desc.writerow(['uuid', 'patent_id', 'text', 'sequence'])

    rel_app_textfile = open(os.path.join(fd2,'rel_app_text.csv'), 'wb')
    rel_app_textfile.write(codecs.BOM_UTF8)
    rel_app = csv.writer(rel_app_textfile, delimiter='\t')
    rel_app.writerow(['uuid', 'patent_id',"text"])

    non_inventor_applicantfile = open(os.path.join(fd2,'non_inventor_applicant.csv'),'wb')
    non_inventor_applicantfile.write(codecs.BOM_UTF8)
    noninventorapplicant = csv.writer(non_inventor_applicantfile,delimiter='\t')
    noninventorapplicant.writerow(['uuid', 'patent_id', "location_id", "last_name", "first_name", "org_name", "sequence", "designation", "applicant_type"])

    pct_datafile = open(os.path.join(fd2,'pct_data.csv'), 'wb')
    pct_datafile.write(codecs.BOM_UTF8)
    pct_data = csv.writer(pct_datafile, delimiter='\t')
    pct_data.writerow(['uuid', 'patent_id', 'rel_id', 'date', '371_date', 'country', 'kind', "doc_type","102_date"])

    botanicfile = open(os.path.join(fd2,'botanic.csv'), 'wb')
    botanicfile.write(codecs.BOM_UTF8)
    botanic_info = csv.writer(botanicfile, delimiter='\t')
    botanic_info.writerow(['uuid', 'patent_id', 'latin_name', "variety"])

    figurefile = open(os.path.join(fd2,'figures.csv'), 'wb')
    figurefile.write(codecs.BOM_UTF8)
    figure_info = csv.writer(figurefile, delimiter='\t')
    figure_info.writerow(['uuid', 'patent_id', 'num_figs', "num_sheets"])
    

    mainclassfile.close()
    examinerfile.close()
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
    

    forpriorityfile.close()
    us_term_of_grantfile.close()
    usreldocfile.close()
    non_inventor_applicantfile.close()
    draw_desc_textfile.close()
    brf_sum_textfile.close()
    rel_app_textfile.close()
    det_desc_textfile.close()
    pct_datafile.close()
    botanicfile.close()
    figurefile.close()

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
                'H1': 'statutory invention registration', #Statutory Invention Registration (SIR) Patent Documents. SIR documents began with the December 3, 1985 issue. 
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
                'S1': 'design', #Design Patent.
                'NULL': 'NULL' #Placeholder for NULL values for duplicates and such.
                 }
    
    parent_status={
                   '00': 'PENDING',
                   '01': 'GRANTED',
                   '03': 'ABANDONED',
                   '04': 'SIR'
                   }    
    ### !For loggroups the last one will never be parsed but needs to be valid and required for parsing everything before it!
    loggroups = ['B100','B200','B300','B400','B510','B521','B522','B540','B561','B562','B570','B580','B590',
    'B600','B721','B731','B732US','B741','B746', 'B747', 'B748US', 'B860','B870','BRFSUM',"DETDESC","DRWDESC", "RELAPP", 'SDOAB','CL']
    numi = 0
    num = 0
    #Rawlocation, mainclass and subclass should write after all else is done to prevent duplicate values
    rawlocation = {}
    mainclassdata = {}
    subclassdata = {}
    
    for d in diri:
      print d
      inp = zipfile.ZipFile(os.path.join(fd,d))
      for i in inp.namelist():
        infile = inp.open(i).read().decode('utf-8','ignore').replace('&angst','&aring')
        infile = infile.encode('utf-8','ignore')
        infile = _char.sub(_char_unescape,infile)
        infile = infile.split('<!DOCTYPE')
        del infile[0]
        numi+=len(infile)
        for i in infile:
            # Get relevant logical groups from patent records according to documentation
            # Some patents can contain several INVT, ASSG and other logical groups - so, is important to retain all
            #i = i.decode()
            avail_fields = {}
            num = 1
            avail_fields['B100'] = i.split('B200')[0]
            runnums = []
            for n in range(1,len(loggroups)):
                try:
                    gg = re.search('\n<'+loggroups[n],i).group()
                    if num-n == 0: 
                        #print loggroups[n-1]
                        # if loggroups[n-1]== 'B731':
                        #     print gg
                        runnums.append(n)
                        num+=1
                        go = list(re.finditer('\n<'+loggroups[n-1],i))
                        if len(go) == 1:
                            needed = i.split(loggroups[n-1])[1]
                            avail_fields[loggroups[n-1]] = needed.split(loggroups[n])[0]
                        elif len(go) > 1:
                            needed = '\n\n\n\n\n'.join(i.split('<'+loggroups[n-1])[1:])
                            avail_fields[loggroups[n-1]] = ' '.join(needed.split('</' + loggroups[n-1])[:-1])
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
            claims = {}
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
            examiner = {}
            pctdata = {}
            prioritydata = {}
            figureinfo = {}
            termofgrant = {}
            drawdescdata = {}
            relappdata = {}
            ###                PARSERS FOR LOGICAL GROUPS                  ###
           
            #PATN
            #updnum = 'NULL'
            #issdate = 'NULL'
            #patkind = 'NULL'
            #patcountry = 'NULL'
            

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

            
            #Term of grant
            try:
                togrant = avail_fields['B400'].split("\n")
                termdisc = ''
                termext = ''
                disclaimerdate = ''
                for line in togrant:
                    if line.startswith('<B473>'):
                        disclaimerdate = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                        if disclaimerdate[6:] != "00":
                            disclaimerdate = disclaimerdate[:4]+'-'+disclaimerdate[4:6]+'-'+disclaimerdate[6:]
                        else:
                            disclaimerdate = disclaimerdate[:4]+'-'+disclaimerdate[4:6]+'-'+'01'
                    if line.startswith('<B473US'):
                        termdisc = 'YES'
                    if line.startswith('<B474>'):
                        term = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                    if line.startswith('<B474US>'):
                        termext = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                termofgrant[id_generator()] = [updnum,'',disclaimerdate,termdisc,term,termext]
            except:
                pass
    
        
            #Application
            #appnum = 'NULL'
            #apptype = 'NULL'
            #appdate = 'NULL'
            
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
            #title = 'NULL'
            try:
                patent = avail_fields['B540']
                title = re.search('<PDAT>(.*?)</PDAT>',patent).group(1)
                #print title
            except:
                pass
            
            
            #Figure info
            numsheets = ''
            numfigs = ''
            try:
                patent = avail_fields['B590']
                numsheets = re.search('<B595><PDAT>(.*?)</PDAT></B595',patent)
                if numsheets:
                    numsheets = numsheets.group(1)
                numfigs = re.search('<B596><PDAT>(.*?)</PDAT></B596',patent)
                if numfigs:
                    numfigs = numfigs.group(1)
            except:
                pass
            
            
            #Related docs
            try:
                patent = avail_fields['B600'].split("\r\n")
                patent = [p for p in patent if p!='>' and p!="</" and p!='']
                patent = ''.join(patent)
                enume = 0
                if re.search('<B610',patent): #ADDITION
                    #addition = re.search('<B610.*?<PDAT>(.*?)</PDAT></B610').group(1)
                    print patent
                if re.search('<B620',patent): #DIVISION
                    division = re.findall('<B620>(.*?)</B620>',patent)
                    for e,div in enumerate(division):
                        child = re.findall('<CDOC>.*?<PDAT>(.*?)</PDAT>.*?</CDOC>',div)
                        parentfull = re.findall('<PDOC>(.*?)</PDOC>',div)
                        parent_grantf = re.findall('<PPUB>(.*?)</PPUB>',div)
                        parent_stat = re.findall('<PSTA>(.*?)</PSTA>',div)
                        for n in range(len(child)):
                            usreldoc[id_generator()] = [updnum,'division','child_doc',child[n].replace("/",''),'','','','',str(enume),'']
                            enume+=1
                        for n in range(len(parentfull)):
                            try:
                                parent = bs(parentfull[n])
                                kind = parent.kind.pdat.string
                                relation = 'parent_doc'
                                relnum = parent.dnum.pdat.string.replace("/",'')
                                reldate = parent.date.pdat.string
                                reldate = reldate[:4]+'-'+reldate[4:6]+'-'+reldate[6:]
                                relctn = parent.ctry.pdat.string
                                status = parent_status[bs(parent_stat[n]).pdat.string]
                                usreldoc[id_generator()] = [updnum,'division',relation,relnum,relctn,reldate,status,str(enume),kind]
                                enume+=1
                            except:
                                pass
                        for n in range(len(parent_grantf)):
                            try:
                                parent_grant = bs(parent_grantf[n])
                                pgrant_num = parent_grant.dnum.pdat.string.replace('Des. ','D')
                                pgrant_ctry = parent_grant.ctry.pdat.string
                                pgrant_kind = parent_grant.kind.pdat.string.replace(' ','')
                                usreldoc[id_generator()] = [updnum,'division','parent_grant_document',pgrant_num,pgrant_ctry,'','',str(enume),pgrant_kind]
                                enume+=1
                            except:
                                pass

                if re.search('<B631',patent): #CONTINUATION
                    division = re.findall('<B631>(.*?)</B631>',patent)
                    for e,div in enumerate(division):
                        child = re.findall('<CDOC>.*?<PDAT>(.*?)</PDAT>.*?</CDOC>',div)
                        parentfull = re.findall('<PDOC>(.*?)</PDOC>',div)
                        parent_grantf = re.findall('<PPUB>(.*?)</PPUB>',div)
                        parent_stat = re.findall('<PSTA>(.*?)</PSTA>',div)
                        for n in range(len(child)):
                            usreldoc[id_generator()] = [updnum,'continuation','child_doc',child[n].replace("/",''),'','','','',str(enume),'']
                            enume+=1
                        for n in range(len(parentfull)):
                            try:
                                parent = bs(parentfull[n])
                                kind = parent.kind.pdat.string
                                relation = 'parent_doc'
                                relnum = parent.dnum.pdat.string.replace("/",'')
                                reldate = parent.date.pdat.string
                                reldate = reldate[:4]+'-'+reldate[4:6]+'-'+reldate[6:]
                                relctn = parent.ctry.pdat.string
                                status = parent_status[bs(parent_stat[n]).pdat.string]
                                usreldoc[id_generator()] = [updnum,'continuation',relation,relnum,relctn,reldate,status,str(enume),kind]
                                enume+=1
                            except:
                                pass
                        for n in range(len(parent_grantf)):
                            try:
                                parent_grant = bs(parent_grantf[n])
                                pgrant_num = parent_grant.dnum.pdat.string.replace('Des. ','D')
                                pgrant_ctry = parent_grant.ctry.pdat.string
                                pgrant_kind = parent_grant.kind.pdat.string.replace(' ','')
                                usreldoc[id_generator()] = [updnum,'continuation','parent_grant_document',pgrant_num,pgrant_ctry,'','',str(enume),pgrant_kind]
                                enume+=1
                            except:
                                pass

                if re.search('<B632',patent): #CONTINUATION-IN-PART
                    division = re.findall('<B632>(.*?)</B632>',patent)
                    for e,div in enumerate(division):
                        child = re.findall('<CDOC>.*?<PDAT>(.*?)</PDAT>.*?</CDOC>',div)
                        parentfull = re.findall('<PDOC>(.*?)</PDOC>',div)
                        parent_grantf = re.findall('<PPUB>(.*?)</PPUB>',div)
                        parent_stat = re.findall('<PSTA>(.*?)</PSTA>',div)
                        for n in range(len(child)):
                            usreldoc[id_generator()] = [updnum,'continuation_in_part','child_doc',child[n].replace("/",''),'','','','',str(enume),'']
                            enume+=1
                        for n in range(len(parentfull)):
                            try:
                                parent = bs(parentfull[n])
                                kind = parent.kind.pdat.string
                                relation = 'parent_doc'
                                relnum = parent.dnum.pdat.string.replace("/",'')
                                reldate = parent.date.pdat.string
                                reldate = reldate[:4]+'-'+reldate[4:6]+'-'+reldate[6:]
                                relctn = parent.ctry.pdat.string
                                status = parent_status[bs(parent_stat[n]).pdat.string]
                                usreldoc[id_generator()] = [updnum,'continuation_in_part',relation,relnum,relctn,reldate,status,str(enume),kind]
                                enume+=1
                            except:
                                pass
                        for n in range(len(parent_grantf)):
                            try:
                                parent_grant = bs(parent_grantf[n])
                                pgrant_num = parent_grant.dnum.pdat.string.replace('Des. ','D')
                                pgrant_ctry = parent_grant.ctry.pdat.string
                                pgrant_kind = parent_grant.kind.pdat.string.replace(' ','')
                                usreldoc[id_generator()] = [updnum,'continuation_in_part','parent_grant_document',pgrant_num,pgrant_ctry,'','',str(enume),pgrant_kind]
                                enume+=1
                            except:
                                pass

                if re.search('<B633',patent): #CONTINUING REISSUE
                    division = re.findall('<B633>(.*?)</B633>',patent)
                    for e,div in enumerate(division):
                        child = re.findall('<CDOC>.*?<PDAT>(.*?)</PDAT>.*?</CDOC>',div)
                        parentfull = re.findall('<PDOC>(.*?)</PDOC>',div)
                        parent_grantf = re.findall('<PPUB>(.*?)</PPUB>',div)
                        parent_stat = re.findall('<PSTA>(.*?)</PSTA>',div)
                        for n in range(len(child)):
                            usreldoc[id_generator()] = [updnum,'continuing_reissue','child_doc',child[n].replace("/",''),'','','','',str(enume),'']
                            enume+=1
                        for n in range(len(parentfull)):
                            try:
                                parent = bs(parentfull[n])
                                kind = parent.kind.pdat.string
                                relation = 'parent_doc'
                                relnum = parent.dnum.pdat.string.replace("/",'')
                                reldate = parent.date.pdat.string
                                reldate = reldate[:4]+'-'+reldate[4:6]+'-'+reldate[6:]
                                relctn = parent.ctry.pdat.string
                                status = parent_status[bs(parent_stat[n]).pdat.string]
                                usreldoc[id_generator()] = [updnum,'continuing_reissue',relation,relnum,relctn,reldate,status,str(enume),kind]
                                enume+=1
                            except:
                                pass
                        for n in range(len(parent_grantf)):
                            try:
                                parent_grant = bs(parent_grantf[n])
                                pgrant_num = parent_grant.dnum.pdat.string.replace('Des. ','D')
                                pgrant_ctry = parent_grant.ctry.pdat.string
                                pgrant_kind = parent_grant.kind.pdat.string.replace(' ','')
                                usreldoc[id_generator()] = [updnum,'continuing_reissue','parent_grant_document',pgrant_num,pgrant_ctry,'','',str(enume),pgrant_kind]
                                enume+=1
                            except:
                                pass
                
                if re.search('<B640',patent): #REISSUE
                    division = re.findall('<B640>(.*?)</B640>',patent)
                    for e,div in enumerate(division):
                        child = re.findall('<CDOC>.*?<PDAT>(.*?)</PDAT>.*?</CDOC>',div)
                        parentfull = re.findall('<PDOC>(.*?)</PDOC>',div)
                        parent_grantf = re.findall('<PPUB>(.*?)</PPUB>',div)
                        parent_stat = re.findall('<PSTA>(.*?)</PSTA>',div)
                        for n in range(len(child)):
                            usreldoc[id_generator()] = [updnum,'reissue','child_doc',child[n].replace("/",''),'','','','',str(enume),'']
                            enume+=1
                        for n in range(len(parentfull)):
                            try:
                                parent = bs(parentfull[n])
                                kind = parent.kind.pdat.string
                                relation = 'parent_doc'
                                relnum = parent.dnum.pdat.string.replace("/",'')
                                reldate = parent.date.pdat.string
                                reldate = reldate[:4]+'-'+reldate[4:6]+'-'+reldate[6:]
                                relctn = parent.ctry.pdat.string
                                status = parent_status[bs(parent_stat[n]).pdat.string]
                                usreldoc[id_generator()] = [updnum,'reissue',relation,relnum,relctn,reldate,status,str(enume),kind]
                                enume+=1
                            except:
                                pass
                        for n in range(len(parent_grantf)):
                            try:
                                parent_grant = bs(parent_grantf[n])
                                pgrant_num = parent_grant.dnum.pdat.string.replace('Des. ','D')
                                pgrant_ctry = parent_grant.ctry.pdat.string
                                pgrant_kind = parent_grant.kind.pdat.string.replace(' ','')
                                usreldoc[id_generator()] = [updnum,'reissue','parent_grant_document',pgrant_num,pgrant_ctry,'','',str(enume),pgrant_kind]
                                enume+=1
                            except:
                                pass
                    
                if re.search('<B641',patent): #divisional_reissue
                    pass
                    #print patent

                if re.search('<B645',patent): #us_reexamination_reissue_merger
                    pass
                    #print patent
                    
                if re.search('<B650',patent): #related_publication; parent_pct_document
                    division = re.findall('<B650>(.*?)</B650>',patent)
                    for e,div in enumerate(division):
                        relation = 'parent_pct_document'
                        doc = re.findall('<DOC>(.*?)</DOC>',div)
                        for n in range(len(doc)):
                            dd = bs(doc[n])
                            pctdd = dd.date.pdat.string
                            pctdd = pctdd[:4]+'-'+pctdd[4:6]+'-'+pctdd[6:]
                            usreldoc[id_generator()] = [updnum,'related_publication','parent_pct_document',dd.dnum.pdat.string,dd.ctry.pdat.string,pctdd,'',str(enume),'']
                            enume+=1
                    
                if re.search('<B660',patent): #substitution
                    division = re.findall('<B660>(.*?)</B660>',patent)
                    for e,div in enumerate(division):
                        child = re.findall('<CDOC>.*?<PDAT>(.*?)</PDAT>.*?</CDOC>',div)
                        parentfull = re.findall('<PDOC>(.*?)</PDOC>',div)
                        parent_grantf = re.findall('<PPUB>(.*?)</PPUB>',div)
                        parent_stat = re.findall('<PSTA>(.*?)</PSTA>',div)
                        for n in range(len(child)):
                            usreldoc[id_generator()] = [updnum,'substitution','child_doc',child[n].replace("/",''),'','','','',str(enume),'']
                            enume+=1
                        for n in range(len(parentfull)):
                            try:
                                parent = bs(parentfull[n])
                                kind = parent.kind.pdat.string
                                relation = 'parent_doc'
                                relnum = parent.dnum.pdat.string.replace("/",'')
                                reldate = parent.date.pdat.string
                                reldate = reldate[:4]+'-'+reldate[4:6]+'-'+reldate[6:]
                                relctn = parent.ctry.pdat.string
                                status = parent_status[bs(parent_stat[n]).pdat.string]
                                usreldoc[id_generator()] = [updnum,'substitution',relation,relnum,relctn,reldate,status,str(enume),kind]
                                enume+=1
                            except:
                                pass
                        for n in range(len(parent_grantf)):
                            try:
                                parent_grant = bs(parent_grantf[n])
                                pgrant_num = parent_grant.dnum.pdat.string.replace('Des. ','D')
                                pgrant_ctry = parent_grant.ctry.pdat.string
                                pgrant_kind = parent_grant.kind.pdat.string.replace(' ','')
                                usreldoc[id_generator()] = [updnum,'substitution','parent_grant_document',pgrant_num,pgrant_ctry,'','',str(enume),pgrant_kind]
                                enume+=1
                            except:
                                pass
                    
                if re.search('<B680',patent): #us_provisional_application
                    division = re.findall('<B680(.*?)</B680',patent)
                    for e,div in enumerate(division):
                        relation = ''
                        doc = re.findall('<DOC>(.*?)</DOC>',div)
                        for n in range(len(doc)):
                            dd = bs(doc[n])
                            pctdd = dd.date.pdat.string
                            pctdd = pctdd[:4]+'-'+pctdd[4:6]+'-'+pctdd[6:]
                            usreldoc[id_generator()] = [updnum,'us_provisional_application',relation,dd.dnum.pdat.string.replace('/',''),'US',pctdd,'',str(enume),dd.kind.pdat.string]
                            enume+=1
                
                if re.search('<B690',patent): #related_publication
                    division = re.findall('<B690(.*?)</B690',patent)
                    for e,div in enumerate(division):
                        doc = re.findall('<DOC>(.*?)</DOC>',div)
                        for n in range(len(doc)):
                            dd = bs(doc[n])
                            pctdd = dd.date.pdat.string
                            pctdd = pctdd[:4]+'-'+pctdd[4:6]+'-'+pctdd[6:]
                            usreldoc[id_generator()] = [updnum,'related_publication','',dd.dnum.pdat.string,dd.ctry.pdat.string,pctdd,'',str(enume),dd.kind.pdat.string]
                            enume+=1
            except:
                pass
            
    
            #PCT data
            try:
                patent = avail_fields['B860']
                doc = re.findall('<B861.*?</B861',patent)
                date371 = re.findall('<B864.*?</B864',patent)
                for e,dd in enumerate(doc):
                    dd = re.search('<DOC>(.*?)</DOC>',dd).group(1)
                    dd = bs(dd)
                    pctnum = dd.dnum.pdat.string
                    pdate = dd.date.pdat.string
                    pdate = pdate[:4]+'-'+pdate[4:6]+'-'+pdate[6:]
                    pctry = 'WO'
                    try:
                        d371=re.search('<PDAT>(.*?)</PDAT>',date371[e]).group(1)
                        d371 = d371[:4]+'-'+d371[4:6]+'-'+d371[6:]
                    except:
                        d371=''
                    pctdata[id_generator()] = [updnum,pctnum,pdate,d371,pctry,'00',"pct_application",''] 
            except:
                pass
            
            try:
                patent = avail_fields['B870']
                doc = re.findall('<B871.*?</B871',patent)
                for e,dd in enumerate(doc):
                    dd = re.search('<DOC>(.*?)</DOC>',dd).group(1)
                    dd = bs(dd)
                    pctnum = dd.dnum.pdat.string
                    pdate = dd.date.pdat.string
                    pdate = pdate[:4]+'-'+pdate[4:6]+'-'+pdate[6:]
                    pctry = 'WO'
                    pctdata[id_generator()] = [updnum,pctnum,pdate,'',pctry,'A',"wo_grant",'']
            except:
                pass
                
            ### priority data
            try:
                patent = avail_fields['B300']
                nums = re.findall('<B310>.*?<PDAT>(.*?)</PDAT>.*?</B310>',patent)
                dates = re.findall('<B320>.*?<PDAT>(.*?)</PDAT>.*?</B320>',patent)
                ctrys = re.findall('<B330>.*?<PDAT>(.*?)</PDAT>.*?</B330>',patent)
                for n in range(len(nums)):
                    prioritydata[id_generator()] = [updnum,str(n),'',nums[n],dates[n],ctrys[n]]
            except:
                pass
                
            #Number of claims
            #numclaims = 'NULL'
            try:
                patent = avail_fields['B570'].split('\n')
                exemplary_list = []
                for line in patent:
                    if line.startswith('<B577>'):    
                        numclaims = re.search('<PDAT>(.*?)</PDAT>',line).group(1)
                    if line.startswith('<B578US>'):
                        exemplary_list.append(re.search('<PDAT>(.*?)</PDAT>',line).group(1))
                        #print exemplaryclaim
            except:
                pass
 
            patent_id = updnum
 
            if numfigs!='' or numsheets!='':
                figureinfo[id_generator()] = [updnum,numfigs,numsheets]
            application[apptype+'/'+appnum[2:]] = [patent_id,apptype,appnum,patcountry,appdate]
            
            # Claims data
            try:
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
                    need = _char.sub(_char_unescape,need)
                    need = _char.sub(_char_unescape,need)
                    exemplary = False
                    if str(clnum) in exemplary_list:
                        exemplary = True
                    claims[id_generator()] = [patent_id,need,dependent,str(clnum), exemplary]
            except:
                pass
            
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
                    rule47 = 'NULL'
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
                    rawlocation[id_generator()] = [loc_idd,"NULL",invtcity,invtstate,invtcountry]
                    
                    if fname == "NULL" and lname == "NULL":
                        pass
                    else:
                        rawinventor[id_generator()] = [patent_id,"NULL",loc_idd,fname,lname,str(n),'']
            except:
                pass

            #ASSG - can be several
            try:
                #if "B731" in avail_fields:
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
                    rawlocation[id_generator()] = [loc_idd,"NULL",assgcity,assgstate,assgcountry]
                    rawassignee[id_generator()] = [patent_id,"NULL",loc_idd,assgtype,assgfname,assglname,assgorg,str(n)]
            except:
                if "B731" in avail_fields:
                    print "Problem with assignees patent ", updnum

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
                            
                ipcr[id_generator()] = [patent_id,"NULL",intsec,mainclass,subclass, group,subgroup,"NULL","NULL","NULL","NULL","NULL",ipcrversion,str(num)]
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
                    mainclassdata[origmainclass] = [origmainclass]
                    uspc[id_generator()] = [patent_id,origmainclass,origmainclass+'/'+origsubclass,'0']
                    subclassdata[origmainclass+'/'+origsubclass] = [origmainclass+'/'+origsubclass]
                
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
                        mainclassdata[crossrefmain] = [crossrefmain]
                        uspc[id_generator()] = [patent_id,crossrefmain,crossrefmain+'/'+crossrefsub,str(n)]
                        subclassdata[crossrefmain+'/'+crossrefsub] = [crossrefmain+'/'+crossrefsub]
            except:
                pass
    
            # U.S. Patent Reference - can be several
                    
            try:
                uspatref = avail_fields['B561'].split("\n\n\n\n\n")
                uspatref = [a for a in uspatref if a != ">\r\n<"]
                uspatseq = 0
                forpatseq = 0
                for n in range(len(uspatref)):
                    refpatnum = 'NULL'
                    refpatname = 'NULL'
                    refpatdate = 'NULL'
                    refpatclass = 'NULL'
                    refpatcountry = 'US'
                    citedby = 'NULL'
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
                        
                        citedbysear = re.search('<CITED-BY-(.*?)/>',line)
                        if citedbysear:
                            citedby = 'cited by '+citedbysear.group(1).lower()
                        
                    if refpatcountry != 'US':
                        if refpatnum != "NULL":
                            foreigncitation[id_generator()] = [patent_id,refpatdate,refpatnum,refpatcountry,citedby,str(forpatseq)]
                            forpatseq+=1
                    else:
                        if refpatnum != "NULL":
                            uspatentcitation[id_generator()] = [patent_id,refpatnum,refpatdate,refpatname ,refpatkind,refpatcountry,citedby,str(uspatseq)]
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

            try:     
                sequence = 0
                id_group = "NULL"
                if "B748US" in avail_fields:
                    grouping = avail_fields["B748US"]
                    id_group = re.search('<PDAT>(.*?)</PDAT>', grouping).group(1)
                
                if "B746" in avail_fields:
                    pexfname = "NULL"
                    pexlname = "NULL"
                    prim_examiners = avail_fields['B746'].split("\n")
                    for line in prim_examiners:
                        if line.startswith("<NAM>"):
                            pexfname = re.search('<FNM><PDAT>(.*?)</PDAT>',line).group(1)
                            pexlname = re.search('<SNM><STEXT><PDAT>(.*?)</PDAT>',line).group(1)
                    examiner[id_generator()] = [patent_id, pexfname, pexlname, "primary", id_group]
                if "B747" in avail_fields:
                    aexfname = "NULL"
                    aexlname = "NULL"
                    assist_examiners = avail_fields['B747'].split("\n")
                    for line in assist_examiners:
                        if line.startswith("<NAM>"):
                            aexfname = re.search('<FNM><PDAT>(.*?)</PDAT>',line).group(1)
                            aexlname = re.search('<SNM><STEXT><PDAT>(.*?)</PDAT>',line).group(1)
                    examiner[id_generator()] = [patent_id, aexfname, aexlname, "assistant", id_group]
            except:
                pass

            #Detailed description
            detdesc = None
            try:
                patent = avail_fields['DETDESC']
                detdesc = re.search('<BTEXT>(.*?)</BTEXT>',patent,re.DOTALL).group(1)
                detdesc = detdesc.replace('<H LVL="1">','')
                detdesc = detdesc.replace('</H>','')
                detdesc = bs(re.sub('\s+',' ',detdesc))
                detdesc = re.sub('<.*?>','',detdesc.get_text())
                try:
                    detdesc = detdesc.decode('utf-8','ignore').encode('utf-8','ignore')
                except:
                    detdesc = unidecode(detdesc)
                detail_desc_tect[id_generator()] = [patent_id,detdesc, len(detdesc)]
                #print detdesc
            except:
                pass

            try:
                patent = avail_fields['DRWDESC']
                lines = patent.split("\n")
                draw_seq = 0
                for line in lines:
                    if line.startswith("<PARA") or line.startswith("<H"):
                        drawdesc = re.findall('<PDAT>(.*?)</PDAT>', line, re.DOTALL)
                        desc = " ".join(drawdesc)
                        drawdescdata[id_generator()] = [patent_id, desc, str(draw_seq)]
                        draw_seq +=1
            except:
                pass

            #Drawing description
            # try:
            #     patent = avail_fields['DRWDESC']
            #     drawdesc = bs(re.search('<BTEXT>(.*?)</BTEXT>',patent,re.DOTALL).group(1))
            #     drawdesc = drawdesc.findAll('pdat')
            #     for e,draw_description in enumerate(drawdesc):
            #         drawdescdata[id_generator()] =[patent_id, re.sub('\s+',' ',re.sub('<.*?>','',draw_description.get_text().decode('utf-8','ignore').encode('utf-8','ignore'))),str(e)] 
            # except:
            #     pass

            #Brief summary
            try:
                bsum = 'NULL'
                if 'BRFSUM' in avail_fields:
                    patent = avail_fields['BRFSUM']

                    bsum = re.search('<BTEXT>(.*?)</BTEXT>',patent,re.DOTALL).group(1)
                    bsum = bsum.split('<STEXT>')
                    #if len(bsum) < 2: #some have ptext instead ofr stext so don't get split on stext; may need to look at this long term
                    if re.search('RELATED APPLICATION',bsum[0]):
                        relapp = re.findall('<PARA.*?<PDAT>(.*?)</PDAT>',bsum[0])
                        relapp = ' '.join(relapp)
                        relapp = h.unescape(unidecode(relapp))
                        if not re.search('None|Not applicable',relapp,re.I):
                            relappdata[id_generator()] = [updnum,re.sub('\s+',' ',relapp)]
                        bsum = '<H LVL="1"><STEXT>'+'<STEXT>'.join(bsum[1:])
                    else:
                        bsum = '<H LVL="1"><STEXT>'+'<STEXT>'.join(bsum)
                    ### need to separate relapp
                    bsum = re.sub('\s+',' ',unidecode(re.sub('<.*?>','',bs(bsum).get_text())))
                    if bsum == "[]":
                        bsum = 'NULL'
            except:
                 pass

            try:
                if "RELAPP" in avail_fields:
                    patent = avail_fields["RELAPP"]
                    relapp = re.findall('<PARA.*?<PDAT>(.*?)</PDAT>',patent)
                    relapp = ' '.join(relapp)
                    relapp = h.unescape(unidecode(relapp))
                    if not re.search('None|Not applicable',relapp,re.I):
                        relappdata[id_generator()] = [updnum,re.sub('\s+',' ',relapp)]
            except:
                pass

            
            if patkind in type_kind:
                patentdata[patent_id] = [type_kind[patkind],updnum,'US',issdate,abst,title,patkind,numclaims,d]
            else:
                patentdata[patent_id] = ['NULL',updnum,'US',issdate,abst,title,patkind,numclaims,d]
            
            brf_sum_textfile = csv.writer(open(os.path.join(fd2,'brf_sum_text.csv'),'ab'),delimiter='\t')
            brf_sum_textfile.writerow([id_generator(),patent_id, bsum])
            
            patfile = csv.writer(open(os.path.join(fd2,'patent.csv'),'ab'),delimiter='\t')
            for k,v in patentdata.items():
                patfile.writerow([k]+v)
            
            det_desc_textfile = csv.writer(open(os.path.join(fd2,'detail_desc_text.csv'),'ab'),delimiter='\t')
            for k,v in detail_desc_text.items():
                    det_desc_textfile.writerow([k], v)
            
            draw_desc_textfile = csv.writer(open(os.path.join(fd2,'draw_desc_text.csv'),'ab'),delimiter='\t')
            for k,v in drawdescdata.items():
                draw_desc_textfile.writerow([k]+v)
            
            figurefile = csv.writer(open(os.path.join(fd2,'figures.csv'),'ab'),delimiter='\t')
            for k,v in figureinfo.items():
                figurefile.writerow([k]+v)
             
            rel_app_textfile = csv.writer(open(os.path.join(fd2,'rel_app_text.csv'),'ab'),delimiter='\t')
            for k,v in relappdata.items():
                v = [vv.encode('utf-8','ignore') for vv in v]
                rel_app_textfile.writerow([k]+v)
                
            pct_datafile = csv.writer(open(os.path.join(fd2,'pct_data.csv'),'ab'),delimiter='\t')
            for k,v in pctdata.items():
                pct_datafile.writerow([k]+v)
            
            usreldocfile = csv.writer(open(os.path.join(fd2,'usreldoc.csv'),'ab'),delimiter='\t')
            for k,v in usreldoc.items():
                usreldocfile.writerow([k]+v)
            
            forpriorityfile = csv.writer(open(os.path.join(fd2,'foreign_priority.csv'),'ab'),delimiter='\t')
            for k,v in prioritydata.items():
                forpriorityfile.writerow([k]+v)
            
            appfile = csv.writer(open(os.path.join(fd2,'application.csv'),'ab'),delimiter='\t')
            for k,v in application.items():
                appfile.writerow([k]+v)
            
            claimsfile = csv.writer(open(os.path.join(fd2,'claim.csv'),'ab'),delimiter='\t')
            for k,v in claims.items():
                claimsfile.writerow([k]+v)
            
            rawinvfile = csv.writer(open(os.path.join(fd2,'rawinventor.csv'),'ab'),delimiter='\t')
            for k,v in rawinventor.items():
                rawinvfile.writerow([k]+v)
    
            rawassgfile = csv.writer(open(os.path.join(fd2,'rawassignee.csv'),'ab'),delimiter='\t')
            for k,v in rawassignee.items():
                rawassgfile.writerow([k]+v)
    
            # """
            # usappcitfile = csv.writer(open(os.path.join(fd2,'usapplicationcitation.csv'),'ab'))
            # for k,v in usappcitation.items():
            #     usappcitfile.writerow([k]+v)
            # """
            
            ipcrfile = csv.writer(open(os.path.join(fd2,'ipcr.csv'),'ab'),delimiter='\t')
            for k,v in ipcr.items():
                ipcrfile.writerow([k]+v)
            
            uspcfile = csv.writer(open(os.path.join(fd2,'uspc.csv'),'ab'),delimiter='\t')
            for k,v in uspc.items():
                uspcfile.writerow([k]+v)
            
            uspatentcitfile = csv.writer(open(os.path.join(fd2,'uspatentcitation.csv'),'ab'),delimiter='\t')
            for k,v in uspatentcitation.items():
                uspatentcitfile.writerow([k]+v)
            
            foreigncitfile = csv.writer(open(os.path.join(fd2,'foreigncitation.csv'),'ab'),delimiter='\t')
            for k,v in foreigncitation.items():
                foreigncitfile.writerow([k]+v)
            
            otherreffile = csv.writer(open(os.path.join(fd2,'otherreference.csv'),'ab'),delimiter='\t')
            for k,v in otherreference.items():
                otherreffile.writerow([k]+v)
            
            rawlawyerfile = csv.writer(open(os.path.join(fd2,'rawlawyer.csv'),'ab'),delimiter='\t')
            for k,v in rawlawyer.items():
                rawlawyerfile.writerow([k]+v)

            examinerfile = csv.writer(open(os.path.join(fd2,'examiner.csv'),'ab'),delimiter='\t')
            for k,v in examiner.items():
                examinerfile.writerow([k]+v)
            
            us_term_of_grantfile = csv.writer(open(os.path.join(fd2,'us_term_of_grant.csv'),'ab'),delimiter='\t')
            for k,v in termofgrant.items():
                us_term_of_grantfile.writerow([k]+v)
            
            

    rawlocfile = csv.writer(open(os.path.join(fd2,'rawlocation.csv'),'ab'),delimiter='\t')
    for k,v in rawlocation.items():
        rawlocfile.writerow(v)

    mainclassfile = csv.writer(open(os.path.join(fd2,'mainclass.csv'),'ab'),delimiter='\t')
    for k,v in mainclassdata.items():
        mainclassfile.writerow(v)
    
    subclassfile = csv.writer(open(os.path.join(fd2,'subclass.csv'),'ab'),delimiter='\t')
    for k,v in subclassdata.items():
        subclassfile.writerow(v)

    print numi
