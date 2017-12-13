def parse_patents(fd,fd2):
    import re,csv,os,codecs,zipfile,traceback
    import string,random,HTMLParser
    
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
    
    reldoctype = [
                'continuation-in-part',
                'continuation_in_part',
                'continuing_reissue',
                'division',
                'reissue',
                'related_publication',
                'substitution',
                'us_provisional_application',
                'us_reexamination_reissue_merger',
                'continuation'
                ]
    
    fd+='/'
    fd2+='/'
    diri = os.listdir(fd)
    diri = [d for d in diri if d.endswith('zip')]

    #Initiate HTML Parser for unescape characters
    h = HTMLParser.HTMLParser()

    #Remove all files from output dir before writing
    outdir = os.listdir(fd2)
    for oo in outdir:
        os.remove(os.path.join(fd2,oo))


    det_desc_textfile = open(os.path.join(fd2,'detail_desc_text.csv'), 'wb')
    det_desc_textfile.write(codecs.BOM_UTF8)
    det_desc = csv.writer(det_desc_textfile, delimiter='\t')
    det_desc.writerow(['uuid', 'patent_id', 'text', 'length'])

   
    det_desc_textfile.close()

    
    loggroups = ['PATN','INVT','ASSG','PRIR','REIS','RLAP','CLAS','UREF','FREF','OREF','LREP','PCTA','ABST','GOVT','PARN','BSUM','DRWD','DETD','CLMS','DCLM']
    
    numii = 0
    rawlocation = {}
    mainclassdata = {}
    subclassdata = {}

    for d in diri:
        print d
        inp = zipfile.ZipFile(os.path.join(fd,d))
        for i in inp.namelist():
            infile = h.unescape(inp.open(i).read().decode('utf-8','ignore').replace('&angst','&aring')).replace("\r","").split('PATN')
            del infile[0]

        for i in infile:
            numii+=1  
            i = i.encode('utf-8','ignore')
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
            examiner = {}
            foreigncitation = {}
            ipcr = {}
            otherreference = {}
            patentdata = {}
            pctdata = {}
            prioritydata = {}
            rawassignee = {}
            rawinventor = {}
            rawlawyer = {}
            usappcitation = {}
            uspatentcitation = {}
            uspc = {}
            usreldoc = {}
            figureinfo = {}
            termofgrant = {}
            drawdescdata = {}
            relappdata = {}
            
            ###                PARSERS FOR LOGICAL GROUPS                  ###

            
            try:
                numfigs = ''
                numsheets = ''
                disclaimerdate=''
                termpat = ''
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
                        patent_id = updnum
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
                        
                        
                    #Figure and sheet info
                    if line.startswith('NDR'):
                        numsheets = re.search('NDR\s+(.*?)$',line).group(1)
                    if line.startswith('NFG'):
                        numfigs = re.search('NFG\s+(.*?)$',line).group(1)
                    
                    #U.S. term of grant
                    if line.startswith('TRM'):
                        termpat = re.sub('[\n\t\r\f]+','',re.search('TRM\s+(.*?)$',line).group(1))
                    if line.startswith('DCD'):
                        disclaimerdate = re.sub('[\n\t\r\f]+','',re.search('DCD\s+(.*?)$',line).group(1))
                        disclaimerdate = disclaimerdate[:4]+'-'+disclaimerdate[4:6]+'-'+disclaimerdate[6:]
                    
                    # Examiner info
                    sequence = 0
                    if line.startswith("EXA"):
                        sequence +=1
                        assistexam = re.search('EXA\s+(.*?)$',line).group(1).split("; ")
                        assistexamfname = assistexam[1]
                        assistexamlname = assistexam[0]
                        examiner[id_generator()] = [patent_id,assistexamfname,assistexamlname,"assistant", "NULL"]
                    if line.startswith("EXP"):
                        sequence +=1
                        primexam = re.search('EXP\s+(.*?)$',line).group(1).split("; ")
                        primexamfname = primexam[1]
                        primexamlname = primexam[0]
                        examiner[id_generator()] = [patent_id,primexamfname,primexamlname,"primary", "NULL"]
                    if line.startswith("ECL"):                     
                        exemplary = re.search('ECL\s+(.*?)$',line).group(1)
                        exemplary_list = exemplary.split(",")
            except:
                pass
            
            patent_id = updnum

            # Detail description
            detdesc = 'NULL'
            try:
                detdesc = re.sub('PAR\s+',' ',avail_fields['DETD'])
                detdesc = re.sub('PAC\s+',' ',detdesc)
                detdesc = re.sub('PA\d+\s+',' ',detdesc)
                detdesc = re.sub('TBL\s+','',detdesc)
                detdesc = re.sub('\s+',' ',detdesc)
            except:
                pass

            det_desc_textfile = csv.writer(open(os.path.join(fd2,'detail_desc_text.csv'),'ab'),delimiter='\t')
            det_desc_textfile.writerow([id_generator(),patent_id,detdesc, len(detdesc)])



parse_patents('H:/share/Science Policy Portfolio/PatentsView IV/Data/1976-2001', 'D:/PV_Patches/DetailDesc/1976_Output')