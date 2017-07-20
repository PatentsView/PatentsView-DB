import re
import numpy as np
import re,csv,os,codecs 
import string,random 
from bs4 import BeautifulSoup as bs 
import HTMLParser 
import htmlentitydefs 
import copy
import sys


def parse_patents(fd, fd2):
    _char = re.compile(r'&(\w+?);')
    
    # Generate some extra HTML entities
    defs=htmlentitydefs.entitydefs
    defs['apos'] = "'"
    #this is slgihgly hacky but allows it to run from update script
    try:
        entities = open('uspto_parsers/htmlentities').read().split('\n')
    except:
        #entities = open('Raw_Data_Parsers/uspto_parsers/htmlentities').read().split('\n')
        pass
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
    diri = [d for d in diri if re.search('XML',d,re.I)]
    print "Just to check the list of XML files is", diri
    
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
    #also no inventor id in UC Berkeley
    rawinv.writerow(['uuid','patent_id', 'inventor_id','rawlocation_id','name_first','name_last', 'sequence', 'rule_47'])
    
    
    rawassgfile = open(os.path.join(fd2,'rawassignee.csv'),'wb')
    rawassgfile.write(codecs.BOM_UTF8)
    rawassg = csv.writer(rawassgfile,delimiter='\t')
    #assignee_id not in UC Berkeley Parser
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
    uspatcit.writerow(['uuid','patent_id','citation_id','date','name','kind','country','category','sequence', 'classification'])
    
    usappcitfile = open(os.path.join(fd2,'usapplicationcitation.csv'),'wb')
    usappcitfile.write(codecs.BOM_UTF8)
    usappcit = csv.writer(usappcitfile, delimiter='\t')
    usappcit.writerow(['uuid','patent_id','application_id','date','name','kind','number','country','category','sequence'])
    
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
    #no lawyer id in UC Berkeley parser
    rawlawyer.writerow(['uuid', 'lawyer_id','patent_id','name_first','name_last','organization','country','sequence'])

    mainclassfile = open(os.path.join(fd2,'mainclass.csv'),'wb')
    mainclassfile.write(codecs.BOM_UTF8)
    mainclass = csv.writer(mainclassfile,delimiter='\t')
    mainclass.writerow(['id'])

    subclassfile = open(os.path.join(fd2,'subclass.csv'),'wb')
    subclassfile.write(codecs.BOM_UTF8)
    subclass = csv.writer(subclassfile,delimiter='\t')
    subclass.writerow(['id'])

    examinerfile = open(os.path.join(fd2,'raw_examiner.csv'),'wb')
    examinerfile.write(codecs.BOM_UTF8)
    exam = csv.writer(examinerfile,delimiter='\t')
    exam.writerow(['id','patent_id','fname','lname','role','group'])

    forpriorityfile = open(os.path.join(fd2,'foreign_priority.csv'),'wb')
    forpriorityfile.write(codecs.BOM_UTF8)
    forpriority = csv.writer(forpriorityfile,delimiter='\t')
    forpriority.writerow(['uuid', 'patent_id', "sequence", "kind", "app_num", "app_date", "country"])

    us_term_of_grantfile = open(os.path.join(fd2,'us_term_of_grant.csv'), 'wb')
    us_term_of_grantfile.write(codecs.BOM_UTF8)
    us_term_of_grant = csv.writer(us_term_of_grantfile, delimiter='\t')
    us_term_of_grant.writerow(['uuid','patent_id','lapse_of_patent', 'disclaimer_date','term_disclaimer', 'term_grant', 'term_ext'])


    usreldocfile = open(os.path.join(fd2,'usreldoc.csv'), 'wb')
    usreldocfile.write(codecs.BOM_UTF8)
    usrel = csv.writer(usreldocfile, delimiter='\t')
    usrel.writerow(['uuid', 'patent_id', 'doc_type',  'relkind', 'reldocno', 'relcountry', 'reldate',  'parent_status', 'rel_seq','kind'])

    draw_desc_textfile = open(os.path.join(fd2,'draw_desc_text.csv'), 'wb')
    draw_desc_textfile.write(codecs.BOM_UTF8)
    drawdesc = csv.writer(draw_desc_textfile, delimiter='\t')
    drawdesc.writerow(['uuid', 'patent_id', 'text'])

    brf_sum_textfile = open(os.path.join(fd2,'brf_sum_text.csv'), 'wb')
    brf_sum_textfile.write(codecs.BOM_UTF8)
    brf_sum = csv.writer(brf_sum_textfile, delimiter='\t')
    brf_sum.writerow(['uuid', 'patent_id', 'text'])

    rel_app_textfile = open(os.path.join(fd2,'rel_app_text.csv'), 'wb')
    rel_app_textfile.write(codecs.BOM_UTF8)
    rel_app = csv.writer(rel_app_textfile, delimiter='\t')
    rel_app.writerow(['uuid', 'patent_id', 'text'])

    det_desc_textfile = open(os.path.join(fd2,'detail_desc_text.csv'), 'wb')
    det_desc_textfile.write(codecs.BOM_UTF8)
    det_desc = csv.writer(det_desc_textfile, delimiter='\t')
    det_desc.writerow(['uuid', 'patent_id', "text"])


    non_inventor_applicantfile = open(os.path.join(fd2,'non_inventor_applicant.csv'),'wb')
    non_inventor_applicantfile.write(codecs.BOM_UTF8)
    noninventorapplicant = csv.writer(non_inventor_applicantfile,delimiter='\t')
    noninventorapplicant.writerow(['uuid', 'patent_id', "location_id", "last_name", "first_name", "org_name", "sequence", "designation", "applicant_type"])

    pct_datafile = open(os.path.join(fd2,'pct_data.csv'), 'wb')
    pct_datafile.write(codecs.BOM_UTF8)
    pct_data = csv.writer(pct_datafile, delimiter='\t')
    pct_data.writerow(['uuid', 'patent_id', 'rel_id', 'date', '371_date', 'country', 'kind', "doc_type", "102_date"])

    botanicfile = open(os.path.join(fd2,'botanic.csv'), 'wb')
    botanicfile.write(codecs.BOM_UTF8)
    botanic_info = csv.writer(botanicfile, delimiter='\t')
    botanic_info.writerow(['uuid', 'patent_id', 'latin_name', "variety"])

    figurefile = open(os.path.join(fd2,'figures.csv'), 'wb')
    figurefile.write(codecs.BOM_UTF8)
    figure_info = csv.writer(figurefile, delimiter='\t')
    figure_info.writerow(['uuid', 'patent_id', 'num_figs', "num_sheets"])
    '''

    new_titles= open(os.path.join(fd2,'new_titles.csv'), 'wb')
    new_titles.write(codecs.BOM_UTF8)
    new_titles_info= csv.writer(new_titles, delimiter='\t')
    new_titles_info.writerow(['patent_id', 'title'])
    
    new_titles.close()
    '''
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
    examinerfile.close()
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
    usappcitfile.close()


    loggroups = ["publication-reference", "application-reference", "us-application-series-code", "number-of-claims", "claims", "?BRFSUM", "?DETDESC", "pct-or-regional-filing-data", "us-botanic",
                 "citations", "assignees", "inventors", "agents", "us-related-documents", "applicants", "us-applicants", 'description-of-drawings', 'description', "pct-or-regional-publishing-data", "us-patent-grant",
                "abstract", "invention-title","classification-national","classification-ipc","classification-ipcr", "examiners", "references-cited", "us-references-cited", "priority-claim", "us-term-of-grant","us-exemplary-claim",
                "us-issued-on-continued-prosecution-application", "field-of-search", "us-field-of-classification-search"]

    numi = 0
    
    #Rawlocation, mainclass and subclass should write after all else is done to prevent duplicate values
    rawlocation = {}
    mainclassdata = {}
    subclassdata = {}
       
    for d in diri:
        print d
        infile = open(fd+d,'rb').read().decode('utf-8','ignore').replace('&angst','&aring')
        infile = infile.encode('utf-8','ignore')
        infile = _char.sub(_char_unescape,infile)
        #infile = h.unescape(infile).encode('utf-8')
        infile = infile.split('<!DOCTYPE')
        del infile[0]
        
        numi+=len(infile)
        
        for i in infile:                                       
            avail_fields = {}
            #parser for logical groups
            for j in loggroups:
                list_for_group = i.split("\n<"+j)
                #this only processes the groups that split it into two exactly (meaning it exists)
                #and also not the logical groups that appear multiple times
                #there can be multiple agents, for example, but they all show up in on "agents" group
                if len(list_for_group) == 2: #this only parses the groups that occur once (so not when it doesn't exist)
                    results = list_for_group[1].split(j+">")
                    avail_fields[j] = results[0]
                if len(list_for_group) > 2:
                    # i think too many classification-national files are being parsed
                    items = []
                    for k in list_for_group[1:]: #drop the first group
                        results = k.split(j+">")
                        items.append(results[0])
                    avail_fields[j] = items
            #special log group parsing for strangely named fields
            try:
                desc = i.split("DETDESC")
                avail_fields["DETDESC"] = desc[1]
            except:
                pass

            try:
                brf = i.split("BRFSUM")
                avail_fields["BRFSUM"] = brf[1]
            except:
                pass
            try:
                relapp = i.split("RELAPP")
                avail_fields["RELAPP"] = relapp[1]
            except:
                pass

            try:
                figures = i.split("<figures>")
                figures = figures[1].split("</figures>")[0]
                avail_fields["figures"] = figures
            except:
                pass
    
            try:
                sir_tag = re.search('us-sir-flag sir-text="(.*?)"', i).group(1)
            except:
                pass
            rule_47 = 0
            if "rule-47-flag" in i:
                rule_47 = 1


            
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
            examiner = {}
            non_inventor_applicant = {}
            us_term_of_grant = {}
            for_priority = {}
            draw_desc_text = {}
            brf_sum_text = {}
            detail_desc_text = {}
            rel_app_text = {}
            regularcitation ={}
            pct_data = {}
            botanic_data = {}
            figure_data = {}
            new_title = {}


            ###                PARSERS FOR LOGICAL GROUPS                  ###
            try:
                publication = avail_fields['publication-reference'].split("\n")
                for line in publication:
                    if line.startswith("<doc-number"):
                        docno = re.search('<doc-number>(.*?)</doc-number>',line).group(1)
                    if line.startswith("<kind"):
                        patkind = re.search('<kind>(.*?)</kind>',line).group(1)
                    if line.startswith("<country"):
                        patcountry = re.search('<country>(.*?)</country>',line).group(1)
                    if line.startswith("<date"):
                        issdate = re.search('<date>(.*?)</date>',line).group(1)
                        if issdate[6:] != "00":
                            issdate = issdate[:4]+'-'+issdate[4:6]+'-'+issdate[6:]
                        else:
                            issdate = issdate[:4]+'-'+issdate[4:6]+'-'+'01'
                            year = issdate[:4]
                num = re.findall('\d+', docno)
                num = num[0] #turns it from list to string
                if num[0].startswith("0"):
                    num = num[1:]
                    let = re.findall('[a-zA-Z]+', docno)
                if let:
                    let = let[0]#list to string
                    docno = let +num
                else:
                    docno = num
                patent_id = docno

            except:
                pass

            try:
                abst = None
                for_abst = avail_fields['abstract']
                split_lines = for_abst.split("\n")
                abst = re.search('">(.*?)</p', split_lines[1]).group(1)
            except:
                pass
            
            #try:
            title = None
            if 'invention-title' in avail_fields:
                title = re.search('>(.*?)<',avail_fields["invention-title"]).group(1)
                if title == '':
                    text = avail_fields['invention-title']
                    title = text[text.find('>')+1 :text.rfind('<')]
                    new_title[patent_id] = title

            try:
                series_code = "NULL"
                app_series_code = avail_fields['us-application-series-code']
                series_code = re.search(">(.*?)</", app_series_code).group(1)
            except:
                pass

            try:
                application_list = avail_fields['application-reference'].split("\n")
                apptype = None
                for line in application_list:
                    if line.startswith("<doc-number"):
                        appnum = re.search('<doc-number>(.*?)</doc-number>',line).group(1)
                    if line.startswith("<country"):
                        appcountry = re.search('<country>(.*?)</country>',line).group(1)
                    if line.startswith("<date"):
                        appdate = re.search('<date>(.*?)</date>',line).group(1)
                        if appdate[6:] != "00":
                            appdate = appdate[:4]+'-'+appdate[4:6]+'-'+ appdate[6:]
                        else:
                            appdate = appdate[:4]+'-'+appdate[4:6]+'-'+'01'
                            year = appdate[:4]
                    if line.startswith(" appl-type"):
                        apptype = re.search('"(.*?)"', line).group(1)
                #modeled on the 2005 approach because apptype can be none in 2005, amking the 2002 approach not work
                #but using the full application number as done in 2005
                application[appdate[:4]+"/"+appnum] = [patent_id, series_code, appnum, patcountry, appdate]
            except:
                pass
            
            try:
                numclaims = 0
                if 'number-of-claims' in avail_fields:
                    no_claims = avail_fields['number-of-claims'].split("\n")
                    for line in no_claims:
                        numclaims = re.search('>(.*?)</', line).group(1)

            except:
                pass

            if "us-exemplary-claim" in avail_fields:
                if type(avail_fields["us-exemplary-claim"]) ==str: #if there is only one claim make list for processing
                        claim = [avail_fields["us-exemplary-claim"]]
                else:
                    claim = avail_fields["us-exemplary-claim"]
                exemplary_claims = []
                for item in claim:
                    exemplary_claims.append(re.search(">(.*?)<", item).group(1))
            

            #claims_list = []
            try:
                claimsdata = re.search('<claims.*?>(.*?)</claims>',i,re.DOTALL).group(1) 
                claim_number = re.finditer('<claim id(.*?)>',claimsdata,re.DOTALL) 
                claims_iter = re.finditer('<claim.*?>(.*?)</claim>',claimsdata,re.DOTALL) 
                
                claim_info = []
                claim_num_info=[]
                for i,claim in enumerate(claims_iter):
                    claim = claim.group(1)
                    this_claim = []
                    try:
                        dependent = re.search('<claim-ref idref="CLM-(\d+)">',claim).group(1)
                        dependent = int(dependent)
                        #this_claim.append(dependent)
                    except:
                        dependent = None
                    text = re.sub('<.*?>|</.*?>','',claim)
                    text = re.sub('[\n\t\r\f]+','',text)
                    text = re.sub('^\d+\.\s+','',text)
                    text = re.sub('\s+',' ',text)
                    sequence = i+1 # claims are 1-indexed
                    this_claim.append(text)
                    #this_claim.append(sequence)
                    this_claim.append(dependent)
                    claim_info.append(this_claim)
                    
                for i, claim_num in enumerate(claim_number):
                    claim_num = claim_num.group(1)
                    clnum= re.search('num="(.*?)"', claim_num).group(1)
                    claim_num_info.append(clnum)
                for i in range(len(claim_info)):
                    #this adds a flag for whether this is an exemplary claim (can be several)
                    exemplary = False
                    #strip leading zeros
                    if claim_num_info[i].lstrip("0") in exemplary_claims:
                        exemplary = True
                    #this would be clearer using a dictionary. it is patent id, text, dependnecy, number
                    claims[id_generator()] = [patent_id,claim_info[i][0],claim_info[i][1],str(claim_num_info[i]), exemplary]
            except:
                pass

            #specially check this
            try:
                if 'classification-ipcr' in avail_fields:
                    ipcr_classification = avail_fields["classification-ipcr"]
                else:
                    ipcr_classification = avail_fields["classification-ipc"]
                if type(ipcr_classification) is str:#makes a single ipc classification into a list so it processes properly
                    ipcr_classification = [ipcr_classification]
                for j in ipcr_classification:
                    num = 0
                    class_level = "NULL"
                    section = "NULL"
                    mainclass = "NULL"
                    subclass = "NULL"
                    group = "NULL"
                    subgroup = "NULL"
                    symbol_position = "NULL"
                    classification_value = "NULL"
                    classification_status = "NULL"
                    classification_source = "NULL"
                    action_date = ""
                    ipcrversion = ""
                    ipcr_fields = j.split("\n")
                    for line in ipcr_fields:
                        if line.startswith("<classification-level"):
                            class_level = re.search('<classification-level>(.*?)</classification-level>',line).group(1)
                        if line.startswith("<section"):
                            section = re.search('<section>(.*?)</section>', line).group(1)
                        if line.startswith("<class>"):
                            mainclass = re.search('<class>(.*?)</class>', line).group(1)
                        if line.startswith("<subclass"):
                            subclass = re.search('<subclass>(.*?)</subclass>', line).group(1)
                        if line.startswith("<main-group"):
                            group = re.search('<main-group>(.*?)</main-group>', line).group(1)
                        if line.startswith("<subgroup"):
                            subgroup = re.search('<subgroup>(.*?)</subgroup>', line).group(1)
                        if line.startswith("<ipc-version-indicator"):
                            ipcrversion = re.search('<date>(.*?)</date>', line).group(1)
                            if ipcrversion[6:] != "00":
                                    ipcrversion = ipcrversion[:4]+'-'+ipcrversion[4:6]+'-'+ipcrversion[6:]
                            else:
                                ipcrversion = ipcrversion[:4]+'-'+ipcrversion[4:6]+'-'+'01' 
                        if line.startswith("<symbol-position"):
                            symbol_position = re.search('<symbol-position>(.*?)</symbol-position>', line).group(1)
                        if line.startswith("<classification-value"):
                            classification_value = re.search('<classification-value>(.*?)</classification-value>', line).group(1)
                        if line.startswith("<classification-status"):
                            classification_status = re.search('<classification-status>(.*?)</classification-status>', line).group(1)
                        if line.startswith("<classification-data-source"):
                            classification_source = re.search('<classification-data-source>(.*?)</classification-data-source>', line).group(1)
                        if line.startswith("<action-date>"):
                            action_date = re.search('<date>(.*?)</date>', line).group(1)
                            if action_date[6:] != "00":
                                    action_date = action_date[:4]+'-'+action_date[4:6]+'-'+action_date[6:]
                            else:
                                action_date = action_date[:4]+'-'+action_date[4:6]+'-'+'01'                    #if all the valu[es are "NULL", set will have len(1) and we can ignore it. This is bad
                    values = set([section, mainclass, subclass, group, subgroup])
                    if len(values)>1: #get rid of the situation in which there is no data
                        ipcr[id_generator()] = [patent_id,class_level,section ,mainclass,subclass, group,subgroup,symbol_position, classification_value, classification_status, classification_source,action_date, ipcrversion,str(num)]
                    num +=1
            except:
                pass
            

          
            #data = {'class': main[0][:3].replace(' ', ''), 
                 #'subclass': crossrefsub} 

            if 'classification-national' in avail_fields:
                national = avail_fields["classification-national"]
                national = national[0]
                national = national.split("\n")
                n = 0
                for line in national:
                    if line.startswith('<main-classification'):
                        main = re.search('<main-classification>(.*?)</main-classification', line).group(1)
                        crossrefsub = main[3:].replace(" ","") 
                        if len(crossrefsub) > 3 and re.search('^[A-Z]',crossrefsub[3:]) is None: 
                            crossrefsub = crossrefsub[:3]+'.'+crossrefsub[3:] 
                        crossrefsub = re.sub('^0+','',crossrefsub) 
                        if re.search('[A-Z]{3}',crossrefsub[:3]): 
                            crossrefsub = crossrefsub.replace(".","")
                        main = main[:3].replace(" ","")
                        sub = crossrefsub
                        mainclassdata[main] = [main]
                        subclassdata[sub] = [sub]
                    elif line.startswith("<further-classification"):
                        further_class = re.search('<further-classification>(.*?)</further-classification', line).group(1)
                        further_sub_class = further_class[3:].replace(" ","")
                        if len(further_sub_class)>3 and re.search('^[A-Z]',further_sub_class[3:]) is None:
                            further_sub_class = further_sub_class[:3]+'.'+further_sub_class[3:]
                        further_sub_class = re.sub('^0+','',further_sub_class)
                        if re.search('[A-Z]{3}', further_sub_class[:3]):
                            further_sub_class = further_sub_class.replace(".","") 
                        further_class = further_class[:3].replace(" ", "")    
                        mainclassdata[further_class] = [further_class] 
                        if further_sub_class != "": 
                            uspc[id_generator()] = [patent_id,further_class,further_class+'/'+further_sub_class,str(n)] 
                            subclassdata[further_class+'/'+further_sub_class] = [further_class+'/'+further_sub_class]
                            n +=1



            #I think it may be the patent cit/other cit problem
            if ("references-cited" in avail_fields)|('us-references-cited' in avail_fields):
                if "references-cited" in avail_fields:
                    citation = re.split("<citation", avail_fields['references-cited'])
                #in 2021 schema changed to us-references-cited instead of references-cited 
                else:
                    citation = re.split("<us-citation", avail_fields['us-references-cited'])
                otherseq = 0
                forpatseq=0
                uspatseq=0 
                appseq = 0              
                for i in citation:
                    #print citation
                    refpatnum = 'NULL' 
                    citpatname = 'NULL' 
                    citdate = 'NULL' 
                    citkind = 'NULL' 
                    citcountry = 'US' 
                    citdocno= 'NULL'
                    citcategory = "NULL"
                    text = "NULL"
                    app_flag = False
                    ref_class = "NULL"
                    
                    #print i
                    to_process = i.split('\n')

                    for line in to_process:
                        if line.startswith("<doc-number"):
                            citdocno = re.search('<doc-number>(.*?)</doc-number>',line).group(1)
                            #figure out if this is a citation to an application
                            if re.match(r'^[A-Z]*\d+$', citdocno):
                                num = re.findall('\d+', citdocno)
                                num = num[0] #turns it from list to string
                                #print num
                                if num[0].startswith("0"):
                                    num = num[1:]
                                    let = re.findall('[a-zA-Z]+', citdocno)
                                if let:
                                    let = let[0]#list to string
                                    citdocno = let +num
                                else:
                                    citdocno = num
                            if not re.match(r'^[A-Z]*\d+$', citdocno):
                                citdocno = citdocno
                                #print citdocno
                                app_flag = True
                            #print citdocno
                        try:
                            if line.startswith("<kind"):
                                citkind = re.search('<kind>(.*?)</kind>',line).group(1)
                            if line.startswith("<category"):
                                citcategory = re.search('<category>(.*?)</category>',line).group(1)
                            if line.startswith("<country"):
                                citcountry = re.search('<country>(.*?)</country>',line).group(1)
                            if line.startswith("<name"):
                                name = re.search("<name>(.*?)</name>",line).group(1)
                            if line.startswith("<classification-national"):
                                ref_class = re.search("<main-classification>(.*?)</main-classification", line).group(1)
                            if line.startswith("<date"):
                                citdate = re.search('<date>(.*?)</date>',line).group(1)
                                if citdate[6:] != "00":
                                    citdate = citdate[:4]+'-'+citdate[4:6]+'-'+citdate[6:]
                                else:
                                    citdate = citdate[:4]+'-'+citdate[4:6]+'-'+'01'
                                    year = citdate[:4]
                            if line.startswith("<othercit"):
                                text = re.search("<othercit>(.*?)</othercit>", line).group(1)
                        except:
                            print "Problem with other variables"
                    if citcountry == "US":
                        if citdocno != "NULL" and not app_flag : 
                            uspatentcitation[id_generator()] = [patent_id,citdocno,citdate,name,citkind,citcountry,citcategory,str(uspatseq), ref_class] 
                            uspatseq+=1
                        if citdocno != 'NULL' and app_flag:
                            usappcitation[id_generator()] = [patent_id,citdocno,citdate,name,citkind, citdocno, citcountry,citcategory,str(appseq)]
                            appseq +=1
                    elif citdocno != "NULL":
                        foreigncitation[id_generator()] = [patent_id,citdate,citdocno,citcountry,citcategory, str(forpatseq)] 
                        forpatseq+=1 
                    if text!= "NULL":
                        otherreference[id_generator()] = [patent_id, text, str(otherseq)]
                        otherseq +=1
            
            try:
                assignees = avail_fields['assignees'].split("</assignee") #splits fields if there are multiple assignees
                assignees = assignees[:-1] #exclude the last chunk which is a waste line from after the
                #list_of_assignee_info = []
                for i in range(len(assignees)):
                    assgorg = None
                    assgfname = None
                    assglname = None
                    assgtype = None
                    assgcountry = 'US'
                    assgstate = None
                    assgcity = None
                    one_assignee = assignees[i].split("\n")
                    for line in one_assignee:
                        if line.startswith("<orgname"):
                            assgorg = re.search('<orgname>(.*?)</orgname>',line).group(1)
                        if line.startswith("<firstname"):
                            assgfname = re.search('<firstname>(.*?)</firstname>',line).group(1)
                        if line.startswith("<lastname"):
                            assglname = re.search('<lastname>(.*?)</lastname>',line).group(1)
                        if line.startswith("<role"):
                            assgtype = re.search('<role>(.*?)</role>',line).group(1)
                            assgtype = assgtype.lstrip("0")
                        if line.startswith("<country"):
                            assgcountry = re.search('<country>(.*?)</country>',line).group(1)
                        if line.startswith("<state"):
                            assgstate = re.search('<state>(.*?)</state>',line).group(1)
                        if line.startswith("<city"):
                            assgcity = re.search('<city>(.*?)</city>',line).group(1)
                    loc_idd = None
                    rawlocation[id_generator()] = [loc_idd,assgcity,assgstate,assgcountry] 
                    rawassignee[id_generator()] = [patent_id, None, loc_idd,assgtype,assgfname,assglname,assgorg,str(i)]
                    #how to handle assignee id, for now it is null
            except:
                pass

            try:
                if 'applicants' in avail_fields:
                    applicant_list = re.split("</applicant>", avail_fields['applicants'])
                    applicant_list = applicant_list[:-1] #gets rid of extra last line
                   #print applicant_list
                else:                   
                    applicant_list = re.split("</us-applicant>", avail_fields['us-applicants'])
                    applicant_list = applicant_list[:-1] #gets rid of extra last line
                inventor_seq = 0
                for person in applicant_list:
                    designation = "NULL"
                    earlier_applicant_type = "NULL"
                    later_applicant_type = "NULL"
                    sequence = "NULL"
                    last_name = "NULL"
                    first_name = 'NULL'
                    orgname = "NULL"
                    street = 'NULL'
                    city = 'NULL'
                    state = "NULL"
                    country = 'NULL'
                    nationality = 'NULL'
                    residence = 'NULL'
                    applicant_lines = person.split("\n")
                    for line in applicant_lines:
                        if line.startswith('<applicant'):
                            sequence = re.search('sequence="(.*?)"', line).group(1)
                            earlier_applicant_type = re.search('app-type="(.*?)"', line).group(1)
                            designation = re.search('designation="(.*?)"', line).group(1)
                        if line.startswith('<us-applicant'):
                            sequence = re.search('sequence="(.*?)"', line).group(1)
                            designation = re.search('designation="(.*?)"', line).group(1)
                            try:
                                later_applicant_type = re.search('applicant-authority-category="(.*?)"', line).group(1)
                            except:
                                pass
                        if line.startswith("<orgname"):
                             orgname = re.search('<orgname>(.*?)</orgname>', line).group(1)
                        if line.startswith("<first-name"):
                             first_name = re.search('<first-name>(.*?)</first-name>', line).group(1)
                        if line.startswith("<last-name"):
                             last_name = re.search('<last-name>(.*?)</last-name>', line).group(1)
                        if line.startswith('<street'):
                            street = re.search('<street>(.*?)</street>', line).group(1)
                        if line.startswith('<city'):
                             city = re.search('<city>(.*?)</city>', line).group(1)
                        if line.startswith("<state"):
                            state = re.search('<state>(.*?)</state>',line).group(1)
                        if line.startswith('<country'):
                             country = re.search('<country>(.*?)</country>', line).group(1)
                    try: #nationality only in earlier years
                        for_nat = person.split("nationality")
                        nation = for_nat[1].split("\n")
                        for line in nation:
                            if line.startswith("<country"):
                                nation = re.search('<country>(.*?)</country>', line).group(1)
                    except:
                        pass
                    for_res = person.split("residence")
                    res = for_res[1].split("\n")
                    for line in res:
                        if line.startswith("<country"):
                             residence = re.search('<country>(.*?)</country>', line).group(1)
                    #this get us the non-inventor applicants in 2013+. Inventor applicants are in applicant and also in inventor.
                    non_inventor_app_types =  ['legal-representative', 'party-of-interest', 'obligated-assignee', 'assignee']
                    if later_applicant_type in non_inventor_app_types:
                            loc_idd = id_generator() 
                            rawlocation[id_generator()] = [loc_idd,city,state, country] 
                            non_inventor_applicant[id_generator()] = [patent_id, loc_idd, last_name, first_name, orgname, sequence, designation, later_applicant_type]
                    #this gets us the inventors from 2005-2012
                    if earlier_applicant_type == "applicant-inventor":
                        loc_idd = id_generator() 
                        rawlocation[id_generator()] = [loc_idd,city,state, country] 
                        rawinventor[id_generator()] = [patent_id,"NULL", loc_idd, first_name,last_name, str(inventor_seq), rule_47]  
                        inventor_seq +=1
                    if (earlier_applicant_type != "applicant-inventor") and earlier_applicant_type!="NULL":
                        loc_idd = id_generator() 
                        rawlocation[id_generator()] = [loc_idd,city,state, country] 
                        non_inventor_applicant[id_generator()] = [patent_id, loc_idd, last_name, first_name, orgname, sequence, designation, earlier_applicant_type]
            except:
                pass



            try:
                #2005-2013 (inclusive) ame of the inventor is only stored in inventor if the inventor is not also the applicant in earlier year
                #mostly thi is because the inventor is dead
                #so inventors has dead /incapacitated invenntors adn "applicant " has live

                #2013 on name of the inventor is stored in the inventors block always
                inventors = re.split("<inventor sequence", avail_fields['inventors'])#split on live or deceased inventor
                #inventors = inventors[1:] #exclude the last chunk which is a waste line from after the inventor
                for i in range(len(inventors)):
                    fname = "NULL"
                    lname = "NULL"
                    invtcountry = 'US'
                    invtstate = "NULL"
                    invtcity = "NULL"
                    invtzip = "NULL"
                    one_inventor = inventors[i].split("\n")
                    for line in one_inventor:
                        if line.startswith("<first-name"):
                            fname = re.search('<first-name>(.*?)</first-name>',line).group(1)
                        if line.startswith("<last-name"):
                            lname = re.search('<last-name>(.*?)</last-name>',line).group(1)
                        if line.startswith("<zip"):
                            invtzip = re.search('<zip>(.*?)</zip>',line).group(1)
                        if line.startswith("<country"):
                            invtcountry = re.search('<country>(.*?)</country>',line).group(1)
                        if line.startswith("<state"):
                            invtstate = re.search('<state>(.*?)</state>',line).group(1)
                        if line.startswith("<city"):
                            invtcity = re.search('<city>(.*?)</city>',line).group(1)
                    loc_idd = id_generator() 
                    rawlocation[id_generator()] = [loc_idd,invtcity,invtstate,invtcountry]   
                    if fname == "NULL" and lname == "NULL": 
                         pass 
                    else: 
                         rawinventor[id_generator()] = [patent_id,"NULL", loc_idd, fname,lname, str(i-1), rule_47]
            except:
                pass
            
            try:
                agent = re.split("</agent", avail_fields['agents'])
                agent = agent[:-1]
                for i in range(len(agent)):
                    fname = "NULL"
                    lname = "NULL"
                    lawcountry = 'NULL'
                    laworg = "NULL"
                    one_agent = agent[i].split("\n")
                    for line in one_agent:
                        if line.startswith("<first-name"):
                            fname = re.search('<first-name>(.*?)</first-name>',line).group(1)
                        if line.startswith("<last-name"):
                            lname = re.search('<last-name>(.*?)</last-name>',line).group(1)
                        if line.startswith("<country"):
                            lawcountry = re.search('<country>(.*?)</country>',line).group(1)
                        if line.startswith("<orgname"):
                            laworg = re.search('<orgname>(.*?)</orgname>',line).group(1)
                        if line.startswith("<agent sequence"):
                            rep_type = re.search('rep-type=\"(.*?)\"',line).group(1)
                    rawlawyer[id_generator()] = ["NULL",patent_id,fname,lname,laworg,lawcountry,str(i)]
            except:
                pass

            
            if 'us-related-documents' in avail_fields:
                related_docs = avail_fields['us-related-documents']
                possible_doc_type = ["</division>", '</continuation>', '</continuation-in-part>','</continuing-reissue>',
                                     '</us-divisional-reissue>', '</reexamination>', '</substitution>', '</us-provisional-application>', '</utility-model-basis>', 
                                     '</reissue>', '</related-publication>', '</correction>',
                                     '</us-provisional-application>','</us-reexamination-reissue-merger>']
                def iter_list_splitter(list_to_split, seperators):
                    for j in seperators:
                        new_list = []
                        for i in list_to_split:
                            split_i =  i.split(j)
                            new_list += split_i
                        list_to_split = copy.deepcopy(new_list)
                    return list_to_split
                split_docs = iter_list_splitter([related_docs], possible_doc_type)
                possible_relations = ['<parent-doc>', '<parent-grant-document>', 
                                      '<parent-pct-document>', '<child-doc>']
                rel_seq = 0
                kind = None
                for i in split_docs:
                    if i!='\r\n</': #sometimes this extra piece exists so we should get rid of it
                        doc_info = i.split("\n")
                        doc_type = doc_info[1][1:-2] #cuts off the < adn >
                        if ((doc_type== "us-provisional-application")|(doc_type =="related-publication")):
                            doc_info = i.split("\n")
                            doc_type = doc_info[1][1:-2] #cuts off the < adn >
                            for line in doc_info:
                                if line.startswith("<doc-number"):
                                    reldocno = re.search('<doc-number>(.*?)</doc-number>',line).group(1)
                                if line.startswith("<kind"):
                                    kind = re.search('<kind>(.*?)</kind>',line).group(1)
                                if line.startswith("<country"):
                                    relcountry = re.search('<country>(.*?)</country>',line).group(1)
                                try:
                                    if line.startswith("<date"):
                                        reldate = re.search('<date>(.*?)</date>',line).group(1)
                                        if reldate[6:] != "00":
                                            reldate = reldate[:4]+'-'+reldate[4:6]+'-'+reldate[6:]
                                        else:
                                            reldate = reldate[:4]+'-'+reldate[4:6]+'-'+'01'
                                            year = reldate[:4]
                                except:
                                    print "Missing date on usreldoc"
                                    reldate = "0000-00-00"
                            usreldoc[id_generator()] = [patent_id, doc_type, "NULL", reldocno, relcountry, reldate, "NULL", rel_seq, kind]
                            #usrel.writerow(['uuid', 'patent_id', 'doc_type',  'relkind', 'reldocno', 'relcountry', 'reldate',  'parent_status', 'rel_seq','kind']) 
                            rel_seq +=1
                        else:
                            split_by_relation = iter_list_splitter([i], possible_relations)
                            for j in split_by_relation[1:]: #the first item in the list is not a document
                                parent_or_child = j.split("\n")
                                reltype = None
                                if re.search("</child-doc", j):
                                    reltype = "child document"
                                elif re.search("</parent-grant-document",j):
                                    reltype = "parent grant document"
                                elif re.search("</parent-pct-document",j):
                                    reltype = "parent pct document"
                                else:
                                    reltype = "parent document" #because of how the text is split parent document doesn't in the doc info for the parent document
                                reldocno=None
                                kind = None
                                relcountry = None
                                reldate = None
                                relparentstatus = None
                                for line in parent_or_child:
                                    if line.startswith("<doc-number"):
                                        reldocno = re.search('<doc-number>(.*?)</doc-number>',line).group(1)
                                    if line.startswith("<kind"):
                                        kind = re.search('<kind>(.*?)</kind>',line).group(1)
                                    if line.startswith("<country"):
                                        relcountry = re.search('<country>(.*?)</country>',line).group(1)
                                    try:
                                        if line.startswith("<date"):
                                            reldate = re.search('<date>(.*?)</date>',line).group(1)
                                            if reldate[6:] != "00":
                                                reldate = reldate[:4]+'-'+reldate[4:6]+'-'+reldate[6:]
                                            else:
                                                reldate = reldate[:4]+'-'+reldate[4:6]+'-'+'01'
                                                year = reldate[:4]
                                    except:
                                        reldate = "0000-00-00"
                                        print "Missing date on reldoc"
                                    if line.startswith("<parent-status"):
                                        relparentstatus = re.search('<parent-status>(.*?)</parent-status', line).group(1)
                                usreldoc[id_generator()] = [patent_id, doc_type, reltype, reldocno, relcountry, reldate,  relparentstatus, rel_seq, kind]
                                rel_seq +=1
                                



            try:
                examiners = avail_fields["examiners"].split("<assistant-examiner")
                department = "Null"
                for i in range(len(examiners)):
                    list_of_examiner = examiners[i].split("\n")
                    fname = "Null"
                    lname = "Null"
                    for line in list_of_examiner:
                        if line.startswith("<first-name"):
                            fname = re.search("<first-name>(.*?)</first-name>",line).group(1)
                        if line.startswith("<last-name"):
                            lname = re.search("<last-name>(.*?)</last-name>", line).group(1)
                        if line.startswith("<department"):
                            department = re.search("<department>(.*?)</department>", line).group(1)
                    if i == 0: #the first examiner is the primary examiner
                        examiner[id_generator()]=[docno, fname, lname, "primary", department]
                    else:
                        examiner[id_generator()]=[docno, fname, lname, "assistant", department] 
            except:
                pass


            if "priority-claim" in avail_fields:
                priority = avail_fields["priority-claim"] [1:] #first entry is a waste
                #print priority
                for i in priority:
                    priority_id = "NULL"
                    priority_requested = "NULL"
                    priority_claim = i.split("\n")
                    for line in priority_claim:
                        if line.startswith("<country"):
                            country = re.search("<country>(.*?)</country>", line).group(1)
                        if line.startswith("<doc-number"):
                            app_num = re.search("<doc-number>(.*?)</doc-number>", line).group(1)
                        if line.startswith("<date"):
                            app_date = re.search("<date>(.*?)</date>", line).group(1)
                            if app_date[6:] != "00":
                                app_date = app_date[:4]+'-'+app_date[4:6]+'-'+ app_date[6:]
                            else:
                                app_date = app_date[:4]+'-'+appd_ate[4:6]+'-'+'01'
                        if line.startswith(" sequence"):
                            sequence = re.search('sequence="(.*?)"', line).group(1)
                            kind =  re.search('kind="(.*?)"', line).group(1)
                        if line.startswith("<id"):
                            priority_id = re.search("<id>(>*?)<id>", line).group(1)
                        if line.startswith("<priority-doc-requested"):
                            priority_requested = re.search("<priority-doc-requested>(.*?)</priority-doc-requested", line).group(1)
                    #priority_id and priority_requested cant be found
                    for_priority[id_generator()] = [patent_id, sequence, kind, app_num, app_date, country]


            #if "description" in avail_fields:
                #print avail_fields["description"]



            try:
                rel_app = avail_fields["RELAPP"].split("\n")
                rel_app_seq = 0
                text_field = ""
                for line in rel_app:
                    if line.startswith("<heading"):
                        rel_app_seq +=1
                        heading = re.search(">(.*?)<", line).group(1)
                        text_field += heading + " "
                        #rel_app_text[id_generator()] = [patent_id,"heading", heading, rel_app_seq]
                    if line.startswith("<p"):
                        rel_app_seq +=1
                        text = re.search(">(.*?)</p>", line).group(1)
                        text_field += text + " "
                rel_app_text[id_generator()] = [patent_id, text_field]
            except:
                pass
           
            try:
                draw_seq = 0
                draw_des = avail_fields['description-of-drawings'].split("\n")
                #print draw_des
                draw_text =''
                for line in draw_des:
                    if line.startswith("<p id"):
                        draw_seq +=1
                        #this hack deals with the fact that some miss the </p> ending
                        start, cut, text = line.partition('">')
                        draw_desc, cut, end = text.partition("</p")
                        if "<" in draw_desc or ">" in draw_desc:
                            soup = bs(draw_desc, "lxml")
                            text = soup.get_text()
                        else:
                            text = draw_desc
                        #text = [piece.encode('utf-8','ignore') for piece in text]
                        #text = "".join(text)
                        #if not (text.strip() in ["BRIEF DESCRIPTION OF THE DRAWINGS", "BRIEF DESCRIPTION OF THE DRAWING", "BRIEF DESCRIPTION OF THE DRAWING"]):
                        if (not text.isupper()) | (any(char.isdigit() for char in text)):
                            draw_desc_text[id_generator()] = [patent_id, text, draw_seq]
                        else:
                            pass #skipping the brief description heading
                    if line.startswith("<heading"):
                        draw_seq +=1

                        heading = re.search(">(.*?)<", line).group(1)
                        #draw_text += " " + heading
                        if (not desc.isupper()) | (any(char.isdigit() for char in desc)):
                            draw_desc_text[id_generator()] = [patent_id, heading, draw_seq]
                        else:
                          pass #skipping the brief description heading
            except:
                pass
                #print "Problem with drawing description"

            try:
                brf_sum = avail_fields['BRFSUM'].split("\n")
                heading = "NULL"
                text = "NULL"
                brf_sum_seq = 0
                brf_text = ''
                for line in brf_sum:
                    if line.startswith("<p id"):
                        brf_sum_seq +=1
                        #this hack deals with the fact that some miss the </p> ending
                        start, cut, text = line.partition('">')
                        brf_sum, cut, end = text.partition("</p")
                        #brf_sum_text[id_generator()] = [patent_id,"text", brf_sum, brf_sum_seq]
                        brf_text += ' ' + brf_sum
                    if line.startswith("<heading"):
                        brf_sum_seq +=1
                        heading = re.search(">(.*?)<", line).group(1)
                        brf_text += " " + heading
                        #brf_sum_text[id_generator()] = [patent_id,"heading", heading, brf_sum_seq]
                brf_sum_text[id_generator()] = [patent_id,brf_text]
            except:
                pass


            try:
                det = avail_fields["DETDESC"].split("\n")
                det_seq = 0
                heading = "NULL"
                detailed_text_field =""
                for line in det:
                    if line.startswith("<p id"):
                        det_seq +=1
                        #this hack deals with the fact that some miss the </p> ending
                        start, cut, text = line.partition('">')
                        det_desc, cut, end = text.partition("</p")
                        #there are empty paragraph tags that flag lists and tables, for now we are skipping them
                        #maybe later import table or list from table and list parsing to improve completeness
                        if len(det_desc) > 5: #only include if there is a detailed desciption text in this tag
                            detailed_text_field += " " + det_desc
                    if line.startswith("<heading"):
                        det_seq +=1
                        heading = re.search(">(.*?)<", line).group(1)
                        detailed_text_field += " " + heading
                        #detail_desc_text[id_generator()] = [patent_id,"heading", heading, det_seq]
                if ("<" in detailed_text_field) or (">" in detailed_text_field):
                    soup = bs(detailed_text_field, "lxml")
                    text = soup.get_text()
                else:
                    text = detailed_text_field
                #text = [piece.encode('utf-8','ignore') for piece in text]
                #text = "".join(text)
                detail_desc_text[id_generator()] = [patent_id, text]
            except:
                pass

            if 'us-term-of-grant' in avail_fields:
                us_term_of_grant_temp = avail_fields['us-term-of-grant']
                us_term_of_grant_temp = us_term_of_grant_temp.split('\n')
                lapse_of_patent = 'NULL'
                text = 'NULL'
                length_of_grant = 'NULL'
                us_term_extension = 'NULL'
                for line in us_term_of_grant_temp:
                    if line.startswith('<lapse-of-patent'):
                         lapse_of_patent = re.search('<lapse-of-patent>(.*?)</lapse-of-patent>', line).group(1)
                    if line.startswith('<text'):
                         text = re.search('<text>(.*?)</text>', line).group(1)
                    if line.startswith('<length-of-grant'):
                         length_of_grant = re.search('<length-of-grant>(.*?)</length-of-grant>', line).group(1)
                    if line.startswith('<us-term-extension'):
                         us_term_extension = re.search('<us-term-extension>(.*?)</us-term-extension>', line).group(1)
                us_term_of_grant[id_generator()] = [patent_id, lapse_of_patent, "NULL", text, length_of_grant, us_term_extension]

            if 'pct-or-regional-publishing-data' in avail_fields:
                number = None
                date = None
                country = None
                rel_id = None
                kind = None
                date_102 = None
                publishing = avail_fields["pct-or-regional-publishing-data"].split("\n")
                for line in publishing:
                    try:
                        if line.startswith("<doc-number"):
                            rel_id = re.search("<doc-number>(.*?)</doc-number>", line).group(1)
                    except:
                        print "Publishing data lacks docno"
                    if line.startswith('<kind'):
                         kind = re.search('<kind>(.*?)</kind>', line).group(1)
                    if line.startswith('<date'):
                        date = re.search('<date>(.*?)</date>', line).group(1)
                        if date[6:] != "00":
                            date = date[:4]+'-'+date[4:6]+'-'+date[6:]
                        else:
                            date = date[:4]+'-'+ date[4:6]+'-'+'01'
                    if line.startswith('<country'):
                         country = re.search('<country>(.*?)</country>', line).group(1)
                pct_data[id_generator()] = [patent_id, rel_id, date, None, country, kind, "wo_grant", None]


            if "pct-or-regional-filing-data" in avail_fields:
                rel_id = None
                number = None
                date = None
                country = None
                kind = None
                date_371= None
                pct = avail_fields["pct-or-regional-filing-data"].split("<us-371c124-date") 
                #split on us_371 date, because there are two date lines
                try:
                    pct_info = pct[0].split("\n")
                    info_371 = pct[1].split("\n")
                except:
                    pct_info = pct[0].split("\n")
                    info_371 = []
                for line in pct_info:
                    if line.startswith("<doc-number"):
                        rel_id = re.search("<doc-number>(.*?)</doc-number>", line).group(1)
                    if line.startswith('<kind'):
                         kind = re.search('<kind>(.*?)</kind>', line).group(1)
                    if line.startswith('<date'):
                        date = re.search('<date>(.*?)</date>', line).group(1)
                        if date[6:] != "00":
                            date = date[:4]+'-'+date[4:6]+'-'+date[6:]
                        else:
                            date = date[:4]+'-'+ date[4:6]+'-'+'01'
                    if line.startswith('<country'):
                         country = re.search('<country>(.*?)</country>', line).group(1)
                for line in info_371:
                    if line.startswith('<date'):
                        date3 = re.search('<date>(.*?)</date>', line).group(1)
                        if date3[6:] != "00":
                            date3 = date3[:4]+'-'+date3[4:6]+'-'+date3[6:]
                        else:
                            date3 = date3[:4]+'-'+ date3[4:6]+'-'+'01'
                        date_371 = date3
                pct_data[id_generator()] = [patent_id, rel_id, date, date_371, country, kind, "pct_application", None]


            #us-issued (add to patents?)
            if "figures" in avail_fields:
                figures = avail_fields['figures'].split("\n")
                sheets = None
                figs = None
                for line in figures:
                    try:
                        if line.startswith('<number-of-drawing-sheets'):
                            sheets = re.search('<number-of-drawing-sheets>(.*?)</number-of-drawing-sheets>', line).group(1)
                        if line.startswith('<number-of-figures'):
                             figs = re.search('<number-of-figures>(.*?)</number-of-figures>', line).group(1)
                    except:
                        print "Missing field from figures"
                figure_data[id_generator()] = [patent_id, figs, sheets]
            if "us-botanic" in avail_fields:
                latin = None
                variety = None
                botanic = avail_fields["us-botanic"].split("\n")
                for line in botanic:
                    try:
                        if line.startswith("<latin-name"):
                            latin_name = re.search('<latin-name>(.*?)</latin-name>', line).group(1)
                        if line.startswith("<variety"):
                            variety = re.search("<variety>(.*?)</variety>", line).group(1)
                    except:
                        print "Problem with botanic"
                botanic_data[id_generator()] = [patent_id, latin_name, variety]

            patentdata[patent_id] = [apptype,docno,'US',issdate,abst,title,patkind,numclaims, d]

            '''
            titlefile= csv.writer(open(os.path.join(fd2,'new_titles.csv'),'ab'),delimiter='\t')
            for k,v in new_title.items():
                titlefile.writerow([k]+[v])  

            '''

            patfile = csv.writer(open(os.path.join(fd2,'patent.csv'),'ab'),delimiter='\t')
            for k,v in patentdata.items():
                patfile.writerow([k]+v)
            
            appfile = csv.writer(open(os.path.join(fd2,'application.csv'),'ab'),delimiter='\t')
            for k,v in application.items():
                appfile.writerow([k]+v)
            
            claimsfile = csv.writer(open(os.path.join(fd2,'claim.csv'),'ab'),delimiter='\t')
            for k,v in claims.items():
                claimsfile.writerow([k]+v)
            
            rawinvfile = csv.writer(open(os.path.join(fd2,'rawinventor.csv'),'ab'),delimiter='\t')
            for k, v in rawinventor.items():
                rawinvfile.writerow([k] + v)
            
            rawassgfile = csv.writer(open(os.path.join(fd2,'rawassignee.csv'),'ab'),delimiter='\t')
            for k,v in rawassignee.items():
                rawassgfile.writerow([k]+v)
            
            ipcrfile = csv.writer(open(os.path.join(fd2,'ipcr.csv'),'ab'),delimiter='\t')
            for k,v in ipcr.items():
                ipcrfile.writerow([k]+v)
            
            uspcfile = csv.writer(open(os.path.join(fd2,'uspc.csv'),'ab'),delimiter='\t')
            for k,v in uspc.items():
                uspcfile.writerow([k]+v)
            
            uspatentcitfile = csv.writer(open(os.path.join(fd2,'uspatentcitation.csv'),'ab'),delimiter='\t')
            for k,v in uspatentcitation.items():
                uspatentcitfile.writerow([k]+v)

            usappcitfile = csv.writer(open(os.path.join(fd2,'usapplicationcitation.csv'),'ab'),delimiter='\t')
            for k,v in usappcitation.items():
                usappcitfile.writerow([k]+v)

            foreigncitfile = csv.writer(open(os.path.join(fd2,'foreigncitation.csv'),'ab'),delimiter='\t')
            for k,v in foreigncitation.items():
                foreigncitfile.writerow([k]+v)
            
            otherreffile = csv.writer(open(os.path.join(fd2,'otherreference.csv'),'ab'),delimiter='\t')
            for k,v in otherreference.items():
                otherreffile.writerow([k]+v)
            
            rawlawyerfile = csv.writer(open(os.path.join(fd2,'rawlawyer.csv'),'ab'),delimiter='\t')
            for k,v in rawlawyer.items():
                rawlawyerfile.writerow([k]+v)


            examinerfile = csv.writer(open(os.path.join(fd2,'raw_examiner.csv'),'ab'),delimiter='\t')
            for k,v in examiner.items():
                examinerfile.writerow([k]+v)

            for_priorityfile = csv.writer(open(os.path.join(fd2,'foreign_priority.csv'),'ab'),delimiter='\t')
            for k,v in for_priority.items():
                for_priorityfile.writerow([k]+v)

            usreldocfile = csv.writer(open(os.path.join(fd2,'usreldoc.csv'),'ab'),delimiter='\t')
            for k,v in usreldoc.items():
                usreldocfile.writerow([k]+v)

            us_term_of_grantfile = csv.writer(open(os.path.join(fd2,'us_term_of_grant.csv'),'ab'),delimiter='\t')
            for k,v in us_term_of_grant.items():
                us_term_of_grantfile.writerow([k]+v)

            non_inventor_applicantfile = csv.writer(open(os.path.join(fd2,'non_inventor_applicant.csv'),'ab'),delimiter='\t')
            for k,v in non_inventor_applicant.items():
                non_inventor_applicantfile.writerow([k]+v)

            draw_desc_textfile = csv.writer(open(os.path.join(fd2,'draw_desc_text.csv'),'ab'),delimiter='\t')
            for k,v in draw_desc_text.items():
                try:
                    draw_desc_textfile.writerow([k]+v)
                except:
                    first = v[0]
                    second = v[1]
                    third = v[2]
                    second = [piece.encode('utf-8','ignore') for piece in second]
                    second= "".join(second)
                    value = []
                    value.append(first)
                    value.append(second)
                    value.append(third)
                    draw_desc_textfile.writerow([k]+value)



            brf_sum_textfile = csv.writer(open(os.path.join(fd2,'brf_sum_text.csv'),'ab'),delimiter='\t')
            for k,v in brf_sum_text.items():
                brf_sum_textfile.writerow([k]+v)

            detail_desc_textfile = csv.writer(open(os.path.join(fd2,'detail_desc_text.csv'),'ab'),delimiter='\t')
            for k,v in detail_desc_text.items():
                try:
                    detail_desc_textfile.writerow([k]+v)
                except:
                    first = v[0]
                    second = v[1]
                    second = [piece.encode('utf-8','ignore') for piece in second]
                    second= "".join(second)
                    value = []
                    value.append(first)
                    value.append(second)
                    detail_desc_textfile.writerow([k]+value)



            rel_app_textfile = csv.writer(open(os.path.join(fd2,'rel_app_text.csv'),'ab'),delimiter='\t')
            for k,v in rel_app_text.items():
                rel_app_textfile.writerow([k]+v)

            pct_datafile = csv.writer(open(os.path.join(fd2,'pct_data.csv'),'ab'),delimiter='\t')
            for k,v in pct_data.items():
                pct_datafile.writerow([k]+v)

            botanicfile = csv.writer(open(os.path.join(fd2,'botanic.csv'),'ab'),delimiter='\t')
            for k,v in botanic_data.items():
                botanicfile.writerow([k]+v)

            figuresfile = csv.writer(open(os.path.join(fd2,'figures.csv'),'ab'),delimiter='\t')
            for k,v in figure_data.items():
                figuresfile.writerow([k]+v)


        

    rawlocfile = csv.writer(open(os.path.join(fd2,'rawlocation.csv'),'ab'),delimiter='\t') 
    for k,v in rawlocation.items(): 
        rawlocfile.writerow([k]+v) 

    mainclassfile = csv.writer(open(os.path.join(fd2,'mainclass.csv'),'ab'),delimiter='\t')
    for k,v in mainclassdata.items():
        mainclassfile.writerow(v)

    subclassfile = csv.writer(open(os.path.join(fd2,'subclass.csv'),'ab'),delimiter='\t')
    for k,v in subclassdata.items():
        subclassfile.writerow(v)
            

            
    print numi

            
