def uspc_table(fd):
    import re,csv,os,urllib2,HTMLParser,zipfile
    from bs4 import BeautifulSoup as bs
    from datetime import date
    from zipfile import ZipFile
    
    import mechanize
    br = mechanize.Browser()
    
    paturl = 'https://eipweb.uspto.gov/'+str(date.today().year)+'/MasterClassPatentGrant/mcfpat.zip'
    appurl = 'https://eipweb.uspto.gov/'+str(date.today().year)+'/MasterClassPatentAppl/mcfappl.zip'
    ctafurl = 'https://eipweb.uspto.gov/'+str(date.today().year)+'/ManualofClass/ctaf.zip'
    br.retrieve(paturl,os.path.join(fd,'mcfpat.zip'))        
    br.retrieve(appurl,os.path.join(fd,'mcfappl.zip'))
    br.retrieve(ctafurl,os.path.join(fd,'ctaf.zip'))         
    fd+='/'
    diri = os.listdir(fd)
    for d in diri:
        if re.search('ctaf.*?zip',d):
            classindxfile = ZipFile(os.path.join(fd, d),'r')
        if re.search('mcfpat.*?zip',d):
            patclassfile = ZipFile(os.path.join(fd, d),'r')
        if re.search('mcfappl.*?zip',d):
            appclassfile = ZipFile(os.path.join(fd, d), 'r')
    
    #Classes Index File parsing for class/subclass text
    classidx = classindxfile.open(classindxfile.namelist()[0]).read().split('\n')
    data = {}
    for n in range(len(classidx)):
        classname = re.sub('[\.\s]+$','',classidx[n][21:])
        mainclass = re.sub('^0+','',classidx[n][:3])
        if classidx[n][6:9] != '000':
            try:
                temp = int(classidx[n][6:9])
                if re.search('[A-Z]{3}',classidx[n][3:6]) is None:
                    subclass = re.sub('^0+','',classidx[n][3:6])+'.'+re.sub('0+','',classidx[n][6:9])
                else:
                    subclass =re.sub('^0+','',classidx[n][3:6])+re.sub('^0+','',classidx[n][6:9])
            except:
                if len(re.sub('0+','',classidx[n][6:9])) > 1:
                    subclass = re.sub('^0+','',classidx[n][3:6])+'.'+re.sub('0+','',classidx[n][6:9])
                else:
                    subclass = re.sub('^0+','',classidx[n][3:6])+re.sub('0+','',classidx[n][6:9])    
        else:
            subclass = re.sub('^0+','',classidx[n][3:6])
        if classidx[n][18:21] != '000':
            try:
                temp = int(classidx[n][18:21])
                if re.search('[A-Z]{3}',classidx[n][15:18]) is None:
                    highersubclass = re.sub('^0+','',classidx[n][15:18])+'.'+re.sub('0+','',classidx[n][18:21])
                else:
                    highersubclass = re.sub('^0+','',classidx[n][15:18])+re.sub('^0+','',classidx[n][18:21])
            except:
                if len(re.sub('0+','',classidx[n][18:21])) > 1:
                    highersubclass = re.sub('^0+','',classidx[n][15:18])+'.'+re.sub('0+','',classidx[n][18:21])
                else:
                    highersubclass = re.sub('^0+','',classidx[n][15:18])+re.sub('0+','',classidx[n][18:21])    
        else:
            highersubclass = re.sub('^0+','',classidx[n][15:18])
        
        try:
            gg = data[mainclass+' '+highersubclass]
            data[mainclass+' '+subclass] = classname+'-'+gg
        except:
            data[mainclass+' '+subclass] = classname
        
            
    
    # Create subclass and mainclass tables out of current output
    outp1 = csv.writer(open(os.path.join(fd,'mainclass.csv'),'wb'))
    outp2 = csv.writer(open(os.path.join(fd,'subclass.csv'),'wb'))
    exist = {}
    for k,v in data.items():
        i = k.split(' ')+[v]
        try:
            if i[1] == '':
                outp1.writerow([i[0],i[2]])
            else:
                try:
                    gg = exist[i[0]+'/'+i[1]]
                except:
                    exist[i[0]+'/'+i[1]] = 1
                    outp2.writerow([i[0]+'/'+i[1],i[2]])
        except:
            try:
                gg = exist[i[0]+'/'+i[1]]
            except:
                exist[i[0]+'/'+i[1]] = 1
                outp2.writerow([i[0]+'/'+i[1],i[2]])
    
    #Get patent-class pairs
    outp = csv.writer(open(os.path.join(fd,'USPC_patent_classes_data.csv'),'wb'))
    pats = {}
    with patclassfile.open(patclassfile.namelist()[0]) as inp:
        for i in inp:
            patentnum = i[:7]
            mainclass = re.sub('^0+','',i[7:10])
            subclass = i[10:-2]
            if subclass[3:] != '000':
                try:
                    temp = int(subclass[3:])
                    if re.search('[A-Z]{3}',subclass[:3]) is None:
                        subclass = re.sub('^0+','',subclass[:3])+'.'+re.sub('0+','',subclass[3:])
                    else:
                        subclass = re.sub('^0+','',subclass[:3])+re.sub('^0+','',subclass[3:])
                except:
                    if len(re.sub('0+','',subclass[3:])) > 1:
                        subclass = re.sub('^0+','',subclass[:3])+'.'+re.sub('0+','',subclass[3:])
                    else:
                        subclass = re.sub('^0+','',subclass[:3])+re.sub('0+','',subclass[3:])    
            else:
                subclass = re.sub('^0+','',subclass[:3])
            if i[-2] == 'O':
                outp.writerow([patentnum,mainclass,subclass,'0'])
            else:
                try:
                    gg = pats[patentnum]
                    outp.writerow([str(patentnum),mainclass,subclass,str(gg)])
                    pats[patentnum]+=1
                except:
                    pats[patentnum] = 2
                    outp.writerow([str(patentnum),mainclass,subclass,'1'])
        
        
    #Get application-class pairs
    outp = csv.writer(open(os.path.join(fd,'USPC_application_classes_data.csv'),'wb'))
    pats = {}
    with appclassfile.open(appclassfile.namelist()[0]) as inp:
        for i in inp:
            patentnum = i[2:6]+'/'+i[2:13]
            mainclass = re.sub('^0+','',i[15:18])
            subclass = i[18:-2]
            if subclass[3:] != '000':
                try:
                    temp = int(subclass[3:])
                    if re.search('[A-Z]{3}',subclass[:3]) is None:
                        subclass = re.sub('^0+','',subclass[:3])+'.'+re.sub('0+','',subclass[3:])
                    else:
                        subclass = re.sub('^0+','',subclass[:3])+re.sub('^0+','',subclass[3:])
                except:
                    if len(re.sub('0+','',subclass[3:])) > 1:
                        subclass = re.sub('^0+','',subclass[:3])+'.'+re.sub('0+','',subclass[3:])
                    else:
                        subclass = re.sub('^0+','',subclass[:3])+re.sub('0+','',subclass[3:])    
            else:
                subclass = re.sub('^0+','',subclass[:3])
            if i[-2] == 'P':
                outp.writerow([patentnum,mainclass,subclass,'0'])
            else:
                try:
                    gg = pats[patentnum]
                    outp.writerow([str(patentnum),mainclass,subclass,str(gg)])
                    pats[patentnum]+=1
                except:
                    pats[patentnum] = 2
                    outp.writerow([str(patentnum),mainclass,subclass,'1'])