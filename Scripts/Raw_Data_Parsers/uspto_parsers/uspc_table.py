def uspc_table(fd):
    import re,csv,os
    fd+='/'
    diri = os.listdir(fd)
    for d in diri:
        if re.search('ctaf.*?txt',d):
            classindxfile = d
        if re.search('mcfpat.*?txt',d):
            patclassfile = d
    
    #Classes Index File parsing for class/subclass text
    classidx = open(os.path.join(fd, classindxfile)).read().split("\n")
    outp = csv.writer(open(os.path.join(fd,'patent_classes_text.csv'),'wb'))
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
        
            
    for k,v in data.items():
        outp.writerow(k.split(" ")+[v])
    
    
    # Create subclass and mainclass tables out of current output
    inp = csv.reader(file(os.path.join(fd,'patent_classes_text.csv'),'rb'))
    inp.next()
    outp1 = csv.writer(open(os.path.join(fd,'mainclass.csv'),'wb'))
    outp2 = csv.writer(open(os.path.join(fd,'subclass.csv'),'wb'))
    exist = {}
    for i in inp:
        try:
            if i[1] == '':
                outp1.writerow([i[0],i[2],"NULL"])
            else:
                try:
                    gg = exist[i[0]+'/'+i[1]]
                except:
                    exist[i[0]+'/'+i[1]] = 1
                    outp2.writerow([i[0]+'/'+i[1],i[2],"NULL"])
        except:
            try:
                gg = exist[i[0]+'/'+i[1]]
            except:
                exist[i[0]+'/'+i[1]] = 1
                outp2.writerow([i[0]+'/'+i[1],i[2],"NULL"])
    
    #Get patent-class pairs
    outp = csv.writer(open(os.path.join(fd,'USPC_patent_classes_data.csv'),'wb'))
    pats = {}
    with open(os.path.join(fd,patclassfile)) as inp:
        for i in inp:
            patentnum = i[:7]
            mainclass = re.sub('^0+','',i[7:10])
            subclass = i[10:-1]
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
            if i[-1] == 'O':
                outp.writerow([patentnum,mainclass,subclass,'0'])
            else:
                try:
                    gg = pats[patentnum]
                    outp.writerow([str(patentnum),mainclass,subclass,str(gg)])
                    pats[patentnum]+=1
                except:
                    pats[patentnum] = 2
                    outp.writerow([str(patentnum),mainclass,subclass,'1'])
        
        
    
