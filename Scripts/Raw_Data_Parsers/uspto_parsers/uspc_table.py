def uspc_table(fd):
    import re,csv,os
    
    diri = os.listdir(fd)
    for d in diri:
        if re.search('ctaf',d):
            classindxfile = d
        if re.search('mcfpat',d):
            patclassfile = d
    #Classes Index File parsing for class/subclass text
    classidx = open(os.path.join(fd, classindxfile)).read().split("\n")
    outp = csv.writer(open(fd+'patent_classes_text.csv','wb'))
    data = {}
    for n in range(len(classidx)):
        classname = re.sub('[\.\s]+$','',classidx[n][21:])
        mainclass = classidx[n][:3]
        if classidx[n][6:9] != '000':
            try:
                temp = int(classidx[n][6:9])
                subclass = classidx[n][3:6]+'.'+re.sub('0+$','',classidx[n][6:9])
            except:
                if len(re.sub('0+','',classidx[n][6:9])) > 1:
                    subclass = re.sub('^0+','',classidx[n][3:6])+'.'+re.sub('0+','',classidx[n][6:9])
                else:
                    subclass = re.sub('^0+','',classidx[n][3:6])+re.sub('0+','',classidx[n][6:9])    
        else:
            subclass = classidx[n][3:6]
        if classidx[n][18:21] != '000':
            try:
                temp = int(classidx[n][18:21])
                highersubclass = classidx[n][15:18]+'.'+re.sub('0+$','',classidx[n][18:21])
            except:
                if len(re.sub('0+','',classidx[n][18:21])) > 1:
                    highersubclass = re.sub('^0+','',classidx[n][15:18])+'.'+re.sub('0+','',classidx[n][18:21])
                else:
                    highersubclass = re.sub('^0+','',classidx[n][15:18])+re.sub('0+','',classidx[n][18:21])    
        else:
            highersubclass = classidx[n][15:18]
        
        try:
            gg = data[mainclass+' '+highersubclass]
            data[mainclass+' '+subclass] = classname+'-'+gg
        except:
            data[mainclass+' '+subclass] = classname
        
            
    for k,v in data.items():
        outp.writerow(k.split(" ")+[v])
    
    #Get patent-class pairs
    inp = open(os.path.join(fd,patclassfile)).read().split("\n")
    outp = csv.writer(open(os.path.join(fd,'USPC_patent_classes_data.csv'),'wb'))
    print len(inp)
    pats = {}
    for i in inp:
        patentnum = i[:7]
        mainclass = i[7:10]
        subclass = i[10:-1]
        if subclass[3:] != '000':
            try:
                temp = int(subclass[3:])
                subclass = subclass[:3]+'.'+re.sub('0+$','',subclass[3:])
            except:
                if len(re.sub('0+','',subclass[3:])) > 1:
                    subclass = re.sub('^0+','',subclass[:3])+'.'+re.sub('0+','',subclass[3:])
                else:
                    subclass = re.sub('^0+','',subclass[:3])+re.sub('0+','',subclass[3:])    
        else:
            subclass = subclass[:3]
        try:
            gg = pats[patentnum]
            outp.writerow([patentnum,mainclass,subclass,str(gg)])
            pats[patentnum]+=1
        except:
            pats[patentnum] = 1
            outp.writerow([patentnum,mainclass,subclass,'0'])
        
        
    # Create subclass and mainclass tables out of current output
    inp = csv.reader(file(fd+'patent_classes_text.csv','rb'))
    inp.next()
    outp1 = csv.writer(open(fd+'mainclass.csv','wb'))
    outp2 = csv.writer(open(fd+'subclass.csv','wb'))
    
    for i in inp:
        try:
            if int(i[1]) == 0:
                outp1.writerow([i[0],i[2],"NULL"])
            else:
                outp2.writerow([i[0]+'/'+i[1],i[2],"NULL"])
        except:
            outp2.writerow([i[0]+'/'+i[1],i[2],"NULL"])
