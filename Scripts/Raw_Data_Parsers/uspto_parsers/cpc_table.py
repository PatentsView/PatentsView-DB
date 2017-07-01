def cpc_table(fd):
    import re,csv,os,urllib2,HTMLParser,zipfile
    from bs4 import BeautifulSoup as bs
    from datetime import date
    from zipfile import ZipFile
    from calendar import monthrange

    ### Get current month,day
    d = date.today()
    curyear = d.year
    curmonth = d.month
    curday = d.day

    days = monthrange(curyear,curmonth)[1]
    if curday != days and curmonth != 1:
        days = monthrange(curyear,curmonth-1)[1]
        if len(str(curmonth-1)) == 1:
            filedates = str(curyear)+'-0'+str(curmonth-1)+'-'+str(days)
        else:
            filedates = str(curyear)+'-'+str(curmonth-1)+'-'+str(days)
    elif curday != days and curmonth == 1:
        filedates = str(curyear-1)+'-12-31'
    else:
        if len(str(curmonth)) == 1:
            filedates = str(curyear)+'-0'+str(curmonth)+'-'+str(days)
        else:
            filedates = str(curyear)+'-'+str(curmonth)+'-'+str(days)

    import mechanize
    br = mechanize.Browser()
    ### Check the most recent date and update here for the classification files
    paturl = 'https://bulkdata.uspto.gov/data2/patent/classification/cpc/US_Grant_CPC_MCF_Text_'+filedates+'.zip'
    appurl = 'https://bulkdata.uspto.gov/data2/patent/classification/cpc/US_PGPub_CPC_MCF_Text_'+filedates+'.zip'
    br.retrieve(paturl,os.path.join(fd,'US_Grant_CPC_MCF_Text_'+filedates+'.zip'))
    br.retrieve(appurl,os.path.join(fd,'US_PGPub_CPC_MCF_Text_'+filedates+'.zip'))
    fd+='/'
    diri = os.listdir(fd)
    for d in diri:
        if re.search('US_Grant_CPC.*?zip',d):
            patclassfile = ZipFile(os.path.join(fd, d),'r')
        if re.search('US_PGPub_CPC.*?zip',d):
            appclassfile = ZipFile(os.path.join(fd, d), 'r')

    #For applications
    outp = csv.writer(open(os.path.join(fd,'applications_classes.csv'),'wb'),delimiter='\t')
    outp.writerow(['application_number','cpc_primary','cpc_additional'])
    data = {}

    def apps(num):
        for e,ii in enumerate(inp[num:]):
            original = ""
            cpc_prim = []
            cpc_add = []
            primate = ''
            appnum = ii[10:14]+'/'+ii[10:21]
            numi = 0
            if ii[45] == 'I':
                cpc_prim.append(re.sub('\s+','',ii[21:36]))
            else:
                cpc_add.append(re.sub('\s+','',ii[21:36]))
            if ii[44] == 'F':
                primate = [0,re.sub('\s+','',ii[21:36])]
            for ee,icheck in enumerate(inp[num+1:]):
                numi+=1
                app = icheck[10:14]+'/'+icheck[10:21]
                if app == appnum:
                    if icheck[45] == 'I':
                        cpc_prim.append(re.sub('\s+','',icheck[21:36]))
                    else:
                        cpc_add.append(re.sub('\s+','',icheck[21:36]))
                    if icheck[44] == 'F':
                        primate = [numi,re.sub('\s+','',ii[21:36])]
                else:
                    cpc_prim = '; '.join(cpc_prim)
                    cpc_add = '; '.join(cpc_add)
                    break
            outp.writerow([appnum,cpc_prim,cpc_add])
            break
        return num+ee+1

    for filename in appclassfile.namelist():
        if filename.endswith('txt'):
            inp = appclassfile.open(filename).read().split('\r\n')
            num = apps(0)
            for n in range(len(inp)):
                try:
                    num = apps(num)
                except:
                    break

    #For grants
    outp = csv.writer(open(os.path.join(fd,'grants_classes.csv'),'wb'),delimiter='\t')
    outp.writerow(['patent_number','cpc_primary','cpc_additional'])

    def grants(num):
        for e,ii in enumerate(inp[num:]):
            original = ""
            cpc_prim = []
            cpc_add = []
            primate = ''
            patnum = ii[10:17]
            patnum = '0'*(7-len(str(int(patnum))))+str(int(patnum))
            numi = 0
            if ii[41] == 'I':
                cpc_prim.append(re.sub('\s+','',ii[17:32]))
            else:
                cpc_add.append(re.sub('\s+','',ii[17:32]))
            if ii[40] == 'F':
                primate = [0,re.sub('\s+','',ii[17:32])]
            for ee,icheck in enumerate(inp[num+1:]):
                numi+=1
                pat = '0'*(7-len(str(int(icheck[10:17]))))+str(int(icheck[10:17]))
                if pat == patnum:
                    if icheck[41] == 'I':
                        cpc_prim.append(re.sub('\s+','',icheck[17:32]))
                    else:
                        cpc_add.append(re.sub('\s+','',icheck[17:32]))
                    if icheck[40] == 'F':
                        primate = [numi,re.sub('\s+','',ii[17:32])]
                else:
                    cpc_prim = '; '.join(cpc_prim)
                    cpc_add = '; '.join(cpc_add)
                    break
            outp.writerow([patnum,cpc_prim,cpc_add])

            break
        return num+ee+1

    for filename in patclassfile.namelist():
        if filename.endswith('txt'):
            inp = patclassfile.open(filename).read().split('\r\n')
            num = grants(0)
            for n in range(len(inp)):
                try:
                    num = grants(num)
                except:
                    break
