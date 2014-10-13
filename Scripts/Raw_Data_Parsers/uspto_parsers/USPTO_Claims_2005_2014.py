def claims_2005(fd,host,username,password,dbname):
    import re,os,csv,MySQLdb,random,string
    from bs4 import BeautifulSoup as bs

    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    
    mydb = MySQLdb.connect(host=host,
        user=username,
        passwd=password,
        db=dbname)
    cursor = mydb.cursor()
    cursor.execute('select id from '+dbname+'.patent where year(date) >= 2005')
    patnums = [f[0] for f in cursor.fetchall()]
    patents = {}
    for p in patnums:
        patents[p] = 1
    
    diri = os.listdir(fd)
    
    #For 2005-2014
    numi = 0
    for d in diri:
            print d
            inp = open(os.path.join(fd,d),'rb').read()
            gg = inp.split('<!DOCTYPE')
            del gg[0]
            for g in gg:
                    ch = re.search('claims',g)
                    nu = 0
                    num = re.search('<doc-number>(\w+)</doc-number>',g).group(1)
                    updnum = re.sub('^H0','H',num)[:8]
                    updnum = re.sub('^RE0','RE',updnum)[:8]
                    updnum = re.sub('^PP0','PP',updnum)[:8]
                    updnum = re.sub('^PP0','PP',updnum)[:8]
                    updnum = re.sub('^D0', 'D', updnum)[:8]
                    if len(num) > 7 and num.startswith('0'):
                        updnum = num[1:8]
                    try:
                        ggg = patents[updnum]
                        cursor.execute('DELETE FROM '+dbname+'.claim where patent_id="'+updnum+'"')
                        text = re.search('<claims(.*)</claims>',g,re.DOTALL).group()
                        soup = bs(text)
                        claims = soup.findAll('claim')
                        for so in claims:
                            nu+=1
                            clnum = so['num']
                            clid = so['id']
                            try:
                                dependent = re.search('<claim-ref idref="CLM-(\d+)">.*?</claim-ref>',str(so),re.DOTALL).group(1)
                                dependent = str(int(dependent))
                            except:
                                dependent = "NULL"
                            need = re.sub('<.*?>|</.*?>','',str(so))
                            need = re.sub('[\n\t\r\f]+','',need)
                            need = re.sub('^\d+\. ','',need)
                            need = need.replace("'",'')
                            need = need.replace('"','')
                            query = 'INSERT INTO '+dbname+'.claim VALUES ("'+'","'.join([id_generator(),updnum,need,dependent,str(int(clnum))])+'")'
                            query = query.replace(',"NULL"',',NULL')
                            cursor.execute(query)
                    except:
                        pass
                
    mydb.commit()

