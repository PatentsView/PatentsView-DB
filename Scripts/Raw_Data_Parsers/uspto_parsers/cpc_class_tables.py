def cpc_class_tables(outputdir,datadir):
    import re,os,csv
    from bs4 import BeautifulSoup as bs
    from unidecode import unidecode
    
    ### get the latest CPC specification in XML from http://www.cooperativepatentclassification.org/cpcSchemeAndDefinitions/Bulk.html
    outp = csv.writer(open(os.path.join(outputdir,'cpc_subsection.csv'),'wb'))
    outp2 = csv.writer(open(os.path.join(outputdir,'cpc_group.csv'),'wb'))
    outp3 = csv.writer(open(os.path.join(outputdir,'cpc_subgroup.csv'),'wb'))
    
    datum = {}
    diri = os.listdir(datadir)
    for d in diri:
        if re.search('-[A-Z].xml$',d):
            print d
            inp = open(os.path.join(datadir,d),'rb').read()
            soup = bs(inp)
            need = soup.findAll('classification-item')
            for s in need:
                level = s['level']
                if int(level) == 4:
                    again = bs(str(s))
                    title = again.findAll('classification-symbol')[0]
                    text = again.findAll('class-title')[0]
                    text_need = bs(str(text))
                    text_need = text_need.findAll('text')
                    text_class = [t.text for t in text if t.text == t.text.upper()]
                    if len(text_class) == 0:
                        text_class = [text_need[0].text]
                    text_class = '; '.join(text_class)
                    outp.writerow([title.text,text_class])
                if int(level) == 5:
                    again = bs(str(s))
                    title = again.findAll('classification-symbol')[0]
                    text = again.findAll('class-title')[0]
                    text_need = bs(str(text))
                    text_need = text_need.findAll('text')
                    text_class = [t.text for t in text_need if t.text == re.sub('E.G.','e.g.',t.text.upper()) or t.text == re.sub('I.E.','i.e.',t.text.upper())]
                    text_class = '; '.join(text_class)
                    try:    
                        outp2.writerow([title.text.decode('utf-8','ignore'),text_class.decode('utf-8','ignore')])
                    except:
                        outp2.writerow([unidecode(title.text),unidecode(text_class)])
    
        if re.search('-[A-Z]\d+[A-Z].xml$',d):
            print d
            inp = open(os.path.join(datadir,d),'rb').read()
            soup = bs(inp)
            need = soup.findAll('classification-item')
            data = {}
            for s in need:
                level = s['level']
                if int(level) == 7:
                        again = bs(str(s))
                        title = again.findAll('classification-symbol')[0]
                        text = again.findAll('class-title')[0]
                        text_need = bs(str(text))
                        text_need = text_need.findAll('text')
                        text_class = [t.text for t in text_need if re.search('^[A-Z]',t.text)]
                        if len(text_class) == 0:
                            try:
                                text_class = [text_need[0].text]
                            except:
                                text_class = ["NULL"]
                        text_class = '; '.join(text_class)
                        try:
                            data[7] = text_class.decode('utf-8','ignore')
                        except:
                            data[7] = unidecode(text_class)
                        outp3.writerow([title.text,data[7]])
                if int(level) == 8:
                        again = bs(str(s))
                        title = again.findAll('classification-symbol')[0]
                        text = again.findAll('class-title')[0]
                        text_need = bs(str(text))
                        text_need = text_need.findAll('text')
                        text_class = [t.text for t in text_need if re.search('^[A-Z]',t.text)]
                        if len(text_class) == 0:
                            try:
                                text_class = [text_need[0].text]
                            except:
                                text_class = ["NULL"]
                        text_class = '; '.join(text_class)
                        try:
                            data[8] = text_class.decode('utf-8','ignore')
                        except:
                            data[8] = unidecode(text_class)
                        outp3.writerow([title.text,data[7]+'-'+data[8]])
                for n in range(9,30):
                    if int(level) == n:
                        again = bs(str(s))
                        title = again.findAll('classification-symbol')[0]
                        text = again.findAll('class-title')[0]
                        text_need = bs(str(text))
                        text_need = text_need.findAll('text')
                        text_class = [t.text for t in text_need]
                        try:
                            data[n] = ' '.join(text_class).encode('utf-8','ignore')
                            query = data[7]
                            for nn in range(8,n+1):
                                query+='-'+data[nn]
                            try:
                                outp3.writerow([title.text,query.decode('utf-8','ignore')])
                            except:
                                outp3.writerow([title.text,query])
                        except:
                            try:
                                data[n] = ' '.join(text_class).decode('utf-8','ignore')
                            except:
                                data[n] = ' '.join(text_class)
                            query = data[7]
                            for nn in range(8,n):
                                query+='-'+data[nn]
                            try:
                                outp3.writerow([title.text,query.decode("utf-8",'ignore')])
                            except:
                                outp3.writerow([title.text,unidecode(query)])
                                
