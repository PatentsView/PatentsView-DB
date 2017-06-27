import re,os,csv

# fd = 'z:/share/Science Policy Portfolio/Patent Data 1976-Present/XML/'
# assumes "data-2002" directory
fd = 'data-2002/'

diri = os.listdir(os.path.join(fd,'2005-2014/'))
outp = csv.writer(open('output-2002-2015.csv','wb'))


#For 2005-2014
numi = 0
for d in diri:
        checks = open('output-check_files-v2.txt','a')
        bad = open('output-bads.txt','a')
        print>>checks,d
        inp = open(os.path.join(fd,'2005-2014/',d),'rb').read()
        gg = inp.split('<!DOCTYPE')
        print len(gg)
        del gg[0]
        for g in gg:
                ch = re.search('GOVINT|government interest',g,re.I)
                if ch:
                    numi+=1
                    try:
                        text = re.search('<\?GOVINT.*?end="lead"\?>(.*?)<\?GOVINT.*?end="tail"',g,re.I|re.DOTALL).group(1)
                        try:
                            num = re.search('<doc-number>(\w+)</doc-number>',g).group(1)
                        except:
                            print "BADDDDDD"

                        try:
                            head = re.search('<heading.*?>(.*?)</heading',text).group(1)
                        except:
                            head = 'STATEMENT OF GOVERNMENT INTEREST'
                        descr = re.search('<p.*?>(.*?)</p>',text).group(1)
                        outp.writerow([ num,head,descr ])
                    except:
                        try:
                            text = re.search('>.*?GOVERNMENT INTEREST.*?<p.*?>(.*?)</p>',g,re.I|re.DOTALL).group(1)
                            try:
                                num = re.search('<doc-number>(\w+)</doc-number>',g).group(1)
                            except:
                                print "BADDDDDD"

                            head = 'STATEMENT OF GOVERNMENT INTEREST'
                            outp.writerow([ num,head,text ])
                        except:
                            print>>bad, d+'\n'+g

        checks.close()
        bad.close()


print numi

diri = os.listdir(os.path.join(fd,'2002-2004/'))

#For 2002-2004
outp = csv.writer(open('output-2002-2004.csv','wb'))
numi = 0
for d in diri:
        checks = open('output-check_files-v2.txt','a')
        bad = open('output-bads.txt','a')
        inp = open(os.path.join(fd,'2002-2004/',d),'rb').read()
        print>>checks,d
        gg = inp.split('<!DOCTYPE')
        print len(gg)
        del gg[0]
        for g in gg:
                ch = re.search('GOVINT|government interest',g,re.I)
                if ch:
                    numi+=1
                    try:
                        text = re.search('government interest.*?<PDAT>(.*?)</PDAT>',g,re.I|re.DOTALL).group(1)
                        try:
                            num = re.search('<DNUM><PDAT>(\w+)',g).group(1)
                        except:
                            print "BADDDDDD"
                        #print text
                        outp.writerow([ num,'STATEMENT OF GOVERNMENT INTEREST',text ])
                    except:
                            text = re.search('<GOVINT>.*?</GOVINT>',g,re.I|re.DOTALL).group()
                            try:
                                num = re.search('<DNUM><PDAT>(\w+)',g).group(1)
                            except:
                                print "BADDDDDD"

                            need = list(re.finditer('<PDAT>(.*?)</PDAT>',text))
                            if len(need) > 1:
                                #print "1",need[0].group(1),need[1].group(1)
                                outp.writerow([ num,need[0].group(1),need[1].group(1) ])
                            else:
                                #print "2",need[0].group(1)
                                outp.writerow([num,'STATEMENT OF GOVERNMENT INTEREST',need[0].group(1)])

        checks.close()
        bad.close()

print numi
