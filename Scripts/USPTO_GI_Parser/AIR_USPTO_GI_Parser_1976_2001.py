import re,os,csv

# fd = 'H:/share/Science Policy Portfolio/Patent Data 1976-Present/XML/'
# assumes "data-1976" directory
fd = 'data-1976/'

diri = os.listdir(fd)
outp = csv.writer(open('output-1975-2001-v2.csv','wb'))
checks = open('output-check_files-v2.txt','w')
bad = open('output-bads.txt','w')
data = {}

for d in diri:
    if re.search('\.txt',d):
        print>>checks,d
        infile = open(os.path.join(fd,d)).read().split('PATN')
        for i in infile:
            check = re.search('GOVERNMENT INTEREST|\nGOVT',i,re.I)
            if check:
                try:
                    num = re.search('WKU\s+(\w+).*?\n',i).group(1)
                except:
                    num = ''
                try:
                    apn = re.search('APN\s+(\w+).*?\n',i).group(1)
                except:
                    apn = ''
                if num == '' and apn =='':
                    print>>bad,d+'\n'+i+'\n'+'____'
                govt = re.search('GOVT\nPAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}',i,re.DOTALL)
                try:
                    text = re.sub('[\n\t\r\f]+','',govt.group(2))
                    text = re.sub('\s+',' ',text)
                    outp.writerow([num,apn,govt.group(1),text])
                except:
                    try:
                        govt = re.search('GOVT\s+\nPAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}',i,re.DOTALL)
                        text = re.sub('[\n\t\r\f]+','',govt.group(2))
                        text = re.sub('\s+',' ',text)
                        outp.writerow([num,apn,govt.group(1),text])
                    except:
                        try:
                            govt = re.search('GOVT.*?PAR\s+(.*?)\n[A-Z]{3,10}',i,re.DOTALL)
                            text = re.sub('[\n\t\r\f]+','',govt.group(1))
                            text = re.sub('\s+',' ',text)
                            outp.writerow([num,apn,'Statement of Government Interest',text])
                        except:
                            try:
                                govt = re.search('PARN.*?PAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}',i,re.DOTALL)
                                text = re.sub('[\n\t\r\f]+','',govt.group(2))
                                text = re.sub('\s+',' ',text)
                                if re.search('government',govt.group(1),re.I):
                                    outp.writerow([num,apn,govt.group(1),text])
                                else:
                                    go = data[num]
                            except:
                                try:
                                    govt = re.search('BSUM.*?PAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}',i,re.DOTALL)
                                    text = re.sub('[\n\t\r\f]+','',govt.group(2))
                                    text = re.sub('\s+',' ',text)
                                    if re.search('government',govt.group(1),re.I):
                                        outp.writerow([num,apn,govt.group(1),text])
                                    else:
                                        go = data[num]
                                except:

                                    try:
                                        govt = re.search('ABST.*?PAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}',i,re.DOTALL)
                                        text = re.sub('[\n\t\r\f]+','',govt.group(2))
                                        text = re.sub('\s+',' ',text)
                                        if re.search('government',govt.group(1),re.I):
                                            outp.writerow([num,apn,govt.group(1),text])
                                        else:
                                            go = data[num]
                                    except:
                                        print>>bad, d+'\n'+i+'____'
                                        print d,i
