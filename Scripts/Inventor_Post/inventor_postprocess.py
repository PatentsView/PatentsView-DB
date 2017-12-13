import re,csv,os,codecs
import MySQLdb
from unidecode import unidecode
from HTMLParser import HTMLParser
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def inventor_process(postprocessed_loc):
    '''
    parameters are full file path to the postprocessed file and then full file paths to the desired locations of the two output files
    '''
    h = HTMLParser()
    inp = csv.reader(open(postprocessed_loc + "/all-results.txt.post-processed",'rb'),delimiter='\t')
    output_file = postprocessed_loc + "/inventor_disambig.csv"
    output_file_pairs = postprocessed_loc + "/inventor_pairs.csv"

    outfile = open(output_file,'wb')
    outfile.write(codecs.BOM_UTF8)
    outp = csv.writer(outfile,delimiter='\t')

    outfile2 = open(output_file_pairs,'wb')
    outfile2.write(codecs.BOM_UTF8)
    outp2 = csv.writer(outfile2,delimiter='\t')

    counter =0
    data = {}
    for i in inp:
    	counter +=1
        idd = i[0].split('-')[0]+'-'+str(int(i[0].split('-')[1])+1)
        if i[5]!='':
            first = i[4]+' '+i[5]
        else:
            first = i[4]
        if i[7] != '':
            last = i[6]+', '+i[7]
        else:
            last = i[6]
        try:
            gg = data[i[2]]
            outp2.writerow([i[1],gg])
        except:
            try:
                data[i[2]] = idd
                outp.writerow([idd,h.unescape(unidecode(first)),h.unescape(unidecode(last))])
                
                outp2.writerow([i[1].split("-")[0],idd])
            except:
                print counter
                print i
                print idd
                print h.unescape(unidecode(first))
                print h.unescape(unidecode(last))
                print first
                print last


            
    print len(data.keys())
