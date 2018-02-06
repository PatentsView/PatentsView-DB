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
        #this is what it used to be, I think it has changed
        #idd = i[0].split('-')[0]+'-'+str(int(i[0].split('-')[1])+1)
        idd = i[3]
        if i[5]!='':
            first = i[4]+' '+i[5]
        else:
            first = i[4]
        if i[7] != '':
            last = i[6]+', '+i[7]
        else:
            last = i[6]
        try:
            gg = data[idd]
            outp2.writerow([i[1].split("-")[0],idd])
        except:
            try:
                data[idd] = idd
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

def make_lookup(disambiguated_folder):
    for_lookup = csv.reader(open(disambiguated_folder + "/all-results.txt.post-processed",'rb'),delimiter='\t')
    lookup = {}
    for i in for_lookup:
        lookup[i[2]] = i[3]
    return lookup
def update(host,username, password, db, disambiguated_folder, lookup):
    inp = csv.reader(open(disambiguated_folder + "/rawinventor_for_update.csv",'rb'),delimiter='\t')
    outp = csv.writer(open(disambiguated_folder + "/rawinventor_updated.csv",'wb'),delimiter='\t')
    inp.next()
    counter = 0
    for i in inp:
        counter +=1
        if counter%500000==0:
            print str(counter)
        if counter == 1:
            outp.writerow(i)
        else:
            i[2]=lookup[i[0]]
            outp.writerow(i)
    mydb = MySQLdb.connect(host,username, password, db)
    cursor = mydb.cursor()
    cursor.execute('alter table rawinventor rename temp_rawinventor_backup')
    cursor.execute('create table rawinventor like temp_rawinventor_backup')
    mydb.commit()

def make_persistent(host,username, password, new_db,old_db, inventor_postprocess_folder):
    '''
    Creates a lookup that matches the previous round of disambiguated ids with the current round. 
    '''

    outp = csv.writer(open(inventor_postprocess_folder+'/inventor_rawinventor_clusters.tsv','wb'),delimiter='\t')
    previous_update_date = old_db[-8:]
    new_update_date = new_db[-8:]
    outp.writerow(['rawinventor_id','disamb_inventor_id_' + str(new_update_date),'disamb_inventor_id_' + str(previous_update_date)])
    cursor.execute('select uuid,inventor_id from ' + old_db + '.rawinventor')
    res1 = cursor.fetchall()
    cursor.execute('select uuid,inventor_id from ' + new_db + '.rawinventor')
    res2 = cursor.fetchall()
    counter = 0
    data = {}
    for r in res1:
        data[r[0]] = r[1]
    unmatched = 0
    existing_ids = set(data.keys())
    for r in res2:
        counter +=1
        if counter%500000==0:
            print counter
        if r[0] in existing_ids:
            existing_id = data[r[0]]
            outp.writerow([r[0],r[1],existing_id])
        else:
            unmatched +=1
            outp.writerow([r[0],'',r[1]])
    #also make table
    cursor.execute('create table ' + new_db + '.persistent_inventor_disambig (rawinventor_id varchar(40), disamb_inventor_id_' + str(new_update_date) + ' varchar(20), disamb_inventor_id_' +str(previous_update_date) + ' varchar(20));')
    mydb.commit()