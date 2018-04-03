from __future__ import division
import csv,random,nltk
import MySQLdb
import re,os
import string
from unidecode import unidecode
import jaro


def get(folder, host, user, password, database, incremental_ind):

    def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    punctuation = "( + ) [ ? : ! . ; ] * # % ` ' / _ = -".split()
    punctuation.append('"')

    ###SETUP MAJOR VARS

    fdmain = folder+ "/location_disambiguation/"
    #need to figure out what this is

    #separate first(0) and incremental(1) disambiguations
    incremental = incremental_ind

    # Step 1
    mydb = MySQLdb.connect(host,
    user,
    password,
    database)
    cursor = mydb.cursor()

    if incremental == 0:
        increm = ''
    else:
        increm = ' AND (location_id is NULL or location_id = "")'   

    print "Step 1..."

    cursor.execute('select distinct country_transformed from rawlocation where country_transformed is not NULL and country_transformed != "" and country_transformed!="s" and country_transformed!="B." and country_transformed!="omitted" '+increm)


    countries = [item[0] for item in cursor.fetchall() if item[0] is not None]
    print countries

    
    os.makedirs(fdmain)
    os.makedirs(fdmain+'uspto_disamb/')
    os.makedirs(fdmain+'uspto_disamb_counts/')
    os.makedirs(fdmain+'uspto_disamb_v2/')
    os.makedirs(fdmain+'uspto_disamb_loc_latlong/')
    os.makedirs(fdmain+'uspto_disamb_only_loc/')


    for c in countries: 
        print c
        datum = {}
        output = open(fdmain+'uspto_disamb/'+c+'.tsv','wb')
        output2 = open(fdmain+'uspto_disamb_counts/'+c+'.tsv','wb')
        outp = csv.writer(output,delimiter='\t')

        outp2 = csv.writer(output2,delimiter='\t')
        cursor.execute("select city,state,country_transformed,count(city) from rawlocation where country_transformed = '"+c+"'"+increm+"  group by city,state order by count(city) desc")
        outp2.writerows(cursor.fetchall())
        cursor.execute('select distinct state from rawlocation where country_transformed = "'+c+'"'+increm)
        states = [f[0] for f in cursor.fetchall()]
        for s in states:
            if str(s) == 'None' or str(s)=='NULL':
                cursor.execute('select id,city from rawlocation where country_transformed = "'+c+'" and (state is NULL or state="NULL")'+increm)
                s = ''
            else:
                s = re.sub('[\n\t\f\r]+','',s.strip())
                cursor.execute('select id,city from rawlocation where country_transformed = "'+c+'" and state ="'+s+'"'+increm)
            locs = [list(f) for f in cursor.fetchall()]
            for l in locs:
                ll = []
                for l1 in l:
                    if l1:
                        ll.append(re.sub('[\n\t\r\f]+','',l1.strip()))
                    else:
                        ll.append('')
                outp.writerow(ll+[s,c])
        output.close()
        output2.close()
        
    print "Step 2..."
    fd = fdmain+'uspto_disamb_counts/'
    diri = os.listdir(fd)

    mastdata = {}
    mastdatum = {}
    for d in diri:
        #this is separate from the forloop below because otherwise places that are in the wrong file break it
        mastdata[d.replace('.tsv','')] = {}
        mastdatum[d.replace('.tsv','')] = {}
    for d in diri:
        input = open(fd+d,'rb')
        inp = csv.reader(input,delimiter='\t')
        try:
            head = inp.next()
            top = int(head[-1])
        except:
            pass
        num = 1
        for i in inp:
            num+=1
        inp = csv.reader(file(fd+d),delimiter='\t')
        for e,i in enumerate(inp):
            if e<=int(num/3) and int(i[-1])>int(top/5):
                city = unidecode(i[0])
                for p in punctuation:
                    city = city.replace(p,'')
                city = re.sub('[0-9]+','',city)
                city = re.sub('^\s+','',city)
                city = re.sub('\s+$','',city)
                city = city.replace(' ','')
                state = i[1]
                state = re.sub('^\s+','',state)
                state = re.sub('\s+$','',state)
                country = i[2]
                key = id_generator(size=12)
                try:
                    gg = mastdata[country][city.lower()+'_'+state.lower()]
                except:
                    #print len(mastdata[country])
                    mastdata[country][city.lower()+'_'+state.lower()] = [key,i[0].strip(),i[1].strip(),i[2],int(i[3])]
                    mastdatum[country][city.lower()] = [key,i[0],i[1].strip(),i[2].strip(),int(i[3])]

        input.close()

    print "Step 3..."
    # Step 3
    fd = fdmain+'uspto_disamb/'
    diri = os.listdir(fd)
    for d in diri:
        output = open(fdmain+'uspto_disamb_v2/'+d,'wb')
        input = open(fd+d,'rb')
        outp = csv.writer(output,delimiter='\t')
        inp = csv.reader(input,delimiter='\t')
        data = mastdata[d.replace('.tsv','')]
        datum = mastdatum[d.replace(".tsv",'')]
        secdata = {}
        secdatum = {}
        for i in inp:
            city = unidecode(i[1])
            state = i[2]
            country = i[3]
            for p in punctuation:
                city = city.replace(p,'')
            city = re.sub('[0-9]+','',city)
            city = re.sub('^\s+','',city)
            city = re.sub('\s+$','',city)
            origcity = city
            city = city.replace(' ','')
                
            try:
                gg = data[city.lower()+'_'+state.lower()]
                outp.writerow(i+gg)
            except:
                try:
                    cit = city.lower().split(",")[0]
                    gg = data[cit.lower()+'_'+state.lower()]
                    
                    outp.writerow(i+gg)
                except:
                    try:
                        cit = city.lower().split("/")
                        for cc in cit:
                            gg = data[cc.lower()+'_'+state.lower()]
                            outp.writerow(i+gg)
                            break
                    except:
                        try:
                            cit = city.lower().split("-")
                            for cc in cit:
                                gg = data[cc.lower()+'_'+state.lower()]
                                outp.writerow(i+gg)
                                break
                        except:
                            try:
                                cit = city.lower().split("&")[0]
                                gg = data[cit.lower()+'_'+state.lower()]
                            
                                outp.writerow(i+gg)
                            except:
                                try:
                                    gg = datum[city.lower()]
                        
                                    outp.writerow(i+gg)
                                except:
                                    try:
                                        
                                        howdy = 0
                                        
                                        for k,v in data.items():
                                                dist = jaro.jaro_winkler_metric((city.lower()+'_'+state.lower()).decode('utf-8','ignore'),k.decode('utf-8','ignore'))
                                                edit = nltk.edit_distance(city.lower()+'_'+state.lower(),k)
                                                if (re.search(k.split("_")[0],city.lower()) and k.split("_")[0]!='') or dist >= 0.95 or (edit==2 and len(city.lower())>5):
                                                    outp.writerow(i+v)
                                                    howdy = 1
                                                    break
                                            
                                        gg = datum[city]
                                    except:
                                        if howdy == 0:
                                            cit = [cc for cc in origcity.lower().split(" ") if len(cc) > 4]
                                            
                                            howdy2 = 0
                                            for cc in cit:
                                                try:
                                                    gg = datum[cc]
                                                    
                                                    outp.writerow(i+gg)
                                                    howdy2 = 1
                                                    break
                                                except:
                                                    pass
                                            
                                            if howdy2 == 0:
                                                try:
                                                    gg = secdata[city.lower()+'_'+state.lower()]
                                                    outp.writerow(i+gg)
                                                except:
                                                    try:
                                                        cit = city.lower().split(",")[0]
                                                        gg = secdata[cit.lower()+'_'+state.lower()]
                                                        outp.writerow(i+gg)
                                                    except:
                                                        try:
                                                            cit = city.lower().split("&")[0]
                                                            gg = secdata[cit.lower()+'_'+state.lower()]
                                                            outp.writerow(i+gg)
                                                        except:
                                                            try:
                                                                gg = secdatum[city.lower()]
                                                                outp.writerow(i+gg)
                                                            except:
                                                                try:
                                                                    howdy = 0
                                                                    gg = datum[city]
                                                                except:
                                                                    if howdy == 0:
                                                                        cit = [cc for cc in origcity.lower().split(" ") if len(cc) > 4]
                                                                        howdy2 = 0
                                                                        for cc in cit:
                                                                            try:
                                                                                gg = secdatum[cc]
                                                                                outp.writerow(i+gg)
                                                                                howdy2 = 1
                                                                                break
                                                                            except:
                                                                                pass
                                                                        if howdy2 == 0:
                                                                            key = id_generator(size=12)
                                                                            secdata[city.lower()+'_'+state.lower()] = [key,i[1],i[2],i[3]]
                                                                            secdatum[city.lower()] = [key,i[1],i[2],i[3]]
                                                                            outp.writerow(i+[key,i[1],i[2],i[3]])
        input.close()
        output.close()
                                    
    print "Step 4..."
    #Step 4
    fd = fdmain+'uspto_disamb_v2/'
    fd3 = fdmain+'uspto_disamb_only_loc/'
    diri = os.listdir(fd)

    for d in diri:
        input = open(fd+d,'rb')
        output = open(fd3+d,'wb')
        inp = csv.reader(input,delimiter='\t')
        outp2 = csv.writer(output,delimiter='\t')
        data = {}
        final = {}
        disamb = {}
        for i in inp:
            try:
                gg = data[' '.join(i[5:])]
                final[i[0]] = i[:4]+[gg]+i[5:]
            except:
                try:
                    data[' '.join(i[5:])] = i[4]
                    final[i[0]] = i
                    disamb[i[4]] = i[4:]
                except:
                    print d,i
        input.close()
        for k,v in disamb.items():
            if len(v) == 5:
                v = v[:-1]
            outp2.writerow(v)
        output.close()

    #exit()
    print "Done Step 1 - 4"
