
def locs(fd, fips_location, api_key):
    import re,os,csv
    
    import requests, json
    from unidecode import unidecode
    import time
    
    # Defining function using Google API to fetch Geo info based on inputs
    def _whois_gmap(**kwargs):
        kwargs['sensor'] = 'false'
        url = 'https://maps.googleapis.com/maps/api/geocode/json'
        r = requests.get(url, params=kwargs)
        return json.loads(r.text)
    
    punctuation = "( + ) [ ? : ! . ; ] * # % ` ' / _ =".split()
    punctuation.append('"')
    
    fips = csv.reader(file(fips_location + '/census_fips.csv','rb'))
    fips.next()
    fipsdata = {}
    for f in fips:
        fipsdata[f[3]+'_'+f[0]] = [f[1],f[2]]
    
    from HTMLParser import HTMLParser
    h = HTMLParser()
    existlocs = {}

    fdlocs = fd+'uspto_disamb_only_loc/'
    output = open(fd+'uspto_disamb_loc_latlong/location_disamb_US_google.tsv','wb')
    outp = csv.writer(output,delimiter='\t')
    diri = ['US.tsv']
    data = {}
    for d in diri:
        inp = csv.reader(file(fdlocs+d,'rb'),delimiter='\t')
        for e,i in enumerate(inp):
            try:
                gg = existlocs['_'.join(i[1:])]
                outp.writerow([i[0]]+gg+i[1:]+['google'])
                continue
            except:
                pass
            county = ''
            state = ''
            cit = ''
            if e%10 == 0:
                time.sleep(1)
            city = re.sub('^\s+','',i[1])
            city = re.sub('\s+$','',city)
            city = unidecode(h.unescape(city))
            for p in punctuation:
                city = city.replace(p,'')
            city = re.sub('[0-9]+','',city)
            if city == '':
                outp.writerow(i[:4]+[0.1,0.1])
                continue
            origcountry = i[3]
            try:
                gg = data[i[0]]
                outp.writerow([i[0]]+gg+i[1:])
            except:
                GmapLocation = _whois_gmap(address = city+'+'+i[2]+'+'+origcountry,key=api_key)
                if GmapLocation['status'] == "ZERO_RESULTS":
                    GmapLocation = _whois_gmap(address = city+'+'+i[2],key=api_key)
                    if GmapLocation['status'] == "ZERO_RESULTS":
                        GmapLocation = _whois_gmap(address = city+'+USA',key=api_key)
                        print city,i[2],origcountry
                if GmapLocation['status'] == 'REQUEST_DENIED':
                    print i[0],e
                if GmapLocation['status'] == 'OVER_QUERY_LIMIT':
                    print "query limit reached",i[0]
                    exit()
                if GmapLocation['status'] == 'OK':
                    lat = GmapLocation['results'][0]['geometry']['location']['lat']
                    long = GmapLocation['results'][0]['geometry']['location']['lng']
                    address = GmapLocation['results'][0]['address_components']
                    state = ''
                    county = ''
                    cit = ''
                    for a in address:
                        if 'locality' in a['types']:
                            cit = a['short_name']
                        if 'country' in a['types']:
                            country = a['short_name']
                        if 'administrative_area_level_1' in a['types']:
                            state = a['short_name']
                        if 'administrative_area_level_2' in a['types']:
                            county = a['short_name']
                    if country=='US':
                        try:
                            fcode = fipsdata[county+'_'+state]
                        except:
                            flag = 0
                            for k,v in fipsdata.items():
                                flag = 1
                                if re.search(county,k) and re.search(state,k):
                                    county = k.split("_")[0]
                                    fcode = v
                            if flag == 0:
                                fcode = ['','']
                        newline = [i[0],cit,state,country,lat,long,county]+fcode+i[1:]+["google"]
                    else:
                        newline = [i[0],cit,state,country,lat,long,'','','']+i[1:]+["google"]
                    print newline,i
                else:
                    howdy = 0
                    if len(city.split(" ")) > 1:
                        seccity = [ii for ii in city.split(' ') if len(ii) > 4]
                        for c in seccity:
                            GmapLocation = _whois_gmap(address = c+'+'+i[2]+'+'+origcountry,key=api_key)
                            if GmapLocation['status'] == 'OK':
                                c = re.sub('^\s+','',c)
                                c = re.sub('\s+$','',c)
                                lat = GmapLocation['results'][0]['geometry']['location']['lat']
                                long = GmapLocation['results'][0]['geometry']['location']['lng']
                                address = GmapLocation['results'][0]['address_components']
                                state = ''
                                county = ''
                                cit = ''
                                for a in address:
                                    if 'locality' in a['types']:
                                        cit = a['short_name']
                                    if 'country' in a['types']:
                                        country = a['short_name']
                                    if 'administrative_area_level_1' in a['types']:
                                        state = a['short_name']
                                    if 'administrative_area_level_2' in a['types']:
                                        county = a['short_name']
                                if country=='US':
                                    try:
                                        fcode = fipsdata[county+'_'+state]
                                    except:
                                        flag = 0
                                        for k,v in fipsdata.items():
                                            flag = 1
                                            if re.search(county,k) and re.search(state,k):
                                                county = k.split("_")[0]
                                                fcode = v
                                        if flag == 0:
                                            fcode = ['','']
                                    newline = [i[0],cit,state,country,lat,long,county]+fcode+i[1:]+["google"]
                                else:
                                    newline = [i[0],cit,state,country,lat,long,'','','']+i[1:]+["google"]
                                print newline,i
                                howdy = 1
                                break
                    if howdy == 0:
                        if len(city.split("-")) > 1:
                            seccity = [ii for ii in city.split('-') if len(ii) > 4]
                            for c in seccity:
                                c = re.sub('^\s+','',c)
                                c = re.sub('\s+$','',c)
                                GmapLocation = _whois_gmap(address = c+'+'+i[2]+'+'+origcountry,key=api_key)
                                if GmapLocation['status'] == 'OK':
                                    lat = GmapLocation['results'][0]['geometry']['location']['lat']
                                    long = GmapLocation['results'][0]['geometry']['location']['lng']
                                    address = GmapLocation['results'][0]['address_components']
                                    state = ''
                                    county = ''
                                    cit = ''
                                    for a in address:
                                        if 'locality' in a['types']:
                                            cit = a['short_name']
                                        if 'country' in a['types']:
                                            country = a['short_name']
                                        if 'administrative_area_level_1' in a['types']:
                                            state = a['short_name']
                                        if 'administrative_area_level_2' in a['types']:
                                            county = a['short_name']
                                    if country=='US':
                                        try:
                                            fcode = fipsdata[county+'_'+state]
                                        except:
                                            flag = 0
                                            for k,v in fipsdata.items():
                                                flag = 1
                                                if re.search(county,k) and re.search(state,k):
                                                    county = k.split("_")[0]
                                                    fcode = v
                                            if flag == 0:
                                                fcode = ['','']
                                        newline = [i[0],cit,state,country,lat,long,county]+fcode+i[1:]+["google"]
                                    else:
                                        newline = [i[0],cit,state,country,lat,long,'','','']+i[1:]+["google"]
                                    print newline,i
                                    howdy = 1
                                    break
                            
                    if howdy == 0:
                        if len(city.split(",")) > 1:
                            seccity = [ii for ii in city.split(',') if len(ii) > 4]
                            for c in seccity:
                                c = re.sub('^\s+','',c)
                                c = re.sub('\s+$','',c)
                                GmapLocation = _whois_gmap(address = c+'+'+i[2]+'+'+origcountry,key=api_key)
                                if GmapLocation['status'] == 'OK':
                                    lat = GmapLocation['results'][0]['geometry']['location']['lat']
                                    long = GmapLocation['results'][0]['geometry']['location']['lng']
                                    address = GmapLocation['results'][0]['address_components']
                                    state =''
                                    county = ''
                                    cit = ''
                                    for a in address:
                                        if 'locality' in a['types']:
                                            cit = a['short_name']
                                        if 'country' in a['types']:
                                            country = a['short_name']
                                        if 'administrative_area_level_1' in a['types']:
                                            state = a['short_name']
                                        if 'administrative_area_level_2' in a['types']:
                                            county = a['short_name']
                                    if country=='US':
                                        try:
                                            fcode = fipsdata[county+'_'+state]
                                        except:
                                            flag = 0
                                            for k,v in fipsdata.items():
                                                flag = 1
                                                if re.search(county,k) and re.search(state,k):
                                                    county = k.split("_")[0]
                                                    fcode = v
                                            if flag == 0:
                                                fcode = ['','']
                                        newline = [i[0],cit,state,country,lat,long,county]+fcode+i[1:]+["google"]
                                    else:
                                        newline = [i[0],cit,state,country,lat,long,'','','']+i[1:]+["google"]
                                    print newline,i
                                    howdy = 1
                                    break
        
                    if howdy == 0:
                        if len(city.split("/")) > 1:
                            seccity = [ii for ii in city.split('/') if len(ii) > 4]
                            for c in seccity:
                                c = re.sub('^\s+','',c)
                                c = re.sub('\s+$','',c)
                                GmapLocation = _whois_gmap(address = c+'+'+i[2]+'+'+origcountry,key=api_key)
                                if GmapLocation['status'] == 'OK':
                                    lat = GmapLocation['results'][0]['geometry']['location']['lat']
                                    long = GmapLocation['results'][0]['geometry']['location']['lng']
                                    address = GmapLocation['results'][0]['address_components']
                                    state = ''
                                    county = ''
                                    cit = ''
                                    for a in address:
                                        if 'locality' in a['types']:
                                            cit = a['short_name']
                                        if 'country' in a['types']:
                                            country = a['short_name']
                                        if 'administrative_area_level_1' in a['types']:
                                            state = a['short_name']
                                        if 'administrative_area_level_2' in a['types']:
                                            county = a['short_name']
                                    if country=='US':
                                        try:
                                            fcode = fipsdata[county+'_'+state]
                                        except:
                                            flag = 0
                                            for k,v in fipsdata.items():
                                                flag = 1
                                                if re.search(county,k) and re.search(state,k):
                                                    county = k.split("_")[0]
                                                    fcode = v
                                            if flag == 0:
                                                fcode = ['','']
                                        newline = [i[0],cit,state,country,lat,long,county]+fcode+i[1:]+["google"]
                                    else:
                                        newline = [i[0],cit,state,country,lat,long,'','','']+i[1:]+["google"]
                                    print newline,i
                                    howdy = 1
                                    break
                            
                    
                    if howdy == 0:
                        newline = i+[0.0,0.0]
                
                try:
                    outp.writerow(newline)
                except:
                    try:
                        newline[1] = unidecode(newline[1])
                        outp.writerow(newline)
                    except:
                        try:
                            newline[2] = ""
                            outp.writerow(newline)
                        except:
                            try:
                                newline[1] = newline[2]
                                outp.writerow(newline)
                            except:
                                print "malformed",i[0]
                                outp.writerow(i+[lat,long])
    output.close()