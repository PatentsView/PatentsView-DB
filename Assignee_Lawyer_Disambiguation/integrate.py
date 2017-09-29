#!/usr/bin/env python
"""
Copyright (c) 2013 The Regents of the University of California, AMERICAN INSTITUTES FOR RESEARCH
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
"""
@author Gabe Fierro gt.fierro@berkeley.edu github.com/gtfierro
"""
"""
Takes in a CSV file that represents the output of the disambiguation engine:
  Patent Number, Firstname, Lastname, Unique_Inventor_ID
Groups by Unique_Inventor_ID and then inserts them into the Inventor table using
lib.alchemy.match
"""

import sys
import lib.alchemy as alchemy
from lib.util.csv_reader import read_file
from lib.alchemy import is_mysql
from lib.alchemy.schema import Inventor, RawInventor, patentinventor, App_Inventor, App_RawInventor, applicationinventor
from lib.handlers.xml_util import normalize_document_identifier
from collections import defaultdict
import cPickle as pickle
import linecache
from datetime import datetime
import pandas as pd
from collections import defaultdict, Counter
from lib.tasks import bulk_commit_inserts, bulk_commit_updates
from unidecode import unidecode
from datetime import datetime
import name_parser

def integrate(disambig_input_file, disambig_output_file):
    """
    We have two files: the input to the disambiguator:
        uuid, first name, middle name, last name, patent, mainclass, subclass, city, state, country, rawassignee, disambiguated assignee
    And the output of the disambiguator:
        uuid, unique inventor id

    The files will line up line by line, so we can easily get the collection of raw
    records that map to a single disambiguated record (D_REC).  For each of the raw records
    for a given disambiguated id (D_ID), we want to vote the most frequent values for
    each of the columns, and use those to populate the D_REC.


    just have to populate the fields of the disambiguated inventor object:
        inventor id, first name, last name, nationality (?)
    """
    disambig_input = pd.read_csv(disambig_input_file,header=None,delimiter='\t',encoding='utf-8')
    disambig_output = pd.read_csv(disambig_output_file,header=None,delimiter='\t',encoding='utf-8')
    disambig_input[0] = disambig_input[0].apply(str)
    disambig_output[0] = disambig_output[0].apply(str)
    print 'finished loading csvs'
    merged = pd.merge(disambig_input, disambig_output, on=0)
    # If there are any prior_inventor_ids at all, then the merge will have 15 columns. Otherwise, if there are not any
    # prior_inventor_ids, then the merge will not have a column for it and there will be only 14 columns.
    if disambig_input.shape[1] > 14:
        merged.columns = ['rawinventor_uuid','isgrant','granted','name_first','name_middle','name_last','patent_id','mainclass','subclass','city','state','country','assignee','rawassignee','prev_inventorid','current_inventorid']
    else:
        merged.columns = ['rawinventor_uuid','isgrant','granted','name_first','name_middle','name_last','patent_id','mainclass','subclass','city','state','country','assignee','rawassignee','current_inventorid']
    print 'finished merging'
    apps = merged[merged['isgrant'] == 0]


    inventor_attributes = merged[['isgrant','rawinventor_uuid','current_inventorid','name_first','name_middle','name_last','patent_id']] # rawinventor uuid, inventor id, first name, middle name, last name, patent_id
    inventor_attributes = inventor_attributes.dropna(subset=['rawinventor_uuid'],how='all')
    inventor_attributes['name_first'] = inventor_attributes['name_first'].fillna('')
    inventor_attributes['name_middle'] = inventor_attributes['name_middle'].fillna('')
    inventor_attributes['name_last'] = inventor_attributes['name_last'].fillna('')

    grants = inventor_attributes[inventor_attributes['isgrant'] == 1]
    apps = inventor_attributes[inventor_attributes['isgrant'] == 0]
    del grants['isgrant']
    del apps['isgrant']

    ####### DO GRANTS #######
    rawinventors = defaultdict(list)
    inventor_inserts = []
    rawinventor_updates = []
    patentinventor_inserts = []
    for row in grants.iterrows():
        uuid = row[1]['current_inventorid']
        rawinventors[uuid].append(row[1])
        patentinventor_inserts.append({'inventor_id': uuid, 'patent_id': row[1]['patent_id']})
    print 'finished associating ids'
    i = 0
    for inventor_id in rawinventors.iterkeys():
        i += 1
        freq = defaultdict(Counter)
        param = {}
        rawuuids = []
        names = []
        for raw in rawinventors[inventor_id]:
            rawuuids.append(raw[0])
            name = ' '.join(x for x in (raw['name_first'], raw['name_middle'], raw['name_last']) if x)
            freq['name'][name] += 1
            for k,v in raw.iteritems():
                freq[k][v] += 1
        param['id'] = inventor_id
        name = freq['name'].most_common(1)[0][0]
        parsedNames = name_parser.parse_name(name_parser.NameFormat.CITESEERX, name)
        name_first = ' '.join(filter(None, (parsedNames.Prefix, parsedNames.GivenName, parsedNames.OtherName)))
        name_last = ' '.join(filter(None, (parsedNames.FamilyName, parsedNames.Suffix)))
        param['name_first'] = name_first
        param['name_last'] = name_last
        param['nationality'] = ''
        assert set(param.keys()) == {'id','name_first','name_last','nationality'}
        inventor_inserts.append(param)
        for rawuuid in rawuuids:
            rawinventor_updates.append({'pk': rawuuid, 'update': param['id']})
        if i % 100000 == 0:
            print i, datetime.now(), rawuuids[0]
    print 'finished voting'
    session_generator = alchemy.session_generator(dbtype='grant')
    session = session_generator()
    if alchemy.is_mysql():
        session.execute('truncate inventor; truncate patent_inventor;')
    else:
        session.execute('delete from inventor; delete from patent_inventor;')

    from lib.tasks import bulk_commit_inserts, bulk_commit_updates
    bulk_commit_inserts(inventor_inserts, Inventor.__table__, is_mysql(), 20000,'grant')
    bulk_commit_inserts(patentinventor_inserts, patentinventor, is_mysql(), 20000,'grant')
    bulk_commit_updates('inventor_id', rawinventor_updates, RawInventor.__table__, is_mysql(), 20000,'grant')


    ###### DO APPLICATIONS ######

    rawinventors = defaultdict(list)
    inventor_inserts = []
    rawinventor_updates = []
    applicationinventor_inserts = []
    for row in apps.iterrows():
        uuid = row[1]['current_inventorid']
        rawinventors[uuid].append(row[1])
        applicationinventor_inserts.append({'inventor_id': uuid, 'application_id': row[1]['patent_id']})
    print 'finished associating ids'
    i = 0
    for inventor_id in rawinventors.iterkeys():
        i += 1
        freq = defaultdict(Counter)
        param = {}
        rawuuids = []
        names = []
        for raw in rawinventors[inventor_id]:
            rawuuids.append(raw[0])
            name = ' '.join(x for x in (raw['name_first'], raw['name_middle'], raw['name_last']) if x)
            freq['name'][name] += 1
            for k,v in raw.iteritems():
                freq[k][v] += 1
        param['id'] = inventor_id
        name = freq['name'].most_common(1)[0][0]
        parsedNames = name_parser.parse_name(name_parser.NameFormat.CITESEERX, name)
        name_first = ' '.join(filter(None, (parsedNames.Prefix, parsedNames.GivenName, parsedNames.OtherName)))
        name_last = ' '.join(filter(None, (parsedNames.FamilyName, parsedNames.Suffix)))
        param['name_first'] = name_first
        param['name_last'] = name_last
        param['nationality'] = ''
        assert set(param.keys()) == {'id','name_first','name_last','nationality'}
        inventor_inserts.append(param)
        for rawuuid in rawuuids:
            rawinventor_updates.append({'pk': rawuuid, 'update': param['id']})
        if i % 100000 == 0:
            print i, datetime.now(), rawuuids[0]
    print 'finished voting'
    session_generator = alchemy.session_generator(dbtype='application')
    session = session_generator()
    if alchemy.is_mysql():
        session.execute('truncate inventor; truncate application_inventor;')
    else:
        session.execute('delete from inventor; delete from application_inventor;')

    from lib.tasks import bulk_commit_inserts, bulk_commit_updates
    bulk_commit_inserts(inventor_inserts, App_Inventor.__table__, is_mysql(), 20000,'application')
    bulk_commit_inserts(applicationinventor_inserts, applicationinventor, is_mysql(), 20000,'application')
    bulk_commit_updates('inventor_id', rawinventor_updates, App_RawInventor.__table__, is_mysql(), 20000,'application')


    session_generator = alchemy.session_generator(dbtype='grant')
    session = session_generator()
    doctype = 'grant'
    session.execute('truncate location_assignee;')
    res = session.execute('select distinct location_id, assignee_id from patent \
        left join rawassignee on rawassignee.patent_id = patent.id \
        left join rawlocation on rawlocation.id = rawassignee.rawlocation_id \
        where assignee_id != "" and location_id != "";')
    assigneelocation = pd.DataFrame.from_records(res.fetchall())
    assigneelocation.columns = ['location_id','assignee_id']
    assigneelocation = assigneelocation.sort('assignee_id')
    print assigneelocation.info()
    locationassignee_inserts = [row[1].to_dict() for row in assigneelocation.iterrows()]
    bulk_commit_inserts(locationassignee_inserts, alchemy.schema.locationassignee, alchemy.is_mysql(), 20000, 'grant')

    session.execute('truncate location_inventor;')
    res = session.execute('select distinct location_id, inventor_id from patent \
        left join rawinventor on rawinventor.patent_id = patent.id \
        left join rawlocation on rawlocation.id = rawinventor.rawlocation_id \
        where inventor_id != "" and location_id != "";')
    inventorlocation = pd.DataFrame.from_records(res.fetchall())
    inventorlocation.columns = ['location_id','inventor_id']
    inventorlocation = inventorlocation.sort('inventor_id')
    print inventorlocation.info()
    locationinventor_inserts = [row[1].to_dict() for row in inventorlocation.iterrows()]
    bulk_commit_inserts(locationinventor_inserts, alchemy.schema.locationinventor, alchemy.is_mysql(), 20000, 'grant')

    doctype = 'application'
    session_generator = alchemy.session_generator(dbtype='application')
    session = session_generator()
    session.execute('truncate location_assignee;')
    res = session.execute('select distinct location_id, assignee_id from application \
        left join rawassignee on rawassignee.application_id = application.id \
        left join rawlocation on rawlocation.id = rawassignee.rawlocation_id \
        where assignee_id != "" and location_id != "";')
    assigneelocation = pd.DataFrame.from_records(res.fetchall())
    assigneelocation.columns = ['location_id','assignee_id']
    assigneelocation = assigneelocation.sort('assignee_id')
    print assigneelocation.info()
    locationassignee_inserts = [row[1].to_dict() for row in assigneelocation.iterrows()]
    bulk_commit_inserts(locationassignee_inserts, alchemy.schema.app_locationassignee, alchemy.is_mysql(), 20000, 'application')

    session.execute('truncate location_inventor;')
    res = session.execute('select distinct location_id, inventor_id from application \
        left join rawinventor on rawinventor.application_id = application.id \
        left join rawlocation on rawlocation.id = rawinventor.rawlocation_id \
        where inventor_id != "" and location_id != "";')
    inventorlocation = pd.DataFrame.from_records(res.fetchall())
    inventorlocation.columns = ['location_id','inventor_id']
    inventorlocation = inventorlocation.sort('inventor_id')
    print inventorlocation.info()
    locationinventor_inserts = [row[1].to_dict() for row in inventorlocation.iterrows()]
    bulk_commit_inserts(locationinventor_inserts, alchemy.schema.app_locationinventor, alchemy.is_mysql(), 20000, 'application')


def main():
    if len(sys.argv) <= 2:
        print 'USAGE: python integrate.py <disambig input file> <disambig output file>'
        sys.exit()
    dis_in = sys.argv[1]
    dis_out = sys.argv[2]
    integrate(dis_in,dis_out)

if __name__ == '__main__':
    main()
