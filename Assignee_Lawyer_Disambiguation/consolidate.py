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
Takes the existing database (as indicated by the alchemy configuration file) and creates
a dump CSV file with the appropriate columns as needed for the disambiguation:

uuid, isgrant, ignore, name_first, name_last, patent number, mainclass, subclass, city, state, country, assignee, rawassignee, inventor_id

'record' refers to the unique tuple of (rawinventor, document number)

uuid: unique identifier assigned to this raw inventor
isgrant: true if this record is a granted patent (as opposed to a published application)
ignore: true if this record is an application that has been granted
name_first: first name of the raw inventor on this record
name_last: last name of the raw inventor on this record
patent number: the document number of this record. Will either be a patent number or an application number
mainclass: the primary main classification of this patent
subclass: the primary subclassification of this patent
city, state, country: the disambiguated location of the rawinventor on this record. To avoid having null
    entries in these columns, locations are (in order of precedence) disambiguated rawinventor location,
    rawinventor rawlocation, disambiguated location of primary inventor (under the assumption that
    coinventor are more likely to be colocated than not).
assignee: disambiguated assignee organization name OR first/last name of assignee (one or the other -- documents do not contain both)
rawassignee: raw assignee organization name OR first/last name of rawassignee as listed on this record
inventor_id: disambiguated inventor id assigned to the rawinventor on this record from the last inventor
    disambiguation run. NULL if this rawinventor record is new.

"""
import codecs
from lib import alchemy
from lib.assignee_disambiguation import get_cleanid
from lib.handlers.xml_util import normalize_utf8
from sqlalchemy.orm import joinedload, subqueryload
from sqlalchemy import extract
from datetime import datetime
import pandas as pd
import sys
import name_parser

#TODO: for ignore rows, use the uuid instead (leave blank if not ignore) and use that to link the ids together for integration

# create CSV file row using a dictionary. Use `ROW(dictionary)`
# isgrant: 1 if granted patent, 0 if application
# ignore: 1 if the record has a granted patent, 0 else
ROW = lambda x: u'{uuid}\t{isgrant}\t{ignore}\t{name_first}\t{name_middle}\t{name_last}\t{number}\t{mainclass}\t{subclass}\t{city}\t{state}\t{country}\t{assignee}\t{rawassignee}\n'.format(**x)

def main(year, doctype):
    # get patents as iterator to save memory
    # use subqueryload to get better performance by using less queries on the backend:
    # --> http://docs.sqlalchemy.org/en/latest/orm/tutorial.html#eager-loading
    session = alchemy.fetch_session(dbtype=doctype)
    schema = alchemy.schema.Patent
    if doctype == 'application':
        schema = alchemy.schema.App_Application
        if year:
            patents = (p for p in session.query(schema).filter(extract('year', schema.date) == year).options(subqueryload('rawinventors'), subqueryload('rawassignees'), subqueryload('classes')).yield_per(1))
        else:
            patents = (p for p in session.query(schema).options(subqueryload('rawinventors'), subqueryload('rawassignees'), subqueryload('classes')).yield_per(1))
    else:
        if year:
            patents = (p for p in session.query(schema).filter(extract('year', schema.date) == year).options(subqueryload('rawinventors'), subqueryload('rawassignees'), subqueryload('current_classes')).yield_per(1))
        else:
            patents = (p for p in session.query(schema).options(subqueryload('rawinventors'), subqueryload('rawassignees'), subqueryload('current_classes')).yield_per(1))
    i = 0
    for patent in patents:
        i += 1
        if i % 100000 == 0:
          print i, datetime.now()
        try:
          # create common dict for this patent
          primrawloc = patent.rawinventors[0].rawlocation
          if primrawloc:
            primloc = patent.rawinventors[0].rawlocation.location
          else:
            primloc = primrawloc
          if doctype == 'application':
            mainclass = patent.classes[0].mainclass_id if patent.classes else ''
            subclass = patent.classes[0].subclass_id if patent.classes else ''
          else:
            mainclass = patent.current_classes[0].mainclass_id if patent.current_classes else ''
            subclass = patent.current_classes[0].subclass_id if patent.current_classes else ''
          row = {'number': patent.id,
                 'mainclass': mainclass,
                 'subclass': subclass,
                 'ignore': 0,
                 }
          if doctype == 'grant':
            row['isgrant'] = 1
          elif doctype == 'application':
            row['isgrant'] = 0
            if patent.granted == True:
              row['ignore'] = 1
          row['assignee'] = get_cleanid(patent.rawassignees[0]) if patent.rawassignees else ''
          row['assignee'] = row['assignee'].split('\t')[0]
          row['rawassignee'] = get_cleanid(patent.rawassignees[0]) if patent.rawassignees else ''
          row['rawassignee'] = row['rawassignee'].split('\t')[0]
          # generate a row for each of the inventors on a patent
          for ri in patent.rawinventors:
              if not len(ri.name_first.strip()):
                  continue
              namedict = {'uuid': ri.uuid}
              parsedNames = name_parser.parse_name(name_parser.NameFormat.CITESEERX, ri.name_first + ' ' + ri.name_last)
              namedict['name_first'] = ' '.join(filter(None, (parsedNames.Prefix, parsedNames.GivenName)))
              namedict['name_middle'] = parsedNames.OtherName if parsedNames.OtherName is not None else ''
              namedict['name_last'] = ' '.join(filter(None, (parsedNames.FamilyName, parsedNames.Suffix)))

              rawloc = ri.rawlocation
              if rawloc:
                if rawloc.location:
                  loc = rawloc.location
                else:
                  loc = primloc
              else:
                loc = primloc
              namedict['state'] = loc.state if loc else ''# if loc else rawloc.state if rawloc else primloc.state if primloc else ''
              namedict['country'] = loc.country if loc else ''# if loc else rawloc.country if rawloc else primloc.country if primloc else ''
              namedict['city'] = loc.city if loc else ''# if loc else rawloc.city if rawloc else primloc.city if primloc else ''
              if '??' in namedict['state'] or len(namedict['state']) == 0:
                namedict['state'] = rawloc.state if rawloc else primloc.state if primloc else ''
              if '??' in namedict['country'] or len(namedict['country']) == 0:
                namedict['country'] = rawloc.country if rawloc else primloc.country if primloc else ''
              if '??' in namedict['city'] or len(namedict['city']) == 0:
                namedict['city'] = rawloc.city if rawloc else primloc.city if primloc else ''
              tmprow = row.copy()
              tmprow.update(namedict)
              newrow = normalize_utf8(ROW(tmprow))
              with codecs.open('disambiguator.csv', 'a', encoding='utf-8') as csv:
                  csv.write(newrow)
        except Exception as e:
          print e
          continue

def join(newfile):
    """
    Does a JOIN on the rawinventor uuid field to associate rawinventors in this
    round with inventor_ids they were assigned in the previous round of
    disambiguation. This improves the runtime of the inventor disambiguator
    """
    new = pd.read_csv(newfile,delimiter='\t',header=None, error_bad_lines=False)
    new[0] = new[0].astype(str)
    ses_gen = alchemy.session_generator(dbtype='grant')
    s = ses_gen()
    old = s.execute('select uuid, inventor_id from rawinventor where inventor_id != "";')
    old = pd.DataFrame.from_records(old.fetchall())
    # Handle both the case when the database already has data in it and when it is empty
    if not old.empty:
      old[0] = old[0].astype(str)
      merged = pd.merge(new,old,on=0,how='left')
    else:
      merged = new
    merged.to_csv('disambiguator_{0}.tsv'.format(datetime.now().strftime('%B_%d')), index=False, header=None, sep='\t')

if __name__ == '__main__':
    for year in range(1975, datetime.today().year+1):
        print 'Running year',year,datetime.now(),'for grant'
        main(year, 'grant')
    for year in range(2001, datetime.today().year+1):
        print 'Running year',year,datetime.now(),'for application'
        main(year, 'application')

    # join files
    join('disambiguator.csv')
