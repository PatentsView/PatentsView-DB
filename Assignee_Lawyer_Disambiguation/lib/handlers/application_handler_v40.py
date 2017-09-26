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
Uses the extended ContentHandler from xml_driver to extract the needed fields
from patent grant documents
"""

from cStringIO import StringIO
from datetime import datetime
from unidecode import unidecode
from handler import Patobj, PatentHandler
import re
import uuid
import xml.sax
import xml_util
import xml_driver

claim_num_regex = re.compile(r'^\d+\. *') # removes claim number from claim text

from HTMLParser import HTMLParser
h = HTMLParser()


class Patent(PatentHandler):

    def __init__(self, xml_string, filename, is_string=False):
        xh = xml_driver.XMLHandler()
        parser = xml_driver.make_parser()

        parser.setContentHandler(xh)
        parser.setFeature(xml_driver.handler.feature_external_ges, False)
        l = xml.sax.xmlreader.Locator()
        xh.setDocumentLocator(l)
        if is_string:
            parser.parse(StringIO(xml_string))
        else:
            parser.parse(xml_string)

        self.attributes = ['app','application','assignee_list','inventor_list',
                      'us_classifications',
                     'claims']

        self.xml = xh.root.us_patent_application
        self.xml_string = xml_string
        self.country = self.xml.publication_reference.contents_of('country', upper=False)[0]
        self.application = xml_util.normalize_document_identifier(self.xml.publication_reference.contents_of('doc_number')[0])
        self.kind = self.xml.publication_reference.contents_of('kind')[0]
        if self.xml.application_reference:
            self.pat_type = self.xml.application_reference[0].get_attribute('appl-type', upper=False)
        else:
            self.pat_type = None
        self.date_app = self.xml.publication_reference.contents_of('date')[0]
        self.clm_num = len(self.xml.claims.claim)
        self.abstract = h.unescape(self.xml.abstract.contents_of('p', '', as_string=True, upper=False))
        self.invention_title = h.unescape(self._invention_title())
        self.filename = re.search('i?pa[0-9]*.*$',filename,re.DOTALL).group()
        
        self.app = {
            "id": self.application,
            "type": self.pat_type,
            "number": self.application,
            "country": self.country,
            "date": self._fix_date(self.date_app),
            "abstract": self.abstract,
            "title": self.invention_title,
            "kind": self.kind,
            "num_claims": self.clm_num,
            "filename": self.filename
        }
        self.app["id"] = str(self.app["date"])[:4] + "/" + self.app["number"]

    def _invention_title(self):
        original = self.xml.contents_of('invention_title', upper=False)[0]
        if isinstance(original, list):
            original = ''.join(original)
        return original

    def _name_helper(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.contents_of('first_name', as_string=True, upper=False)
        lastname = tag_root.contents_of('last_name', as_string=True, upper=False)
        return xml_util.associate_prefix(firstname, lastname)

    def _name_helper_dict(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.contents_of('first_name', as_string=True, upper=False)
        try:
            middlename = tag_root.contents_of('middle_name',as_string=True,upper=False)
            firstname+=' '+middlename
        except:
            pass
        lastname = tag_root.contents_of('last_name', as_string=True, upper=False)
        return {'name_first': firstname, 'name_last': lastname}

    def _fix_date(self, datestring):
        """
        Converts a number representing YY/MM to a Date
        """
        if not datestring:
            return None
        elif datestring[:4] < "1900":
            return None
        # default to first of month in absence of day
        if datestring[-4:-2] == '00':
            datestring = datestring[:-4] + '01' + datestring[-2:]
        if datestring[-2:] == '00':
            datestring = datestring[:6] + '01'
        try:
            datestring = datetime.strptime(datestring, '%Y%m%d')
            return datestring
        except Exception as inst:
            print inst, datestring
            return None

    @property
    def assignee_list(self):
        """
        Returns list of dictionaries:
        assignee:
          name_last
          name_first
          residence
          nationality
          organization
          sequence
        location:
          id
          city
          state
          country
        """
        assignees = self.xml.assignees.assignee
        if not assignees:
            return []
        res = []
        for i, assignee in enumerate(assignees):
            # add assignee data
            asg = {}
            asg.update(self._name_helper_dict(assignee))  # add firstname, lastname
            asg['organization'] = assignee.contents_of('orgname', as_string=True, upper=False)
            asg['type'] = assignee.contents_of('role', as_string=True)
            if assignee.nationality.contents_of('country'):
                asg['nationality'] = assignee.nationality.contents_of('country', as_string=True)
                asg['residence'] = assignee.nationality.contents_of('country', as_string=True)
            # add location data for assignee
            loc = {}
            for tag in ['city', 'state', 'country']:
                loc[tag] = assignee.contents_of(tag, as_string=True, upper=False)
            #this is created because of MySQL foreign key case sensitivities
            loc['id'] = unidecode(u"|".join([loc['city'], loc['state'], loc['country']]).lower())
            if any(asg.values()) or any(loc.values()):
                asg['sequence'] = i
                asg['uuid'] = str(uuid.uuid4())
                res.append([asg, loc])
        return res

    @property
    def inventor_list(self):
        """
        Returns list of lists of applicant dictionary and location dictionary
        applicant:
          name_last
          name_first
          sequence
        location:
          id
          city
          state
          country
        """
        applicants = self.xml.applicants.applicant
        if not applicants:
            return []
        res = []
        for i, applicant in enumerate(applicants):
            # add applicant data
            app = {}
            app.update(self._name_helper_dict(applicant.addressbook))
            app['nationality'] = applicant.nationality.contents_of('country', as_string=True)
            # add location data for applicant
            loc = {}
            for tag in ['city', 'state', 'country']:
                loc[tag] = applicant.addressbook.contents_of(tag, as_string=True, upper=False)
            #this is created because of MySQL foreign key case sensitivities
            loc['id'] = unidecode("|".join([loc['city'], loc['state'], loc['country']]).lower())
            del app['nationality']
            if any(app.values()) or any(loc.values()):
                app['sequence'] = i
                app['uuid'] = str(uuid.uuid4())
                res.append([app, loc])
        return res

    def _get_doc_info(self, root):
        """
        Accepts an XMLElement root as an argument. Returns list of
        [country, doc-number, kind, date] for the given root
        """
        res = {}
        for tag in ['country', 'kind', 'date']:
            data = root.contents_of(tag)
            res[tag] = data[0] if data else ''
        res['number'] = xml_util.normalize_document_identifier(
            root.contents_of('doc_number')[0])
        return res

    @property
    def us_classifications(self):
        """
        Returns list of dictionaries representing us classification
        main:
          class
          subclass
        """
        classes = []
        i = 0
        main = self.xml.classification_national.contents_of('main_classification')
        if len(main[0]) > 0:
            crossrefsub = main[0][3:].replace(" ","")
            if len(crossrefsub) > 3 and re.search('^[A-Z]',crossrefsub[3:]) is None:
                crossrefsub = crossrefsub[:3]+'.'+crossrefsub[3:]
            crossrefsub = re.sub('^0+','',crossrefsub)
            if re.search('[A-Z]{3}',crossrefsub[:3]):
                    crossrefsub = crossrefsub.replace(".","")
            crossrefsub = re.sub('\.000$','',crossrefsub)
            crossrefsub = re.sub('0+$','',crossrefsub)
            
            data = {'class': re.sub('^0+','',main[0][:3].replace(' ', '')),
                    'subclass': crossrefsub}
            if any(data.values()):
                classes.append([
                    {'uuid': str(uuid.uuid4()), 'sequence': i},
                    {'id': data['class'].upper()},
                    {'id': "{class}/{subclass}".format(**data).upper()}])
                i = i + 1
            if self.xml.classification_national.further_classification:
                further = self.xml.classification_national.contents_of('further_classification')
                for classification in further:
                    crossrefsub = classification[3:].replace(" ","")
                    if len(crossrefsub) > 3 and re.search('^[A-Z]',crossrefsub[3:]) is None:
                        crossrefsub = crossrefsub[:3]+'.'+crossrefsub[3:]
                    crossrefsub = re.sub('^0+','',crossrefsub)
                    if re.search('[A-Z]{3}',crossrefsub[:3]):
                            crossrefsub = crossrefsub.replace(".","")
                    crossrefsub = re.sub('\.000$','',crossrefsub)
                    crossrefsub = re.sub('0+$','',crossrefsub)
                    
                    data = {'class': re.sub('^0+','',classification[:3].replace(' ', '')),
                            'subclass': crossrefsub}
                    if any(data.values()):
                        classes.append([
                            {'uuid': str(uuid.uuid4()), 'sequence': i},
                            {'id': data['class'].upper()},
                            {'id': "{class}/{subclass}".format(**data).upper()}])
                        i = i + 1
        return classes

    @property
    def claims(self):
        """
        Returns list of dictionaries representing claims
        claim:
          text
          dependent -- -1 if an independent claim, else this is the number
                       of the claim this one is dependent on
          sequence
        """
        res = []
        if re.search('<claims.*?>(.*?)</claims>',self.xml_string,re.DOTALL) != None:
            claimsdata = re.search('<claims.*?>(.*?)</claims>',self.xml_string,re.DOTALL).group(1)
            claims = re.finditer('<claim.*?>(.*?)</claim>',claimsdata,re.DOTALL)
            
            for i,claim in enumerate(claims):
                claim = claim.group(1)
                data = {}
                try:
                    dependent = re.search('<claim-ref idref="CLM-(\d+)">',claim).group(1)
                    data['dependent'] = int(dependent)
                except:
                    pass
                data['text'] = re.sub('<.*?>|</.*?>','',claim)
                data['text'] = re.sub('[\n\t\r\f]+','',data['text'])
                data['text'] = re.sub('^\d+\.\s+','',data['text'])
                data['text'] = re.sub('\s+',' ',data['text'])
                data['sequence'] = i+1 # claims are 1-indexed
                data['uuid'] = str(uuid.uuid4())
                res.append(data)
        """
        claims = self.xml.claim
        res = []
        for i, claim in enumerate(claims):
            data = {}
            data['text'] = claim.contents_of('claim_text', as_string=True, upper=False)
            # remove leading claim num from text
            data['text'] = claim_num_regex.sub('', data['text'])
            print data['text']
            data['sequence'] = i+1 # claims are 1-indexed
            if claim.claim_ref:
                # claim_refs are 'claim N', so we extract the N
                claim_str = claim.contents_of('claim_ref',\
                                        as_string=True).split(' ')[-1]
                data['dependent'] = int(''.join(c for c in claim_str if c.isdigit()))
            data['uuid'] = str(uuid.uuid4())
            res.append(data)
        """
        return res
