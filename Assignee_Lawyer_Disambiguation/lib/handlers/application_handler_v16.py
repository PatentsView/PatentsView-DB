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
#Type kind crosswalk - lookup table
type_kind = {
            "A": 'utility',   #Utility Patent issued prior to January 2, 2001. 
            "A1": 'utility', #Utility Patent Application published on or after January 2, 2001. 
            "A2": 'utility', #Second or subsequent publication of a Utility Patent Application. 
            'A9': 'utility', #Corrected published Utility Patent Application. 
            'Bn': 'reexamination certificate', #Reexamination Certificate issued prior to January 2, 2001. NOTE: "n" represents a value 1 through 9. 
            'B1': 'utility', #Utility Patent (no pre-grant publication) issued on or after January 2, 2001. 
            'B2': 'utility', #Utility Patent (with pre-grant publication) issued on or after January 2, 2001. 
            'Cn': 'utility', #Reexamination Certificate issued on or after January 2, 2001. NOTE: "n" represents a value 1 through 9 denoting the publication level. 
            'E1': 'reissue', #Reissue Patent. 
            'Fn': 'reexamination certificate', #Reexamination Certificate of a Reissue Patent NOTE: "n" represents a value 1 through 9 denoting the publication level. 
            'H1': 'statutory invention registration', #Statutory Invention Registration (SIR) Patent Documents. SIR documents began with the December 3, 1985 issue. 
            'I1': 'reissue', #"X" Patents issued from July 31, 1790 to July 13, 1836. 
            'I2': 'reissue', #"X" Reissue Patents issued from July 31, 1790 to July 13, 1836. 
            'I3': 'additional improvements', #Additional Improvements - Patents issued between 1838 and 1861. 
            'I4': 'defensive publication', #Defensive Publication - Documents issued from November 5, 1968 through May 5, 1987. 
            'I5': 'TVPP', #Trial Voluntary Protest Program (TVPP) Patent Documents. 
            'NP': 'non-patent literature', #Non-Patent Literature. 
            'P': 'plant', #Plant Patent issued prior to January 2, 2001. 
            'P1': 'plant', #Plant Patent Application published on or after January 2, 2001. 
            'P2': 'plant', #Plant Patent (no pre-grant publication) issued on or after January 2, 2001. 
            'P3': 'plant', #Plant Patent (with pre-grant publication) issued on or after January 2, 2001. 
            'P4': 'plant', #Second or subsequent publication of a Plant Patent Application. 
            'P9': 'plant', #Correction publication of a Plant Patent Application. 
            'S1': 'design', #Design Patent.
            'S' : 'design',
            'NULL': 'NULL' #Placeholder for NULL values for duplicates and such.
             }

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

        self.xml = xh.root.patent_application_publication
        self.xml_string = xml_string
        
        if filter(lambda x: not isinstance(x, list), self.xml.contents_of('country_code')):
            self.country = filter(lambda x: not isinstance(x, list), self.xml.contents_of('country_code'))[0]
        else:
            self.country = ''
        self.application = xml_util.normalize_document_identifier(self.xml.document_id.contents_of('doc_number')[0])
        self.kind = self.xml.document_id.contents_of('kind_code')[0]
        try:
            self.pat_type = type_kind[self.kind]
        except:
            self.pat_type = None
        self.date_app = self.xml.document_id.contents_of('document_date')[0]
        self.clm_num = len(self.xml.subdoc_claims.claim)
        #self.abstract = self.xml.subdoc_abstract.contents_of('paragraph', '', as_string=True, upper=False)
        try:
            self.abstract = re.search('<subdoc-abstract>(.*?)</subdoc-abstract>',xml_string,re.DOTALL).group(1)
            self.abstract = re.sub('<.*?>|</.*?>','',self.abstract)
            self.abstract = re.sub('[\n\t\r\f]+','',self.abstract)
            self.abstract = re.sub('\s+',' ',self.abstract)
            self.abstract = h.unescape(self.abstract)
        except:
            self.abstract = ''
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
        original = self.xml.contents_of('title_of_invention', upper=False)[0]
        if isinstance(original, list):
            original = ''.join(original)
        return original

    def _name_helper(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.contents_of('given_name', as_string=True, upper=False)
        if not firstname:
            firstname = tag_root.contents_of('name_1', as_string=True, upper=False)
        lastname = tag_root.contents_of('family_name', as_string=True, upper=False)
        if not lastname:
            lastname = tag_root.contents_of('name_2', as_string=True, upper=False)
        return xml_util.associate_prefix(firstname, lastname)

    def _name_helper_dict(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.contents_of('given_name', as_string=True, upper=False)
        try:
            middlename = tag_root.contents_of('middle_name',as_string=True,upper=False)
            firstname+=' '+middlename
        except:
            pass
        lastname = tag_root.contents_of('family_name', as_string=True, upper=False)
        
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
        assignees = self.xml.assignee
        
        if not assignees:
            return []
        res = []
        for i, assignee in enumerate(assignees):
            # add assignee data
            asg = {}
            asg.update(self._name_helper_dict(assignee))  # add firstname, lastname
            asg['organization'] = assignee.contents_of('organization_name', as_string=True, upper=False)
            asg['type'] = assignee.contents_of('assignee_type', as_string=True)
            if assignee.contents_of('country_code'):
                asg['nationality'] = assignee.contents_of('country_code', as_string=True)
                asg['residence'] = assignee.contents_of('country_code', as_string=True)
            # add location data for assignee
            loc = {}
            for tag in ['city', 'state']:
                loc[tag] = assignee.contents_of(tag, as_string=True, upper=False)
            if 'nationality' in asg:
                loc['country'] = asg['nationality']
            #this is created because of MySQL foreign key case sensitivities
                loc['id'] = unidecode(u"|".join([loc['city'], loc['state'], loc['country']]).lower())
            else:
                loc['id'] = u''
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
        applicants = self.xml.first_named_inventor + self.xml.inventors.inventor
        if not applicants:
            return []
        res = []
        for i, applicant in enumerate(applicants):
            # add applicant data
            app = {}
            app.update(self._name_helper_dict(applicant.name))
            app['nationality'] = applicant.contents_of('country_code', as_string=True)
            # add location data for applicant
            loc = {}
            for tag in ['city', 'state']:
                loc[tag] = applicant.residence.contents_of(tag, as_string=True, upper=False)
            loc['country'] = app['nationality']
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
        country = root.contents_of('country_code')[0] if root.contents_of('country_code') else ''
        kind = root.contents_of('kind_code')[0] if root.contents_of('kind_code') else ''
        date = root.contents_of('document_date')[0] if root.contents_of('document_date') else ''
        res['country'] = country if country else ''
        res['kind'] = kind if kind else ''
        res['date'] = date if date else ''
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
        main = self.xml.classification_us.classification_us_primary.uspc
        crossrefsub = main.contents_of('subclass', as_string=True)
        if crossrefsub[3:] != '000':
            try:
                temp = int(crossrefsub[3:])
                if re.search('[A-Z]{3}',crossrefsub[0:3]) is None:
                    subclass = re.sub('^0+','',crossrefsub[:3])+'.'+crossrefsub[3:]
                else:
                    subclass =re.sub('^0+','',crossrefsub[:3])+crossrefsub[3:]
            except:
                if len(re.sub('0+','',crossrefsub[3:])) > 1:
                    subclass = re.sub('^0+','',crossrefsub[:3])+'.'+crossrefsub[3:]
                else:
                    subclass = re.sub('^0+','',crossrefsub[:3])+crossrefsub[3:]    
        else:
            subclass = re.sub('^0+','',crossrefsub[:3])
        
        data = {'class': re.sub('^0+','',main.contents_of('class', as_string=True)),
              'subclass': subclass}
        if any(data.values()):
            classes.append([
                {'uuid': str(uuid.uuid4()), 'sequence': i},
                {'id': data['class'].upper()},
                {'id': "{class}/{subclass}".format(**data).upper()}])
            i = i + 1
        if self.xml.classification_us.classification_us_secondary:
            further = self.xml.classification_us.classification_us_secondary
            for classification in further:
                crossrefsub = classification.contents_of('subclass', as_string=True)
                if crossrefsub[3:] != '000':
                    try:
                        temp = int(crossrefsub[3:])
                        if re.search('[A-Z]{3}',crossrefsub[0:3]) is None:
                            subclass = re.sub('^0+','',crossrefsub[:3])+'.'+re.sub('0+','',crossrefsub[3:])
                        else:
                            subclass =re.sub('^0+','',crossrefsub[:3])+re.sub('^0+','',crossrefsub[3:])
                    except:
                        if len(re.sub('0+','',crossrefsub[3:])) > 1:
                            subclass = re.sub('^0+','',crossrefsub[:3])+'.'+re.sub('0+','',crossrefsub[3:])
                        else:
                            subclass = re.sub('^0+','',crossrefsub[:3])+re.sub('0+','',crossrefsub[3:])    
                else:
                    subclass = re.sub('^0+','',crossrefsub[:3])
                
        
                data = {'class': re.sub('^0+','',classification.contents_of('class', as_string=True)),
                        'subclass': subclass}
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
        if re.search('<subdoc-claims>(.*?)</subdoc-claims>',self.xml_string,re.DOTALL) != None:
            claimsdata = re.search('<subdoc-claims>(.*?)</subdoc-claims>',self.xml_string,re.DOTALL).group(1)
            claims = re.finditer('<claim.*?>(.*?)</claim>',claimsdata,re.DOTALL)
            #claims = self.xml.claim
            
            for i,claim in enumerate(claims):
                claim = claim.group(1)
                data = {}
                try:
                    dependent = re.search('<dependent-claim-reference depends_on="CLM-(\d+)">',claim).group(1)
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
        for i, claim in enumerate(claims):
            print claim
            data = {}
            data['text'] = claim.contents_of('claim_text', as_string=True, upper=False)
            # remove leading claim num from text
            data['text'] = claim_num_regex.sub('', data['text'])
            data['text'] = re.sub('[\n\t\r\f]+','',data['text'])
            data['text'] = re.sub('\s+',' ',data['text'])
            data['text'] = re.sub('^\.\s+','',data['text'])
            data['sequence'] = i+1 # claims are 1-indexed
            if claim.dependent_claim_reference and claim.dependent_claim_reference.claim_text:
                # claim_refs are 'claim N', so we extract the N
                data['dependent'] = claim.dependent_claim_reference.contents_of('claim_text',\
                                        as_string=True).split(' ')[-1]
            elif claim.dependent_claim_reference:
                data['dependent'] = claim.contents_of('dependent_claim_reference',\
                                        as_string=True).split(' ')[-1]
            if 'dependent' in data:
                data['dependent'] = int(''.join(c for c in data['dependent'] if c.isdigit()))
            
        """
        return res