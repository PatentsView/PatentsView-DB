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

        self.attributes = ['pat','app','assignee_list','patent','inventor_list','lawyer_list',
                     'us_relation_list','us_classifications','ipcr_classifications',
                     'citation_list','claims']

        self.xml = xh.root.us_patent_grant
        self.xml_string = xml_string
        self.country = self.xml.publication_reference.contents_of('country', upper=False)[0]
        self.patent = xml_util.normalize_document_identifier(self.xml.publication_reference.contents_of('doc_number')[0])
        self.kind = self.xml.publication_reference.contents_of('kind')[0]
        self.date_grant = self.xml.publication_reference.contents_of('date')[0]
        if self.xml.application_reference:
            self.pat_type = self.xml.application_reference[0].get_attribute('appl-type', upper=False)
        else:
            self.pat_type = None
        self.date_app = self.xml.application_reference.contents_of('date')[0]
        self.country_app = self.xml.application_reference.contents_of('country')[0]
        self.patent_app = self.xml.application_reference.contents_of('doc_number')[0]
        self.code_app = self.xml.contents_of('us_application_series_code')[0]
        self.clm_num = self.xml.contents_of('number_of_claims')[0]
        self.abstract = h.unescape(xh.root.us_patent_grant.abstract.contents_of('p', '', as_string=True, upper=False))
        self.invention_title = h.unescape(self._invention_title())
        self.filename = re.search('ipg.*$',filename,re.DOTALL).group()

        self.pat = {
            "id": self.patent,
            "type": self.pat_type,
            "number": self.patent,
            "country": self.country,
            "date": self._fix_date(self.date_grant),
            "abstract": self.abstract,
            "title": self.invention_title,
            "kind": self.kind,
            "num_claims": self.clm_num,
            "filename": self.filename
        }
        self.app = {
            "type": self.code_app,
            "number": self.patent_app,
            "country": self.country_app,
            "date": self._fix_date(self.date_app)
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
        return firstname, lastname

    def _name_helper_dict(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.contents_of('first_name', as_string=True, upper=False)
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
          type
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
            asg['type'] = str(int(assignee.contents_of('role', as_string=True)))
            #asg['nationality'] = assignee.nationality.contents_of('country')[0]
            #asg['residence'] = assignee.nationality.contents_of('country')[0]
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
    def citation_list(self):
        """
        Returns a list of two lists. The first list is normal citations,
        the second is other citations.
        citation:
          date
          name
          kind
          country
          category
          number
          sequence
        OR
        otherreference:
          text
          sequence
        """
        citations = self.xml.references_cited.citation
        if not citations:
            return [[], []]
        regular_cits = []
        other_cits = []
        ocnt = 0
        ccnt = 0
        for citation in citations:
            data = {}
            if citation.othercit:
                data['text'] = citation.contents_of('othercit', as_string=True, upper=False)
                if any(data.values()):
                    data['sequence'] = ocnt
                    data['uuid'] = str(uuid.uuid4())
                    other_cits.append(data)
                    ocnt += 1
            else:
                for tag in ['kind', 'category']:
                    data[tag] = citation.contents_of(tag, as_string=True, upper=False)
                data['date'] = self._fix_date(citation.contents_of('date', as_string=True))
                data['country'] = citation.contents_of('country', default=[''])[0]
                doc_number = citation.contents_of('doc_number', as_string=True)
                data['number'] = xml_util.normalize_document_identifier(doc_number)
                if any(data.values()):
                    data['sequence'] = ccnt
                    data['uuid'] = str(uuid.uuid4())
                    regular_cits.append(data)
                    ccnt += 1
        return [regular_cits, other_cits]

    @property
    def inventor_list(self):
        """
        Returns list of lists of inventor dictionary and location dictionary
        inventor:
          name_last
          name_first
          sequence
        location:
          id
          city
          state
          country
        """
        inventors = self.xml.parties.applicant
        if not inventors:
            return []
        res = []
        for i, inventor in enumerate(inventors):
            # add inventor data
            inv = {}
            inv.update(self._name_helper_dict(inventor.addressbook))
            # add location data for inventor
            loc = {}
            for tag in ['city', 'state', 'country']:
                loc[tag] = inventor.addressbook.contents_of(tag, as_string=True, upper=False)
            #this is created because of MySQL foreign key case sensitivities
            loc['id'] = unidecode("|".join([loc['city'], loc['state'], loc['country']]).lower())
            if any(inv.values()) or any(loc.values()):
                inv['sequence'] = i
                inv['uuid'] = str(uuid.uuid4())
                res.append([inv, loc])
        return res

    @property
    def lawyer_list(self):
        """
        Returns a list of lawyer dictionary
        lawyer:
            name_last
            name_first
            organization
            country
            sequence
        """
        lawyers = self.xml.parties.agents.agent
        if not lawyers:
            return []
        res = []
        lawseq = 0
        for i, lawyer in enumerate(lawyers):
            law = {}
            law.update(self._name_helper_dict(lawyer))
            law['country'] = lawyer.contents_of('country', as_string=True)
            law['organization'] = lawyer.contents_of('orgname', as_string=True, upper=False)
            if any(law.values()):
                law['uuid'] = str(uuid.uuid4())
                law['sequence'] = lawseq
                res.append(law)
            lawseq+=1
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
    def us_relation_list(self):
        """
        returns list of dictionaries for us reldoc:
        usreldoc:
          doctype
          status (parent status)
          date
          number
          kind
          country
          relationship
          sequence
        """
        root = self.xml.us_related_documents
        if not root:
            return []
        root = root[0]
        res = []
        i = 0
        for reldoc in root.children:
            if reldoc._name == 'related_publication' or \
               reldoc._name == 'us_provisional_application':
                data = {'doctype': reldoc._name}
                data.update(self._get_doc_info(reldoc))
                data['date'] = self._fix_date(data['date'])
                if any(data.values()):
                    data['sequence'] = i
                    data['uuid'] = str(uuid.uuid4())
                    i = i + 1
                    res.append(data)
            for relation in reldoc.relation:
                for relationship in ['parent_doc', 'parent_grant_document',
                                     'parent_pct_document', 'child_doc']:
                    data = {'doctype': reldoc._name}
                    doc = getattr(relation, relationship)
                    if not doc:
                        continue
                    data.update(self._get_doc_info(doc[0]))
                    data['date'] = self._fix_date(data['date'])
                    data['status'] = doc[0].contents_of('parent_status', as_string=True)
                    data['relationship'] = relationship  # parent/child
                    if any(data.values()):
                        data['sequence'] = i
                        data['uuid'] = str(uuid.uuid4())
                        i = i + 1
                        res.append(data)
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
        crossrefsub = main[0][3:].replace(" ","")
        if len(crossrefsub) > 3 and re.search('^[A-Z]',crossrefsub[3:]) is None:
            crossrefsub = crossrefsub[:3]+'.'+crossrefsub[3:]
        crossrefsub = re.sub('^0+','',crossrefsub)
        if re.search('[A-Z]{3}',crossrefsub[:3]):
                crossrefsub = crossrefsub.replace(".","")
        
        data = {'class': main[0][:3].replace(' ', ''),
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
 
                data = {'class': classification[:3].replace(' ', ''),
                        'subclass': crossrefsub}
                if any(data.values()):
                    classes.append([
                        {'uuid': str(uuid.uuid4()), 'sequence': i},
                        {'id': data['class'].upper()},
                        {'id': "{class}/{subclass}".format(**data).upper()}])
                    i = i + 1
        return classes

    @property
    def ipcr_classifications(self):
        """
        Returns list of dictionaries representing ipcr classifications
        ipcr:
          ipc_version_indicator
          classification_level
          section
          class
          subclass
          main_group
          subgroup
          symbol_position
          classification_value
          action_date
          classification_status
          classification_data_source
          sequence
        """
        ipcr_classifications = self.xml.classifications_ipcr
        if not ipcr_classifications:
            return []
        res = []
        # we can safely use [0] because there is only one ipcr_classifications tag
        for i, ipcr in enumerate(ipcr_classifications.classification_ipcr):
            data = {}
            for tag in ['classification_level', 'section',
                        'class', 'subclass', 'main_group', 'subgroup', 'symbol_position',
                        'classification_value', 'classification_status',
                        'classification_data_source']:
                if tag == 'class':
                    data['ipc_class'] = ipcr.contents_of(tag, as_string=True)
                else: 
                    data[tag] = ipcr.contents_of(tag, as_string=True)
            data['ipc_version_indicator'] = self._fix_date(ipcr.ipc_version_indicator.contents_of('date', as_string=True))
            data['action_date'] = self._fix_date(ipcr.action_date.contents_of('date', as_string=True))
            if any(data.values()):
                data['sequence'] = i
                data['uuid'] = str(uuid.uuid4())
                res.append(data)
        return res

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
            data['sequence'] = i+1 # claims are 1-indexed
            if claim.claim_ref:
                # claim_refs are 'claim N', so we extract the N
                data['dependent'] = int(claim.contents_of('claim_ref',\
                                        as_string=True).split(' ')[-1])
            data['uuid'] = str(uuid.uuid4())
            res.append(data)
        """
        return res
