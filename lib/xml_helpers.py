from lxml import etree
from collections import defaultdict
import re
from lib.utilities import id_generator

#####################################
### Main Data Processing Functions
#####################################
def get_xml(valid_xml_file):
    '''
    Return a xml object with patent data
    :param valid_xml_file: a valid xml object
    :return the root object of the xml file, ready to parse
    '''
    tree = etree.parse(valid_xml_file)
    root = tree.getroot()
    return root

def get_entity(patent, entity_name, attribute_list=None):
    '''
    :params patent: take the xml object representing a patent
    :params entity_name: a string with the xml tag for an entity with single or multiple entities
    :returns a list of default dictionaries with all the data for the entity and processes dates
    '''
    var_list=[]
    xml = patent.findall('.//'+entity_name)
    for field in xml:
        data=defaultdict(lambda : None)
        if attribute_list:
            for attribute in attribute_list:
                data[attribute]=field.attrib[attribute]
        #recursive function modifies data dictionary defined above
        results_list = recursive_children(field)
        data.update(dict(results_list))
        for key in data.keys():
            if 'date' in key:
                data[key] = process_date(data[key])
        var_list.append(data)
    if var_list ==[]:
        return [None]
    return var_list

def get_main_text_fields(patent):
    #TODO: This could use improvements to be less convoluted
    #this is very complicated becuase both headings and information processing tags are used to delimit fields
    #and there are lots of heading varients
    #test this on 050803 data specifically patent 6865523, 6863488 for GI
    #specifically  6862844, 6862822 for other ref
    data = defaultdict(list)
    titles = ['BACKGROUND OF THE INVENTION', 'FIELD OF THE INVENTION', 'TECHNICAL FIELD',
               'SUMMARY', 'SUMMARY OF THE INVENTION','FIELD OF INVENTION', 'FIELD','DETAILED BOTANICAL DESCRIPTION',
              'DETAILED DESCRIPTION OF THE VARIETY', 'DETAILED DESCRIPTION', 'DETAILED DESCRIPTION OF THE INVENTION',
               'DESCRIPTION OF EMBODIMENTS','DESCRIPTION OF THE PREFERRED EMBODIMENT', 'DETAILED DESCRIPTION OF THE PREFERRED EMBODIMENTS',
               'DESCRIPTION OF THE INVENTION', 'BRIEF DESCRIPTION OF DRAWINGS']
    government  = ['STATEMENT OF GOVERNMENT INTEREST','GOVERNMENT INTEREST', 'STATEMENT REGARDING FEDERALLY SPONSORED RESEARCH OR DEVELOPMENT', 'STATEMENT AS TO RIGHTS TO INVENTIONS MADE UNDER FEDERALLY SPONSORED RESEARCH AND DEVELOPMENT']
    other_patent = ['RELATED APPLICATION','CROSS REFERENCES TO RELATED APPLICATIONS','CROSS-REFERENCE TO RELATED PATENT APPLICATIONS', 'OTHER PATENT RELATIONS', 'CROSS REFERENCE TO RELATED APPLICATIONS','CROSS-REFERENCE TO RELATED APPLICATIONS', 'CROSS-REFERENCES TO RELATED APPLICATIONS']
    not_applicable = ['None','None.', 'none', 'none.' , '(Not applicable)','(Not Applicable)','“Not Applicable”','Not applicable','Not applicable.', 'Not Applicable', 'Not Applicable.','' 'NA', 'n/a', 'N/A', 'na']
    to_skip = titles + government + other_patent + not_applicable
    description = patent.find('description')
    #temp things to help test
    if description is not None:
        switch_back = False
        next_switch = False
        for item in description: 
            if getattr(item, 'tag', None) is etree.ProcessingInstruction:
                field = item.attrib['description']
            else:
                if item.tag == 'heading':
                    item_text=get_text(item)
                    #special processing section to get the GI statement if it is delimited by a heading instead of a processing tag
                    if item_text and len(item_text)>0 and item_text[0] in government and field != 'Government Interest':
                        if not switch_back: #don't make old field in GI if that came first
                            old_field = field
                        next_switch = False
                        field = 'Government Interest'
                        switch_back = True
                    #specially processing to get other reference with wierd tag as well
                    elif item_text and len(item_text)>0 and  item_text[0] in other_patent and field != 'Other Patent Relations':
                        if not switch_back: #don't make old field in GI if that came first
                            old_field = field
                        field = 'Other Patent Relations'
                        next_switch = False
                        switch_back = True
                    elif switch_back: #switch back to main tag after getting GI statement/other ref
                        field = old_field
                        switch_back = False
                #sometimes there is no heading after the heading delimited GI or Other Ref
                #so move on next time if its just NA
                if next_switch:
                    field = old_field

                if item.text in not_applicable and switch_back:
                    next_switch = True
                if item.tag == 'description-of-drawings':
                    for sub_item in item:
                        data[field].append(" ".join(get_text(sub_item)))
                else:
                    data[field].extend(get_text(item))
                    for sub_item in item:
                        if sub_item.tag == 'ul' or sub_item.tag == 'ol' : #get the list items using special list function
                            data[field].extend(recursive_list(sub_item))
        to_remove = []
        for key in data.keys():
            clean_list = list(filter(lambda x: x is not None and not x in to_skip and not re.sub(r'\s+', '', x) == "", data[key]))
            #skip ones that have no data, mostly cause they have 'not applicable'
            if len(clean_list) > 0:
                if key == 'Brief Description of Drawings':
                    data[key] = clean_list
                else:
                    data[key]  = (" ".join( clean_list)).strip().replace("\r\n"," ").replace("\n", " ").replace("\t", " ").replace("  ", " ")
            else: #remove empty items
                to_remove.append(key) #can't do this in place because of the loop
        for key in to_remove:
            del data[key]

    return data

#####################################
### Get Data for Specific Fields
#####################################
def get_uspc(patent):
    uspc_list = []
    for uspc in patent.findall('.//us-bibliographic-data-grant/classification-national'):
        uspc_data = defaultdict(list)
        for element in uspc:
            uspc_data[element.tag].append(element.text)
        uspc_list.append(uspc_data)
    return uspc_list

def get_claims_data(patent):
    '''
    :params patent: take the xml object representing a patent
    :returns a list of claims and dependencies in the correct format 
    '''
    xml = patent.findall('.//'+'claims'+"/")
    claims_list = []
    for element in xml:
        claim_dict = defaultdict(lambda : [])
        entries = {}
        text = []
        if element.text:
            text.append(element.text)
        for sub_element in element:
            text.extend(get_text_and_tail(sub_element))
            for sub_sub_element in sub_element:
                if sub_sub_element.tag == 'claim-ref':
                    claim_dict['dependency'].append(sub_sub_element.text.split(" ")[-1])
                text.extend(get_text_and_tail(sub_sub_element))
        claim_dict['claim'] = re.sub('\s+', ' ', " ".join(filter(lambda x : x is not None, text))).strip()
        if claim_dict['dependency'] == []:
            claim_dict['dependency']='-1'
        else: #the join handles claims with multiple dependencies
            claim_dict['dependency']=" ,".join(claim_dict['dependency'])
        claims_list.append(claim_dict)
    return claims_list

def get_usreldocs(patent):
    usreldoc_list = []
    sequence = 0
    usreldocs = patent.find('.//'+'us-related-documents')
    if usreldocs is not None:
        for item in usreldocs:
            doc_type = item.tag
            for sub_item in item:  
                if sub_item.tag == 'document-id': 
                    data = defaultdict(lambda: None)
                    data['doc-type'] = doc_type
                    for sub_sub_item in sub_item:
                        data[sub_sub_item.tag] = sub_sub_item.text
                    data['sequence'] = sequence
                    sequence +=1
                    usreldoc_list.append(data)
                else:
                    for partial_doc in sub_item:
                        data = defaultdict(lambda: None)
                        data['doc-type'] = doc_type
                        data['relation'] = partial_doc.tag
                        for field in partial_doc.find('document-id'):
                            data[field.tag] = field.text
                        data['sequence'] = sequence
                        sequence +=1
                        status = partial_doc.find('parent-status')
                        if status is not None:
                            data['status'] = status.text
                        usreldoc_list.append(data)
                        for field in partial_doc:
                            if field.tag in ['parent-grant-document', 
                                              'parent-pct-document']:
                                data = defaultdict(lambda: None)
                                data['doc-type'] = doc_type
                                data['relation'] = field.tag
                                for sub_field in field.find('document-id'):
                                    data[sub_field.tag] = sub_field.text
                                data['sequence'] = sequence
                                sequence +=1
                                usreldoc_list.append(data)
        for data_dict in usreldoc_list:   
            for key in data_dict.keys():
                if 'date' in key:
                    data_dict[key] = process_date(data_dict[key])
    return usreldoc_list

def get_citations(patent):
    '''
    :params patent: the xml object representing a patent
    :returns a list of default dictionary with code for each citation
    '''
    patent_cite_list = []
    non_patent_cite_list= []
    citations = patent.findall(".//us-citation")
    if citations ==[]:
        citations = patent.findall(".//citation")
    if citations != []:
        for citation in citations:
            cite_data = defaultdict(lambda : None)
            for element in citation:
                if element.tag == 'category':
                    cite_data['category'] = element.text
                elif element.tag =='classification-cpc-text':
                    pass
                else:
                    if element.tag in ['nplcit', 'patcit']:
                        cite_data['type'] == element.tag
                    if element.tag=='nplcit':
                        cite_data['text']= []
                        for sub_element in element:
                            cite_data['text'].append(sub_element.text)
                            for sub_sub_element in sub_element:
                                cite_data['text'].extend([sub_sub_element.text, sub_sub_element.tail])
                        cite_data['text'] = " ".join([item for item in cite_data['text'] if item is not None])
                        non_patent_cite_list.append(cite_data)
                    elif element.tag in ['classification-national', 'classification-ipc', 'classification-ipcr']:
                        pass
                    else:
                        doc_info = element.find('document-id')
                        for sub_element in doc_info:
                            cite_data[sub_element.tag] = sub_element.text
                        for key in cite_data.keys():
                            if 'date' in key:
                                cite_data[key] = process_date(cite_data[key])
                        patent_cite_list.append(cite_data)
        return patent_cite_list, non_patent_cite_list
    else:
        return [None, None]

def process_citations(patent_citations, nonpatent_citations, citing_docnum):
    uspatseq = 0
    appseq = 0
    forpatseq = 0
    otherseq = 0
    results = {}

    if patent_citations is not None:
        # citation list can only be empty for plant patents?
        for citation in patent_citations:
            cited_doc_num = citation['doc-number']

            is_app = True  # indicator for whether something being cited is a patent application

            if cited_doc_num:
                # basically if there is anything other than number and digits its an application
                if re.match(r'^[A-Z]*\.?\s?\d+$', cited_doc_num):
                    num = re.findall('\d+', cited_doc_num)
                    num = num[0]  # turns it from list to string
                    if num[0] == '0':  # drop leading zeros
                        num = num[1:]
                    let = re.findall('[a-zA-Z]+', cited_doc_num)
                    if let:
                        let = let[0]  # list to string
                        cited_doc_num = let + num
                    else:
                        cited_doc_num = num
                    is_app = False

            if citation['country'] == "US":
                if cited_doc_num and not is_app:  # citations without document numbers are otherreferences
                    cited_doc_num = process_patent_numbers(cited_doc_num)
                    results['uspatentcitation'].append(
                            [id_generator(), citing_docnum, cited_doc_num, citation['date'],
                                citation['name'],
                                citation['kind'], citation['country'],
                                citation['category'], str(uspatseq)])
                    uspatseq += 1
                if cited_doc_num and is_app:
                    cit_app_id_transformed = cited_doc_num[:5] + cited_doc_num[:4] + cited_doc_num[5:]
                    cit_app_number_transformed = cited_doc_num.replace('/', '')
                    results['usapplicationcitation'].append(
                            [id_generator(), citing_docnum, cited_doc_num, citation['date'],
                                citation['name'],
                                citation['kind'], cited_doc_num, citation['country'], citation['category'],
                                str(appseq), cit_app_id_transformed, cit_app_number_transformed])
                    appseq += 1
            elif cited_doc_num:
                results['foreigncitation'].append(
                        [id_generator(), citing_docnum, citation['date'], cited_doc_num,
                            citation['country'], citation['category'], str(forpatseq)])
                forpatseq += 1
    if nonpatent_citations is not None:
        for citation in nonpatent_citations:
            results['otherreference'].append(
                    [id_generator(), citing_docnum, citation['text'].replace("\\", "/"), str(otherseq)])
            otherseq += 1

    return (results)

v1_cit_type = {'cited-patent-literature': 'patcit', 'cited-non-patent-literature': 'nplcit'}

def v1_get_citations(patent):
    '''
    :params patent: the xml object representing a patent
    :returns a list of default dictionary with code for each citation
    '''
    patent_cite_list = []
    non_patent_cite_list= []
    citations = patent.findall(".//citation")
    if citations != []:
        for citation in citations:
            cite_data = defaultdict(lambda : None)
            for element in citation:
                if element.tag == 'category':
                    cite_data['category'] = element.text
                elif element.tag =='classification-cpc-text':
                    pass
                else:
                    if element.tag in ['cited-patent-literature', 'cited-non-patent-literature']:
                        cite_data['type'] == v1_cit_type[element.tag]
                    if element.tag=='cited-non-patent-literature':
                        cite_data['text']= []
                        for sub_element in element:
                            cite_data['text'].append(sub_element.text)
                            for sub_sub_element in sub_element:
                                cite_data['text'].extend([sub_sub_element.text, sub_sub_element.tail])
                        cite_data['text'] = " ".join([item for item in cite_data['text'] if item is not None])
                        non_patent_cite_list.append(cite_data)
                    elif element.tag in ['classification-us', 'classification-ipc']:
                        pass
                    else:
                        doc_info = element.find('document-id')
                        for sub_element in doc_info:
                            cite_data[sub_element.tag] = sub_element.text
                        for key in cite_data.keys():
                            if 'date' in key:
                                cite_data[key] = process_date(cite_data[key])
                        patent_cite_list.append(cite_data)
        return patent_cite_list, non_patent_cite_list
    else:
        return [None, None]


#####################################
### Data Processing Helper Functions
#####################################
def recursive_children(xml_element, parent_field=""):
    '''
    :params xml_element: xml object can be nested
    :params parent_field: parent of nested xml object
    :returns a dictionary of the tags and texts of nested xml object
    '''
    test_list = []
    #we want to skip processing instructions
    if len(xml_element)==0 and not getattr(xml_element, 'tag', None) is etree.ProcessingInstruction:
        if parent_field:
            test_list.append((parent_field+"-"+xml_element.tag, xml_element.text))
        else:
            test_list.append((xml_element.tag, xml_element.text)) 
    else:  
        parent_field = xml_element.tag
        for element in xml_element:
            test_list += recursive_children(element, parent_field)

    return test_list

def get_text(element):

    entries = []
    if element is not None: #some patents lack an abstract
        if element.text:
            entries.extend(get_text_and_tail(element))
        for sub_element in element:
            if sub_element.tag == 'ul':
                pass
                #entries.extend(recursive_list(sub_item))
            else:
                entries.extend(get_text_and_tail(sub_element))
                for sub_sub_element in sub_element:
                    entries.extend(get_text_and_tail(sub_sub_element))           
        return list(filter(lambda x: x is not None and not x == "\n", entries))
    else:
        return [None]

def get_text_and_tail(element):
    '''
    Filters out the information processing tag information that is oddly appearing
    the extra text that appears after a child tag is, confusingly, the tail of the child tag rather than of the parent tag it seems to belong to
    :params element: xml element, or sub element, that has both text and a tail (the extra text that appears after a tag)
    '''
    results_list = []
    if not etree.tostring(element, encoding = 'unicode').startswith("<?"):
        results_list.extend([element.text, element.tail])
    if etree.tostring(element, encoding = 'unicode').startswith("<?"):
        results_list.extend([element.tail])
    return results_list
def recursive_list(field):
    '''
    Flattens out an xml list and returns the flattend version
    :params field that is an xml list
    '''
    list_of_lists = []
    if len(field)>0:
        list_of_lists.append(field.text)
        for sub_field in field:
            list_of_lists += recursive_list(sub_field)
    else:
        list_of_lists += get_text(field)
    return list_of_lists


#####################################
### Post-Processing Functions
#####################################
def process_patent_numbers(raw_patent_num):
    '''
    Helper function ot transform patent ids into thier final format
    :param raw_patent_num: patent number extracted from the raw XML
    :return cleaned patent id.
    '''
    num = re.findall('\d+', raw_patent_num)[0] #get the just numbers in string form
    if num[0].startswith("0"):
        num = num[1:]
    #Sept 5, 2016 - must triple check that moving the let definition out side of the if statement above does not mess up the patent numbers
    let = re.findall('[a-zA-Z]+', raw_patent_num) #get the letter prefixes
    if let:
        let = let[0]#list to string
        clean_patent_num = let + num
    else:
        clean_patent_num = num
    return clean_patent_num

def process_date(date): 
    '''
    Takes a date formated as 6 numbers and returns it with dashes and days that are 00 replaced with 01
    :params date: a date object formatted as 6 numbers
    :returns cleaned up date
    '''
    if date is not None:
        if date[6:] != "00":
            date = date[:4]+'-'+date[4:6]+'-'+date[6:]
        else:
            date = date[:4]+'-'+date[4:6]+'-'+'01'
    return date

def process_uspc_class_sub(classification):
    '''
    :params classification:  a uspc classification entry
    :returns cleaned up version of the classification entry
    '''
    # most un-processed uspc class text is 6 characters
    crossrefsub = classification[3:].replace(" ","") # the back 3 characters, spaces removed
    if len(crossrefsub) > 3 and re.search('^[A-Z]',crossrefsub[3:]) is None: # if the classification was longer than 6 characters and starts with a letter
        crossrefsub = crossrefsub[:3]+'.'+crossrefsub[3:] 
    crossrefsub = re.sub('^0+','',crossrefsub) # delete leading zeros
    if re.search('[A-Z]{3}',crossrefsub[:3]): 
        crossrefsub = crossrefsub.replace(".","") # remove periods from classes with letters in the front half
    return crossrefsub
    
def clean_country(country):
    if country is None or country == 'unknown':
        return None
    elif len(country) >=3:
        return country[:2]
    else:
        return country
