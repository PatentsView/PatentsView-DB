import os
import sys
import csv
import re
import pandas as pd
import simplejson as json
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import output, xml_helpers, general_helpers


from lxml import etree
from collections import defaultdict
import string
import random
import multiprocessing
import copy
import configparser


def get_results(patents, field_dictionary): 
    results = {}
    for field in field_dictionary.keys(): 
        results['{}'.format(field)] = []

    error_log = []
    already_seen_patents = []
    for patent in patents:
        
        patent_data =  xml_helpers.get_entity(patent, 'publication-reference')[0]

        patent_id = xml_helpers.process_patent_numbers(patent_data['document-id-doc-number'])
        #there are a small number (~150 per file) patents that have a second patent document
        #this second one contains DNA sequence information and we should skip it for now
        if patent_id in already_seen_patents:
            continue
        already_seen_patents.append(patent_id)
        patent_date = patent_data['document-id-date']
        abstract = xml_helpers.get_text(patent.find(".//abstract"))
        if abstract[0] is not None:
            abstract = "".join(abstract).replace("\t", " ").replace('\n', ' ').replace('\r\n',' ')
        else:
            abstract = None

        title = "".join(xml_helpers.get_text(patent.find(".//invention-title"))).replace('\t', ' ')
        num_claims_data = xml_helpers.get_entity(patent, 'number-of-claims')[0]
        if num_claims_data:
            num_claims = num_claims_data['number-of-claims']
        else:
            num_claims = None
            error_log.append([patent_id, 'num_claims'])
        filename = 'ipg{0}{1}{2}.xml'.format(patent_date[2:4], patent_date[5:7], patent_date[8:])
        
        app_data = xml_helpers.get_entity(patent, 'application-reference', attribute_list=['appl-type'])[0]
        if app_data is not None:
            date = app_data['document-id-date']
            if not date:
                #it is '01' not zero because of Solr
                date = '01-01-0001'
            app_type = app_data['appl-type']
            if app_type == 'SIR': #replace this will the full name
                app_type = 'statutory invention registration'
            application_num = app_data['document-id-doc-number']
            applicationid = date[:4]+"/"+application_num
            series_code = patent.find('.//us-application-series-code').text
            #logic for creating transformed columns
            if patent_id.startswith('D'):
    	        id_transformed = application_num
    	        number_transformed = application_num
    	        series_code_transformed_from_type = 'D'
            else:
    	        id_transformed = '{}/{}'.format(application_num[:2], application_num[2:])
    	        number_transformed = application_num
    	        series_code_transformed_from_type = series_code

            results['application'].append([applicationid, patent_id, series_code, app_data['document-id-doc-number'], app_data['document-id-country'], date, id_transformed, number_transformed, series_code_transformed_from_type])
        else:
            error_log.append([patent_id, 'application'])
            app_type = None
        #assigned after application to extract application data
        results['patent'].append([patent_id,app_type , patent_id, patent_data['document-id-country'],
                           patent_date, abstract, title, patent_data['document-id-kind'], num_claims, filename, '0'])

        
        exemplary = xml_helpers.get_entity(patent, 'us-exemplary-claim')
        if exemplary[0] is not None:
            exemplary = [int(list(item_dict.values())[0]) for item_dict in exemplary if list(item_dict.values())[0] is not None] #list of exemplary claims as strings
        else:
            error_log.append([patent_id, 'exemplary'])
        claims = xml_helpers.get_claims_data(patent)
        for num, claim in enumerate(claims):
            claim_num = num +1 #add one because they are 1 indexed not 0 indexed
            is_exemplary = claim_num  in exemplary 
            results['claim'].append([general_helpers.id_generator(), patent_id, claim['claim'], claim['dependency'], claim_num , is_exemplary])

        #############################
        # Text Fields
        ##############################
        text_data = xml_helpers.get_main_text_fields(patent)
        
        if text_data:
            detail_desc_text_data = text_data['Detailed Description']
            if detail_desc_text_data !=[]:
                results['detail_desc_text'].append([general_helpers.id_generator(), patent_id, detail_desc_text_data, len(detail_desc_text_data)])
            else:
                if not patent_id[0] in ['R', 'P', 'H', 'D']: #these types are allowed to not have detailed descriptions
                    error_log.append([patent_id, 'detail-description'])

            brf_sum_text_data = text_data['Brief Summary']
            if brf_sum_text_data !=[]:
                results['brf_sum_text'].append([general_helpers.id_generator(), patent_id, brf_sum_text_data])

            draw_desc = text_data['Brief Description of Drawings']
            for i, description in enumerate(draw_desc):
                results['draw_desc_text'].append([general_helpers.id_generator(), patent_id, description, i])

            rel_app_text_data = text_data['Other Patent Relations']
            if rel_app_text_data !=[]:
                results['rel_app_text'].append([general_helpers.id_generator(), patent_id, rel_app_text_data, 0])
                
            government_interest_data = text_data['Government Interest']
            if government_interest_data !=[]:
                results['government_interest'].append([patent_id, government_interest_data])
            text_fields = ['Detailed Description', 'Brief Summary', 'Brief Description of Drawings',
                           'Other Patent Relations','Government Interest']
            if not set(text_data.keys()).issubset(text_fields):
                error_log.append([patent_id, text_data.keys()])
        else:
            error_log.append([patent_id, 'description'])

        #############################
        # People
        ##############################

        rule_47_flag = 0
        if patent.find(".//rule-47-flag") is not None:
            rule_47_flag = 1
        inventor_data = xml_helpers.get_entity(patent, 'inventor', attribute_list=['sequence'])
        if inventor_data[0] is not None:
            for inventor in inventor_data:
                rawlocid = general_helpers.id_generator()
                results['rawlocation'].append([rawlocid, None, inventor['address-city'], inventor['address-state'], inventor['address-country'], xml_helpers.clean_country(inventor['address-country'])])
                deceased = False
                if 'deceased' in output.get_alt_tags(inventor, ['addressbook-lastname', 'addressbook-last-name']):
                    deceased = True
                results['rawinventor'].append([general_helpers.id_generator(), patent_id, None, rawlocid, output.get_alt_tags(inventor, ['addressbook-firstname', 'addressbook-first-name']), output.get_alt_tags(inventor, ['addressbook-lastname', 'addressbook-last-name']), int(inventor['sequence']) -1, rule_47_flag, deceased])
                output.mandatory_fields('inventor', patent_id, error_log, [output.get_alt_tags(inventor, ['addressbook-firstname', 'addressbook-first-name']), output.get_alt_tags(inventor, ['addressbook-lastname', 'addressbook-last-name'])])
        else:
            error_log.append([patent_id, 'inventor'])
        deceased_inventors = xml_helpers.get_entity(patent, 'us-deceased-inventor', attribute_list=None)
        
        if deceased_inventors[0] is not None:
            if [patent_id, 'inventor'] in error_log: error_log.remove([patent_id, 'inventor'])
            for inventor in deceased_inventors:
                rawlocid = general_helpers.id_generator()
                results['rawlocation'].append([rawlocid, None, inventor['address-city'], inventor['address-state'], inventor['address-country'], xml_helpers.clean_country(inventor['address-country'])])
                if inventor['sequence']:
                    inventor['sequence'] = int(inventor['sequence']) -1
                results['rawinventor'].append([general_helpers.id_generator(), patent_id, None, rawlocid, 
                                         output.get_alt_tags(inventor, ['addressbook-firstname', 'addressbook-first-name']), 
                                         output.get_alt_tags(inventor, ['addressbook-lastname', 'addressbook-last-name']),
                                         inventor['sequence'], rule_47_flag, True])
                output.mandatory_fields('inventor', patent_id, error_log, [output.get_alt_tags(inventor, ['addressbook-firstname', 'addressbook-first-name']), output.get_alt_tags(inventor, ['addressbook-lastname', 'addressbook-last-name'])])
        
        #applicant has inventor info for some earlier time periods
        app_inv_data = xml_helpers.get_entity(patent, 'applicant', attribute_list=['sequence', 'app-type', 'designation'])
        inventor_app_seq = 0
        has_inventor_applicant = False
        if app_inv_data[0] is not None:
            if [patent_id, 'inventor'] in error_log: error_log.remove([patent_id, 'inventor'])
            for applicant in app_inv_data:
                rawlocid = general_helpers.id_generator() 
                results['rawlocation'].append([rawlocid, None,applicant['address-city'],applicant['address-state'], applicant['address-country'], xml_helpers.clean_country(applicant['address-country'])]) 
                if applicant['app-type'] == "applicant-inventor":
                    #rule_47 flag always 0 for applicant-inventors (becauase they must be alive)
                    results['rawinventor'].append([general_helpers.id_generator(), patent_id,None, rawlocid, applicant['addressbook-first-name'],
                                              applicant['addressbook-last-name'], str(inventor_app_seq), 0 , False])
                    inventor_app_seq +=1
                    output.mandatory_fields('inventor_applicant', patent_id, error_log,[applicant['addressbook-first-name'],
                                         applicant['addressbook-last-name']])
                else:
                    results['non_inventor_applicant'].append([general_helpers.id_generator(), patent_id,rawlocid, applicant['addressbook-first-name'],
                                              applicant['addressbook-last-name'],applicant['addressbook-orgname'],
                                            str(int(applicant['sequence'])), applicant['designation'], applicant['app-type']])
                    output.mandatory_fields('non_inventor_applicant', patent_id, error_log,[applicant['addressbook-first-name'],
                                         applicant['addressbook-last-name'],applicant['addressbook-orgname']])
        
        non_inventor_app_data = xml_helpers.get_entity(patent, 'us-applicant', attribute_list=['sequence', 'app-type', 'designation'])
        if non_inventor_app_data[0] is not None:
            for applicant in non_inventor_app_data:
                rawlocid = general_helpers.id_generator() 
                results['rawlocation'].append([rawlocid, None,applicant['address-city'],applicant['address-state'], applicant['address-country'], xml_helpers.clean_country(applicant['address-country'])]) 
                results['non_inventor_applicant'].append([general_helpers.id_generator(), patent_id,rawlocid, applicant['addressbook-first-name'],
                                          applicant['addressbook-last-name'],applicant['addressbook-orgname'],
                                        str(int(applicant['sequence'])), applicant['designation'], applicant['app-type']])
                output.mandatory_fields('non_inventor_applicant', patent_id, error_log,[applicant['addressbook-first-name'],
                                          applicant['addressbook-last-name'],applicant['addressbook-orgname']])
        
        assignee_data = xml_helpers.get_entity(patent, 'assignee')
        if assignee_data[0] is not None:
            for i, assignee_dict in enumerate(assignee_data):
                assignee = copy.deepcopy(assignee_dict) #make a copy in order to not modify in place
                for key in assignee_dict.keys():
                    if key.startswith('assignee'):
                        assignee[key.replace('assignee', 'addressbook')] = assignee[key]
                rawlocid = general_helpers.id_generator()
                results['rawlocation'].append([rawlocid, None, assignee['address-city'], assignee['address-state'], assignee['address-country'], xml_helpers.clean_country(assignee['address-country'])])
                if assignee['addressbook-role']: #can only strip the leading 0 if exists
                    assignee['addressbook-role'] = assignee['addressbook-role'].lstrip("0")
                results['rawassignee'].append([general_helpers.id_generator(), patent_id, None, rawlocid, assignee['addressbook-role'], output.get_alt_tags(assignee, ['addressbook-firstname', 'addressbook-first-name']), 
                                         output.get_alt_tags(assignee, ['addressbook-lastname', 'addressbook-last-name']), 
                                         assignee['addressbook-orgname'], str(i)])
                name_and_org = [output.get_alt_tags(assignee, ['addressbook-firstname', 'addressbook-first-name']), 
                                         output.get_alt_tags(assignee, ['addressbook-lastname', 'addressbook-last-name']), 
                                         assignee['addressbook-orgname']]
                output.mandatory_fields('assignee', patent_id, error_log, name_and_org)
                
        else:
            error_log.append([patent_id, 'assignee'])
        lawyer_data = xml_helpers.get_entity(patent, 'agent', attribute_list=['sequence'])
        if lawyer_data[0] is not None: #not all patents have a lawyer, becuase you can self-file
            for lawyer in lawyer_data:
                results['rawlawyer'].append([general_helpers.id_generator(), None, patent_id, output.get_alt_tags(lawyer, ['addressbook-firstname', 'addressbook-first-name']), output.get_alt_tags(lawyer, ['addressbook-lastname', 'addressbook-last-name']),lawyer['addressbook-orgname'],lawyer['addressbook-country'], int(lawyer['sequence'])])
                output.mandatory_fields('rawlawyer', patent_id, error_log,[output.get_alt_tags(lawyer, ['addressbook-firstname', 'addressbook-first-name']), output.get_alt_tags(lawyer, ['addressbook-lastname', 'addressbook-last-name']),lawyer['addressbook-orgname']])
        examiner_data = xml_helpers.get_entity(patent, 'examiners')[0]
        results['rawexaminer'].append([general_helpers.id_generator(), patent_id, output.get_alt_tags(examiner_data, ['primary-examiner-firstname', 'primary-examiner-first-name']),
                                      output.get_alt_tags(examiner_data, ['primary-examiner-lastname', 'primary-examiner-last-name']),
                                       'primary',examiner_data['primary-examiner-department']])
        #not all patents have assisstant examiners
        if output.get_alt_tags(examiner_data, ['assistant-examiner-lastname', 'assistant-examiner-last-name']) is not None:
                results['rawexaminer'].append([general_helpers.id_generator(), patent_id, output.get_alt_tags(examiner_data, ['assistant-examiner-firstname', 'assistant-examiner-first-name']),
                                              output.get_alt_tags(examiner_data, ['assistant-examiner-lastname', 'assistant-examiner-last-name']),
                                               'assistant',examiner_data['primary-examiner-department']])
        output.mandatory_fields('rawexaminer', patent_id, error_log, [output.get_alt_tags(examiner_data, ['primary-examiner-firstname', 'primary-examiner-first-name']), output.get_alt_tags(examiner_data, ['primary-examiner-lastname', 'primary-examiner-last-name'])])
 
        #############################
        # Citations
        ##############################
        #initialize all the sequence variables as 0
        uspatseq = 0
        appseq = 0
        forpatseq= 0
        otherseq = 0
    
        patent_citations, nonpatent_citations = xml_helpers.get_citations(patent)
        if patent_citations is not None:
            #citation list can only be empty for plant patents?
            for citation in patent_citations:
                cited_doc_num = citation['doc-number']

                is_app = True #indicator for whether something being cited is a patent application

                if cited_doc_num:
                    if re.match(r'^[A-Z]*\.?\s?\d+$', cited_doc_num): #basically if there is anything other than number and digits its an application
                        num = re.findall('\d+', cited_doc_num)
                        num = num[0] #turns it from list to string
                        if num[0] == '0': #drop leading zeros
                            num = num[1:]
                        let = re.findall('[a-zA-Z]+', cited_doc_num)
                        if let:
                            let = let[0]#list to string
                            cited_doc_num = let +num
                        else:
                            cited_doc_num = num
                        is_app = False



                if citation['country'] == "US":
                    if cited_doc_num and not is_app: #citations without document numbers are otherreferences
                        cited_doc_num = xml_helpers.process_patent_numbers(cited_doc_num)
                        results['uspatentcitation'].append([general_helpers.id_generator(), patent_id, cited_doc_num, citation['date'], citation['name'],
                                                   citation['kind'], citation['country'],
                                                  citation['category'],str(uspatseq)])
                        uspatseq+=1
                    if cited_doc_num  and is_app:
                        cit_app_id_transformed = cited_doc_num[:5] + cited_doc_num[:4] + cited_doc_num[5:]
                        cit_app_number_transformed = cited_doc_num.replace('/', '')
                        results['usapplicationcitation'].append([general_helpers.id_generator(), patent_id,cited_doc_num, citation['date'], citation['name'],
                                      citation['kind'], cited_doc_num, citation['country'], citation['category'],
                                                        str(appseq), cit_app_id_transformed, cit_app_number_transformed])
                        appseq +=1
                elif cited_doc_num:
                    results['foreigncitation'].append([general_helpers.id_generator(), patent_id, citation['date'] ,cited_doc_num,
                                              citation['country'], citation['category'], str(forpatseq)])
                    forpatseq+=1 
                else:
                    error_log.append([patent_id, citation])
        elif app_type !='plant': #only plant patents are allowed to not have a patent citation
            error_log.append([patent_id, 'citations'])        
        if nonpatent_citations is not None:
            for citation in nonpatent_citations:
                results['otherreference'].append([general_helpers.id_generator(), patent_id, citation['text'].replace("\\", "/"), str(otherseq)])
                otherseq +=1
        ##########################################
        # Classifications
        #################################
        
        uspcs = xml_helpers.get_uspc(patent)
        uspc_seq = 0
        if uspcs!=[]:
            for uspc in uspcs:
                main_class = uspc['main-classification'][0][:3].replace(" ","")
                results['mainclass'].append(main_class)
                if uspc['further-classification']:
                    for further_class in uspc['further-classification']:
                        further_main_class = further_class[:3].replace(" ","")
                        results['mainclass'].append(further_main_class)
                        further_sub_class = xml_helpers.process_uspc_class_sub(further_class)
                        results['subclass'].append(further_sub_class)
                        if further_sub_class != '':
                            #why do we only do this for further class?
                            further_combined_class = "{0}/{1}".format(further_main_class, further_sub_class)
                            results['subclass'].append(further_combined_class) 
                            results['uspc'].append([general_helpers.id_generator(), patent_id, further_main_class, further_combined_class, str(uspc_seq)])
                            uspc_seq +=1
                main_sub_class = xml_helpers.process_uspc_class_sub(uspc['main-classification'][0])
                results['subclass'].append(main_sub_class)

        ipcr_data = xml_helpers.get_entity(patent, 'classifications-ipcr/')
        if ipcr_data[0] is not None:
            for i, ipcr in enumerate(ipcr_data):
                results['ipcr'].append([general_helpers.id_generator(), patent_id, ipcr['classification-ipcr-classification-level'],
                                       ipcr['classification-ipcr-section'],ipcr['classification-ipcr-class'],ipcr['classification-ipcr-subclass'],
                                       ipcr['classification-ipcr-main-group'],ipcr['classification-ipcr-subgroup'],
                                       ipcr['classification-ipcr-symbol-position'],ipcr['classification-ipcr-classification-value'],
                                       ipcr['classification-ipcr-classification-status'],
                                       ipcr['classification-ipcr-classification-data-source'],ipcr['action-date-date'],
                                       ipcr['ipc-version-indicator-date'],i])
        #for the year 2005 data is often in classification-ipc and needs to be preprocessed
        ipc_data = xml_helpers.get_entity(patent, 'classification-ipc')
        if ipc_data[0] is not None:
            for i, ipc in enumerate(ipc_data):
                main = ipc['classification-ipc-main-classification']
                intsec = main[0]
                mainclass = main[1:3]
                group, subgroup = None, None
                if patent_id.startswith("D"): #special processing
                    intsec = 'D'
                    mainclass = main[0]
                    subclass = main[1:5]
                else:
                    subclass = main[3]
                    group = re.sub('^\s+','',main[4:7])
                    subgroup = re.sub('^\s+','',main[7:])
                # ipc['classification-ipc-edition'] has classification info, either 7 or unknown (as far as I can see)
                #this is not in the same format as for later years, so I'm skipping this for now
                results['ipcr'].append([general_helpers.id_generator(), patent_id,None,intsec,mainclass,subclass, group,subgroup,None,None,None,None,None,None,str(i)])

        
        
        #############################
        # usreldocs
        ##############################
        usreldoc_data = xml_helpers.get_usreldocs(patent)
        if usreldoc_data != []:
            for doc in usreldoc_data:
                results['usreldoc'].append([general_helpers.id_generator(), patent_id, doc['doc-type'], doc['relation'],doc['doc-number'],
                                  doc['country'],doc['date'], doc['status'], doc['sequence'], doc['kind']])


        ########################
        # Various Optional Fields
        ##########################

        botanic_data = xml_helpers.get_entity(patent, 'us-botanic')[0]
        if botanic_data is not None:
            results['botanic'].append([general_helpers.id_generator(), patent_id, botanic_data['us-botanic-latin-name'], botanic_data['us-botanic-variety']])

        # Foreign Priority List       
        foreign_priority = xml_helpers.get_entity(patent, 'priority-claim', attribute_list = ['kind'])
        if foreign_priority[0] is not None:
            for i, priority_claim in enumerate(foreign_priority):
                results['foreign_priority'].append([general_helpers.id_generator(), patent_id,str(i), priority_claim['kind'],
                                            priority_claim['priority-claim-doc-number'], priority_claim['priority-claim-date'],
                                            priority_claim['priority-claim-country'], xml_helpers.clean_country(priority_claim['priority-claim-country'])])
        #US term of Grant
        ustog = xml_helpers.get_entity(patent, 'us-term-of-grant/')[0]
        if ustog is not None:
            results['us_term_of_grant'].append([general_helpers.id_generator(), patent_id, ustog['lapse-of-patent'],ustog['disclaimer-date'],
                                    ustog['disclaimer-text'],ustog['length-of-grant'], ustog['us-term-extension']])

        #figures list
        fig_data = xml_helpers.get_entity(patent, 'figures')[0]
        if fig_data is not None:
            results['figures'].append([general_helpers.id_generator(), patent_id, fig_data['figures-number-of-figures'],
                                 fig_data['figures-number-of-drawing-sheets']])

        # PCT Data
        #102_data is ALWAYS null, need to check this
        pct_filing_data = xml_helpers.get_entity(patent, 'pct-or-regional-filing-data')[0]
        if pct_filing_data is not None: #the 371 date is sometimes 371c124 and sometimes 371c12
            if pct_filing_data['us-371c124-date-date'] is not None:
                pct_filing_data['us-371c12-date-date'] = pct_filing_data['us-371c124-date-date']
            results['pct_data'].append([general_helpers.id_generator(), patent_id, pct_filing_data['document-id-doc-number'],
                             pct_filing_data['document-id-date'], pct_filing_data['us-371c12-date-date'],
                             pct_filing_data['document-id-country'], pct_filing_data['document-id-kind'],
                             "pct_application", None])
        pct_pub_data = xml_helpers.get_entity(patent, 'pct-or-regional-publishing-data')[0]
        if pct_pub_data is not None:
            results['pct_data'].append([general_helpers.id_generator(), patent_id, pct_pub_data['document-id-doc-number'],
                             pct_pub_data['document-id-date'], None,
                             pct_pub_data['document-id-country'], pct_pub_data['document-id-kind'],
                             "wo_grant", None])  
    return results, error_log




def main_process(data_file, outloc, field_dictionary):
    patent_xml = xml_helpers.get_xml(data_file)
    results, error_log = get_results(patent_xml, field_dictionary)
    error_data = pd.DataFrame(error_log)
    error_data.columns = ['patent_id', 'field']
    error_counts = error_data.groupby("field").count()
    os.mkdir(outloc)
    error_data.to_csv('{0}/error_data.csv'.format(outloc))
    error_counts.to_csv('{0}/error_counts.csv'.format(outloc))
    output.write_partial(results, outloc, field_dictionary)



if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')
    #TO run Everything:
    with open('{}/field_dict.json'.format(config['FOLDERS']['PERSISTENT_FILES'])) as myfile:
        field_dictionary = json.load(myfile)
    
    input_folder = '{}/clean_data'.format(config['FOLDERS']['WORKING_FOLDER'])
    output_folder = '{}/parsed_data'.format(config['FOLDERS']['WORKING_FOLDER'])
    #this is the folder with the xml files that we want to reparse
    in_files = ['{0}/{1}'.format(input_folder, item) for item in os.listdir(input_folder)]
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    out_files= ['{0}/{1}'.format(output_folder, item[-16:-10]) 
                   for item in in_files]
    fields = [field_dictionary for item in in_files]
    files = zip(in_files, out_files, fields)
    desired_processes = 7 # ussually num cpu - 1
    jobs = []
    for f in files:
        jobs.append(multiprocessing.Process(target = main_process, args=(f)))
    for segment in general_helpers.chunks(jobs, desired_processes):
        print(segment)
        for job in segment:
            job.start()
            job.join()
