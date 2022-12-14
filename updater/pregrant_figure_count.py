import os
import re
import traceback
import pandas as pd
from tqdm import tqdm
from lxml import etree
from datetime import date, datetime
from sqlalchemy import create_engine



from updater.xml_to_sql.parser import extract_document
from lib.configuration import get_current_config


def count_figures(start, end, raisefail=True, upload=True, output=False):
    start = re.sub('[^\d]','', start)[-6:] #remove non-digits and get 6 digits
    end = re.sub('[^\d]','', end)[-6:] #remove non-digits and get 6 digits
    assert re.fullmatch('[0-9]{6}', start) and re.fullmatch('[0-9]{6}',end), 'enter start and end dates as "yymmdd" or "yyyymmdd" (punctuation separators allowed)'

    config = get_current_config(type='pgpubs', **{"execution_date": date.today()})
    folder = config['FOLDERS']['pgpubs_bulk_xml_location']
    folder_files = os.listdir(folder)

    usefiles = [fnam for fnam in folder_files if 
                        re.fullmatch("i?pa([0-9]{6}).xml",fnam) is not None          and
                        re.fullmatch("i?pa([0-9]{6}).xml",fnam).group(1) <= end      and
                        re.fullmatch("i?pa([0-9]{6}).xml",fnam).group(1) >= start]
    usefiles.sort()

    database = '{}'.format(config['PATENTSVIEW_DATABASES']['REPARSE'])
    host = '{}'.format(config['DATABASE_SETUP']['HOST'])
    user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
    password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
    port = '{}'.format(config['DATABASE_SETUP']['PORT'])

    engine = create_engine(
            'mysql+pymysql://{0}:{1}@{2}:{3}/?charset=utf8mb4'.format(user, password, host, port))
    pubnum = ''
    err_ct = 0


    for file in tqdm(usefiles):
        recordlist = []
        filedate = '20' + re.fullmatch('i?p[ag]([0-9]{6}).xml', file).group(1)
        try:
            for current_xml in extract_document(f'{folder}/{file}'):
                if file.startswith('pa'): #pgpubs versions 1.5 and 1.6 need an HTML parser instead
                    parser = etree.HTMLParser(no_network=False)
                    pub_doc = etree.HTML(current_xml.encode('utf-8'), parser=parser)
                    pubbase = pub_doc.find("body//patent-application-publication")
                    if pubbase is None:
                        continue
                    # parse out the number of figs
                    figparent = pubbase.find('subdoc-drawings')
                    if figparent is None:
                        continue
                    pubnum = pubbase.find("subdoc-bibliographic-information/document-id/doc-number").text
                    figlist = figparent.findall('figure')
                    docpages = []
                    repfigtag = figparent.find('representative-figure')
                    repfig = repfigtag.text if repfigtag is not None else None
                elif file.startswith('ipa'): #pgpubs version 4.x
                    parser = etree.XMLParser(load_dtd=True, no_network=False)
                    pub_doc = etree.XML(current_xml.encode('utf-8'), parser=parser)
                    if pub_doc.tag == 'sequence-cwu':
                        continue
                    #parse out the number of figs
                    figparent = pub_doc.find('drawings')
                    if figparent is None:
                        continue
                    pubnum = pub_doc.find("us-bibliographic-data-application/publication-reference/document-id/doc-number").text
                    docpages = figparent.findall('doc-page')
                    figlist = figparent.findall('figure')
                    repfig = None
                
                record = {
                    'document_number'   : pubnum,
                    'num_figures'       : len(figlist),
                    'num_pages'         : len(docpages),
                    'rep_figure'        : repfig,
                    'version_indicator' : datetime.strptime(filedate,'%Y%m%d').date()
                }
                recordlist.append(record)
            record_df = pd.DataFrame(recordlist)
            if output:
                return record_df
            if upload:
                l1 = record_df.shape[0]
                record_df.drop_duplicates(inplace=True)
                l2 = record_df.shape[0]
                if (l1-l2 > 0): print(f"dropped {l1-l2} duplicate record(s) in file {filedate}")
                record_df.to_sql(name='pg_figures', con=engine, schema=database, if_exists='append', index=False)
        
        except Exception as e:
            print(f"errorred with pubnum = {pubnum} and filedate = {filedate}")
            if raisefail:
                raise
            else:
                print(f"{type(e).__name__}: {e}")
                print(traceback.format_exc())
                err_ct +=1
                print(f"error number {err_ct}")

    print(f"completed {len(usefiles)} files; encountered {err_ct} errors")

