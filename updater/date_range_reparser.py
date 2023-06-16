import os
import re
import traceback
from datetime import date, datetime, timedelta
from tqdm import tqdm

from updater.xml_to_sql.parser import queue_parsers
from lib.configuration import get_current_config, get_config

def reparse(start, end, clearfirst = True, pubtype = 'pgpubs', raisefail=True):
    start = re.sub('[^\d]','', start)[-6:] #remove non-digits and get 6 digits
    end = re.sub('[^\d]','', end)[-6:] #remove non-digits and get 6 digits
    assert re.fullmatch('[0-9]{6}', start) and re.fullmatch('[0-9]{6}',end), 'enter start and end dates as "yymmdd" or "yyyymmdd" (punctuation separators allowed)'
    assert pubtype in ('pgpubs','granted_patent'), f"pubtype must be either 'pgpubs'(default) or 'granted_patent'; {pubtype} provided" 

    # config = get_current_config(pubtype, **{"execution_date": date.today()})
    config = get_config()
    folder_files = os.listdir(config['FOLDERS'][f'{pubtype}_bulk_xml_location'])

    usefiles = [fnam for fnam in folder_files if 
                        re.fullmatch("i?p[ag]([0-9]{6}).xml",fnam) is not None          and
                        re.fullmatch("i?p[ag]([0-9]{6}).xml",fnam).group(1) <= end      and
                        re.fullmatch("i?p[ag]([0-9]{6}).xml",fnam).group(1) >= start]
    usefiles.sort()

    if clearfirst:
        import json
        from sqlalchemy import create_engine, inspect
        host = '{}'.format(config['DATABASE_SETUP']['HOST'])
        user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
        password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
        port = '{}'.format(config['DATABASE_SETUP']['PORT'])
        database = '{}'.format(config['PATENTSVIEW_DATABASES']['REPARSE'])

        engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/?charset=utf8mb4'.format(user, password, host, port))
        inspector = inspect(engine)

        tabletoggle = json.load(open(config['XML_PARSING']['table_toggle']))
        if pubtype == 'pgpubs':
            tabletoggle = tabletoggle['pgpubs']
        else: 
            tabletoggle = tabletoggle['granted_patent']
        cleartables = [table for table in tabletoggle if tabletoggle[table]]

    config["DATES"] = {}
    for file in tqdm(usefiles):
        filedate = '20' + re.fullmatch('i?p[ag]([0-9]{6}).xml', file).group(1)
        config['DATES']['END_DATE'] = filedate
        config['DATES']['START_DATE'] = (datetime.strptime(filedate, "%Y%m%d") + timedelta(-6)).strftime("%Y%m%d")
        try:
            if clearfirst:
                existing_tables = inspector.get_table_names(database)
                for table in cleartables:
                    if table in existing_tables:
                        # remove data from weekly temp db in corresponding tables
                        engine.execute(f"DELETE FROM {database}.{table} WHERE version_indicator = '{filedate}'")
            queue_parsers(config, pubtype, destination = 'REPARSE')
        except Exception as e:
            if raisefail:
                raise
            else:
                print(f"{type(e).__name__}: {e}")
                print(traceback.format_exc())
