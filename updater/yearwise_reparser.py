import os
import re
from datetime import date, datetime, timedelta
from tqdm import tqdm

from updater.xml_to_sql.parser import queue_parsers
from lib.configuration import get_current_config

def runyear(year='01'):
    config = get_current_config('pgpubs', **{"execution_date": date.today()})
    folder_files = os.listdir(config['FOLDERS']['pgpubs_bulk_xml_location'])
    old_xml_files = list(filter( lambda f: re.fullmatch("i?pa{year}[0-9]{{4}}.xml".format(year=year),f)  , folder_files))
    old_xml_files.sort()

    for file in tqdm(old_xml_files):
        try:
            filedate = '20' + re.search('i?pa([0-9]{6}).xml', file).group(1)
            config['DATES']['START_DATE'] = filedate
            config['DATES']['END_DATE'] = (datetime.strptime(filedate, "%Y%m%d") + timedelta(6)).strftime("%Y%m%d")
            queue_parsers(config,'pgpubs')
        except Exception as e:
            print(e)