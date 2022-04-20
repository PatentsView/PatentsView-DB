import csv
import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from lib.configuration import get_config, get_current_config
# from helpers import general_helpers


def parse_and_write_cpc(**kwargs):
    """ Parse CPC Classifications """
    config = get_current_config(db, schedule='quarterly', **kwargs)
    inputdir = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
    for filename in os.listdir(inputdir):
        if (filename.startswith('US_PGPub_CPC_MCF_') and filename.endswith('.txt')):
            df_list = parse_pgpub_file(inputdir +'/'+filename)
            
            df = pd.DataFrame(df_list, columns = ['document_number', 'sequence', 'version', 'section_id', 'subsection_id', 'group_id', 
                    'subgroup_id', 'symbol_position', 'value'])

            df['category'] = None
            df['category'] = np.select([df['value'] == 'I',df['value'] == 'A'],['inventional','additional'],df['category'])

            database = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
            host = '{}'.format(config['DATABASE_SETUP']['HOST'])
            user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
            password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
            port = '{}'.format(config['DATABASE_SETUP']['PORT'])

            engine = create_engine(
                    'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))
            df.to_sql('cpc_current', con=engine, if_exists='append', index=False)

def parse_pgpub_file(**kwargs):
    """ Extract CPC classification from ~35 million applications """
    config = get_current_config('pgpubs', schedule='quarterly', **kwargs)
    cpc_input_path = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
    filepath = cpc_input_path + '/' + "CPC_pgpub_mcf.zip"
    with open(filepath) as f:
        input_rows = f.readlines()
        print("Parsing app file: {}; rows: {}".format(filepath, len(input_rows)))

    # Since applications are already sorted by app_number, we can check if the
    # current application has the same number as the last one seen.
    # Once we see a new application, save the classifications that have been
    # recorded and reset the lists of recorded classifications
    results = []

    # Initial values -- this will give us a first row with no data.
    last_application_seen = ''
    primary_classifications = []
    additional_classifications = []
    sequence = 0

    for i in range(len(input_rows)):
        row = input_rows[i]

        # Skip blank rows
        if row != '':
            app_number = row[10:21]
            cpc_section = row[21]
            cpc_subsection = cpc_section + row[22:24]
            cpc_group = cpc_subsection + row[24]
            cpc_subgroup = cpc_group + strip_whitespace(row[25:36])
            symbol_position = row[44]
            value = row[45]
            version = row[36:44]
            classification = strip_whitespace(row[21:36])
            if i == 0:
                last_application_seen = app_number
                
        if i != 0 and app_number == last_application_seen:
            sequence += 1
        else:
            sequence = 0

        # Save the classifications found to our results dataset
        results.append([app_number, sequence, version, cpc_section, cpc_subsection, cpc_group, 
                        cpc_subgroup, symbol_position, value])

        # Start recording for a new application
        last_application_seen = app_number

        # There is a problematic line that is cut short; as a result, we don't
        # know whether it is primary or secondary; so, skip this classification
        if len(row) <= 45:
            continue

    # Return all except the first row, which had empty placeholder values
    return results

def strip_whitespace(s):
    """ Strip whitespace really fast:

        re.sub('\s+', '', s)           2.33 usec per loop
        ''.join(s.split())             0.47 usec per loop
        s.replace(' ','')              0.40 usec per loop

    """
    return ''.join(s.split())


if __name__ == '__main__':
    config = get_config()

    location_of_cpc_files = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'], 'cpc_input')
    parse_and_write_cpc(location_of_cpc_files, config)
