#Temp Upload Counts
import pandas as pd
import os
import configparser
import sys
from sqlalchemy import create_engine
project_home = os.environ['PACKAGE_HOME']
import configparser
config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')
from Development.helpers import general_helpers

host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']
previous_qa_loc = config['FOLDERS']['OLD_QA_LOC']
new_qa_loc = config['FOLDERS']['NEW_QA_LOC']
latest_expected_date = config['DATES']['END_DATE']


def temp_upload_count (temporary_upload, tables):
    new_counts = []
    description= []
    #engine.execute('use {}'.format(temporary_upload))
    for table in tables:
        print(table)
        conn = engine.connect()
        count = conn.execute('select count(*) from {}'.format(table))
        conn.close()
        new_counts.append(count)
    for count in new_counts:
        if count == 0:
            description.append("Problem: Empty Table")
        else:
            description.append("No Problem!")

    df=pd.DataFrame({'Table': tables, 'Count':new_counts, 'Description':description})
    df_temp_upload=df[['Table', 'Count', 'Description']] #get results in correct order
    return df_temp_upload

def write_temp(df, new_qa_loc):
    df.to_csv('{0}/01_table_counts_temp_upload.csv'.format(new_qa_loc), index = False)


if __name__ == '__main__':
    if not os.path.exists(new_qa_loc):
        os.mkdir(new_qa_loc)
    engine = general_helpers.connect_to_db(host, username, password, temporary_upload)
    tables=['application',	'botanic',	'brf_sum_text',	'claim','detail_desc_text',	'draw_desc_text',	'figures',	'foreign_priority',	'foreigncitation',	'government_interest',	'ipcr',	'mainclass',	'non_inventor_applicant',	'otherreference',	'patent',	'pct_data',	'rawassignee',	'rawexaminer',	'rawinventor',	'rawlawyer',	'rawlocation',	'rel_app_text',	'subclass',	'us_term_of_grant',	'usapplicationcitation',	'uspatentcitation',	'uspc',		'usreldoc']
    df = temp_upload_count (temporary_upload, tables)
    write_temp(df, new_qa_loc)
