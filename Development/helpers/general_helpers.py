#import MySQLdb
from sqlalchemy import create_engine
import csv
import string
import re,os,random,string,codecs
import requests
from clint.textui import progress
from itertools import (takewhile,repeat)

def chunks(l,n):
    '''Yield successive n-sized chunks from l. Useful for multi-processing'''
    chunk_list =[]
    for i in range(0, len(l), n):
        chunk_list.append(l[i:i + n])
    return chunk_list

def connect_to_db(host, username, password, database,server_side_cursors=False):
    engine = create_engine('mysql+mysqldb://{}:{}@{}/{}?charset=utf8mb4'.format(username, password, host, database ), encoding='utf-8', pool_size=30, max_overflow=0, server_side_cursors=server_side_cursors )
    return engine

def send_slack_notification(message, slack_client, slack_channel, section="DB Update", level="info"):
    color_map = {"info": "#6699cc", "success": "#aad922", "warning": "#FFFF00", "error": "#d62828"}
    print(color_map[level])
    return slack_client.api_call(
        "chat.postMessage",
        channel=slack_channel,
        text=section,
        attachments=[
            {
                "color": color_map[level],
                "text": message
            }
        ]
    )

def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def write_csv(rows, outputdir, filename):
    """ Write a list of lists to a csv file """
    print(outputdir)
    print(os.path.join(outputdir, filename))
    writer = csv.writer(open(os.path.join(outputdir, filename), 'w',encoding='utf-8'))
    writer.writerows(rows)

def download(url, filepath):
    """ Download data from a URL with a handy progress bar """

    print("Downloading: {}".format(url))
    r = requests.get(url, stream=True)

    with open(filepath, 'wb') as f:
        content_length = int(r.headers.get('content-length'))
        for chunk in progress.bar(r.iter_content(chunk_size=1024),
                                  expected_size=(content_length/1024) + 1):
            if chunk:
                f.write(chunk)
                f.flush()


def get_patent_ids(db_con, new_db):

    patent_data = db_con.execute('select id, number from {}.patent'.format(new_db))
    patnums = {}
    for patent in patent_data:
        patnums[patent['number']] = patent['id']
    return set(patnums.keys()), patnums


def better_title(text):
    title = " ".join([item if item not in ["Of", "The", "For", "And", "On"] else item.lower() for item in str(text).title().split( )])
    return re.sub('['+string.punctuation+']', '', title)


# REQUIRES: a table's column info from information schema and table prefix for join statement
# MODIFIES: nothing
# EFFECTS: returns string statements for create, insert, and select components 
#      in order to build query for a table 
def get_column_info(table_col_info, table_prefix):
   
    column_data =[item for item in table_col_info]
    
    # create cols string for create, insert, and select statements for 1) rawinventor 
    table_cols_create_stmt = [item[0] + ' ' + item[1] for item in column_data]
    table_cols_create_str = ', '.join(table_cols_create_stmt)
    
    table_cols_insert_stmt = [item[0] for item in column_data]
    table_cols_insert_str = ', '.join(table_cols_insert_stmt)
   
    table_cols_select_stmt = [table_prefix + item for item in table_cols_insert_stmt]
    table_cols_select_str = ', '.join(table_cols_select_stmt)
    
    return table_cols_create_str, table_cols_insert_str, table_cols_select_str
    
# REQUIRES: two table component strings
# MODIFIES: nothing
# EFFECTS: returns full string column component    
def get_full_column_strings(table1_str, table2_str):
    
    full_cols_str = table1_str  + ',' + table2_str
    
    return full_cols_str





def rawbigcount(filename):
    f = open(filename, 'rb')
    bufgen = takewhile(lambda x: x, (f.raw.read(1024*1024) for _ in repeat(None)))
    return sum( buf.count(b'\n') for buf in bufgen if buf )
