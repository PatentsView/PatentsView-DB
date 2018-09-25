#import MySQLdb
from sqlalchemy import create_engine
import csv
import re,os,random,string,codecs
import requests
from clint.textui import progress

def chunks(l,n):
    '''Yield successive n-sized chunks from l. Useful for multi-processing'''
    chunk_list =[]
    for i in range(0, len(l), n):
        chunk_list.append(l[i:i + n])
    return chunk_list

def connect_to_db(host, username, password, database):
    engine = create_engine('mysql+mysqldb://{}:{}@{}/{}?charset=utf8'.format(username, password, host, database ), encoding='utf-8')
    return engine



def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def write_csv(rows, outputdir, filename):
    """ Write a list of lists to a csv file """
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
