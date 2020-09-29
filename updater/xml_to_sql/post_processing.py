import configparser
import pymysql
from sqlalchemy import create_engine
import re
import pandas as pd
import time
import os

from lib.configuration import get_config
from lib.configuration import update_config_date
from QA.text_parser.AppTest import AppUploadTest


def pct_data_doc_type(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    engine = create_engine(
        'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    engine.execute(
        'UPDATE pct_data SET doc_type = "pct_application" WHERE kind = "00";')
    engine.execute(
        'UPDATE pct_data SET doc_type = "wo_grant" WHERE kind = "A";')


def consolidate_uspc(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    engine = create_engine(
        'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    engine.execute(
        "DELETE  FROM  rawuspc  WHERE  LENGTH(classification) < 3;")
    engine.execute(
        "INSERT INTO uspc (id, document_number, mainclass_id, subclass_id, sequence, filename) SELECT id, document_number, TRIM(SUBSTRING(classification,1,3)), TRIM(CONCAT(SUBSTRING(classification,1,3), '/', TRIM(SUBSTRING(classification,4, LENGTH(classification))))), sequence, filename FROM rawuspc;")


def consolidate_rawlocation(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    engine = create_engine(
        'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    engine.execute(
        'INSERT INTO rawlocation (id, city, state, country, filename) SELECT rawlocation_id, city, state, country, filename FROM rawassignee;')
    engine.execute(
        'INSERT INTO rawlocation (id, city, state, country, filename) SELECT rawlocation_id, city, state, country, filename FROM rawinventor;')


def consolidate_cpc(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    engine = create_engine(
        'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    engine.execute(
        "INSERT INTO cpc (id,document_number,sequence,version,section_id,subsection_id,group_id,subgroup_id,symbol_position,`value`,action_date,filename,created_date,updated_date) SELECT id,document_number,sequence,version,section as section_id,concat(section, class) as subsection_id,concat(section, class, subclass) as group_id,concat(section, class, subclass, main_group, '/', subgroup) as subgroup_id,symbol_position,`value`,action_date,filename,created_date,updated_date from main_cpc;")
    engine.execute(
        "INSERT INTO cpc (id,document_number,sequence,version,section_id,subsection_id,group_id,subgroup_id,symbol_position,`value`,action_date,filename,created_date,updated_date) SELECT id,document_number,(sequence+1),version,section as section_id,concat(section, class) as subsection_id,concat(section, class, subclass) as group_id,concat(section, class, subclass, main_group, '/', subgroup) as subgroup_id,symbol_position,`value`,action_date,filename,created_date,updated_date from further_cpc;")
    engine.execute(
        "UPDATE cpc SET category = 'inventional' WHERE value = 'I';")
    engine.execute(
        "UPDATE cpc SET category = 'additional' WHERE value != 'I';")


def consolidate_usreldoc(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    engine = create_engine(
        'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    engine.execute(
        'DELETE FROM usreldoc_single WHERE related_doc_number IS NULL;')
    engine.execute(
        'INSERT INTO usreldoc SELECT * FROM usreldoc_parent_child;')
    engine.execute(
        'INSERT INTO usreldoc SELECT * FROM usreldoc_single;')
    engine.execute(
        'INSERT INTO usreldoc SELECT * FROM usreldoc_related;')


def consolidate_claim(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    engine = create_engine(
        'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    engine.execute(
        "UPDATE claim SET dependent = replace(dependent, 'claim ', '');")

    engine.execute(
        "UPDATE claim SET dependent = replace(dependent, 'Claim ', '');")

    engine.execute(
        "UPDATE claim SET dependent = replace(dependent, 'claims ', '');")

    engine.execute(
        "UPDATE claim SET dependent = replace(dependent, 'Claims ', '');")

    engine.execute(
        "UPDATE claim SET dependent = concat('claim ', dependent) WHERE dependent NOT LIKE '%%,%%';")

    engine.execute(
        "UPDATE claim SET dependent = concat('claims ', dependent) WHERE dependent LIKE '%%,%%';")


def detail_desc_length(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    engine = create_engine(
        'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

    engine.execute(
        'UPDATE detail_desc_text d SET `length` = LENGTH(d.`text`);')


def yearly_claim(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    con = pymysql.connect(host, user, password, database)

    with con:
        cur = con.cursor()
        cur.execute("SELECT DISTINCT SUBSTRING(c.document_number, 1, 4) FROM claim c")

        rows = cur.fetchall()

    for row in rows:
        year = row[0]

        engine = create_engine(
            'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

        engine.execute(
            "insert into claim_{} select * from claim c where substring(c.document_number, 1, 4) = '{}';".format(year,
                                                                                                                 year))


def yearly_brf_sum_text(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    con = pymysql.connect(host, user, password, database)

    with con:
        cur = con.cursor()
        cur.execute("SELECT DISTINCT SUBSTRING(b.document_number, 1, 4) FROM brf_sum_text b")

        rows = cur.fetchall()

    for row in rows:
        year = row[0]

        engine = create_engine(
            'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

        engine.execute(
            "insert into brf_sum_text_{} select * from brf_sum_text b where substring(b.document_number, 1, 4) = '{}';".format(
                year, year))


def yearly_draw_desc_text(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    con = pymysql.connect(host, user, password, database)

    with con:
        cur = con.cursor()
        cur.execute("SELECT DISTINCT SUBSTRING(d.document_number, 1, 4) FROM draw_desc_text d")

        rows = cur.fetchall()

    for row in rows:
        year = row[0]

        engine = create_engine(
            'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

        engine.execute(
            "insert into draw_desc_text_{} select * from draw_desc_text d where substring(d.document_number, 1, 4) = '{}';".format(
                year, year))


def yearly_detail_desc_text(config):
    database = '{}'.format(config['DATABASE']['TEMP_DATABASE'])
    host = '{}'.format(config['DATABASE']['HOST'])
    user = '{}'.format(config['DATABASE']['USERNAME'])
    password = '{}'.format(config['DATABASE']['PASSWORD'])
    port = '{}'.format(config['DATABASE']['PORT'])

    con = pymysql.connect(host, user, password, database)

    with con:
        cur = con.cursor()
        cur.execute("SELECT DISTINCT SUBSTRING(d.document_number, 1, 4) FROM detail_desc_text d")

        rows = cur.fetchall()

    for row in rows:
        year = row[0]

        engine = create_engine(
            'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

        engine.execute(
            "insert into detail_desc_text_{} select * from detail_desc_text d where substring(d.document_number, 1, 4) = '{}';".format(
                year, year))


def begin_post_processing(**kwargs):
    config = update_config_date(**kwargs)
    consolidate_rawlocation(config)
    consolidate_cpc(config)
    detail_desc_length(config)
    consolidate_uspc(config)
    pct_data_doc_type(config)
    consolidate_claim(config)
    consolidate_usreldoc(config)
    yearly_claim(config)
    yearly_brf_sum_text(config)
    yearly_draw_desc_text(config)
    yearly_detail_desc_text(config)


def post_upload_database(**kwargs):
    config = update_config_date(**kwargs)
    qc = AppUploadTest(config)
    qc.runTests()


if __name__ == "__main__":
    config = get_config('application')
    consolidate_rawlocation(config)
    consolidate_cpc(config)
    detail_desc_length(config)
    consolidate_uspc(config)
    pct_data_doc_type(config)
    consolidate_claim(config)
    consolidate_usreldoc(config)
    yearly_claim(config)
    yearly_brf_sum_text(config)
    yearly_draw_desc_text(config)
    yearly_detail_desc_text(config)
