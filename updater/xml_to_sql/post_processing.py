import datetime
import os
import json

import pymysql
from sqlalchemy import create_engine

from QA.text_parser.AppTest import AppUploadTest
from lib.configuration import get_connection_string, get_current_config
from lib import utilities
from updater.disambiguation.location_disambiguation.osm_location_match import create_location_match_table

def pct_data_doc_type(config):
    print('fixing pct doc types')
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    engine = create_engine(cstr)

    if config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'][:7] == 'pgpubs_':
        engine.execute('UPDATE pct_data SET kind = "00" WHERE `kind` IS NULL;') # for pgpubs this should generally be all records

    engine.execute('UPDATE pct_data SET doc_type = "pct_application" WHERE kind = "00";')
    engine.execute('UPDATE pct_data SET doc_type = "wo_grant" WHERE kind = "A";')


def consolidate_uspc(config):
    print('consolidating rawuspc into uspc')
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    engine = create_engine(cstr)
    engine.execute(
            "DELETE  FROM  rawuspc  WHERE  LENGTH(classification) < 3;")
    engine.execute(
            "INSERT IGNORE INTO uspc (id, document_number, mainclass_id, subclass_id, sequence, filename, version_indicator) SELECT id, document_number, TRIM(SUBSTRING(classification,1,3)), TRIM(CONCAT(SUBSTRING(classification,1,3), '/', TRIM(SUBSTRING(classification,4, LENGTH(classification))))), sequence, filename, version_indicator FROM rawuspc;")


def consolidate_rawlocation(config):
    print('consolidating rawlocations from inventors, assignees, and applicants')
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    applicant_table = 'non_inventor_applicant' if config['PATENTSVIEW_DATABASES']['PROD_DB'] == 'patent' else 'us_parties'
    engine = create_engine(cstr)
    engine.execute(
            'INSERT IGNORE INTO rawlocation (id, city, state, country, filename, version_indicator) SELECT rawlocation_id, city, state, country, filename, version_indicator FROM rawassignee;')
    engine.execute(
            'INSERT IGNORE INTO rawlocation (id, city, state, country, filename, version_indicator) SELECT rawlocation_id, city, state, country, filename, version_indicator FROM rawinventor;')
    engine.execute(
            f'INSERT IGNORE INTO rawlocation (id, city, state, country, filename, version_indicator) SELECT rawlocation_id, city, state, country, filename, version_indicator FROM {applicant_table};')


def create_country_transformed(config):
    print('creating country_transformed')
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    engine = create_engine(cstr)
    engine.execute(
        "UPDATE rawlocation SET country_transformed = SUBSTRING(country, 1, 2) WHERE country != 'unknown' AND country != 'omitted';")



def consolidate_cpc(config):
    print('consolidating cpc main and further into cpc')
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    engine = create_engine(cstr)
    engine.execute(
            "INSERT IGNORE INTO cpc (id,document_number,sequence,version,section_id,subsection_id,group_id,subgroup_id,symbol_position,`value`,action_date,filename,version_indicator,created_date,updated_date) SELECT id,document_number,sequence,version,section as section_id,concat(section, class) as subsection_id,concat(section, class, subclass) as group_id,concat(section, class, subclass, main_group, '/', subgroup) as subgroup_id,symbol_position,`value`,action_date,filename,version_indicator,created_date,updated_date from main_cpc;")
    engine.execute(
            "INSERT IGNORE INTO cpc (id,document_number,sequence,version,section_id,subsection_id,group_id,subgroup_id,symbol_position,`value`,action_date,filename,version_indicator,created_date,updated_date) SELECT id,document_number,(sequence+1),version,section as section_id,concat(section, class) as subsection_id,concat(section, class, subclass) as group_id,concat(section, class, subclass, main_group, '/', subgroup) as subgroup_id,symbol_position,`value`,action_date,filename,version_indicator,created_date,updated_date from further_cpc;")
    engine.execute(
            "UPDATE cpc SET category = 'inventional' WHERE value = 'I';")
    engine.execute(
            "UPDATE cpc SET category = 'additional' WHERE value != 'I';")


def consolidate_usreldoc(config):
    print('consolidating usreldoc from parent_child, single, and related tables')
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    engine = create_engine(cstr)
    engine.execute(
            'DELETE FROM usreldoc') # should be empty before being populated below. In some instances of repeated post-processing, duplicates were created.
    engine.execute(
            'DELETE FROM usreldoc_single WHERE related_doc_number IS NULL;')
    engine.execute(
            'INSERT IGNORE INTO usreldoc SELECT * FROM usreldoc_parent_child;')
    engine.execute(
            'INSERT IGNORE INTO usreldoc SELECT * FROM usreldoc_single;')
    engine.execute(
            'INSERT IGNORE INTO usreldoc SELECT * FROM usreldoc_related;')


def consolidate_claim(config):
    print('cleaning claims dependent text')
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    engine = create_engine(cstr)
    engine.execute(
            "UPDATE claims SET dependent = replace(dependent, 'claim ', '');")

    engine.execute(
            "UPDATE claims SET dependent = replace(dependent, 'Claim ', '');")

    engine.execute(
            "UPDATE claims SET dependent = replace(dependent, 'claims ', '');")

    engine.execute(
            "UPDATE claims SET dependent = replace(dependent, 'Claims ', '');")

    engine.execute(
            "UPDATE claims SET dependent = concat('claim ', dependent) WHERE dependent NOT LIKE '%%,%%';")

    engine.execute(
            "UPDATE claims SET dependent = concat('claims ', dependent) WHERE dependent LIKE '%%,%%';")


def detail_desc_length(config):
    print('measuring detail description text length')
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    engine = create_engine(cstr)
    engine.execute(
            'UPDATE detail_desc_text d SET `description_length` = LENGTH(d.`description_text`);')


def yearly_claim(config):
    print('migrating claims to yearly tables')
    database = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    host = '{}'.format(config['DATABASE_SETUP']['HOST'])
    user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
    password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
    port = '{}'.format(config['DATABASE_SETUP']['PORT'])

    con = pymysql.connect(host=host, user=user, password=password, database=database)

    with con.cursor() as cur:
        cur.execute("SELECT DISTINCT SUBSTRING(c.pgpub_id, 1, 4) FROM claims c")

        rows = cur.fetchall()

    for row in rows:
        year = row[0]

        engine = create_engine(
                'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

        engine.execute(
                "insert ignore into claims_{} select * from claims c where substring(c.pgpub_id, 1, 4) = '{}';".format(
                        year,
                        year))


def yearly_brf_sum_text(config):
    print('migrating brief summary texts to yearly tables')
    database = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    host = '{}'.format(config['DATABASE_SETUP']['HOST'])
    user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
    password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
    port = '{}'.format(config['DATABASE_SETUP']['PORT'])

    con = pymysql.connect(host=host, user=user, password=password, database=database)

    with con.cursor() as cur:
        cur.execute("SELECT DISTINCT SUBSTRING(b.pgpub_id, 1, 4) FROM brf_sum_text b")

        rows = cur.fetchall()

    for row in rows:
        year = row[0]

        engine = create_engine(
                'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

        engine.execute(
                "insert ignore into brf_sum_text_{} select * from brf_sum_text b where substring(b.pgpub_id, 1, 4) = '{}';".format(
                        year, year))


def yearly_draw_desc_text(config):
    print('migrating drawing descriptions to yearly tables')
    database = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    host = '{}'.format(config['DATABASE_SETUP']['HOST'])
    user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
    password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
    port = '{}'.format(config['DATABASE_SETUP']['PORT'])

    con = pymysql.connect(host=host, user=user, password=password, database=database)

    with con.cursor() as cur:
        cur.execute("SELECT DISTINCT SUBSTRING(d.pgpub_id, 1, 4) FROM draw_desc_text d")

        rows = cur.fetchall()

    for row in rows:
        year = row[0]

        engine = create_engine(
                'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

        engine.execute(
                "insert ignore into draw_desc_text_{} select * from draw_desc_text d where substring(d.pgpub_id, 1, 4) = '{}';".format(
                        year, year))


def yearly_detail_desc_text(config):
    print('migrating detail description texts to yearly tables')
    database = '{}'.format(config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB'])
    host = '{}'.format(config['DATABASE_SETUP']['HOST'])
    user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
    password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
    port = '{}'.format(config['DATABASE_SETUP']['PORT'])

    con = pymysql.connect(host=host, user=user, password=password, database=database)

    with con.cursor() as cur:
        cur.execute("SELECT DISTINCT SUBSTRING(d.pgpub_id, 1, 4) FROM detail_desc_text d")

        rows = cur.fetchall()

    for row in rows:
        year = row[0]

        engine = create_engine(
                'mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database))

        engine.execute(
                "insert ignore into detail_desc_text_{} select * from detail_desc_text d where substring(d.pgpub_id, "
                "1, 4) = '{}';".format(
                        year, year))


def consolidate_granted_cpc(config):
    print('consolidating cpc from main and further cpc tables')
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    engine = create_engine(cstr)
    engine.execute(
            """
INSERT INTO cpc (uuid, patent_id, section_id, subsection_id, group_id, subgroup_id, category, action_date, sequence, symbol_position, version_indicator)
SELECT uuid,
       patent_id,
       section,
       concat(section, class),
       concat(section, class, subclass),
       concat(section, class, subclass, main_group, '/', subgroup),
       IF(value = 'I', 'inventional', 'additional'),
       action_date,
       sequence,
       symbol_position,
       version_indicator
from main_cpc;""")
    engine.execute(
            """
INSERT INTO cpc (uuid, patent_id, section_id, subsection_id, group_id, subgroup_id, category, action_date, sequence, symbol_position, version_indicator)
SELECT uuid,
       patent_id,
       section,
       concat(section, class),
       concat(section, class, subclass),
       concat(section, class, subclass, main_group, '/', subgroup),
       IF(value = 'I', 'inventional', 'additional'),
       action_date,
       sequence + 1,
       symbol_position,
       version_indicator
from further_cpc
WHERE NOT (
        section IS NULL
    AND class IS NULL
    AND subclass IS NULL
    AND main_group IS NULL
    AND subgroup IS NULL
    AND symbol_position IS NULL
);""")

def trim_rawassignee(config):
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
    engine = create_engine(cstr)
    print("Removing NULLS from rawassignee")
    engine.execute(
        """DELETE FROM rawassignee WHERE
        (name_first IS NULL) AND
        (name_last IS NULL) AND
        (organization IS NULL);""")

def fix_rawassignee_wrong_org(config):
    cstr = get_connection_string(config, 'TEMP_UPLOAD_DB')
#     print(cstr)
    engine = create_engine(cstr)
    print("Fixing Wrong Organization Landing")
    # remove tables first in event of rerun.
    engine.execute("DROP TABLE IF EXISTS temp_rawassignee_org_fixes_nf;")
    engine.execute("DROP TABLE IF EXISTS temp_rawassignee_org_fixes_nl;")
    # First Name contains Organization
    engine.execute(
        """
create table temp_rawassignee_org_fixes_nf (
SELECT * 
FROM rawassignee 
	where name_first is not null and name_last is null
)        
        """)
    engine.execute(
        """
update rawassignee 
set organization=name_first, type = (CASE WHEN country = 'US' THEN 2 ELSE 3 END)
where organization is null and id in (select id from temp_rawassignee_org_fixes_nf);
        """)
    engine.execute(
        """
update rawassignee 
set name_first = null
where id in (select id from temp_rawassignee_org_fixes_nf)  
        """)
    # Last Name contains Organization
    engine.execute(
        """
create table temp_rawassignee_org_fixes_nl (
SELECT * 
FROM rawassignee 
	where name_first is null and name_last is not null and name_last REGEXP 'inc|ltd|technologies|limited|corp|llc|co.'
)
        """)
    engine.execute(
        """
update rawassignee 
set organization=name_last, type = (CASE WHEN country = 'US' THEN 2 ELSE 3 END) 
where organization is null and id in (select id from temp_rawassignee_org_fixes_nl);
        """)
    engine.execute(
        """
update rawassignee 
set name_last = null
where id in (select id from temp_rawassignee_org_fixes_nl);
        """)
    engine.execute(
        """
insert into patent.pv_data_change_log (db, t_name, unique_key_name, unique_key_value, lookup_key_name, lookup_key_value, c_name, original_column_value, new_column_value, version_indicator, pipeline_changed)
select 'pgpubs', 'rawassignee', 'id', id, 'document_number', document_number, 'organization', null, name_first, version_indicator, 'weekly_pgpubs_parser'
from temp_rawassignee_org_fixes_nf;
        """)
    engine.execute(
        """
insert into patent.pv_data_change_log (db, t_name, unique_key_name, unique_key_value, lookup_key_name, lookup_key_value, c_name, original_column_value, new_column_value, version_indicator, pipeline_changed)
select 'pgpubs', 'rawassignee', 'id', id, 'document_number', document_number, 'organization', null, name_last, version_indicator, 'weekly_pgpubs_parser'
from temp_rawassignee_org_fixes_nl;
        """)
    fixcheck = engine.execute(
            """
SELECT COUNT(*) FROM rawassignee
WHERE (name_first IS NULL
AND name_last IS NOT NULL
AND name_last REGEXP 'inc|ltd|technologies|limited|corp|llc|co.')
OR (name_first IS NOT NULL 
AND name_last IS NULL)
            """
    )
    missedcount = fixcheck.first()[0]
    if missedcount > 0:
        raise Exception(
                f"{missedcount} entries with only one name remain after adjustment"
        )
# Make sure that the new table patent.pv_data_change_log works and then we can enable dropping the temp tables
#     engine.execute(
#         """
# drop table temp_rawassignee_org_fixes_nf;
# drop table temp_rawassignee_org_fixes_nl;
#         """)


def begin_post_processing(**kwargs):
    config = get_current_config(type='pgpubs', **kwargs)
    utilities.trim_whitespace(config)
    trim_rawassignee(config)
    fix_rawassignee_wrong_org(config)
    consolidate_rawlocation(config)
    create_country_transformed(config)
    create_location_match_table(config)
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
    config = get_current_config(**kwargs)
    qc = AppUploadTest(config)
    qc.runTests()


if __name__ == "__main__":
    begin_post_processing(**{
            "execution_date": datetime.date(2022, 1, 13)
            })
