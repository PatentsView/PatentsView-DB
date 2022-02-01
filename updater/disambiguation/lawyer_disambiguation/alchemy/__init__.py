"""
Copyright (c) 2013 The Regents of the University of California, AMERICAN INSTITUTES FOR RESEARCH
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
from lib.configuration import get_connection_string

"""
@author Gabe Fierro gt.fierro@berkeley.edu github.com/gtfierro
"""
"""
Helper functions for database-related functionality.
"""
import os
import re
import configparser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql import exists
from collections import defaultdict
from . import schema
from .match import *
import uuid

from sqlalchemy import exc
from sqlalchemy import event
from sqlalchemy.pool import Pool

from html.parser import HTMLParser
import os

project_home = os.environ['PACKAGE_HOME']

config = configparser.ConfigParser()
config.read(project_home + '/config.ini')

h = HTMLParser()


def unescape_html(x):
    return h.unescape(x)


import html.entities

_char = re.compile(r'&(\w+?);')

# Generate some extra HTML entities
defs = html.entities.entitydefs
defs['apos'] = "'"
# need to fix this to pull the database location from the config file

entities = open(project_home + '/persistent_files/htmlentities').read().split('\n')
for e in entities:
    try:
        first = re.sub('\s+|\"|;|&', '', e[3:15])
        second = re.sub('\s+|\"|;|&', '', e[15:24])
        define = re.search("(?<=\s\s\').*?$", e).group()
        defs[first] = define[:-1].encode('utf-8')
        defs[second] = define[:-1].encode('utf-8')
    except:
        pass


def _char_unescape(m, defs=defs):
    try:
        return defs[m.group(1)].encode('utf-8', 'ignore')
    except:
        return m.group()


def fixid(x):
    if 'id' in x:
        x['id'] = str(uuid.uuid4())
    elif 'uuid' in x:
        x['uuid'] = str(uuid.uuid4())
    return x


@event.listens_for(Pool, "checkout")
def ping_connection(dbapi_connection, connection_record, connection_proxy):
    """
    This keeps the database connection alive over long-running processes (like assignee and location disambiguations)
    """
    cursor = dbapi_connection.cursor()
    if not hasattr(cursor, 'MySQLError'):
        return
    try:
        # reset the connection settings
        cursor.execute("SELECT 1;")

        cursor.execute("set foreign_key_checks = 0; set unique_checks = 0;", multi=True)
    except:
        # raise DisconnectionError - pool will try
        # connecting again up to three times before raising.
        raise exc.DisconnectionError()
    cursor.close()


# def get_config(localfile=project_home+"/Development/config.ini", default_file=False):
#     """
#     This grabs a configuration file and converts it into
#     a dictionary.

#     The default filename is called config.ini
#     First we load the GLOBAL file, then we load a local file
#     """
#     if default_file:
#         openfile = "{0}/config.ini".format(os.path.dirname(os.path.realpath(__file__)))
#     else:
#         openfile = localfile
#     config = defaultdict(dict)
#     if os.path.isfile(openfile):
#         cfg = configparser.ConfigParser()
#         cfg.read(openfile)
#         for s in cfg.sections():
#             for k, v in cfg.items(s):
#                 dec = re.compile(r'^\d+(\.\d+)?$')
#                 if v in ("True", "False") or v.isdigit() or dec.match(v):
#                     v = eval(v)
#                 config[s][k] = v

#     # this enables us to load a local file
#     if default_file:
#         newconfig = get_config(localfile, default_file=False)
#         for section in newconfig:
#             for item in newconfig[section]:
#                 config[section][item] = newconfig[section][item]
#     return config

def session_generator(dbtype='grant'):
    """
    Read from ../project/Development/config.ini file and load appropriate database

    @dbtype: string indicating if we are fetching the session for
             the grant database or the application database
    session_generator will return an object that can be called
    to retrieve more sessions, e.g.
    sg = session_generator(dbtype='grant')
    session1 = sg()
    session2 = sg()
    etc.
    These sessions will be protected with the ping refresher above
    """
    # config = get_config()
    # echo = config.get('GLOBAL').get('echo')
    echo = True
    read_database = "RAW_DB"
    cstr = get_connection_string(config, read_database)
    engine = create_engine(cstr)

    #     engine = create_engine('mysql+mysqldb://{0}:{1}@{2}/{3}?charset=utf8mb4'.format(
    #         config.get('DATABASE').get('user'),
    #         config.get('DATABASE').get('password'),
    #         config.get('DATABASE').get('host'),
    #         config.get('DATABASE').get(read_database), echo=echo), pool_size=3, pool_recycle=3600, echo_pool=True)

    schema.GrantBase.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, _enable_transaction_accounting=False)
    return scoped_session(Session)


def fetch_session(dbtype='grant'):
    """
    Read from ../project/Development/config.ini file and load appropriate database

    @dbtype: string indicating if we are fetching the session for
             the grant database or the application database
    """
    # config = get_config()
    echo = True
    if dbtype == 'grant':  # this is here so we can port to work for applicaitons also
        read_database = "RAW_DB"
        cstr = get_connection_string(config, read_database)
        engine = create_engine(cstr)

    #     engine = create_engine('mysql+mysqldb://{0}:{1}@{2}/{3}?charset=utf8'.format(
    #         config['DATABASE']['USERNAME'],
    #         config['DATABASE']['PASSWORD'],
    #         config['DATABASE']['HOST'],
    #         config['DATABASE'][read_database], echo=echo))

    schema.GrantBase.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, _enable_transaction_accounting=False)
    session = Session()
    return session


def add_grant(obj, override=True, temp=False):
    """
    PatentGrant Object converting to tables via SQLAlchemy
    Necessary to convert dates to datetime because of SQLite (OK on MySQL)

    Case Sensitivity and Table Reflection
    MySQL has inconsistent support for case-sensitive identifier names,
    basing support on specific details of the underlying operating system.
    However, it has been observed that no matter what case sensitivity
    behavior is present, the names of tables in foreign key declarations
    are always received from the database as all-lower case, making it
    impossible to accurately reflect a schema where inter-related tables
    use mixed-case identifier names.

    Therefore it is strongly advised that table names be declared as all
    lower case both within SQLAlchemy as well as on the MySQL database
    itself, especially if database reflection features are to be used.
    """

    # if a patent exists, remove it so we can replace it
    (patent_exists,), = grantsession.query(exists().where(schema.Patent.number == obj.patent))
    # pat_query = grantsession.query(Patent).filter(Patent.number == obj.patent)
    # if pat_query.count():
    if patent_exists:
        if override:
            pat_query = grantsession.query(schema.Patent).filter(schema.Patent.id == obj.patent)
            grantsession.delete(pat_query.one())
        else:
            return
    if len(obj.pat["number"]) < 3:
        return

    pat = schema.Patent(**obj.pat)
    pat.application = schema.Application(**obj.app)
    # lots of abstracts seem to be missing. why?
    add_all_fields(obj, pat)

    grantsession.execute('set foreign_key_checks = 0;')
    grantsession.execute('set unique_checks = 0;')

    # grantsession.commit()

    grantsession.merge(pat)


def add_all_fields(obj, pat):
    add_asg(obj, pat)
    add_inv(obj, pat)
    add_law(obj, pat)
    add_usreldoc(obj, pat)
    add_classes(obj, pat)
    add_ipcr(obj, pat)
    add_citations(obj, pat)
    add_claims(obj, pat)
    add_current_classes(obj, pat)


def add_asg(obj, pat):
    for asg, loc in obj.assignee_list:
        asg = fixid(asg)
        asg['organization'] = unescape_html(asg['organization'])
        loc = fixid(loc)
        asg = schema.RawAssignee(**asg)
        loc = schema.RawLocation(**loc)
        grantsession.merge(loc)
        asg.rawlocation = loc
        pat.rawassignees.append(asg)


def add_inv(obj, pat):
    for inv, loc in obj.inventor_list:
        inv = fixid(inv)
        loc = fixid(loc)
        inv = schema.RawInventor(**inv)
        loc = schema.RawLocation(**loc)
        grantsession.merge(loc)
        inv.rawlocation = loc
        pat.rawinventors.append(inv)


def add_law(obj, pat):
    for law in obj.lawyer_list:
        law = fixid(law)
        law = schema.RawLawyer(**law)
        pat.rawlawyers.append(law)


def add_usreldoc(obj, pat):
    for usr in obj.us_relation_list:
        usr = fixid(usr)
        usr["rel_id"] = usr["number"]
        usr = schema.USRelDoc(**usr)
        pat.usreldocs.append(usr)


def add_classes(obj, pat):
    for uspc, mc, sc in obj.us_classifications:
        uspc = fixid(uspc)
        uspc = schema.USPC(**uspc)
        mc = schema.MainClass(**mc)
        sc = schema.SubClass(**sc)
        grantsession.merge(mc)
        grantsession.merge(sc)
        uspc.mainclass = mc
        uspc.subclass = sc
        pat.classes.append(uspc)


def add_current_classes(obj, pat):
    for uspc_current, mc, sc in obj.us_classifications:
        uspc_current = fixid(uspc_current)
        uspc_current = schema.USPC_current(**uspc_current)
        mc = schema.MainClass_current(**mc)
        sc = schema.SubClass_current(**sc)
        grantsession.merge(mc)
        grantsession.merge(sc)
        uspc_current.mainclass_current = mc
        uspc_current.subclass_current = sc
        pat.current_classes.append(uspc_current)


def add_ipcr(obj, pat):
    for ipc in obj.ipcr_classifications:
        ipc = schema.IPCR(**ipc)
        pat.ipcrs.append(ipc)


def add_citations(obj, pat):
    cits, refs = obj.citation_list
    for cit in cits:
        if cit['country'] == 'US':
            # granted patent doc number
            if re.match(r'^[A-Z]*\d+$', cit['number']):
                cit['citation_id'] = cit['number']
                cit = fixid(cit)
                cit = schema.USPatentCitation(**cit)
                pat.uspatentcitations.append(cit)
            # if not above, it's probably an application
            else:
                cit['application_id'] = cit['number']
                cit = fixid(cit)
                cit = schema.USApplicationCitation(**cit)
                pat.usapplicationcitations.append(cit)
        # if not US, then foreign citation
        else:
            cit = fixid(cit)
            cit = schema.ForeignCitation(**cit)
            pat.foreigncitations.append(cit)
    for ref in refs:
        ref = fixid(ref)
        ref = schema.OtherReference(**ref)
        pat.otherreferences.append(ref)


def add_claims(obj, pat):
    claims = obj.claims
    for claim in claims:
        claim = fixid(claim)
        claim['text'] = unescape_html(claim['text'])
        claim['text'] = _char.sub(_char_unescape, claim['text'])
        clm = schema.Claim(**claim)
        pat.claims.append(clm)


def commit():
    try:
        grantsession.commit()
    except Exception as e:
        grantsession.rollback()
        print(str(e))


grantsession = fetch_session(dbtype='grant')
session = grantsession  # default for clean and consolidate
