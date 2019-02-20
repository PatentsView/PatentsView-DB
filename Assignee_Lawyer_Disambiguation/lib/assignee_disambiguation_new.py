from collections import defaultdict, deque
import uuid
from string import lowercase as alphabet
import re
import md5
import cPickle as pickle
import alchemy
from collections import Counter
from Levenshtein import jaro_winkler
from alchemy import get_config, match
from alchemy.schema import *
from alchemy.match import commit_inserts, commit_updates
from handlers.xml_util import normalize_utf8
from datetime import datetime
from sqlalchemy.sql import or_
from sqlalchemy.sql.expression import bindparam
from unidecode import unidecode
from tasks import bulk_commit_inserts, bulk_commit_updates
import multiprocessing
import itertools
import sys
import json

config = get_config()

THRESHOLD = config.get("assignee").get("threshold")

uuid_to_object = {}
uuid_to_cleanid = {}
letter_to_cleanid = {}
uuids_by_cleanidletter = defaultdict(list)

grant_uuids = set()
app_uuids = set()
grantsessiongen = alchemy.session_generator(dbtype='grant')
appsessiongen = alchemy.session_generator(dbtype='application')

nodigits = re.compile(r'[a-z ]')
stoplist = ['the','of','and','a','an','at']
substitutions = json.load(open('nber_substitutions.json'))

def isgrant(obj):
    """
    returns True of obj is from Grant table, False if from App table
    """
    return hasattr(obj, 'patent')

def get_cleanid(obj):
    """
    Returns a cleaned string version of the object representation:

    if obj has an organization, uses that. If obj has first/last name,
    uses "firstname|lastname"

    Changes severything to lowercase and removes everything that isn't [a-z ]
    """
    cleanid = ''
    if obj.organization:
        cleanid = obj.organization
    else:
        try:
            cleanid = obj.name_first + obj.name_last
        except:
            cleanid = ''
    cleanid = cleanid.lower()
    cleanid = ' '.join(filter(lambda x:
                        x not in stoplist,
                        cleanid.split()))
    cleanid = ''.join(nodigits.findall(cleanid)).strip()
    for pair in substitutions:
        cleanid = cleanid.replace(pair[0], pair[1])
    return cleanid

def get_similarity(uuid1, uuid2):
    clean1 = uuid_to_cleanid[uuid1]
    clean2 = uuid_to_cleanid[uuid2]
    if clean1 == clean2: 
        return 1.0
    return jaro_winkler(clean1,clean2,0.0)

def disambiguate_letter(letter):
    groups = defaultdict(list)
    bucket = uuids_by_cleanidletter[letter]
    uuidsremaining = bucket[:]
    groupkeys = []
    print len(bucket),'raw assignees for letter:', letter
    i = 1
    while True:
        if not uuidsremaining:
            break
        i += 1
        uuid = uuidsremaining.pop()
        if i%10000 == 0:
            print i, datetime.now()
        matcheduuid = False
        for groupkey in groupkeys:
            if get_similarity(uuid, groupkey) >= THRESHOLD:
                groups[groupkey].append(uuid)
                matcheduuid = True
                break
        if matcheduuid: continue
        groups[uuid].append(uuid)
        groupkeys.append(uuid)
    return groups

def create_disambiguated_record_for_block(block):
    grant_assignee_inserts = []
    app_assignee_inserts = []
    patentassignee_inserts = []
    applicationassignee_inserts = []
    grant_rawassignee_updates = []
    app_rawassignee_updates = []
    ra_objs = [uuid_to_object[uuid] for uuid in block]
    # vote on the disambiguated assignee parameters
    if len(block)>3000:
        print len(block)
    freq = defaultdict(Counter)
    param = {}

    for ra in ra_objs:
        for k,v in ra.summarize.items():
            if not v:
                v = ''
            param[k] = v
    if not param.has_key('organization'):
        param['organization'] = ''
    if not param.has_key('type'):
        param['type'] = ''
    if not param.has_key('name_last'):
        param['name_last'] = ''
    if not param.has_key('name_first'):
        param['name_first'] = ''
    if param.has_key('type'):
        try:
            if not param['type'].isdigit():
                param['type'] = ''
        except:
            param['type'] = ''
    # create persistent identifier
    if param["organization"]:
        param["id"] = md5.md5(unidecode(param["organization"])).hexdigest()
    elif param["name_last"]:
        param["id"] = md5.md5(unidecode(param["name_last"]+param["name_first"])).hexdigest()
    else:
        param["id"] = md5.md5('').hexdigest()
    grant_assignee_inserts.append(param)
    app_assignee_inserts.append(param)
    # inserts for patent_assignee and appliation_assignee tables
    patents = filter(lambda x: x, map(lambda x: getattr(x,'patent_id',None), ra_objs))
    patentassignee_inserts.extend({'patent_id': x, 'assignee_id': param['id']} for x in patents)
    applications = filter(lambda x: x, map(lambda x: getattr(x,'application_id',None), ra_objs))
    applicationassignee_inserts.extend([{'application_id': x, 'assignee_id': param['id']} for x in applications])
    # update statements for rawassignee tables
    for ra in ra_objs:
        if isgrant(ra):
            grant_rawassignee_updates.append({'pk': ra.uuid, 'update': param['id']})
        else:
            app_rawassignee_updates.append({'pk': ra.uuid, 'update': param['id']})
    return grant_assignee_inserts, app_assignee_inserts, patentassignee_inserts, applicationassignee_inserts, grant_rawassignee_updates, app_rawassignee_updates

grtsesh = grantsessiongen()
#appsesh = appsessiongen()
print 'fetching raw assignees',datetime.now()
rawassignees = list(grtsesh.query(RawAssignee))
print len(rawassignees)
#rawassignees.extend(list(appsesh.query(App_RawAssignee)))
# clear the destination tables
if alchemy.is_mysql():
    grtsesh.execute('truncate assignee; truncate patent_assignee;')
    #appsesh.execute('truncate assignee; truncate application_assignee;')
else:
    grtsesh.execute('delete from assignee; delete from patent_assignee;')
    appsesh.execute('delete from assignee; delete from patent_assignee;')
print 'cleaning ids', datetime.now()

counter = 0
for ra in rawassignees:
    counter +=1
    if counter%1000000 == 0:
        print counter
    uuid_to_object[ra.uuid] = ra
    cleanid = get_cleanid(ra)
    uuid_to_cleanid[ra.uuid] = cleanid
    if not cleanid:
        continue
    firstletter = cleanid[0]
    uuids_by_cleanidletter[firstletter].append(ra.uuid)
print "cleaned ids", datetime.now()

grant_assignee_inserts = []
app_assignee_inserts = []
patentassignee_inserts = []
applicationassignee_inserts = []
grant_rawassignee_updates = []
app_rawassignee_updates = []

def run_letter(letter):
    allrecords = []
    print 'disambiguating','({0})'.format(letter),datetime.now()
    print datetime.now()
    lettergroup = disambiguate_letter(letter)
    allrecords.extend(lettergroup.values())
    print 'got',len(lettergroup),'records'
    print 'creating disambiguated records','({0})'.format(letter),datetime.now()   
    return allrecords
def map_disamb(allrecords):
    res = []
    print datetime.now()
    counter = 0
    for i in allrecords:
        counter +=1
        if counter%10000 ==0:
            print counter
        res.append(create_disambiguated_record_for_block(i))
    print datetime.now()
    return res
def post_process(res):
    print "Itertools"
    print datetime.now()
    mid = itertools.izip(*res)
    print "making lists"
    print datetime.now()
    grant_assignee_inserts.extend(list(itertools.chain.from_iterable(mid.next())))
    app_assignee_inserts.extend(list(itertools.chain.from_iterable(mid.next())))
    patentassignee_inserts.extend(list(itertools.chain.from_iterable(mid.next())))
    applicationassignee_inserts.extend(list(itertools.chain.from_iterable(mid.next())))
    grant_rawassignee_updates.extend(list(itertools.chain.from_iterable(mid.next())))
    app_rawassignee_updates.extend(list(itertools.chain.from_iterable(mid.next())))


for letter in alphabet:
    allrecords = run_letter(letter)
    print "Done disambiguating"
    processed = map_disamb(allrecords)
    post_process(processed)

bulk_commit_inserts(grant_assignee_inserts, Assignee.__table__, alchemy.is_mysql(), 10000, 'grant')
bulk_commit_inserts(patentassignee_inserts, patentassignee, alchemy.is_mysql(), 10000, 'grant')
print "May stick here after a while, use Scripts/Temporary/assignee_patch.py to fix"
bulk_commit_updates('assignee_id', grant_rawassignee_updates, RawAssignee.__table__, alchemy.is_mysql(), 10000, 'grant')
print "Done!"