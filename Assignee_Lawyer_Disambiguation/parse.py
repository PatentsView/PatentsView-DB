#!/usr/bin/env python
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
"""
@author Gabe Fierro gt.fierro@berkeley.edu github.com/gtfierro
"""
"""
Parses USPTO released XML dumps of granted patents and published applications
according to the handlers in lib/handlers/ and inserts the parsed records
into the database indicated by lib/alchemy/config.ini.

This file can be called directly `python parse.py <flags>` or can be configured
by process.cfg and called by `python start.py process.cfg`. See process.cfg
for explanation of configuration options
"""

import logging
import os
import datetime
import re
import sys
import lib.argconfig_parse as argconfig_parse
import lib.alchemy as alchemy
import shutil
from lib.config_parser import get_xml_handlers
from lib.alchemy.match import commit_inserts, commit_updates

logfile = "./" + 'xml-parsing.log'
logging.basicConfig(filename=logfile, level=logging.DEBUG)
commit_frequency = alchemy.get_config().get('parse').get('commit_frequency')


def list_files(patentroot, xmlregex):
    """
    Returns listing of all files within patentroot
    whose filenames match xmlregex
    """
    files = [patentroot+'/'+fi for fi in os.listdir(patentroot)
             if re.search(xmlregex, fi, re.I) is not None]
    if not files:
        logging.error("No files matching {0} found in {1}".format(xmlregex, patentroot))
        #sys.exit(1)
    return files


def _get_date(filename, dateformat='ipg%y%m%d.xml'):
    """
    Given a [filename], returns the expanded year.
    The optional [dateformat] argument allows for different file formats
    """
    filename = re.search(r'ip[ag]\d{6}', filename) or re.search(r'p[ag]\d{6}', filename)
    if not filename:
        return 'default'
    filename = filename.group() + '.xml'
    dateobj = datetime.datetime.strptime(filename.replace('ipa', 'ipg').replace('pa', 'ipg'), dateformat)
    return int(dateobj.strftime('%Y%m%d'))  # returns YYYYMMDD


def _get_parser(date, doctype='grant'):
    """
    Given a [date], returns the class of parser needed
    to parse it
    """
    xmlhandlers = get_xml_handlers('process.cfg', doctype)
    for daterange in xmlhandlers.iterkeys():
        if daterange[0] <= date <= daterange[1]:
            return xmlhandlers[daterange]
    return xmlhandlers['default']


def extract_xml_strings(filename):
    """
    Given a string [filename], opens the file and returns a generator
    that yields tuples. A tuple is of format (year, xmldoc string). A tuple
    is returned for every valid XML doc in [filename]
    """
    # search for terminating XML tag
    endtag_regex = re.compile('^<!DOCTYPE (.*) SYSTEM')
    endtag = ''
    with open(filename, 'r') as f:
        doc = ''  # (re)initialize current XML doc to empty string
        for line in f:
            doc += line
            endtag = endtag_regex.findall(line) if not endtag else endtag
            if not endtag:
                continue
            terminate = re.compile('^</{0}>'.format(endtag[0]))
            if terminate.findall(line):
                yield (_get_date(filename), doc)
                endtag = ''
                doc = ''


def parse_files(filelist, doctype='grant'):
    """
    Takes in a list of patent file names (from __main__() and start.py) and commits
    them to the database. This method is designed to be used sequentially to
    account for db concurrency.  The optional argument `commit_frequency`
    determines the frequency with which we commit the objects to the database.
    If set to 0, it will commit after all patobjects have been added.  Setting
    `commit_frequency` to be low (but not 0) is helpful for low memory machines.
    """
    if not filelist:
        return
    commit = alchemy.commit
    for filename in filelist:
        print filename
        for i, xmltuple in enumerate(extract_xml_strings(filename)):
            patobj = parse_patent(xmltuple, filename, doctype)
            if doctype == 'grant':
                alchemy.add_grant(patobj)
                commit = alchemy.commit
            else:
                alchemy.add_application(patobj)
                commit = alchemy.commit_application
            if commit_frequency and ((i+1) % commit_frequency == 0):
                commit()
                logging.info("{0} - {1} - {2}".format(filename, (i+1), datetime.datetime.now()))
                print " *", (i+1), datetime.datetime.now()
            
        commit()
        print " *", "Complete", datetime.datetime.now()


def parse_patent(xmltuple, filename, doctype='grant'):
    """
    Parses an xml string given as [xmltuple] with the appropriate parser (given
    by the first part of the tuple). Returns list of objects
    to be inserted into the database using SQLAlchemy
    """
    if not xmltuple:
        return
    try:
        date, xml = xmltuple  # extract out the parts of the tuple
        patent = _get_parser(date, doctype).Patent(xml, filename, True)
    except Exception as inst:
        logging.error(inst)
        logging.error("  - Error parsing patent: %s" % (xml[:400]))
        return
    del xmltuple
    return patent.get_patobj()


def move_tables(output_directory):
    """
    Moves the output sqlite3 files to the output directory
    """
    if output_directory == ".":
        return
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    dbtype = alchemy.config.get('global', 'database')
    dbfile = alchemy.config.get(dbtype, 'database')
    try:
        shutil.move(dbfile,
                    '{0}/{1}'.format(output_directory, dbfile))
    except:
        print 'Database file {0} does not exist'.format(dbfile)

def mark_granted():
    from lib.tasks import bulk_commit_updates
    grantsessiongen = alchemy.session_generator(dbtype='grant')
    appsessiongen = alchemy.session_generator(dbtype='application')
    grantsession = grantsessiongen()
    granted_apps = [{'pk': x.id, 'update': 1} for x in list(grantsessiongen.query(alchemy.schema.Application))]

    appsession = appsessiongen()
    bulk_commit_updates('granted', granted_apps, alchemy.schema.App_Application.__table__, alchemy.is_mysql(), 20000, 'application')

def main(patentroot, xmlregex, verbosity, output_directory='.', doctype='grant'):
    logfile = "./" + 'xml-parsing.log'
    logging.basicConfig(filename=logfile, level=verbosity)

    logging.info("Starting parse on {0} on directory {1}".format(str(datetime.datetime.today()), patentroot))
    files = list_files(patentroot, xmlregex)

    logging.info("Found all files matching {0} in directory {1}".format(xmlregex, patentroot))
    parse_files(files, doctype)
    move_tables(output_directory)

    logging.info("SQL tables moved to {0}".format(output_directory))
    logging.info("Parse completed at {0}".format(str(datetime.datetime.today())))

    logging.info("Marking granted applications")
    mark_granted()


if __name__ == '__main__':
    args = argconfig_parse.ArgHandler(sys.argv[1:])

    XMLREGEX = args.get_xmlregex()
    PATENTROOT = args.get_patentroot()
    VERBOSITY = args.get_verbosity()
    PATENTOUTPUTDIR = args.get_output_directory()
    DOCUMENTTYPE = args.get_document_type()

    main(PATENTROOT, XMLREGEX, VERBOSITY, PATENTOUTPUTDIR, DOCUMENTTYPE)
