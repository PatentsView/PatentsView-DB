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
Parses the process.cfg file
"""
import importlib
from ConfigParser import ConfigParser

defaults = {'parse': 'defaultparse',
            'clean': 'True',
            'consolidate': 'True',
            'datadir': '/data/patentdata/patents/2013',
            'grantregex': 'ipg\d{6}.xml',
            'applicationregex': 'ipa\d{6}.xml',
            'years': None,
            'downloaddir' : None}

def extract_process_options(handler, config_section):
    """
    Extracts the high level options from the [process] section
    of the configuration file. Returns a dictionary of the options
    """
    result = {}
    result['parse'] = handler.get('process','parse')
    result['clean'] = handler.get('process','clean') == 'True'
    result['consolidate'] = handler.get('process','consolidate') == 'True'
    result['doctype'] = handler.get(config_section,'doctype')
    return result

def extract_parse_options(handler, config_section):
    """
    Extracts the specific parsing options from the parse section
    as given by the [parse] config option in the [process] section
    """
    options = {}
    options['datadir'] = handler.get(config_section,'datadir')
    options['grantregex'] = handler.get(config_section,'grantregex')
    options['applicationregex'] = handler.get(config_section, 'applicationregex')
    options['years'] = handler.get(config_section,'years')
    options['downloaddir'] = handler.get(config_section,'downloaddir')
    if options['years'] and options['downloaddir']:
        options['datadir'] = options['downloaddir']
    return options

def get_config_options(configfile):
    """
    Takes in a filepath to a configuration file, returns
    two dicts representing the process and parse configuration options.
    See `process.cfg` for explanation of the optiosn
    """
    handler = ConfigParser(defaults)
    try:
        handler.read(configfile)
    except IOError:
        print('Error reading config file ' + configfile)
        exit()
    process_config = extract_process_options(handler, 'process')
    parse_config = extract_parse_options(handler, process_config['parse'])
    return process_config, parse_config

def get_dates(yearstring):
    """
    Given a [yearstring] of forms
    year1
    year1-year2
    year1,year2,year3
    year1-year2,year3-year4
    Creates tuples of dates
    """
    years = []
    for subset in yearstring.split(','):
        if subset == 'default':
            years.append('default')
            continue
        sublist = subset.split('-')
        # left-justify the strings with 0s to add support
        # for days and weeks in the date
        start = int(sublist[0].ljust(8,'0'))
        end = int(sublist[1].ljust(8,'0')) if len(sublist) > 1 else float('inf')
        years.append((start,end))
    return years


def get_xml_handlers(configfile, document_type='grant'):
    """
    Called by parse.py to generate a lookup dictionary for which parser should
    be used for a given file
    """
    handler = ConfigParser()
    handler.read(configfile)
    xmlhandlers = {}
    config_item = 'grant-xml-handlers' if document_type == 'grant' \
                   else 'application-xml-handlers'
    for yearrange, handler in handler.items(config_item):
        for year in get_dates(yearrange):
            try:
                xmlhandlers[year] = importlib.import_module(handler)
            except:
                importlib.sys.path.append('..')
                xmlhandlers[year] = importlib.import_module(handler)
    return xmlhandlers
