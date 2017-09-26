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
Handles argument parsing for parse.py
"""

import sys
import os
import argparse
import logging

class ArgHandler(object):

    def __init__(self, arglist):
        self.arglist = arglist

        # setup argparse
        self.parser = argparse.ArgumentParser(description=\
                'Specify source directory/directories for xml files to be parsed')
        self.parser.add_argument('--patentroot','-p', type=str, nargs='?',
                default=os.environ['PATENTROOT'] \
                if os.environ.has_key('PATENTROOT') else '.',
                help='root directory of all patent files')
        self.parser.add_argument('--xmlregex','-x', type=str,
                nargs='?',
                help='regex used to match xml files in the PATENTROOT directory.\
                     Defaults to ipg\d{6}.xml')
        self.parser.add_argument('--verbosity', '-v', type = int,
                nargs='?', default=0,
                help='Set the level of verbosity for the computation. The higher the \
                verbosity level, the less restrictive the print policy. 0 (default) \
                = error, 1 = warning, 2 = info, 3 = debug')
        self.parser.add_argument('--output-directory', '-o', type=str, nargs='?',
                default=os.environ['PATENTOUTPUTDIR'] \
                if os.environ.has_key('PATENTOUTPUTDIR') else '.',
                help='Set the output directory for the resulting sqlite3 files. Defaults\
                     to the current directory "."')
        self.parser.add_argument('--document-type', '-d', type=str, nargs='?', 
                default='grant',
                help='Set the type of patent document to be parsed: grant (default) \
                or application')

        # parse arguments and assign values
        args = self.parser.parse_args(self.arglist)
        self.xmlregex = args.xmlregex
        self.patentroot = args.patentroot
        self.output_directory = args.output_directory
        self.document_type = args.document_type
        if self.xmlregex == None: # set defaults for xmlregex here depending on doctype
            if self.document_type == 'grant':
                self.xmlregex = r"ipg\d{6}.xml"
            else:
                self.xmlregex = r"i?pa\d{6}.xml"

        # adjust verbosity levels based on specified input
        logging_levels = {0: logging.ERROR,
                          1: logging.WARNING,
                          2: logging.INFO,
                          3: logging.DEBUG}
        self.verbosity = logging_levels[args.verbosity]

    def get_xmlregex(self):
        return self.xmlregex

    def get_patentroot(self):
        return self.patentroot

    def get_verbosity(self):
        return self.verbosity

    def get_output_directory(self):
        return self.output_directory

    def get_document_type(self):
        return self.document_type

    def get_help(self):
        self.parser.print_help()
        sys.exit(1)
