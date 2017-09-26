#!/usr/bin/env python

import os
import sys
import unittest
import logging
import re
from collections import Iterable

sys.path.append('../')
import parse
import lib.handlers.grant_handler_v42 as grant_handler_v42

basedir = os.path.dirname(__file__)
testdir = os.path.join(basedir, './fixtures/xml/')
testfileone = 'ipg120327.one.xml'
testfiletwo = 'ipg120327.two.xml'
regex = re.compile(r"""([<][?]xml version.*?[>]\s*[<][!]DOCTYPE\s+([A-Za-z-]+)\s+.*?/\2[>])""", re.S+re.I)

class TestParseFile(unittest.TestCase):
    
    def setUp(self):
        pass

    def test_extract_xml_strings_one(self):
        parsed_output = parse.extract_xml_strings(testdir+testfileone)
        self.assertTrue(isinstance(parsed_output, list))
        self.assertTrue(len(parsed_output) == 1)
        self.assertTrue(isinstance(parsed_output[0], tuple))
        self.assertTrue(isinstance(parsed_output[0][1], str))
        self.assertTrue(regex.match(parsed_output[0][1]))

    def test_parse_files_one(self):
        filelist = [testdir+testfileone]
        parsed_output = parse.parse_files(filelist)
        self.assertTrue(isinstance(parsed_output,Iterable))
        parsed_output = list(parsed_output)
        self.assertTrue(len(parsed_output) == 1)
        self.assertTrue(isinstance(parsed_output[0], tuple))
        self.assertTrue(isinstance(parsed_output[0][1], str))
        self.assertTrue(regex.match(parsed_output[0][1]))

    def test_extract_xml_strings_two(self):
        parsed_output = parse.extract_xml_strings(testdir+testfiletwo)
        self.assertTrue(isinstance(parsed_output, Iterable))
        parsed_output = list(parsed_output)
        self.assertTrue(len(parsed_output) == 2)
        self.assertTrue(isinstance(parsed_output[0], tuple))
        self.assertTrue(isinstance(parsed_output[0][1], str))
        self.assertTrue(isinstance(parsed_output[1], tuple))
        self.assertTrue(isinstance(parsed_output[1][1], str))
        self.assertTrue(regex.match(parsed_output[0][1]))
        self.assertTrue(regex.match(parsed_output[1][1]))

    def test_parse_files_two(self):
        filelist = [testdir+testfiletwo]
        parsed_output = parse.parse_files(filelist)
        self.assertTrue(isinstance(parsed_output,Iterable))
        parsed_output = list(parsed_output)
        self.assertTrue(len(parsed_output) == 2)
        self.assertTrue(isinstance(parsed_output[0], tuple))
        self.assertTrue(isinstance(parsed_output[0][1], str))
        self.assertTrue(isinstance(parsed_output[1], tuple))
        self.assertTrue(isinstance(parsed_output[1][1], str))
        self.assertTrue(regex.match(parsed_output[0][1]))
        self.assertTrue(regex.match(parsed_output[1][1]))
    
    def test_use_parse_files_one(self):
        filelist = [testdir+testfileone]
        parsed_output = list(parse.parse_files(filelist))
        patobj = grant_handler_v42.PatentGrant(parsed_output[0][1], True)
        self.assertTrue(patobj)

    def test_use_parse_files_two(self):
        filelist = [testdir+testfiletwo]
        parsed_output = parse.parse_files(filelist)
        parsed_xml = []
        for us_patent_grant in parsed_output:
            self.assertTrue(isinstance(us_patent_grant, tuple))
            self.assertTrue(isinstance(us_patent_grant[1], str))
            patobj = grant_handler_v42.PatentGrant(us_patent_grant[1], True)
            self.assertTrue(patobj)
    
    def test_list_files(self):
        testdir = os.path.join(basedir, './fixtures/xml')
        xmlregex = r'ipg120327.one.xml'
        files = parse.list_files(testdir, xmlregex)
        self.assertTrue(isinstance(files, list))
        self.assertTrue(len(files) == 1)
        self.assertTrue(all(filter(lambda x: isinstance(x, str), files)))
        self.assertTrue(all(map(lambda x: os.path.exists(x), files)))

if __name__ == '__main__':
    unittest.main()
