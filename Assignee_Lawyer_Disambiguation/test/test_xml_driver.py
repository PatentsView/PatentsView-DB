#!/usr/bin/env python

import os
import re
import sys
import unittest
from xml.sax import make_parser, handler
from cgi import escape as html_escape

sys.path.append('../lib/handlers/')
from xml_driver import XMLElement, XMLHandler

# Directory of test files
basedir = os.curdir
testdir = os.path.join(basedir, 'fixtures/xml/')

class Test_XMLElement_Basic(unittest.TestCase):
    
    def setUp(self):
        # setup basic.xml parser/handler
        xmlhandler = XMLHandler()
        parser = make_parser()
        parser.setContentHandler(xmlhandler)
        parser.setFeature(handler.feature_external_ges, False)
        parser.parse(testdir+'basic.xml')
        self.assertTrue(xmlhandler.root)
        self.root = xmlhandler.root

    def test_basic_xml_tag_counts(self):
        self.assertTrue(len(self.root.a) == 1)
        self.assertTrue(len(self.root.a.b) == 2)
        self.assertTrue(len(self.root.a.b.c) == 3)
        self.assertTrue(len(self.root.a.b.d) == 2)
        self.assertTrue(len(self.root.a.c) == 3)

    def test_basic_xml_tag_contents(self):
        self.assertTrue(self.root.a.b.c[0].get_content()  == 'HELLO', \
            "{0} should be {1}".format(self.root.a.b.c[0].get_content(), 'HELLO'))
        self.assertTrue(self.root.a.b.c[1].get_content()  == 'WORLD', \
            "{0} should be {1}".format(self.root.a.b.c[1].get_content(), 'WORLD'))
        self.assertTrue(self.root.a.b.c[2].get_content()  == '3', \
            "{0} should be {1}".format(self.root.a.b.c[2].get_content(), '3'))
        self.assertTrue(self.root.a.b.d[0].get_content()  == '1', \
            "{0} should be {1}".format(self.root.a.b.c[0].get_content(), '1'))
        self.assertTrue(self.root.a.b.d[1].get_content()  == '2', \
            "{0} should be {1}".format(self.root.a.b.c[1].get_content(), '2'))
    
    def test_basic_xml_contents_of(self):
        self.assertTrue(self.root.a.b.contents_of('c') == ['HELLO','WORLD','3'])
        self.assertTrue(self.root.a.b[0].contents_of('c') == ['HELLO','WORLD'])

unittest.main()
