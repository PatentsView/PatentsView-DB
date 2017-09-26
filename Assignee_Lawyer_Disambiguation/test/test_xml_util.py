#!/usr/bin/env python

import os
import sys
import re
import unittest
from cgi import escape as html_escape

sys.path.append('../lib/handlers/')
import xml_util

text_fixtures = os.path.join(os.curdir, 'fixtures/text/')
with open(text_fixtures+'accented_characters.txt') as f:
    accent_file = f.read()

class Test_xml_util(unittest.TestCase):

    def setUp(self):
        pass

    def test_flatten(self):
        testlist = [ [1,4,7], [2,5,8], [3,6,9] ]
        reslist = xml_util.flatten(testlist)
        goallist = [ [1,2,3], [4,5,6], [7,8,9] ]
        self.assertTrue(reslist == goallist, \
            "{0}\nshould be\n{1}".format(reslist,goallist))

    def test_extend_padding(self):
        testlist = [ [1,2,3], [4,5], [5,6,7,8] ]
        reslist = xml_util.extend_padding(testlist,0)
        goallist = [ [1,2,3,0], [4,5,0,0], [5,6,7,8] ]
        self.assertTrue(reslist == goallist, \
            "{0}\nshould be\n{1}".format(reslist,goallist))

    def test_extend_padding_string(self):
        testlist = [ ['a','b','c'], ['d'] ]
        reslist = xml_util.extend_padding(testlist)
        goallist = [ ['a','b','c'], ['d','',''] ]
        self.assertTrue(reslist == goallist, \
            "{0}\nshould be\n{1}".format(reslist,goallist))

    def test_flatten_with_extend(self):
        testlist = [ [1,4,7], [2,5,8], [3,6] ]
        testlist = xml_util.extend_padding(testlist,0)
        reslist = xml_util.flatten(testlist)
        goallist = [ [1,2,3], [4,5,6], [7,8,0] ]
        self.assertTrue(reslist == goallist, \
            "{0}\nshould be\n{1}".format(reslist,goallist))

    def test_flatten_with_extend_multiple(self):
        testlist = [ [1,4,7], [2], [3,6] ]
        testlist = xml_util.extend_padding(testlist,0)
        reslist = xml_util.flatten(testlist)
        goallist = [ [1,2,3], [4,0,6], [7,0,0] ]
        self.assertTrue(reslist == goallist, \
            "{0}\nshould be\n{1}".format(reslist,goallist))

    def test_escape_html_nosub(self):
        teststring = "<tag1> ampersand here: & </tag1>"
        resstring = xml_util.escape_html_nosub(teststring)
        goalstring = html_escape(teststring)
        self.assertTrue(resstring == goalstring, \
            "{0}\nshould be\n{1}".format(resstring,goalstring))

    def test_escape_html_nosub2(self):
        substart = "<sub>"
        subend = "</sub>"
        teststring = "<escape & skip sub tags>"
        resstring = xml_util.escape_html_nosub(substart+teststring+subend)
        goalstring = substart+html_escape(teststring)+subend
        self.assertTrue(resstring == goalstring, \
            "{0}\nshould be\n{1}".format(resstring,goalstring))

    def test_normalize_utf8_defaultstring(self):
        # this is a PYTHON DEFAULT string consisting of the characters supported by unicode
        teststring_normal = """!@#$%^&*()_+-=QWERTYqwerty<>,.:";'?/{}[]|\\"""
        resstring = xml_util.normalize_utf8(teststring_normal)
        self.assertTrue(teststring_normal == resstring, \
            "{0}\nshould be\n{1}".format(resstring, teststring_normal))

    def test_normalize_utf8_unicodestring(self):
        # this is a UNICODE string consisting of the characters supported by unicode
        teststring_normal = unicode("""!@#$%^&*()_+-=QWERTYqwerty<>,.:";'?/{}[]|\\""")
        resstring = xml_util.normalize_utf8(teststring_normal)
        self.assertTrue(teststring_normal == resstring, \
            "{0}\nshould be\n{1}".format(resstring, teststring_normal))

    def test_normalize_utf8_withaccents(self):
        teststring = accent_file
        resstring = xml_util.normalize_utf8(teststring)
        self.assertTrue(teststring.decode('utf-8') == resstring, resstring)

    def test_remove_escape_squences(self):
        teststring = "\r\n\a\tasdf"
        resstring = xml_util.remove_escape_sequences(teststring)
        self.assertTrue(resstring == " asdf", \
            "{0}\nshould be\n{1}".format(resstring," asdf"))

    def test_remove_escape_squences_raw(self):
        teststring = r"\r\n\a\tasdf"
        resstring = xml_util.remove_escape_sequences(teststring)
        self.assertTrue(resstring == teststring, \
            "{0}\nshould be\n{1}".format(resstring,teststring))

    def test_remove_escape_squences_space(self):
        teststring = "\r \n\a\tas \ndf\f"
        resstring = xml_util.remove_escape_sequences(teststring)
        self.assertTrue(resstring == " as df ", \
            "{0}\nshould be\n{1}".format(resstring," as df "))

    def test_translate_underscore_same(self):
        teststring = "<sub>&&&&&&&&</sub>"
        resstring = xml_util.translate_underscore(teststring)
        self.assertTrue(resstring == teststring, \
            "{0}\nshould be\n{1}".format(resstring,teststring))

    def test_translate_underscore(self):
        teststring = "<sub>&#x2014;</sub>"
        resstring = xml_util.translate_underscore(teststring)
        self.assertTrue(resstring == "_", \
            "{0}\nshould be\n{1}".format(resstring,"_"))

    def test_translate_underscore2(self):
        teststring = "<sub>-</sub>"
        resstring = xml_util.translate_underscore(teststring)
        self.assertTrue(resstring == "_", \
            "{0}\nshould be\n{1}".format(resstring,"_"))

    def test_escape_html(self):
        teststring = "<body>texthere</body>"
        resstring = xml_util.escape_html(teststring)
        self.assertTrue(resstring == "&lt;body&gt;texthere&lt;/body&gt;", \
            "{0}\nshould be\n{1}".format(resstring, "&lt;body&gt;texthere&lt;/body&gt;"))

    def test_escape_html_withsub(self):
        teststring = "<body>texthere <sub>&#x2014;</sub></body>"
        resstring = xml_util.escape_html(teststring)
        self.assertTrue(resstring == "&lt;body&gt;texthere _&lt;/body&gt;", \
            "{0}\nshould be\n{1}".format(resstring, "&lt;body&gt;texthere _&lt;/body&gt;"))

    def test_normalize_document_identifier(self):
        teststring = "US05260728"
        resstring = xml_util.normalize_document_identifier(teststring)
        self.assertTrue(resstring == "US5260728",resstring)
        teststring = "USD05260728"
        resstring = xml_util.normalize_document_identifier(teststring)
        self.assertTrue(resstring == "USD5260728")

    def test_normalize_document_identifier2(self):
        teststring = "US5260728"
        resstring = xml_util.normalize_document_identifier(teststring)
        self.assertTrue(resstring == "US5260728")
        teststring = "USD5260728"
        resstring = xml_util.normalize_document_identifier(teststring)
        self.assertTrue(resstring == "USD5260728")

    def test_normalize_document_identifier3(self):
        teststring = "D0123456"
        resstring = xml_util.normalize_document_identifier(teststring)
        self.assertTrue(resstring == "D123456",resstring)
        teststring = "D123456"
        resstring = xml_util.normalize_document_identifier(teststring)
        self.assertTrue(resstring == "D123456",resstring)

    def test_associate_prefix_1(self):
        firstname = "Troy Van Der"
        lastname = "Whol"
        newfirst, newlast = xml_util.associate_prefix(firstname, lastname)
        self.assertTrue(newfirst == "Troy" and newlast == "Van Der Whol", \
            "{0}\nshould be\n{1}\nand\n{2}\nshould be\n{3}".format\
            (newfirst, "Troy", newlast, "Van Der Whol"))

    def test_associate_prefix_2(self):
        firstname = "Troy"
        lastname = "Van Der Whol"
        newfirst, newlast = xml_util.associate_prefix(firstname, lastname)
        self.assertTrue(newfirst == "Troy" and newlast == "Van Der Whol", \
            "{0}\nshould be\n{1}\nand\n{2}\nshould be\n{3}".format\
            (newfirst, "Troy", newlast, "Van Der Whol"))

unittest.main()
