#!/usr/bin/env python

# `fwork.py` will probably be renamed to something a little
# more suggestive as to its purpose.

import unittest
import sys

sys.path.append( '.' )
sys.path.append( '../lib/' )

#import imp
#from yaml import load, dump

from  fwork import *

class TestFWork(unittest.TestCase):

    def removeFile(self, file):
        #delete a file if it exists
        if os.path.isfile(file):
            os.system("rm {file}".format(file=file))


    def setUp(self):
        self.foo = 'bar'

    def test_dummy(self):
        assert(1 == 1)

    def test_int(self):
        assert('1' == ascit('1'))

    def test_float(self):
        # Default strict=True removes periods.
        result = ascit('1.0', strict=False)
        assert('1.0' == result)

    def test_remove_period(self):
        assert('10' == ascit('1.0', strict=True))

    def test_retain_hyphen(self):
        assert('KIN-JOE' == ascit('KIN-JOE', strict=True))

#    def test_quickSQL(self):
#        import sqlite3
#	self.conn = sqlite3.connect(":memory:")
#	self.cursor = self.conn.cursor()
#	data = [ ["Unique_ID", "Name"], [1, 1], [2, 2], [3, 3], [4, 4] ]
#	quickSQL(self.cursor, data, table="test")
#	self.conn.close


    def test_get_ctypes(self):
        assert('VARCHAR' == get_ctypes("FOO"))
        assert('REAL'    == get_ctypes(4.2))
        assert('INTEGER' == get_ctypes(42))

    def get_quicksql_data(self):
	return [
		[u'UniqueID', u'Patent', u'Lastname', u'Firstname'],
		[u'1', u'0.8194655', u'PISTER', u'KRISTOPHER S J'],
		[u'1', u'0.8190055', u'PISTER', u'KRISTOPHER S J']
               ]

    def test_is_real(self):
	data = u'0.1234'
	assert(1 == is_real(data))
	data = u'01234'
	assert(0 == is_real(data))


    def get_typelist(self):
	#return [u'Patent REAL', u'Lastname INTEGER'] 
	return [u'Lastname INTEGER'] 

    def test_text_type(self):
	data = 'foo'
	assert(True == text_type(data))
	data = 123
	assert(False == text_type(data))
	data = 1.23
	assert(False == text_type(data))
	data = u'123'
	assert(True == text_type(data))
	data = '1.23'
	assert(True == text_type(data))

    def test_create_column_labels(self):
	assert(1 == 1)


    def test_have_schema_type(self):
	tl = self.get_typelist()
        assert(-1 == have_schema_type(tl, 'UNIQUEID'))
        #assert(3  == have_schema_type(tl, 'PATENT'))
        #assert(19 == have_schema_type(tl, 'LASTNAME'))
        assert(-1 == have_schema_type(tl, 'FIRSTNAME'))

    def test_quickSQL2(self):
        import sqlite3
	dbfilename = "fwork.sqlite3"
	self.removeFile(dbfilename)
	self.conn = sqlite3.connect(dbfilename)
	self.cursor = self.conn.cursor()
	data = self.get_quicksql_data()
	typelist = self.get_typelist()
	quickSQL2(self.cursor, data, table="test", header=True, typeList=typelist)
	self.conn.commit()
	self.cursor.close()

if __name__ == '__main__':
    unittest.main()
