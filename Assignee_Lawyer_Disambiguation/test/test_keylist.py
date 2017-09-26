#!/usr/bin/env python

import unittest
import os
import sqlite3
import sys
sys.path.append( '../lib/' )
import SQLite

class TestSQLite(unittest.TestCase):

    def removeFile(self, fname):
        #delete a fname if it exists
        if os.path.isfile(fname):
            os.remove(fname)

    def createFile(self, fname, ftype=None, data="1,2,3"):
        #create a fname db, csv
        if fname.split(".")[-1] == "db" or ftype == "db":
            conn = sqlite3.connect(fname)
            c = conn.cursor()
            c.executescript(""" 
                CREATE TABLE test (a, B, c);
                CREATE TABLE main (d, E, f);
                INSERT INTO test VALUES ({data});
                INSERT INTO main VALUES ({data});
                CREATE INDEX idx ON test (a);
                CREATE INDEX idy ON test (a, b);
                """.format(data=data)) #"""
            conn.commit()
            c.close()
            conn = sqlite3.connect(fname)
        elif fname.split(".")[-1] == "csv" or ftype == "csv":
            os.system("echo '{data}' >> {fname}".\
                format(data=data, fname=fname))

    def setUp(self):
        self.removeFile("test.db")
        self.removeFile("test.csv")
        self.removeFile("test2.db")
        self.removeFile("test2.csv")
        # create a really basic dataset
        self.createFile(fname="test.db")
        self.s = SQLite.SQLite(db="test.db", tbl="test")
        self.createFile("test2.db")
        s = SQLite.SQLite("test2.db", tbl="test")
        self.s.attach(s)

    def tearDown(self):
        self.s.close()
        self.removeFile("test.db")
        self.removeFile("test.csv")
        self.removeFile("test2.db")
        self.removeFile("test2.csv")
        self.removeFile("errlog")

    def test_keyList(self):
        #key = self.s._keyList('foo', kwargs={'tbl': 'main'})
        #print "key from test: ", key
        #key = self.s._keyList('foo', kwargs={"keys": ['bar', 'baz'], 'tbl': 'main'})
        #print "key from test: ", key
        #key = self.s._keyList('foo', kwargs={"keys": 'bar', 'tbl': 'main'})
        #print "key from test: ", key
        #key = self.s._keyList('foo', keys={"bar": 'baz'})
        #print "key from test: ", key
        key = self.s._keyList('foo', keys={"bar",'baz'})
        print "key from test: ", key
        print "key[0] from test: ", key[0]

        self.assertEquals(1,1)


if __name__ == '__main__':
    unittest.main()

