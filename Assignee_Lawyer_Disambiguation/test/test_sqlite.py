#!/usr/bin/env python

import unittest
import os
import sqlite3
import sys
sys.path.append( '../lib/' )
import SQLite

class TestSQLite(unittest.TestCase):

    def removeFile(self, file):
        #delete a file if it exists
        try:
            os.remove(file)
        except OSError:
            pass

    def createFile(self, file, type=None, data="1,2,3"):
        #create a file db, csv
        if file.split(".")[-1] == "db" or type == "db":
            conn = sqlite3.connect(file)
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
            conn = sqlite3.connect(file)
        elif file.split(".")[-1] == "csv" or ftype == "csv":
            #print data
            f = open(file, 'a+')
            f.write(data)
            f.write('\n')   #Mimics the original behavior of echo, which was the original design of the program
            f.close()

    def setUp(self):
        self.removeFile("test.db")
        self.removeFile("test.csv")
        self.removeFile("test2.db")
        self.removeFile("test2.csv")
        # create a really basic dataset
        self.createFile(file="test.db")
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

    def test___init__(self):
        s = SQLite.SQLite()
        self.assertEqual("main", s.tbl)
        self.assertEqual(":memory:", s.path)
        self.assertEqual("test.db", self.s.path)
        self.assertEqual("test", self.s.tbl)
        self.assertFalse(self.s.output)

    def test_chgTbl(self):
        self.s.chgTbl("test2")
        self.assertEqual("test2", self.s.tbl)

    def test__dbAdd(self):
        s = SQLite.SQLite()
        self.assertEqual(s._dbAdd(), "main")
        self.assertEqual(s._dbAdd(db="db"), "db.main")
        self.assertEqual(s._dbAdd(tbl="temp"), "temp")
        self.assertEqual(s._dbAdd(db="db", tbl="temp"), "db.temp")

    def test_csvInput(self):
        self.createFile("test.csv")
        self.assertEqual([['1','2','3']],
            self.s.csvInput("test.csv"))
        self.assertEqual([['1','2','3']],
            [x for x in self.s.csvInput("test.csv", iter=True)])

        #test for different delimiters
        self.createFile("test2.csv", data="1|2|3")
        self.assertEqual([['1','2','3']],
            self.s.csvInput("test2.csv", delimiter="|"))
        self.removeFile("test2.csv")

    def test__getSelf(self):
        self.assertEquals(self.s._getSelf(tbl="foo"), "foo")
        self.assertEquals(self.s._getSelf(table="foo"), "foo")
        self.assertEquals(self.s._getSelf(db="db"), "db")
        self.assertItemsEqual(["db", "foo"],
            self.s._getSelf(db="db", table="foo"))
        self.assertEquals([None, "foo"],
            self.s._getSelf(field=["db", "table"], table="foo"))
        self.assertEquals("test", self.s._getSelf(field=["table"]))

    def test__sqlmasterLookup(self):
        self.assertIn('test',
            self.s._sqlmasterScan(var="tbl_name", type="table"))
        self.assertIn('test',
            self.s._sqlmasterScan(var="tbl_name", type="table", db="db"))
        self.assertIn('idx',
            self.s._sqlmasterScan(var="name", type="index", db="db"))

    def test_indexes(self):
        self.assertIn('idx', self.s.indexes())
        self.assertTrue(self.s.indexes(lookup="idx"))
        self.assertFalse(self.s.indexes(lookup="xdi"))
        self.assertEquals([0,0], self.s.indexes(seq="xdi"))
        self.assertEquals([1,1], self.s.indexes(seq="idx"))
        self.s.c.executescript(""" 
            CREATE INDEX idx1 ON test (b);
            CREATE INDEX idx2 ON test (c);
            CREATE INDEX idx5x3 ON test (a);
            CREATE INDEX idx10x ON test (a);
            """)
        self.assertEquals([1,3], self.s.indexes(seq="idx"))

    def test_count(self):
        self.s.c.execute("INSERT INTO test VALUES ('2','3','4')")
        self.s.commit()
        self.assertEqual(2, self.s.count())
        self.assertEqual(0, self.s.count(table="foo"))
        self.assertEqual(1, self.s.count(table="test", db="db"))

    def test_column(self):
        self.assertEqual(['a','B','c'], self.s.columns())
        self.assertEqual(['a','b','c'], self.s.columns(lower=True))
        self.assertEqual([], self.s.columns(tbl="foo"))
        self.assertTrue(self.s.columns(lookup='a'))
        self.assertFalse(self.s.columns(lookup='A'))
        self.assertTrue(self.s.columns(lower=True, lookup='a'))
        self.assertTrue(self.s.columns(lower=True, lookup='A'))

    def test_fetch(self):
        self.s.c.execute("INSERT INTO test VALUES ('2','3','4')")
        self.s.c.execute("INSERT INTO test VALUES ('2','3','4')")
        self.s.c.execute("INSERT INTO test VALUES ('2','3','4')")
        self.s.c.execute("INSERT INTO test VALUES ('2','3','4')")
        self.assertEqual(5, len(self.s.fetch()))
        self.assertFalse(self.s.fetch(table="foo"))
        self.assertTrue((1,2,3) in self.s.fetch(random=True))
        self.assertTrue(1, len(self.s.fetch(limit=1)))
        self.assertEqual("Cursor",
            self.s.fetch(iter=True).__class__.__name__)
        self.assertEqual([(1,), ('2',), ('2',), ('2',), ('2',)],
            self.s.fetch(field=["a"]))

#    def test_add(self):
#        self.s.add("d")
#        self.s.add("e", typ="ron")
#        self.s.add(keys=["e", "f"], typ="ron")
#        self.s.add({"A":"g", "B":"h"}, table="main")
#        self.assertItemsEqual(["B","a","c","d","e","f"], 
#            self.s.columns())
#        self.assertItemsEqual(["A","B","E","d","f"], 
#            self.s.columns(table="main"))
#        self.s.add("d", db="db")
#        self.s.add(keys="e", typ="ron", db="db")
#        self.s.add(["e", "f"], typ="ron", db="db")
#        self.s.add({"A":"g", "B":"h"}, table="main", db="db")
#        self.assertItemsEqual(["B","a","c","d","e","f"], 
#            self.s.columns(db="db"))
#        self.assertItemsEqual(["A","B","E","d","f"], 
#            self.s.columns(table="main", db="db"))

    def test__baseIndex(self):
        self.assertItemsEqual(['test (a)', 'test (a,b)'],
            self.s._baseIndex(db="db"))
        self.assertEqual('test (a)',
            self.s._baseIndex(idx="idx"))
        self.assertEqual('foo (bar,foo)',
            self.s._baseIndex(idx="create index x on foo (foo, bar)"))
        self.assertEqual('unique foo (foo)',
            self.s._baseIndex(idx="create unique index x on foo (foo)"))

    def test_drop(self):
        self.s.drop(key="b")
        self.assertEqual(['idx'], self.s.indexes())
        self.assertItemsEqual(['a','c'], self.s.columns())
        self.s.drop(key=["e", "f"], table="main")
        self.assertItemsEqual(['d'], self.s.columns(table="main"))

    def test_delete(self):
        self.s.delete("main")
        self.assertEqual(['test'], self.s.tables())

    def test_index(self):
        self.s.index([['a','c']])
        self.assertIn('test (a,c)', self.s._baseIndex())
        self.s.index('a', unique=True)
        self.assertIn('test (a)', self.s._baseIndex())
        self.assertFalse(self.s.index(['a','c']))
        self.s.index('f', tbl="main")
        self.assertIn('main (f)', self.s._baseIndex())
        self.assertFalse(self.s.index('a', tbl="main"))
        self.s.index([['e', 'f']], combo=True, tbl="main")
        # This is broken as a result of the change made to the
        # SQLite indezing file.
        #self.assertIn('main (e)', self.s._baseIndex(tbl="main"))
        self.assertIn('main (e,f)', self.s._baseIndex(tbl="main"))
        self.s.index([['a','c']], db="db")
        self.assertIn('test (a,c)', self.s._baseIndex(db="db"))

    def test_insert(self):
        self.createFile("test.csv", data='X,X,X')
        self.createFile("test2.csv", data='c,b,a')
        self.createFile("test2.csv", data='V,U,T')
        self.s.insert([99,99,99])
        self.s.insert({'a':1})
        self.s.insert([{'a':'a'},{'b':'b'}])
        self.s.insert([{'a':'z', 'c':'z'},[5,10,15]], field=['c','b','a'])
        self.s.insert([[5,5],[6,6]], field=['a','b'])
        self.s.insert(self.s.csvInput("test.csv", iter=True))
        self.s.insert(self.s.csvInput("test2.csv", iter=True), header=True)
        self.s.insert([['a','c'],['Z','Z']], header=True)
        self.s.insert([['a','c'],['Z','Z']], tbl="foo", header=True)
        self.s.insert([['a','c'],['Z','Z']], tbl="bar")
        self.s.insert([['Z','Z','Z']], tbl="bar", errlog="err")

        self.assertIn((99,99,99), self.s.fetch())
        self.assertIn((1,None,None), self.s.fetch())
        self.assertIn(('a',None,None), self.s.fetch())
        self.assertIn((None,'b',None), self.s.fetch())
        self.assertIn(('z',None,'z'), self.s.fetch())
        self.assertIn(('Z',None,'Z'), self.s.fetch())
        self.assertIn((15,10,5), self.s.fetch())
        self.assertIn((5,5,None), self.s.fetch())
        self.assertIn((6,6,None), self.s.fetch())
        self.assertIn(('X','X','X'), self.s.fetch())
        self.assertIn(('T','U','V'), self.s.fetch())
        self.assertIn(('Z','Z'), self.s.fetch(tbl="foo"))
        self.assertIn(('a','c'), self.s.fetch(tbl="bar"))
        self.assertEquals(12, len(self.s.fetch()))
        self.s.insert(tbl='main', db='db')
        self.assertEquals(13, len(self.s.fetch()))

    def test_addSQL(self):
        self.createFile("test.csv", data='X,X,X')
        self.s.addSQL("test.csv")
        self.s.addSQL("test.csv", tbl="foo")
        self.s.addSQL("test.csv", header=True, tbl="bar")
        self.s.addSQL((99,99,99))
        self.s.addSQL("main")
        self.assertIn(('X','X','X'), self.s.fetch())
        self.assertIn((99,99,99), self.s.fetch())
        self.assertIn(('X','X','X'), self.s.fetch(tbl='foo'))
        self.assertEquals(4, len(self.s.fetch()))

    def test_replicate(self):
        self.s.replicate(tbl="test", db="db", tableTo="foo")
        self.assertEquals(
            self.s.c.execute("SELECT count(*) FROM db.sqlite_master WHERE tbl_name='test'").fetchone(),
            self.s.c.execute("SELECT count(*) FROM sqlite_master WHERE tbl_name='foo'").fetchone())
        self.assertItemsEqual(self.s.columns(tbl="test", db="db"),
            self.s.columns(tbl="foo"))
        self.s.replicate(tbl="test", tableTo="bar")
        self.assertEquals(
            self.s.c.execute("SELECT count(*) FROM sqlite_master WHERE tbl_name='bar'").fetchone(),
            self.s.c.execute("SELECT count(*) FROM sqlite_master WHERE tbl_name='test'").fetchone())
        self.assertItemsEqual(self.s.columns(tbl="test"),
            self.s.columns(tbl="foo"))

if __name__ == '__main__':
    unittest.main()

