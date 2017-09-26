#!/usr/bin/env python

import unittest
import sys
import sqlite3
sys.path.append('../')
sys.path.append('../lib')
import SQLite

# TODO: Get a database connection for testing merge

def create_connections():
    cls.conn1 = sqlite3.connect(':memory:')
    cls.conn2 = sqlite3.connect(':memory:')

def close_connections():
    conn1.close()
    conn2.close()

def create_assignee_schema(cursor):
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS assignee (
            Patent VARCHAR(8),      AsgType INTEGER,        Assignee VARCHAR(30),
            City VARCHAR(10),       State VARCHAR(2),       Country VARCHAR(2),
            Nationality VARCHAR(2), Residence VARCHAR(2),   AsgSeq INTEGER);
        CREATE UNIQUE INDEX IF NOT EXISTS uqAsg ON assignee (Patent, AsgSeq);
        DROP INDEX IF EXISTS idx_pata;
        DROP INDEX IF EXISTS idx_patent;
        DROP INDEX IF EXISTS idx_asgtyp;
        DROP INDEX IF EXISTS idx_stt;
        DROP INDEX IF EXISTS idx_cty;
        """)

def initialize_assignees(conn):
    q = ('D0656296',2,'Frito-Lay North America, Inc.','Plano','TX','US','','',0)
    conn.cursor().execute("""INSERT OR IGNORE INTO assignee VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", q)
    conn.commit()

class TestSQLite(unittest.TestCase):

    @classmethod
    def setUp(cls):
        #print "Setting up..."
        cls.conn1 = sqlite3.connect(':memory:')
        cls.conn2 = sqlite3.connect(':memory:')
        #create_connections()

    @classmethod
    def tearDown(cls):
        #print "Tearing down..."
        cls.conn1.close()
        cls.conn2.close()
        #close_connections()

    def test_constructor_empty(self):
        s = SQLite.SQLite()
        assert(s.db == ':memory:')
        assert(s.tbl == 'main')

    def test_constructor_dbname(self):
        s = SQLite.SQLite(db='foobar.sqlite3')
        assert(s.db == 'foobar.sqlite3')
        assert(s.tbl == 'main')

    def test_constructor_dbname_tbl(self):
        s = SQLite.SQLite(db='foobar.sqlite3', tbl='tbl_foo')
        assert(s.db == 'foobar.sqlite3')
        assert(s.tbl == 'tbl_foo')

    def test_constructor_dbname_tbl_table(self):
        s = SQLite.SQLite(db='foobar.sqlite3', tbl='tbl_foo', table='table_foo')
        assert(s.db == 'foobar.sqlite3')
        assert(s.tbl == 'tbl_foo')

    def test_constructor_dbname_table(self):
        s = SQLite.SQLite(db='foobar.sqlite3', table='table_foo')
        assert(s.db == 'foobar.sqlite3')
        assert(s.tbl == 'table_foo')

#    def test_merge(self):
#        s = SQLite.SQLite()
#        s.merge(key=[['AsgNum', 'pdpass']], on=[['assigneeAsc', 'assignee']],
#                keyType=['INTEGER'], tableFrom='main', db='db')
#        assert(1 == 1)

    def test_index(self):
        s = SQLite.SQLite('test.sqlite3')
        create_assignee_schema(s.c)
        initialize_assignees(s.conn)
        assert(1 == 1)

if __name__ == '__main__':
    unittest.main()
