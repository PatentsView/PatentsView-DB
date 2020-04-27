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
from collections import defaultdict
from collections import Counter
from sqlalchemy.sql.expression import bindparam
from sqlalchemy import create_engine, MetaData, Table, inspect, VARCHAR, Column
from sqlalchemy.orm import sessionmaker

from datetime import datetime

def commit_inserts(session, insert_statements, table, is_mysql, commit_frequency = 1000):
    """
    Executes bulk inserts for a given table. This is typically much faster than going through
    the SQLAlchemy ORM. The insert_statement list of dictionaries may fall victim to SQLAlchemy
    complaining that certain columns are null, if you did not specify a value for every single
    column for a table.

    Args:
    session -- alchemy session object
    insert_statements -- list of dictionaries where each dictionary contains key-value pairs of the object
    table -- SQLAlchemy table object. If you have a table reference, you can use TableName.__table__
    is_mysql -- adjusts syntax based on if we are committing to MySQL or SQLite. You can use alchemy.is_mysql() to get this
    commit_frequency -- tune this for speed. Runs "session.commit" every `commit_frequency` items
    """
    if is_mysql:
        ignore_prefix = ("IGNORE",)
        session.execute("set foreign_key_checks = 0; set unique_checks = 0;")
        session.commit()
    else:
        ignore_prefix = ("OR IGNORE",)
    numgroups = len(insert_statements) // commit_frequency
    for ng in range(numgroups):
        if numgroups == 0:
            break
        chunk = insert_statements[ng*commit_frequency:(ng+1)*commit_frequency]
        session.connection().execute(table.insert(prefixes=ignore_prefix), chunk)
        print("committing chunk",ng+1,"of",numgroups,"with length",len(chunk),"at",datetime.now())
        session.commit()
    last_chunk = insert_statements[numgroups*commit_frequency:]
    if last_chunk:
        print("committing last",len(last_chunk),"records at",datetime.now())
        session.connection().execute(table.insert(prefixes=ignore_prefix), last_chunk)
        print("last chunk in, committing")
        session.commit()

def commit_updates(session, update_key, update_statements, table, commit_frequency = 1000):
    """
    Executes bulk updates for a given table. This is typically much faster than going through
    the SQLAlchemy ORM. In order to be flexible, the update statements must be set up in a specific
    way. You can only update one column at a time. The dictionaries in the list `update_statements`
    must have two keys: `pk`, which is the primary_key for the record to be updated, and `update`
    which is the new value for the column you want to change. The column you want to change
    is specified as a string by the argument `update_key`.

    This method will work regardless if you run it over MySQL or SQLite, but with MySQL, it is
    usually faster to use the bulk_commit_updates method (see lib/tasks.py), because it uses
    a table join to do the updates instead of executing individual statements.

    Args:
    session -- alchemy session object
    update_key -- the name of the column we want to update
    update_statements -- list of dictionaries of updates. See above description
    table -- SQLAlchemy table object. If you have a table reference, you can use TableName.__table
    commit_frequency -- tune this for speed. Runs "session.commit" every `commit_frequency` items
    """
    primary_key = list(table.primary_key.columns.values())[0]
    update_key = table.columns[update_key]
    u = table.update().where(primary_key==bindparam('pk')).values({update_key: bindparam('update')})
    numgroups = len(update_statements) / commit_frequency
    for ng in range(numgroups):
        if numgroups == 0:
            break
        chunk = update_statements[ng*commit_frequency:(ng+1)*commit_frequency]
        session.connection().execute(u, *chunk)
        print("committing chunk",ng+1,"of",numgroups,"with length",len(chunk),"at",datetime.now())
        session.commit()
    last_chunk = update_statements[numgroups*commit_frequency:]
    if last_chunk:
        print("committing last",len(last_chunk),"records at",datetime.now())
        print(" If it sticks here, use the assignee_patch.py file to fix it!")
        session.connection().execute(u, *last_chunk)
        session.commit()
