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
Functions for doing bulk inserts and bulk updates
"""
from alchemy.match import commit_inserts, commit_updates
from alchemy import session_generator
from alchemy.schema import temporary_update, app_temporary_update
from sqlalchemy import create_engine, MetaData, Table, inspect, VARCHAR, Column
from sqlalchemy.orm import sessionmaker

# fetch reference to temporary_update table.

def bulk_commit_inserts(insert_statements, table, commit_frequency = 1000, dbtype='grant'):
    """
    Executes bulk inserts for a given table. This is typically much faster than going through
    the SQLAlchemy ORM. The insert_statement list of dictionaries may fall victim to SQLAlchemy
    complaining that certain columns are null, if you did not specify a value for every single
    column for a table.

    A session is generated using the scoped_session factory through SQLAlchemy, and then
    the actual lib.alchemy.match.commit_inserts task is dispatched.

    Args:
    insert_statements -- list of dictionaries where each dictionary contains key-value pairs of the object
    table -- SQLAlchemy table object. If you have a table reference, you can use TableName.__table__
    commit_frequency -- tune this for speed. Runs "session.commit" every `commit_frequency` items
    dbtype -- which base schema to use. Either 'grant' or 'application'
    """
    session = session_generator(dbtype=dbtype)
    commit_inserts(session, insert_statements, table, commit_frequency)
    session.commit()

def bulk_commit_updates(update_key, update_statements, table, commit_frequency = 1000, dbtype='grant'):
    """
    Executes bulk updates for a given table. This is typically much faster than going through
    the SQLAlchemy ORM. In order to be flexible, the update statements must be set up in a specific
    way. You can only update one column at a time. The dictionaries in the list `update_statements`
    must have two keys: `pk`, which is the primary_key for the record to be updated, and `update`
    which is the new value for the column you want to change. The column you want to change
    is specified as a string by the argument `update_key`.

    If is_mysql is True, then the update will be performed by inserting the record updates
    into the table temporary_update and then executing an UPDATE/JOIN. If is_mysql is False,
    then SQLite is assumed, and traditional updates are used (lib.alchemy.match.commit_updates)

    A session is generated using the scoped_session factory through SQLAlchemy, and then
    the actual task is dispatched.

    Args:
    update_key -- the name of the column we want to update
    update_statements -- list of dictionaries of updates. See above description
    table -- SQLAlchemy table object. If you have a table reference, you can use TableName.__table
    is_mysql -- adjusts syntax based on if we are committing to MySQL or SQLite. You can use alchemy.is_mysql() to get this
    commit_frequency -- tune this for speed. Runs "session.commit" every `commit_frequency` items
    dbtype -- which base schema to use. Either 'grant' or 'application'
    """
    session = session_generator(dbtype=dbtype)

    session.rollback()

    session.execute('truncate temporary_update;')

    if dbtype == 'grant':
        commit_inserts(session, update_statements, temporary_update,  10000)
    else:
        commit_inserts(session, update_statements, app_temporary_update, 10000)
    # now update using the join
    primary_key = table.primary_key.columns.values()[0]
    update_key = table.columns[update_key]
    session.execute("UPDATE {0} join temporary_update ON temporary_update.pk = {1} SET {2} = temporary_update.update;".format(table.name, primary_key.name, update_key.name ))
    session.commit()

    session.execute('truncate temporary_update;')

    session.commit()
