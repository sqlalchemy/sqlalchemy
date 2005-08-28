# sqlite.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.


import sys, StringIO, string, types

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
from sqlalchemy.ansisql import *

from pysqlite2 import dbapi2 as sqlite

colspecs = {
    schema.INT : "INTEGER",
    schema.CHAR : "CHAR(%(length)s)",
    schema.VARCHAR : "VARCHAR(%(length)s)",
    schema.TEXT : "TEXT",
    schema.FLOAT : "NUMERIC(%(precision)s, %(length)s)",
    schema.DECIMAL : "NUMERIC(%(precision)s, %(length)s)",
    schema.TIMESTAMP : "TIMESTAMP",
    schema.DATETIME : "TIMESTAMP",
    schema.CLOB : "TEXT",
    schema.BLOB : "BLOB",
    schema.BOOLEAN : "BOOLEAN",
}

def engine(filename, opts, **params):
    return SQLiteSQLEngine(filename, opts, **params)

class SQLiteSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, filename, opts, **params):
        self.filename = filename
        self.opts = opts or {}
        ansisql.ANSISQLEngine.__init__(self, **params)

    def post_exec(self, connection, cursor, statement, parameters, echo = None, compiled = None, **kwargs):
        if compiled is None: return
        if getattr(compiled, "isinsert", False):
            self.context.last_inserted_ids = [cursor.lastrowid]

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def connect_args(self):
        return ([self.filename], self.opts)

    def compile(self, statement, bindparams):
        compiler = SQLiteCompiler(self, statement, bindparams)
        statement.accept_visitor(compiler)
        return compiler

    def dbapi(self):
        return sqlite

    def columnimpl(self, column):
        return SQLiteColumnImpl(column)

    def reflecttable(self, table):
        raise NotImplementedError()

class SQLiteCompiler(ansisql.ANSICompiler):
    pass


class SQLiteColumnImpl(sql.ColumnSelectable):
    def get_specification(self):
        coltype = self.column.type
        if isinstance(coltype, types.ClassType):
            key = coltype
        else:
            key = coltype.__class__

        colspec = self.name + " " + colspecs[key] % {'precision': getattr(coltype, 'precision', None), 'length' : getattr(coltype, 'length', None)}
        if self.column.primary_key:
            colspec += " PRIMARY KEY"
        return colspec
