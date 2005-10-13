# postgres.py
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

import sys, StringIO, string

import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
from sqlalchemy.ansisql import *

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


def engine(**params):
    return PGSQLEngine(**params)

class PGSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, **params):
        ansisql.ANSISQLEngine.__init__(self, **params)

    def connect_args(self):
        return [[], {}]

    def compile(self, statement, bindparams):
        compiler = PGCompiler(self, statement, bindparams)
        statement.accept_visitor(compiler)
        return compiler

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def pre_exec(self, connection, cursor, statement, parameters, echo = None, compiled = None, **kwargs):
        if compiled is None: return
        if getattr(compiled, "isinsert", False):
            last_inserted_ids = []
            for primary_key in compiled.statement.table.primary_keys:
                # pseudocode
                if parameters[primary_key.key] is None:
                    if echo is True:
                        self.log(primary_key.sequence.text)
                    res = cursor.execute(primary_key.sequence.text)
                    newid = res.fetchrow()[0]
                    parameters[primary_key.key] = newid
                    last_inserted_ids.append(newid)
            self.context.last_inserted_ids = last_inserted_ids

    def dbapi(self):
        return None
#        return psycopg

    def columnimpl(self, column):
        return PGColumnImpl(column)

    def reflecttable(self, table):
        raise NotImplementedError()

class PGCompiler(ansisql.ANSICompiler):
    pass

class PGColumnImpl(sql.ColumnSelectable):
    def get_specification(self):
        coltype = self.column.type
        if isinstance(coltype, types.ClassType):
            key = coltype
        else:
            key = coltype.__class__

        return self.name + " " + colspecs[key] % {'precision': getattr(coltype, 'precision', None), 'length' : getattr(coltype, 'length', None)}
