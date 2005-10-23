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

import sys, StringIO, string, types, re

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
from sqlalchemy.ansisql import *
try:
    import psycopg2 as psycopg
except:
    try:
        import psycopg
    except:
        psycopg = None
        
class PGNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class PGInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class PGDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"
class PGText(sqltypes.TEXT):
    def get_col_spec(self):
        return "TEXT"
class PGString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class PGChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class PGBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB"
class PGBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"
        
colspecs = {
    sqltypes.Integer : PGInteger,
    sqltypes.Numeric : PGNumeric,
    sqltypes.DateTime : PGDateTime,
    sqltypes.String : PGString,
    sqltypes.Binary : PGBinary,
    sqltypes.Boolean : PGBoolean,
    sqltypes.TEXT : PGText,
    sqltypes.CHAR: PGChar,
}

def engine(opts, **params):
    return PGSQLEngine(opts, **params)

class PGSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, module = None, **params):
        if module is None:
            if psycopg is None:
                raise "Couldnt locate psycopg1 or psycopg2: specify postgres module argument"
            self.module = psycopg
        else:
            self.module = module
        self.opts = opts or {}
        ansisql.ANSISQLEngine.__init__(self, **params)

    def connect_args(self):
        return [[], self.opts]


    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def compiler(self, statement, bindparams):
        return PGCompiler(self, statement, bindparams)

    def schemagenerator(self, proxy, **params):
        return PGSchemaGenerator(proxy, **params)

    def reflecttable(self, table):
        raise "not implemented"
        
    def last_inserted_ids(self):
        table = self.context.last_inserted_table
        if self.context.lastrowid is not None and table is not None and len(table.primary_keys):
            row = sql.select(table.primary_keys, table.rowid_column == self.context.lastrowid).execute().fetchone()
            return [v for v in row]
        else:
            return None
            
    def pre_exec(self, connection, cursor, statement, parameters, echo = None, compiled = None, **kwargs):
        if True: return
        # if a sequence was explicitly defined we do it here
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

    def _executemany(self, c, statement, parameters):
        """we need accurate rowcounts for updates, inserts and deletes.  psycopg2 is not nice enough
        to produce this correctly for an executemany, so we do our own executemany here."""
        rowcount = 0
        for param in parameters:
            c.execute(statement, param)
            rowcount += c.rowcount
        self.context.rowcount = rowcount

    def post_exec(self, connection, cursor, statement, parameters, echo = None, compiled = None, **kwargs):
        if compiled is None: return
        if getattr(compiled, "isinsert", False):
            table = compiled.statement.table
            self.context.last_inserted_table = table
            self.context.lastrowid = cursor.lastrowid
            
    def dbapi(self):
        return self.module

    def reflecttable(self, table):
        raise NotImplementedError()

class PGCompiler(ansisql.ANSICompiler):
    def bindparam_string(self, name):
        return "%(" + name + ")s"

class PGSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column):
        colspec = column.name
        if column.primary_key and isinstance(column.type, types.Integer):
            colspec += " SERIAL"
        else:
            colspec += " " + column.column.type.get_col_spec()

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key:
            colspec += " PRIMARY KEY"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.column.foreign_key.column.table.name, column.column.foreign_key.column.name) 
        return colspec
