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


import sys, StringIO, string, types, re

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
from sqlalchemy.ansisql import *

from pysqlite2 import dbapi2 as sqlite

class SLNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class SLInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class SLDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"
class SLText(sqltypes.TEXT):
    def get_col_spec(self):
        return "TEXT"
class SLString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class SLChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class SLBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB"
class SLBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"
        
colspecs = {
    sqltypes.Integer : SLInteger,
    sqltypes.Numeric : SLNumeric,
    sqltypes.DateTime : SLDateTime,
    sqltypes.String : SLString,
    sqltypes.Binary : SLBinary,
    sqltypes.Boolean : SLBoolean,
    sqltypes.TEXT : SLText,
    sqltypes.CHAR: SLChar,
}

pragma_names = {
    'INTEGER' : SLInteger,
    'VARCHAR' : SLString,
    'CHAR' : SLChar,
    'TEXT' : SLText,
    'NUMERIC' : SLNumeric,
    'TIMESTAMP' : SLDateTime,
    'BLOB' : SLBinary,
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

    def type_descriptor(self, typeobj):
        return typeobj.typeself.type_descriptor(colspecs)
        
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
        c = self.execute("PRAGMA table_info(" + table.name + ")", {})
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "row! " + repr(row)
            (name, type, nullable, primary_key) = (row[1], row[2].upper(), not row[3], row[5])
            
            match = re.match(r'(\w+)(\(.*?\))?', type)
            coltype = match.group(1)
            args = match.group(2)
            
            #print "coltype: " + repr(coltype) + " args: " + repr(args)
            coltype = pragma_names[coltype]
            if args is not None:
                args = re.findall(r'(\d+)', args)
                #print "args! " +repr(args)
                coltype = coltype(*args)
            table.append_item(schema.Column(name, coltype, primary_key = primary_key, nullable = nullable))
        c = self.execute("PRAGMA foreign_key_list(" + table.name + ")", {})
        while True:
            row = c.fetchone()
            if row is None:
                break
            (tablename, localcol, remotecol) = (row[2], row[3], row[4])
            #print "row! " + repr(row)
            remotetable = Table(tablename, self, autoload = True)
            table.c[localcol].foreign_key = schema.ForeignKey(remotetable.c[remotecol])
            
class SQLiteCompiler(ansisql.ANSICompiler):
    pass


class SQLiteColumnImpl(sql.ColumnSelectable):
    def get_specification(self):
        colspec = self.name + " " + self.column.type.get_col_spec()
        if not self.column.nullable:
            colspec += " NOT NULL"
        if self.column.primary_key:
            colspec += " PRIMARY KEY"
        if self.column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (self.column.foreign_key.column.table.name, self.column.foreign_key.column.name) 
        return colspec
