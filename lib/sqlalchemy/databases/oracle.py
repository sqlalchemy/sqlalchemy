# oracle.py
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
import sqlalchemy.types as sqltypes

try:
    import cx_Oracle
except:
    cx_Oracle = None
        
class OracleNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class OracleInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class OracleDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"
class OracleText(sqltypes.TEXT):
    def get_col_spec(self):
        return "TEXT"
class OracleString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class OracleChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class OracleBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB"
class OracleBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"
        
colspecs = {
    sqltypes.Integer : OracleInteger,
    sqltypes.Numeric : OracleNumeric,
    sqltypes.DateTime : OracleDateTime,
    sqltypes.String : OracleString,
    sqltypes.Binary : OracleBinary,
    sqltypes.Boolean : OracleBoolean,
    sqltypes.TEXT : OracleText,
    sqltypes.CHAR: OracleChar,
}

def engine(*args, **params):
    return OracleSQLEngine(*args, **params)
    
class OracleSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, use_ansi = True, module = None, **params):
        self._use_ansi = use_ansi
        self.opts = opts or {}
        if module is None:
            self.module = cx_Oracle
        else:
            self.module = module
        ansisql.ANSISQLEngine.__init__(self, **params)

    def dbapi(self):
        return self.module

    def connect_args(self):
        return [[], self.opts]
        
    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def compiler(self, statement, bindparams):
        return OracleCompiler(self, statement, bindparams)

    def schemagenerator(self, proxy, **params):
        return OracleSchemaGenerator(proxy, **params)

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
        # if a sequence was explicitly defined we do it here
        if compiled is None: return
        if getattr(compiled, "isinsert", False):
            for primary_key in compiled.statement.table.primary_keys:
                if primary_key.sequence is not None and not primary_key.sequence.optional and parameters[primary_key.key] is None:
                    if echo is True or self.echo:
                        self.log("select %s.nextval from dual" % primary_key.sequence.name)
                    cursor.execute("select %s.nextval from dual" % primary_key.sequence.name)
                    newid = cursor.fetchone()[0]
                    parameters[primary_key.key] = newid

    def post_exec(self, connection, cursor, statement, parameters, echo = None, compiled = None, **kwargs):
        if compiled is None: return
        if getattr(compiled, "isinsert", False):
            table = compiled.statement.table
            self.context.last_inserted_table = table
            self.context.lastrowid = cursor.lastrowid

    def _executemany(self, c, statement, parameters):
        rowcount = 0
        for param in parameters:
            c.execute(statement, param)
            rowcount += c.rowcount
        self.context.rowcount = rowcount

class OracleCompiler(ansisql.ANSICompiler):
    """oracle compiler modifies the lexical structure of Select statements to work under 
    non-ANSI configured Oracle databases, if the use_ansi flag is False."""
    
    def __init__(self, engine, statement, bindparams, use_ansi = True):
        self._outertable = None
        self._use_ansi = use_ansi
        ansisql.ANSICompiler.__init__(self, engine, statement, bindparams)
        
    def visit_join(self, join):
        if self._use_ansi:
            return ansisql.ANSICompiler.visit_join(self, join)
            
        self.froms[join] = self.get_from_text(join.left) + ", " + self.get_from_text(join.right)
        self.wheres[join] = join.onclause
        
        if join.isouter:
            # if outer join, push on the right side table as the current "outertable"
            outertable = self._outertable
            self._outertable = join.right

            # now re-visit the onclause, which will be used as a where clause
            # (the first visit occured via the Join object itself right before it called visit_join())
            join.onclause.accept_visitor(self)

            self._outertable = outertable
        
    def visit_column(self, column):
        if self._use_ansi:
            return ansisql.ANSICompiler.visit_column(self, column)
            
        if column.table is self._outertable:
            self.strings[column] = "%s.%s(+)" % (column.table.name, column.name)
        else:
            self.strings[column] = "%s.%s" % (column.table.name, column.name)
        
    def visit_insert(self, insert):
        for c in insert.table.primary_keys:
            if c.sequence is not None and not c.sequence.optional:
                self.bindparams[c.key] = None
                #if not insert.parameters.has_key(c.key):
                 #   insert.parameters[c.key] = sql.bindparam(c.key)
        return ansisql.ANSICompiler.visit_insert(self, insert)

class OracleSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column):
        colspec = column.name
        colspec += " " + column.type.get_col_spec()

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key:
            colspec += " PRIMARY KEY"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.column.foreign_key.column.table.name, column.column.foreign_key.column.name) 
        return colspec

    def visit_sequence(self, sequence):
        self.append("CREATE SEQUENCE %s" % sequence.name)
	print "HI"
        self.execute()
	print "THERE"
