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
from sqlalchemy import *
import sqlalchemy.databases.information_schema as ischema

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
class PGFloat(sqltypes.Float):
    def get_col_spec(self):
        return "FLOAT(%(precision)s)" % {'precision': self.precision}
class PGInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class PG2DateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"
class PG1DateTime(sqltypes.DateTime):
    def convert_bind_param(self, value):
        # TODO: perform appropriate postgres1 conversion between Python DateTime/MXDateTime
        return value
    def convert_result_value(self, value):
        # TODO: perform appropriate postgres1 conversion between Python DateTime/MXDateTime
        return value
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
        return "BYTEA"
class PGBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"
        
pg2_colspecs = {
    sqltypes.Integer : PGInteger,
    sqltypes.Numeric : PGNumeric,
    sqltypes.Float : PGFloat,
    sqltypes.DateTime : PG2DateTime,
    sqltypes.String : PGString,
    sqltypes.Binary : PGBinary,
    sqltypes.Boolean : PGBoolean,
    sqltypes.TEXT : PGText,
    sqltypes.CHAR: PGChar,
}
pg1_colspecs = pg2_colspecs.copy()
pg1_colspecs[sqltypes.DateTime] = PG1DateTime

pg2_ischema_names = {
    'integer' : PGInteger,
    'bigint' : PGInteger,
    'character varying' : PGString,
    'character' : PGChar,
    'text' : PGText,
    'numeric' : PGNumeric,
    'float' : PGFloat,
    'real' : PGFloat,
    'double precision' : PGFloat,
    'timestamp without time zone' : PG2DateTime,
    'bytea' : PGBinary,
    'boolean' : PGBoolean,
}
pg1_ischema_names = pg2_ischema_names.copy()
pg1_ischema_names['timestamp without time zone'] = PG1DateTime


def engine(opts, **params):
    return PGSQLEngine(opts, **params)

def descriptor():
    return {'name':'postgres',
    'description':'PostGres',
    'arguments':[
        ('user',"Database Username",None),
        ('password',"Database Password",None),
        ('database',"Database Name",None),
        ('host',"Hostname", None),
    ]}

class PGSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, module = None, **params):
        if module is None:
            if psycopg is None:
                raise "Couldnt locate psycopg1 or psycopg2: specify postgres module argument"
            self.module = psycopg
        else:
            self.module = module
        # figure psycopg version 1 or 2    
        if self.module.__name__ == 'psycopg2':
            self.version = 2
        else:
            self.version = 1
        self.opts = opts or {}
        ansisql.ANSISQLEngine.__init__(self, **params)

    def connect_args(self):
        return [[], self.opts]

    def type_descriptor(self, typeobj):
        if self.version == 2:
            return sqltypes.adapt_type(typeobj, pg2_colspecs)
        else:
            return sqltypes.adapt_type(typeobj, pg1_colspecs)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def compiler(self, statement, bindparams, **kwargs):
        return PGCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, proxy, **params):
        return PGSchemaGenerator(proxy, **params)

    def schemadropper(self, proxy, **params):
        return PGSchemaDropper(proxy, **params)
        
    def get_default_schema_name(self):
        if not hasattr(self, '_default_schema_name'):
            self._default_schema_name = text("select current_schema()", self).scalar()
        return self._default_schema_name
        
    def last_inserted_ids(self):
        # if we used sequences or already had all values for the last inserted row,
        # return that list
        if self.context.last_inserted_ids is not None:
            return self.context.last_inserted_ids
        
        # else we have to use lastrowid and select the most recently inserted row    
        table = self.context.last_inserted_table
        if self.context.lastrowid is not None and table is not None and len(table.primary_key):
            row = sql.select(table.primary_key, table.rowid_column == self.context.lastrowid).execute().fetchone()
            return [v for v in row]
        else:
            return None
            
    def pre_exec(self, connection, cursor, statement, parameters, echo = None, compiled = None, **kwargs):
        if compiled is None: return
        if getattr(compiled, "isinsert", False):
            if isinstance(parameters, list):
                plist = parameters
            else:
                plist = [parameters]
            # inserts are usually one at a time.  but if we got a list of parameters,
            # it will calculate last_inserted_ids for just the last row in the list. 
            # TODO: why not make last_inserted_ids a 2D array since we have to explicitly sequence
            # it or post-select anyway   
            for param in plist:
                last_inserted_ids = []
                need_lastrowid=False
                for primary_key in compiled.statement.table.primary_key:
                    if not param.has_key(primary_key.key) or param[primary_key.key] is None:
                        if primary_key.sequence is not None and not primary_key.sequence.optional:
                            if echo is True or self.echo:
                                self.log("select nextval('%s')" % primary_key.sequence.name)
                            cursor.execute("select nextval('%s')" % primary_key.sequence.name)
                            newid = cursor.fetchone()[0]
                            param[primary_key.key] = newid
                            last_inserted_ids.append(param[primary_key.key])
                        else:
                            need_lastrowid = True
                    else:
                        last_inserted_ids.append(param[primary_key.key])
                if need_lastrowid:
                    self.context.last_inserted_ids = None
                else:
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
        if self.version == 2:
            ischema_names = pg2_ischema_names
        else:
            ischema_names = pg1_ischema_names

        ischema.reflecttable(self, table, ischema_names)

class PGCompiler(ansisql.ANSICompiler):
    def visit_insert(self, insert):
        """inserts are required to have the primary keys be explicitly present.
         mapper will by default not put them in the insert statement to comply
         with autoincrement fields that require they not be present.  so, 
         put them all in for columns where sequence usage is defined."""
        for c in insert.table.primary_key:
            if c.sequence is not None and not c.sequence.optional:
                self.bindparams[c.key] = None
        return ansisql.ANSICompiler.visit_insert(self, insert)
        
class PGSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, **kwargs):
        colspec = column.name
        if column.primary_key and isinstance(column.type, types.Integer) and (column.sequence is None or column.sequence.optional):
            colspec += " SERIAL"
        else:
            colspec += " " + column.type.get_col_spec()

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key and not override_pk:
            colspec += " PRIMARY KEY"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.column.foreign_key.column.table.name, column.column.foreign_key.column.name) 
        return colspec

    def visit_sequence(self, sequence):
        if not sequence.optional:
            self.append("CREATE SEQUENCE %s" % sequence.name)
            self.execute()
            
class PGSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_sequence(self, sequence):
        if not sequence.optional:
            self.append("DROP SEQUENCE %s" % sequence.name)
            self.execute()
