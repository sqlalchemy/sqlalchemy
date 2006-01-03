# mysql.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import sys, StringIO, string, types, re

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
from sqlalchemy import *
import sqlalchemy.databases.information_schema as ischema

try:
    import MySQLdb as mysql
except:
    mysql = None
    
class MSNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class MSFloat(sqltypes.Float):
    def get_col_spec(self):
        return "FLOAT(%(precision)s)" % {'precision': self.precision}
class MSInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class MSDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATETIME"
class MSText(sqltypes.TEXT):
    def get_col_spec(self):
        return "TEXT"
class MSString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class MSChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class MSBinary(sqltypes.Binary):
    def get_col_spec(self):
        if self.length is not None and self.length <=255:
            # the binary type seems to return a value that is null-padded
            return "BINARY(%d)" % self.length
        else:
            return "BLOB"

class MSBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"
        
colspecs = {
    sqltypes.Integer : MSInteger,
    sqltypes.Numeric : MSNumeric,
    sqltypes.Float : MSFloat,
    sqltypes.DateTime : MSDateTime,
    sqltypes.String : MSString,
    sqltypes.Binary : MSBinary,
    sqltypes.Boolean : MSBoolean,
    sqltypes.TEXT : MSText,
    sqltypes.CHAR: MSChar,
}

ischema_names = {
    'int' : MSInteger,
    'varchar' : MSString,
    'char' : MSChar,
    'text' : MSText,
    'decimal' : MSNumeric,
    'float' : MSFloat,
    'timestamp' : MSDateTime,
    'datetime' : MSDateTime,
    'binary' : MSBinary,
    'blob' : MSBinary,
}


def engine(opts, **params):
    return MySQLEngine(opts, **params)

def descriptor():
    return {'name':'mysql',
    'description':'MySQL',
    'arguments':[
        ('user',"Database Username",None),
        ('passwd',"Database Password",None),
        ('db',"Database Name",None),
        ('host',"Hostname", None),
    ]}

class MySQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, module = None, **params):
        if module is None:
            self.module = mysql
        self.opts = opts or {}
        ansisql.ANSISQLEngine.__init__(self, **params)

    def connect_args(self):
        return [[], self.opts]

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def supports_sane_rowcount(self):
        return False

    def tableimpl(self, table):
        """returns a new sql.TableImpl object to correspond to the given Table object."""
        return MySQLTableImpl(table)

    def compiler(self, statement, bindparams, **kwargs):
        return MySQLCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, proxy, **params):
        return MySQLSchemaGenerator(proxy, **params)

    def get_default_schema_name(self):
        if not hasattr(self, '_default_schema_name'):
            self._default_schema_name = text("select database()", self).scalar()
        return self._default_schema_name
        
    def last_inserted_ids(self):
        return self.context.last_inserted_ids
            
    def post_exec(self, proxy, compiled, parameters, **kwargs):
        if getattr(compiled, "isinsert", False):
            self.context.last_inserted_ids = [proxy().lastrowid]
    
    # executemany just runs normally, since we arent using rowcount at all with mysql
#    def _executemany(self, c, statement, parameters):
 #       """we need accurate rowcounts for updates, inserts and deletes.  mysql is *also* is not nice enough
 #       to produce this correctly for an executemany, so we do our own executemany here."""
  #      rowcount = 0
  #      for param in parameters:
  #          c.execute(statement, param)
  #          rowcount += c.rowcount
  #      self.context.rowcount = rowcount

    def dbapi(self):
        return self.module

    def reflecttable(self, table):
        ischema.reflecttable(self, table, ischema_names, use_mysql=True)

class MySQLTableImpl(sql.TableImpl):
    """attached to a schema.Table to provide it with a Selectable interface
    as well as other functions
    """
    pass

class MySQLCompiler(ansisql.ANSICompiler):
    def limit_clause(self, select):
        text = ""
        if select.limit is not None:
            text +=  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                # striaght from the MySQL docs, I kid you not
                text += " \n LIMIT 18446744073709551615"
            text += " OFFSET " + str(select.offset)
        return text
        
class MySQLSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, first_pk=False):
        colspec = column.name + " " + column.type.get_col_spec()

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key:
            if not override_pk:
                colspec += " PRIMARY KEY"
            if first_pk and isinstance(column.type, types.Integer):
                colspec += " AUTO_INCREMENT"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.column.foreign_key.column.table.name, column.column.foreign_key.column.name) 
        return colspec

