# sqlite.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import sys, StringIO, string, types, re

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
from sqlalchemy.exceptions import *
from sqlalchemy.ansisql import *
import datetime,time

pysqlite2_timesupport = False   # Change this if the init.d guys ever get around to supporting time cols

try:
    from pysqlite2 import dbapi2 as sqlite
except:
    try:
        sqlite = __import__('sqlite') # skip ourselves
    except:
        sqlite = None

class SLNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class SLInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class SLSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"
class SLDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"
    def _cvt(self, value, engine, fmt):
        if value is None:
            return None
        parts = value.split('.')
        try:
            (value, microsecond) = value.split('.')
            microsecond = int(microsecond)
        except ValueError:
            (value, microsecond) = (value, 0)
        return time.strptime(value, fmt)[0:6] + (microsecond,)
    def convert_result_value(self, value, engine):
        tup = self._cvt(value, engine, "%Y-%m-%d %H:%M:%S")
        return tup and datetime.datetime(*tup)
class SLDate(SLDateTime):
    def get_col_spec(self):
        return "DATE"
    def convert_result_value(self, value, engine):
        tup = self._cvt(value, engine, "%Y-%m-%d")
        return tup and datetime.date(*tup[0:3])
class SLTime(SLDateTime):
    def get_col_spec(self):
        return "TIME"
    def convert_result_value(self, value, engine):
        tup = self._cvt(value, engine, "%H:%M:%S")
        return tup and datetime.time(*tup[4:7])
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
    sqltypes.Smallinteger : SLSmallInteger,
    sqltypes.Numeric : SLNumeric,
    sqltypes.Float : SLNumeric,
    sqltypes.DateTime : SLDateTime,
    sqltypes.Date : SLDate,
    sqltypes.String : SLString,
    sqltypes.Binary : SLBinary,
    sqltypes.Boolean : SLBoolean,
    sqltypes.TEXT : SLText,
    sqltypes.CHAR: SLChar,
}

pragma_names = {
    'INTEGER' : SLInteger,
    'SMALLINT' : SLSmallInteger,
    'VARCHAR' : SLString,
    'CHAR' : SLChar,
    'TEXT' : SLText,
    'NUMERIC' : SLNumeric,
    'FLOAT' : SLNumeric,
    'TIMESTAMP' : SLDateTime,
    'DATETIME' : SLDateTime,
    'DATE' : SLDate,
    'BLOB' : SLBinary,
}

if pysqlite2_timesupport:
    colspecs.update({sqltypes.Time : SLTime})
    pragma_names.update({'TIME' : SLTime})
    
def engine(opts, **params):
    return SQLiteSQLEngine(opts, **params)

def descriptor():
    return {'name':'sqlite',
    'description':'SQLite',
    'arguments':[
        ('filename', "Database Filename",None)
    ]}
    
class SQLiteSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, **params):
        if sqlite is None:
            raise ArgumentError("Couldn't import sqlite or pysqlite2")
        self.filename = opts.pop('filename', ':memory:')
        self.opts = opts or {}
        params['poolclass'] = sqlalchemy.pool.SingletonThreadPool
        ansisql.ANSISQLEngine.__init__(self, **params)

    def post_exec(self, proxy, compiled, parameters, **kwargs):
        if getattr(compiled, "isinsert", False):
            self.context.last_inserted_ids = [proxy().lastrowid]

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)
        
    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def oid_column_name(self):
        return "oid"

    def connect_args(self):
        return ([self.filename], self.opts)

    def compiler(self, statement, bindparams, **kwargs):
        return SQLiteCompiler(statement, bindparams, engine=self, **kwargs)

    def dbapi(self):
        return sqlite

    def schemagenerator(self, **params):
        return SQLiteSchemaGenerator(self, **params)

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
            coltype = pragma_names.get(coltype, SLString)
            if args is not None:
                args = re.findall(r'(\d+)', args)
                #print "args! " +repr(args)
                coltype = coltype(*[int(a) for a in args])
            table.append_item(schema.Column(name, coltype, primary_key = primary_key, nullable = nullable))
        c = self.execute("PRAGMA foreign_key_list(" + table.name + ")", {})
        while True:
            row = c.fetchone()
            if row is None:
                break
            (tablename, localcol, remotecol) = (row[2], row[3], row[4])
            #print "row! " + repr(row)
            # look up the table based on the given table's engine, not 'self',
            # since it could be a ProxyEngine
            remotetable = Table(tablename, table.engine, autoload = True)
            table.c[localcol].append_item(schema.ForeignKey(remotetable.c[remotecol]))
        # check for UNIQUE indexes
        c = self.execute("PRAGMA index_list(" + table.name + ")", {})
        unique_indexes = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            if (row[2] == 1):
                unique_indexes.append(row[1])
        # loop thru unique indexes for one that includes the primary key
        for idx in unique_indexes:
            c = self.execute("PRAGMA index_info(" + idx + ")", {})
            cols = []
            while True:
                row = c.fetchone()
                if row is None:
                    break
                cols.append(row[2])
                col = table.columns[row[2]]
            # unique index that includes the pk is considered a multiple primary key
            for col in cols:
                column = table.columns[col]
                table.columns[col]._set_primary_key()
                    
class SQLiteCompiler(ansisql.ANSICompiler):
    def __init__(self, *args, **params):
        params.setdefault('paramstyle', 'named')
        ansisql.ANSICompiler.__init__(self, *args, **params)
    def limit_clause(self, select):
        text = ""
        if select.limit is not None:
            text +=  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                text += " \n LIMIT -1"
            text += " OFFSET " + str(select.offset)
        else:
            text += " OFFSET 0"
        return text
    def binary_operator_string(self, binary):
        if isinstance(binary.type, sqltypes.String) and binary.operator == '+':
            return '||'
        else:
            return ansisql.ANSICompiler.binary_operator_string(self, binary)
        
class SQLiteSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, **kwargs):
        colspec = column.name + " " + column.type.get_col_spec()
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key and not override_pk:
            colspec += " PRIMARY KEY"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.foreign_key.column.table.name, column.foreign_key.column.name) 
        return colspec
    def visit_table(self, table):
        """sqlite is going to create multi-primary keys with just a UNIQUE index."""
        self.append("\nCREATE TABLE " + table.fullname + "(")

        separator = "\n"

        have_pk = False
        use_pks = len(table.primary_key) == 1
        for column in table.columns:
            self.append(separator)
            separator = ", \n"
            self.append("\t" + self.get_column_specification(column, override_pk=not use_pks))
                
        if len(table.primary_key) > 1:
            self.append(", \n")
            # put all PRIMARY KEYS in a UNIQUE index
            self.append("\tUNIQUE (%s)" % string.join([c.name for c in table.primary_key],', '))

        self.append("\n)\n\n")
        self.execute()        
        if hasattr(table, 'indexes'):
            for index in table.indexes:
                self.visit_index(index)

        
