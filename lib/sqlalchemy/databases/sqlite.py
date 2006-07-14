# sqlite.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import sys, StringIO, string, types, re

from sqlalchemy import sql, engine, schema, ansisql, exceptions, pool
import sqlalchemy.engine.default as default
import sqlalchemy.types as sqltypes
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
    def convert_bind_param(self, value, dialect):
        if value is not None:
            return str(value)
        else:
            return None
    def _cvt(self, value, dialect, fmt):
        if value is None:
            return None
        parts = value.split('.')
        try:
            (value, microsecond) = value.split('.')
            microsecond = int(microsecond)
        except ValueError:
            (value, microsecond) = (value, 0)
        return time.strptime(value, fmt)[0:6] + (microsecond,)
    def convert_result_value(self, value, dialect):
        tup = self._cvt(value, dialect, "%Y-%m-%d %H:%M:%S")
        return tup and datetime.datetime(*tup)
class SLDate(SLDateTime):
    def get_col_spec(self):
        return "DATE"
    def convert_result_value(self, value, dialect):
        tup = self._cvt(value, dialect, "%Y-%m-%d")
        return tup and datetime.date(*tup[0:3])
class SLTime(SLDateTime):
    def get_col_spec(self):
        return "TIME"
    def convert_result_value(self, value, dialect):
        tup = self._cvt(value, dialect, "%H:%M:%S")
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
    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        return value and True or False
        
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

def descriptor():
    return {'name':'sqlite',
    'description':'SQLite',
    'arguments':[
        ('database', "Database Filename",None)
    ]}


class SQLiteExecutionContext(default.DefaultExecutionContext):
    def post_exec(self, engine, proxy, compiled, parameters, **kwargs):
        if getattr(compiled, "isinsert", False):
            self._last_inserted_ids = [proxy().lastrowid]
    
class SQLiteDialect(ansisql.ANSIDialect):
    def __init__(self, **kwargs):
        def vers(num):
            return tuple([int(x) for x in num.split('.')])
        self.supports_cast = (vers(sqlite.sqlite_version) >= vers("3.2.3"))
        ansisql.ANSIDialect.__init__(self, **kwargs)
    def compiler(self, statement, bindparams, **kwargs):
        return SQLiteCompiler(self, statement, bindparams, **kwargs)
    def schemagenerator(self, *args, **kwargs):
        return SQLiteSchemaGenerator(*args, **kwargs)
    def create_connect_args(self, url):
        filename = url.database or ':memory:'
        return ([filename], {})
    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)
    def create_execution_context(self):
        return SQLiteExecutionContext(self)
    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def oid_column_name(self):
        return "oid"

    def connect_args(self):
        return ([self.filename], self.opts)

    def dbapi(self):
        if sqlite is None:
            raise exceptions.ArgumentError("Couldn't import sqlite or pysqlite2")
        return sqlite
        
    def has_table(self, connection, table_name):
        cursor = connection.execute("PRAGMA table_info(" + table_name + ")", {})
        row = cursor.fetchone()
        
        # consume remaining rows, to work around: http://www.sqlite.org/cvstrac/tktview?tn=1884
        while cursor.fetchone() is not None:pass
        
        return (row is not None)

    def reflecttable(self, connection, table):
        c = connection.execute("PRAGMA table_info(" + table.name + ")", {})
        found_table = False
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "row! " + repr(row)
            found_table = True
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
        
        if not found_table:
            raise exceptions.NoSuchTableError(table.name)
        
        c = connection.execute("PRAGMA foreign_key_list(" + table.name + ")", {})
        while True:
            row = c.fetchone()
            if row is None:
                break
            (tablename, localcol, remotecol) = (row[2], row[3], row[4])
            #print "row! " + repr(row)
            # look up the table based on the given table's engine, not 'self',
            # since it could be a ProxyEngine
            remotetable = schema.Table(tablename, table.metadata, autoload=True, autoload_with=connection)
            table.c[localcol].append_item(schema.ForeignKey(remotetable.c[remotecol]))
        # check for UNIQUE indexes
        c = connection.execute("PRAGMA index_list(" + table.name + ")", {})
        unique_indexes = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            if (row[2] == 1):
                unique_indexes.append(row[1])
        # loop thru unique indexes for one that includes the primary key
        for idx in unique_indexes:
            c = connection.execute("PRAGMA index_info(" + idx + ")", {})
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
    def visit_cast(self, cast):
        if self.dialect.supports_cast:
            super(SQLiteCompiler, self).visit_cast(cast)
        else:
            if len(self.select_stack):
                # not sure if we want to set the typemap here...
                self.typemap.setdefault("CAST", cast.type)
            self.strings[cast] = self.strings[cast.clause]
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
    def get_column_specification(self, column, **kwargs):
        colspec = column.name + " " + column.type.engine_impl(self.engine).get_col_spec()
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    # this doesnt seem to be needed, although i suspect older versions of sqlite might still
    # not directly support composite primary keys
    #def visit_primary_key_constraint(self, constraint):
    #    if len(constraint) > 1:
    #        self.append(", \n")
    #        # put all PRIMARY KEYS in a UNIQUE index
    #        self.append("\tUNIQUE (%s)" % string.join([c.name for c in constraint],', '))
    #    else:
    #        super(SQLiteSchemaGenerator, self).visit_primary_key_constraint(constraint)
            
dialect = SQLiteDialect
poolclass = pool.SingletonThreadPool       
