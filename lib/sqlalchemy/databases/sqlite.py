# sqlite.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import sys, StringIO, string, types, re

from sqlalchemy import sql, engine, schema, ansisql, exceptions, pool, PassiveDefault
import sqlalchemy.engine.default as default
import sqlalchemy.types as sqltypes
import datetime,time, warnings
import sqlalchemy.util as util

    
class SLNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

class SLInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class SLSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class DateTimeMixin(object):
    def convert_bind_param(self, value, dialect):
        if value is not None:
            if getattr(value, 'microsecond', None) is not None:
                return value.strftime(self.__format__ + "." + str(value.microsecond))
            else:
                return value.strftime(self.__format__)
        else:
            return None

    def _cvt(self, value, dialect):
        if value is None:
            return None
        try:
            (value, microsecond) = value.split('.')
            microsecond = int(microsecond)
        except ValueError:
            (value, microsecond) = (value, 0)
        return time.strptime(value, self.__format__)[0:6] + (microsecond,)

class SLDateTime(DateTimeMixin,sqltypes.DateTime):
    __format__ = "%Y-%m-%d %H:%M:%S"
    
    def get_col_spec(self):
        return "TIMESTAMP"

    def convert_result_value(self, value, dialect):
        tup = self._cvt(value, dialect)
        return tup and datetime.datetime(*tup)

class SLDate(DateTimeMixin, sqltypes.Date):
    __format__ = "%Y-%m-%d"

    def get_col_spec(self):
        return "DATE"

    def convert_result_value(self, value, dialect):
        tup = self._cvt(value, dialect)
        return tup and datetime.date(*tup[0:3])

class SLTime(DateTimeMixin, sqltypes.Time):
    __format__ = "%H:%M:%S"

    def get_col_spec(self):
        return "TIME"

    def convert_result_value(self, value, dialect):
        tup = self._cvt(value, dialect)
        return tup and datetime.time(*tup[3:7])

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

    def convert_bind_param(self, value, dialect):
        if value is None:
            return None
        return value and 1 or 0

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
    sqltypes.Time : SLTime,
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

def descriptor():
    return {'name':'sqlite',
    'description':'SQLite',
    'arguments':[
        ('database', "Database Filename",None)
    ]}

class SQLiteExecutionContext(default.DefaultExecutionContext):
    def post_exec(self):
        if self.compiled.isinsert:
            if not len(self._last_inserted_ids) or self._last_inserted_ids[0] is None:
                self._last_inserted_ids = [self.cursor.lastrowid] + self._last_inserted_ids[1:]
                
        super(SQLiteExecutionContext, self).post_exec()
        
class SQLiteDialect(ansisql.ANSIDialect):
    
    def __init__(self, **kwargs):
        ansisql.ANSIDialect.__init__(self, default_paramstyle='qmark', **kwargs)
        def vers(num):
            return tuple([int(x) for x in num.split('.')])
        if self.dbapi is not None:
            sqlite_ver = self.dbapi.version_info
            if sqlite_ver < (2,1,'3'):
                warnings.warn(RuntimeWarning("The installed version of pysqlite2 (%s) is out-dated, and will cause errors in some cases.  Version 2.1.3 or greater is recommended." % '.'.join([str(subver) for subver in sqlite_ver])))
            if vers(self.dbapi.sqlite_version) < vers("3.3.13"):
                warnings.warn(RuntimeWarning("The installed version of sqlite (%s) is out-dated, and will cause errors in some cases.  Version 3.3.13 or greater is recommended." % self.dbapi.sqlite_version))
        self.supports_cast = (self.dbapi is None or vers(self.dbapi.sqlite_version) >= vers("3.2.3"))
        
    def dbapi(cls):
        try:
            from pysqlite2 import dbapi2 as sqlite
        except ImportError, e:
            try:
                from sqlite3 import dbapi2 as sqlite #try the 2.5+ stdlib name.
            except ImportError:
                try:
                    sqlite = __import__('sqlite') # skip ourselves
                except ImportError:
                    raise e
        return sqlite
    dbapi = classmethod(dbapi)

    def compiler(self, statement, bindparams, **kwargs):
        return SQLiteCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, *args, **kwargs):
        return SQLiteSchemaGenerator(self, *args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return SQLiteSchemaDropper(self, *args, **kwargs)

    def supports_alter(self):
        return False

    def preparer(self):
        return SQLiteIdentifierPreparer(self)

    def create_connect_args(self, url):
        filename = url.database or ':memory:'

        opts = url.query.copy()
        util.coerce_kw_type(opts, 'timeout', float)
        util.coerce_kw_type(opts, 'isolation_level', str)
        util.coerce_kw_type(opts, 'detect_types', int)
        util.coerce_kw_type(opts, 'check_same_thread', bool)
        util.coerce_kw_type(opts, 'cached_statements', int)

        return ([filename], opts)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def create_execution_context(self, **kwargs):
        return SQLiteExecutionContext(self, **kwargs)

    def supports_unicode_statements(self):
        return True

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def oid_column_name(self, column):
        return "oid"

    def has_table(self, connection, table_name, schema=None):
        cursor = connection.execute("PRAGMA table_info(%s)" %
           self.identifier_preparer.quote_identifier(table_name), {})
        row = cursor.fetchone()

        # consume remaining rows, to work around: http://www.sqlite.org/cvstrac/tktview?tn=1884
        while cursor.fetchone() is not None:pass

        return (row is not None)

    def reflecttable(self, connection, table):
        c = connection.execute("PRAGMA table_info(%s)" % self.preparer().format_table(table), {})
        found_table = False
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "row! " + repr(row)
            found_table = True
            (name, type, nullable, has_default, primary_key) = (row[1], row[2].upper(), not row[3], row[4] is not None, row[5])
            name = re.sub(r'^\"|\"$', '', name)
            match = re.match(r'(\w+)(\(.*?\))?', type)
            if match:
                coltype = match.group(1)
                args = match.group(2)
            else:
                coltype = "VARCHAR"
                args = ''

            #print "coltype: " + repr(coltype) + " args: " + repr(args)
            coltype = pragma_names.get(coltype, SLString)
            if args is not None:
                args = re.findall(r'(\d+)', args)
                #print "args! " +repr(args)
                coltype = coltype(*[int(a) for a in args])

            colargs= []
            if has_default:
                colargs.append(PassiveDefault('?'))
            table.append_column(schema.Column(name, coltype, primary_key = primary_key, nullable = nullable, *colargs))

        if not found_table:
            raise exceptions.NoSuchTableError(table.name)

        c = connection.execute("PRAGMA foreign_key_list(%s)" % self.preparer().format_table(table), {})
        fks = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            (constraint_name, tablename, localcol, remotecol) = (row[0], row[2], row[3], row[4])
            tablename = re.sub(r'^\"|\"$', '', tablename)
            localcol = re.sub(r'^\"|\"$', '', localcol)
            remotecol = re.sub(r'^\"|\"$', '', remotecol)
            try:
                fk = fks[constraint_name]
            except KeyError:
                fk = ([],[])
                fks[constraint_name] = fk

            #print "row! " + repr([key for key in row.keys()]), repr(row)
            # look up the table based on the given table's engine, not 'self',
            # since it could be a ProxyEngine
            remotetable = schema.Table(tablename, table.metadata, autoload=True, autoload_with=connection)
            constrained_column = table.c[localcol].name
            refspec = ".".join([tablename, remotecol])
            if constrained_column not in fk[0]:
                fk[0].append(constrained_column)
            if refspec not in fk[1]:
                fk[1].append(refspec)
        for name, value in fks.iteritems():
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1]))
        # check for UNIQUE indexes
        c = connection.execute("PRAGMA index_list(%s)" % self.preparer().format_table(table), {})
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

    def for_update_clause(self, select):
        # sqlite has no "FOR UPDATE" AFAICT
        return ''

    def binary_operator_string(self, binary):
        if isinstance(binary.type, sqltypes.String) and binary.operator == '+':
            return '||'
        else:
            return ansisql.ANSICompiler.binary_operator_string(self, binary)

class SQLiteSchemaGenerator(ansisql.ANSISchemaGenerator):

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + column.type.dialect_impl(self.dialect).get_col_spec()
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

class SQLiteSchemaDropper(ansisql.ANSISchemaDropper):
    pass

class SQLiteIdentifierPreparer(ansisql.ANSIIdentifierPreparer):
    def __init__(self, dialect):
        super(SQLiteIdentifierPreparer, self).__init__(dialect, omit_schema=True)

dialect = SQLiteDialect
dialect.poolclass = pool.SingletonThreadPool
