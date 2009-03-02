# sqlite.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Support for the SQLite database.

For information on connecting using a specific driver, see the documentation
section regarding that driver.

Date and Time Types
-------------------

SQLite does not have built-in DATE, TIME, or DATETIME types, and pysqlite does not provide 
out of the box functionality for translating values between Python `datetime` objects
and a SQLite-supported format.  SQLAlchemy's own :class:`~sqlalchemy.types.DateTime`
and related types provide date formatting and parsing functionality when SQlite is used.
The implementation classes are :class:`SLDateTime`, :class:`SLDate` and :class:`SLTime`.
These types represent dates and times as ISO formatted strings, which also nicely
support ordering.   There's no reliance on typical "libc" internals for these functions
so historical dates are fully supported.


"""

import datetime, re, time

from sqlalchemy import sql, schema, exc, pool, DefaultClause
from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.sql import compiler, functions as sql_functions
from sqlalchemy.util import NoneType

class NumericMixin(object):
    def bind_processor(self, dialect):
        type_ = self.asdecimal and str or float
        def process(value):
            if value is not None:
                return type_(value)
            else:
                return value
        return process

class SLNumeric(NumericMixin, sqltypes.Numeric):
    pass

class SLFloat(NumericMixin, sqltypes.Float):
    pass

# since SQLite has no date types, we're assuming that SQLite via ODBC
# or JDBC would similarly have no built in date support, so the "string" based logic
# would apply to all implementing dialects.
class DateTimeMixin(object):
    def _bind_processor(self, format, elements):
        def process(value):
            if not isinstance(value, (NoneType, datetime.date, datetime.datetime, datetime.time)):
                raise TypeError("SQLite Date, Time, and DateTime types only accept Python datetime objects as input.")
            elif value is not None:
                return format % tuple([getattr(value, attr, 0) for attr in elements])
            else:
                return None
        return process

    def _result_processor(self, fn, regexp):
        def process(value):
            if value is not None:
                return fn(*[int(x or 0) for x in regexp.match(value).groups()])
            else:
                return None
        return process

class SLDateTime(DateTimeMixin, sqltypes.DateTime):
    __legacy_microseconds__ = False

    def bind_processor(self, dialect):
        if self.__legacy_microseconds__:
            return self._bind_processor(
                        "%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d.%s", 
                        ("year", "month", "day", "hour", "minute", "second", "microsecond")
                        )
        else:
            return self._bind_processor(
                        "%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d.%06d", 
                        ("year", "month", "day", "hour", "minute", "second", "microsecond")
                        )

    _reg = re.compile(r"(\d+)-(\d+)-(\d+)(?: (\d+):(\d+):(\d+)(?:\.(\d+))?)?")
    def result_processor(self, dialect):
        return self._result_processor(datetime.datetime, self._reg)

class SLDate(DateTimeMixin, sqltypes.Date):
    def bind_processor(self, dialect):
        return self._bind_processor(
                        "%4.4d-%2.2d-%2.2d", 
                        ("year", "month", "day")
                )

    _reg = re.compile(r"(\d+)-(\d+)-(\d+)")
    def result_processor(self, dialect):
        return self._result_processor(datetime.date, self._reg)

class SLTime(DateTimeMixin, sqltypes.Time):
    __legacy_microseconds__ = False

    def bind_processor(self, dialect):
        if self.__legacy_microseconds__:
            return self._bind_processor(
                            "%2.2d:%2.2d:%2.2d.%s", 
                            ("hour", "minute", "second", "microsecond")
                    )
        else:
            return self._bind_processor(
                            "%2.2d:%2.2d:%2.2d.%06d", 
                            ("hour", "minute", "second", "microsecond")
                    )

    _reg = re.compile(r"(\d+):(\d+):(\d+)(?:\.(\d+))?")
    def result_processor(self, dialect):
        return self._result_processor(datetime.time, self._reg)


class SLBoolean(sqltypes.Boolean):
    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return value and 1 or 0
        return process

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return value and True or False
        return process

colspecs = {
    sqltypes.Boolean: SLBoolean,
    sqltypes.Date: SLDate,
    sqltypes.DateTime: SLDateTime,
    sqltypes.Float: SLFloat,
    sqltypes.Numeric: SLNumeric,
    sqltypes.Time: SLTime,
}

ischema_names = {
    'BLOB': sqltypes.BLOB,
    'BOOL': sqltypes.BOOLEAN,
    'BOOLEAN': sqltypes.BOOLEAN,
    'CHAR': sqltypes.CHAR,
    'DATE': sqltypes.DATE,
    'DATETIME': sqltypes.DATETIME,
    'DECIMAL': sqltypes.DECIMAL,
    'FLOAT': sqltypes.FLOAT,
    'INT': sqltypes.INTEGER,
    'INTEGER': sqltypes.INTEGER,
    'NUMERIC': sqltypes.NUMERIC,
    'REAL': sqltypes.Numeric,
    'SMALLINT': sqltypes.SMALLINT,
    'TEXT': sqltypes.TEXT,
    'TIME': sqltypes.TIME,
    'TIMESTAMP': sqltypes.TIMESTAMP,
    'VARCHAR': sqltypes.VARCHAR,
}



class SQLiteCompiler(compiler.SQLCompiler):
    functions = compiler.SQLCompiler.functions.copy()
    functions.update (
        {
            sql_functions.now: 'CURRENT_TIMESTAMP',
            sql_functions.char_length: 'length%(expr)s'
        }
    )

    def visit_cast(self, cast, **kwargs):
        if self.dialect.supports_cast:
            return super(SQLiteCompiler, self).visit_cast(cast)
        else:
            return self.process(cast.clause)

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text +=  " \n LIMIT " + str(select._limit)
        if select._offset is not None:
            if select._limit is None:
                text += " \n LIMIT -1"
            text += " OFFSET " + str(select._offset)
        else:
            text += " OFFSET 0"
        return text

    def for_update_clause(self, select):
        # sqlite has no "FOR UPDATE" AFAICT
        return ''


class SQLiteDDLCompiler(compiler.DDLCompiler):

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + self.dialect.type_compiler.process(column.type)
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

class SQLiteTypeCompiler(compiler.GenericTypeCompiler):
    def visit_binary(self, type_):
        return self.visit_BLOB(type_)
    
class SQLiteIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = set([
        'add', 'after', 'all', 'alter', 'analyze', 'and', 'as', 'asc',
        'attach', 'autoincrement', 'before', 'begin', 'between', 'by',
        'cascade', 'case', 'cast', 'check', 'collate', 'column', 'commit',
        'conflict', 'constraint', 'create', 'cross', 'current_date',
        'current_time', 'current_timestamp', 'database', 'default',
        'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct',
        'drop', 'each', 'else', 'end', 'escape', 'except', 'exclusive',
        'explain', 'false', 'fail', 'for', 'foreign', 'from', 'full', 'glob',
        'group', 'having', 'if', 'ignore', 'immediate', 'in', 'index',
        'initially', 'inner', 'insert', 'instead', 'intersect', 'into', 'is',
        'isnull', 'join', 'key', 'left', 'like', 'limit', 'match', 'natural',
        'not', 'notnull', 'null', 'of', 'offset', 'on', 'or', 'order', 'outer',
        'plan', 'pragma', 'primary', 'query', 'raise', 'references',
        'reindex', 'rename', 'replace', 'restrict', 'right', 'rollback',
        'row', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'to',
        'transaction', 'trigger', 'true', 'union', 'unique', 'update', 'using',
        'vacuum', 'values', 'view', 'virtual', 'when', 'where',
        ])

class SQLiteDialect(default.DefaultDialect):
    name = 'sqlite'
    supports_alter = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_default_values = True
    supports_empty_insert = False
    supports_cast = True
    default_paramstyle = 'qmark'
    statement_compiler = SQLiteCompiler
    ddl_compiler = SQLiteDDLCompiler
    type_compiler = SQLiteTypeCompiler
    preparer = SQLiteIdentifierPreparer
    ischema_names = ischema_names
    colspecs = colspecs
    
    def table_names(self, connection, schema):
        if schema is not None:
            qschema = self.identifier_preparer.quote_identifier(schema)
            master = '%s.sqlite_master' % qschema
            s = ("SELECT name FROM %s "
                 "WHERE type='table' ORDER BY name") % (master,)
            rs = connection.execute(s)
        else:
            try:
                s = ("SELECT name FROM "
                     " (SELECT * FROM sqlite_master UNION ALL "
                     "  SELECT * FROM sqlite_temp_master) "
                     "WHERE type='table' ORDER BY name")
                rs = connection.execute(s)
            except exc.DBAPIError:
                raise
                s = ("SELECT name FROM sqlite_master "
                     "WHERE type='table' ORDER BY name")
                rs = connection.execute(s)

        return [row[0] for row in rs]

    def has_table(self, connection, table_name, schema=None):
        quote = self.identifier_preparer.quote_identifier
        if schema is not None:
            pragma = "PRAGMA %s." % quote(schema)
        else:
            pragma = "PRAGMA "
        qtable = quote(table_name)
        cursor = connection.execute("%stable_info(%s)" % (pragma, qtable))
        row = cursor.fetchone()

        # consume remaining rows, to work around
        # http://www.sqlite.org/cvstrac/tktview?tn=1884
        while cursor.fetchone() is not None:
            pass

        return (row is not None)

    @reflection.cache
    def get_columns(self, connection, tablename, schemaname=None,
                                                        info_cache=None):
        quote = self.identifier_preparer.quote_identifier
        if schemaname is not None:
            pragma = "PRAGMA %s." % quote(schemaname)
        else:
            pragma = "PRAGMA "
        qtable = quote(tablename)
        c = connection.execute("%stable_info(%s)" % (pragma, qtable))
        found_table = False
        columns = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            (name, type_, nullable, default, has_default, primary_key) = (row[1], row[2].upper(), not row[3], row[4], row[4] is not None, row[5])
            name = re.sub(r'^\"|\"$', '', name)
            match = re.match(r'(\w+)(\(.*?\))?', type_)
            if match:
                coltype = match.group(1)
                args = match.group(2)
            else:
                coltype = "VARCHAR"
                args = ''
            try:
                coltype = self.ischema_names[coltype]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" %
                          (coltype, name))
                coltype = sqltypes.NullType
            if args is not None:
                args = re.findall(r'(\d+)', args)
                coltype = coltype(*[int(a) for a in args])
            colargs = []
            if has_default:
                colargs.append(DefaultClause(sql.text(default)))
            columns.append({
                'name' : name,
                'type' : coltype,
                'nullable' : nullable,
                'default' : default,
                'colargs' : colargs,
                'primary_key': primary_key
            })
        return columns

    @reflection.cache
    def get_foreign_keys(self, connection, tablename, schemaname=None,
                                                        info_cache=None):
        quote = self.identifier_preparer.quote_identifier
        if schemaname is not None:
            pragma = "PRAGMA %s." % quote(schemaname)
        else:
            pragma = "PRAGMA "
        qtable = quote(tablename)
        c = connection.execute("%sforeign_key_list(%s)" % (pragma, qtable))
        fkeys = []
        fks = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            (constraint_name, rtbl, lcol, rcol) = (row[0], row[2], row[3], row[4])
            rtbl = re.sub(r'^\"|\"$', '', rtbl)
            lcol = re.sub(r'^\"|\"$', '', lcol)
            rcol = re.sub(r'^\"|\"$', '', rcol)
            try:
                fk = fks[constraint_name]
            except KeyError:
                fk = {
                    'name' : constraint_name,
                    'constrained_columns' : [],
                    'referred_schema' : None,
                    'referred_table' : rtbl,
                    'referred_columns' : []
                }
                fkeys.append(fk)
                fks[constraint_name] = fk

            # look up the table based on the given table's engine, not 'self',
            # since it could be a ProxyEngine
            if lcol not in fk['constrained_columns']:
                fk['constrained_columns'].append(lcol)
            if rcol not in fk['referred_columns']:
                fk['referred_columns'].append(rcol)
        return fkeys

    def get_unique_indexes(self, connection, tablename, schemaname=None,
                                                            info_cache=None):
        quote = self.identifier_preparer.quote_identifier
        if schemaname is not None:
            pragma = "PRAGMA %s." % quote(schemaname)
        else:
            pragma = "PRAGMA "
        qtable = quote(tablename)
        c = connection.execute("%sindex_list(%s)" % (pragma, qtable))
        unique_indexes = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            if (row[2] == 1):
                unique_indexes.append(row[1])
        # loop thru unique indexes for one that includes the primary key
        for idx in unique_indexes:
            c = connection.execute("%sindex_info(%s)" % (pragma, idx))
            cols = []
            while True:
                row = c.fetchone()
                if row is None:
                    break
                cols.append(row[2])
        return unique_indexes

    def reflecttable(self, connection, table, include_columns):
        preparer = self.identifier_preparer
        tablename = table.name
        schemaname = table.schema
        found_table = False
        info_cache = SQLiteInfoCache()

        # columns
        for column in self.get_columns(connection, tablename, schemaname,
                                                                info_cache):
            name = column['name']
            coltype = column['type']
            nullable = column['nullable']
            default = column['default']
            colargs = column['colargs']
            primary_key = column['primary_key']
            found_table = True
            if include_columns and name not in include_columns:
                continue
            table.append_column(schema.Column(name, coltype, primary_key = primary_key, nullable = nullable, *colargs))
        if not found_table:
            raise exc.NoSuchTableError(table.name)

        # foreign keys
        for fkey_d in self.get_foreign_keys(connection, tablename, schemaname,
                                                                   info_cache):

            rtbl = fkey_d['referred_table']
            rcols = fkey_d['referred_columns']
            lcols = fkey_d['constrained_columns']
            # look up the table based on the given table's engine, not 'self',
            # since it could be a ProxyEngine
            remotetable = schema.Table(rtbl, table.metadata, autoload=True, autoload_with=connection)
            refspecs = ["%s.%s" % (rtbl, rcol) for rcol in rcols]
            table.append_constraint(schema.ForeignKeyConstraint(lcols, refspecs, link_to_name=True))
        # this doesn't do anything ???
        unique_indexes = self.get_unique_indexes(connection, tablename, 
                                                 schemaname, info_cache)
