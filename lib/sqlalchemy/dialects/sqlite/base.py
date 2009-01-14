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
from sqlalchemy.engine import default
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.sql import compiler, functions as sql_functions
from types import NoneType

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
    
    def visit_CLOB(self, type_):
        return self.visit_TEXT(type_)

    def visit_NCHAR(self, type_):
        return self.visit_CHAR(type_)
    
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
    supports_default_values = True
    supports_empty_insert = False
    supports_cast = True
    default_paramstyle = 'qmark'
    statement_compiler = SQLiteCompiler
    ddl_compiler = SQLiteDDLCompiler
    type_compiler = SQLiteTypeCompiler
    preparer = SQLiteIdentifierPreparer

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

    def reflecttable(self, connection, table, include_columns):
        preparer = self.identifier_preparer
        if table.schema is None:
            pragma = "PRAGMA "
        else:
            pragma = "PRAGMA %s." % preparer.quote_identifier(table.schema)
        qtable = preparer.format_table(table, False)

        c = connection.execute("%stable_info(%s)" % (pragma, qtable))
        found_table = False
        while True:
            row = c.fetchone()
            if row is None:
                break

            found_table = True
            (name, type_, nullable, default, has_default, primary_key) = (row[1], row[2].upper(), not row[3], row[4], row[4] is not None, row[5])
            name = re.sub(r'^\"|\"$', '', name)
            if include_columns and name not in include_columns:
                continue
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
            table.append_column(schema.Column(name, coltype, primary_key = primary_key, nullable = nullable, *colargs))

        if not found_table:
            raise exc.NoSuchTableError(table.name)

        c = connection.execute("%sforeign_key_list(%s)" % (pragma, qtable))
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
                fk = ([], [])
                fks[constraint_name] = fk

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
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1], link_to_name=True))
        # check for UNIQUE indexes
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

