# sqlite.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the SQLite database.

Driver
------

When using Python 2.5 and above, the built in ``sqlite3`` driver is 
already installed and no additional installation is needed.  Otherwise,
the ``pysqlite2`` driver needs to be present.  This is the same driver as
``sqlite3``, just with a different name.

The ``pysqlite2`` driver will be loaded first, and if not found, ``sqlite3``
is loaded.  This allows an explicitly installed pysqlite driver to take
precedence over the built in one.   As with all dialects, a specific 
DBAPI module may be provided to :func:`~sqlalchemy.create_engine()` to control 
this explicitly::

    from sqlite3 import dbapi2 as sqlite
    e = create_engine('sqlite:///file.db', module=sqlite)

Full documentation on pysqlite is available at:
`<http://www.initd.org/pub/software/pysqlite/doc/usage-guide.html>`_

Connect Strings
---------------

The file specification for the SQLite database is taken as the "database" portion of
the URL.  Note that the format of a url is::

    driver://user:pass@host/database
    
This means that the actual filename to be used starts with the characters to the
**right** of the third slash.   So connecting to a relative filepath looks like::

    # relative path
    e = create_engine('sqlite:///path/to/database.db')
    
An absolute path, which is denoted by starting with a slash, means you need **four**
slashes::

    # absolute path
    e = create_engine('sqlite:////path/to/database.db')

To use a Windows path, regular drive specifications and backslashes can be used.  
Double backslashes are probably needed::

    # absolute path on Windows
    e = create_engine('sqlite:///C:\\\\path\\\\to\\\\database.db')

The sqlite ``:memory:`` identifier is the default if no filepath is present.  Specify
``sqlite://`` and nothing else::

    # in-memory database
    e = create_engine('sqlite://')

Threading Behavior
------------------

Pysqlite connections do not support being moved between threads, unless
the ``check_same_thread`` Pysqlite flag is set to ``False``.  In addition,
when using an in-memory SQLite database, the full database exists only within 
the scope of a single connection.  It is reported that an in-memory
database does not support being shared between threads regardless of the 
``check_same_thread`` flag - which means that a multithreaded
application **cannot** share data from a ``:memory:`` database across threads
unless access to the connection is limited to a single worker thread which communicates
through a queueing mechanism to concurrent threads.

To provide a default which accomodates SQLite's default threading capabilities
somewhat reasonably, the SQLite dialect will specify that the :class:`~sqlalchemy.pool.SingletonThreadPool`
be used by default.  This pool maintains a single SQLite connection per thread
that is held open up to a count of five concurrent threads.  When more than five threads
are used, a cleanup mechanism will dispose of excess unused connections.   

Two optional pool implementations that may be appropriate for particular SQLite usage scenarios:

 * the :class:`sqlalchemy.pool.StaticPool` might be appropriate for a multithreaded
   application using an in-memory database, assuming the threading issues inherent in 
   pysqlite are somehow accomodated for.  This pool holds persistently onto a single connection
   which is never closed, and is returned for all requests.
   
 * the :class:`sqlalchemy.pool.NullPool` might be appropriate for an application that
   makes use of a file-based sqlite database.  This pool disables any actual "pooling"
   behavior, and simply opens and closes real connections corresonding to the :func:`connect()`
   and :func:`close()` methods.  SQLite can "connect" to a particular file with very high 
   efficiency, so this option may actually perform better without the extra overhead
   of :class:`SingletonThreadPool`.  NullPool will of course render a ``:memory:`` connection
   useless since the database would be lost as soon as the connection is "returned" to the pool.

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

Unicode
-------

In contrast to SQLAlchemy's active handling of date and time types for pysqlite, pysqlite's 
default behavior regarding Unicode is that all strings are returned as Python unicode objects
in all cases.  So even if the :class:`~sqlalchemy.types.Unicode` type is 
*not* used, you will still always receive unicode data back from a result set.  It is 
**strongly** recommended that you do use the :class:`~sqlalchemy.types.Unicode` type
to represent strings, since it will raise a warning if a non-unicode Python string is 
passed from the user application.  Mixing the usage of non-unicode objects with returned unicode objects can
quickly create confusion, particularly when using the ORM as internal data is not 
always represented by an actual database result string.

"""


import datetime, re, time

from sqlalchemy import sql, schema, exc, pool, DefaultClause
from sqlalchemy.engine import default
import sqlalchemy.types as sqltypes
import sqlalchemy.util as util
from sqlalchemy.sql import compiler, functions as sql_functions
from types import NoneType

class SLNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        type_ = self.asdecimal and str or float
        def process(value):
            if value is not None:
                return type_(value)
            else:
                return value
        return process

    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(scale)s)" % {'precision': self.precision, 'scale' : self.scale}

class SLFloat(sqltypes.Float):
    def bind_processor(self, dialect):
        type_ = self.asdecimal and str or float
        def process(value):
            if value is not None:
                return type_(value)
            else:
                return value
        return process

    def get_col_spec(self):
        return "FLOAT"
    
class SLInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class SLSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

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

    def get_col_spec(self):
        return "TIMESTAMP"

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
    def get_col_spec(self):
        return "DATE"

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

    def get_col_spec(self):
        return "TIME"

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

class SLUnicodeMixin(object):
    def bind_processor(self, dialect):
        if self.convert_unicode or dialect.convert_unicode:
            if self.assert_unicode is None:
                assert_unicode = dialect.assert_unicode
            else:
                assert_unicode = self.assert_unicode
                
            if not assert_unicode:
                return None
                
            def process(value):
                if not isinstance(value, (unicode, NoneType)):
                    if assert_unicode == 'warn':
                        util.warn("Unicode type received non-unicode bind "
                                  "param value %r" % value)
                        return value
                    else:
                        raise exc.InvalidRequestError("Unicode type received non-unicode bind param value %r" % value)
                else:
                    return value
            return process
        else:
            return None

    def result_processor(self, dialect):
        return None
    
class SLText(SLUnicodeMixin, sqltypes.Text):
    def get_col_spec(self):
        return "TEXT"

class SLString(SLUnicodeMixin, sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR" + (self.length and "(%d)" % self.length or "")

class SLChar(SLUnicodeMixin, sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR" + (self.length and "(%d)" % self.length or "")

class SLBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB"

class SLBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"

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
            return value == 1
        return process

colspecs = {
    sqltypes.Binary: SLBinary,
    sqltypes.Boolean: SLBoolean,
    sqltypes.CHAR: SLChar,
    sqltypes.Date: SLDate,
    sqltypes.DateTime: SLDateTime,
    sqltypes.Float: SLFloat,
    sqltypes.Integer: SLInteger,
    sqltypes.NCHAR: SLChar,
    sqltypes.Numeric: SLNumeric,
    sqltypes.Smallinteger: SLSmallInteger,
    sqltypes.String: SLString,
    sqltypes.Text: SLText,
    sqltypes.Time: SLTime,
}

ischema_names = {
    'BLOB': SLBinary,
    'BOOL': SLBoolean,
    'BOOLEAN': SLBoolean,
    'CHAR': SLChar,
    'DATE': SLDate,
    'DATETIME': SLDateTime,
    'DECIMAL': SLNumeric,
    'FLOAT': SLFloat,
    'INT': SLInteger,
    'INTEGER': SLInteger,
    'NUMERIC': SLNumeric,
    'REAL': SLNumeric,
    'SMALLINT': SLSmallInteger,
    'TEXT': SLText,
    'TIME': SLTime,
    'TIMESTAMP': SLDateTime,
    'VARCHAR': SLString,
}

class SQLiteExecutionContext(default.DefaultExecutionContext):
    def post_exec(self):
        if self.compiled.isinsert and not self.executemany:
            if not len(self._last_inserted_ids) or self._last_inserted_ids[0] is None:
                self._last_inserted_ids = [self.cursor.lastrowid] + self._last_inserted_ids[1:]

class SQLiteDialect(default.DefaultDialect):
    name = 'sqlite'
    supports_alter = False
    supports_unicode_statements = True
    default_paramstyle = 'qmark'
    supports_default_values = True
    supports_empty_insert = False

    def __init__(self, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)
        def vers(num):
            return tuple([int(x) for x in num.split('.')])
        if self.dbapi is not None:
            sqlite_ver = self.dbapi.version_info
            if sqlite_ver < (2, 1, '3'):
                util.warn(
                    ("The installed version of pysqlite2 (%s) is out-dated "
                     "and will cause errors in some cases.  Version 2.1.3 "
                     "or greater is recommended.") %
                    '.'.join([str(subver) for subver in sqlite_ver]))
            if self.dbapi.sqlite_version_info < (3, 3, 8):
                self.supports_default_values = False
        self.supports_cast = (self.dbapi is None or vers(self.dbapi.sqlite_version) >= vers("3.2.3"))

    def dbapi(cls):
        try:
            from pysqlite2 import dbapi2 as sqlite
        except ImportError, e:
            try:
                from sqlite3 import dbapi2 as sqlite #try the 2.5+ stdlib name.
            except ImportError:
                raise e
        return sqlite
    dbapi = classmethod(dbapi)

    def server_version_info(self, connection):
        return self.dbapi.sqlite_version_info

    def create_connect_args(self, url):
        if url.username or url.password or url.host or url.port:
            raise exc.ArgumentError(
                "Invalid SQLite URL: %s\n"
                "Valid SQLite URL forms are:\n"
                " sqlite:///:memory: (or, sqlite://)\n"
                " sqlite:///relative/path/to/file.db\n"
                " sqlite:////absolute/path/to/file.db" % (url,))
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

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.ProgrammingError) and "Cannot operate on a closed database." in str(e)

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
        cursor = _pragma_cursor(connection.execute("%stable_info(%s)" % (pragma, qtable)))
            
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

        c = _pragma_cursor(connection.execute("%stable_info(%s)" % (pragma, qtable)))
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
                coltype = ischema_names[coltype]
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

        c = _pragma_cursor(connection.execute("%sforeign_key_list(%s)" % (pragma, qtable)))
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
        c = _pragma_cursor(connection.execute("%sindex_list(%s)" % (pragma, qtable)))
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

def _pragma_cursor(cursor):
    if cursor.closed:
        cursor._fetchone_impl = lambda: None
    return cursor
        
class SQLiteCompiler(compiler.DefaultCompiler):
    functions = compiler.DefaultCompiler.functions.copy()
    functions.update (
        {
            sql_functions.now: 'CURRENT_TIMESTAMP',
            sql_functions.char_length: 'length%(expr)s'
        }
    )

    extract_map = compiler.DefaultCompiler.extract_map.copy()
    extract_map.update({
        'month': '%m',
        'day': '%d',
        'year': '%Y',
        'second': '%S',
        'hour': '%H',
        'doy': '%j',
        'minute': '%M',
        'epoch': '%s',
        'dow': '%w',
        'week': '%W'
    })

    def visit_cast(self, cast, **kwargs):
        if self.dialect.supports_cast:
            return super(SQLiteCompiler, self).visit_cast(cast)
        else:
            return self.process(cast.clause)

    def visit_extract(self, extract):
        try:
            return "CAST(STRFTIME('%s', %s) AS INTEGER)" % (
                self.extract_map[extract.field], self.process(extract.expr))
        except KeyError:
            raise exc.ArgumentError(
                "%s is not a valid extract argument." % extract.field)

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


class SQLiteSchemaGenerator(compiler.SchemaGenerator):

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + column.type.dialect_impl(self.dialect).get_col_spec()
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

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
        'vacuum', 'values', 'view', 'virtual', 'when', 'where', 'indexed',
        ])

    def __init__(self, dialect):
        super(SQLiteIdentifierPreparer, self).__init__(dialect)

dialect = SQLiteDialect
dialect.poolclass = pool.SingletonThreadPool
dialect.statement_compiler = SQLiteCompiler
dialect.schemagenerator = SQLiteSchemaGenerator
dialect.preparer = SQLiteIdentifierPreparer
dialect.execution_ctx_cls = SQLiteExecutionContext
