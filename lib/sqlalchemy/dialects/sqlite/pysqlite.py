"""Support for the SQLite database via pysqlite.

Note that pysqlite is the same driver as the ``sqlite3``
module included with the Python distribution.

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
    e = create_engine('sqlite+pysqlite:///file.db', module=sqlite)

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

Compatibility with sqlite3 "native" date and datetime types
-----------------------------------------------------------

The pysqlite driver includes the sqlite3.PARSE_DECLTYPES and 
sqlite3.PARSE_COLNAMES options, which have the effect of any column
or expression explicitly cast as "date" or "timestamp" will be converted
to a Python date or datetime object.  The date and datetime types provided 
with the pysqlite dialect are not currently compatible with these options, 
since they render the ISO date/datetime including microseconds, which 
pysqlite's driver does not.   Additionally, SQLAlchemy does not at
this time automatically render the "cast" syntax required for the 
freestanding functions "current_timestamp" and "current_date" to return
datetime/date types natively.   Unfortunately, pysqlite 
does not provide the standard DBAPI types in `cursor.description`,
leaving SQLAlchemy with no way to detect these types on the fly 
without expensive per-row type checks.

Usage of PARSE_DECLTYPES can be forced if one configures 
"native_datetime=True" on create_engine()::

    engine = create_engine('sqlite://', 
                    connect_args={'detect_types': sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES},
                    native_datetime=True
                    )

With this flag enabled, the DATE and TIMESTAMP types (but note - not the DATETIME
or TIME types...confused yet ?) will not perform any bind parameter or result
processing. Execution of "func.current_date()" will return a string.
"func.current_timestamp()" is registered as returning a DATETIME type in
SQLAlchemy, so this function still receives SQLAlchemy-level result processing.

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

from sqlalchemy.dialects.sqlite.base import SQLiteDialect, DATETIME, DATE
from sqlalchemy import schema, exc, pool
from sqlalchemy.engine import default
from sqlalchemy import types as sqltypes
from sqlalchemy import util


class _SQLite_pysqliteTimeStamp(DATETIME):
    def bind_processor(self, dialect):
        if dialect.native_datetime:
            return None
        else:
            return DATETIME.bind_processor(self, dialect)
            
    def result_processor(self, dialect, coltype):
        if dialect.native_datetime:
            return None
        else:
            return DATETIME.result_processor(self, dialect, coltype)

class _SQLite_pysqliteDate(DATE):
    def bind_processor(self, dialect):
        if dialect.native_datetime:
            return None
        else:
            return DATE.bind_processor(self, dialect)
            
    def result_processor(self, dialect, coltype):
        if dialect.native_datetime:
            return None
        else:
            return DATE.result_processor(self, dialect, coltype)

class SQLiteDialect_pysqlite(SQLiteDialect):
    default_paramstyle = 'qmark'
    poolclass = pool.SingletonThreadPool

    colspecs = util.update_copy(
        SQLiteDialect.colspecs,
        {
            sqltypes.Date:_SQLite_pysqliteDate,
            sqltypes.TIMESTAMP:_SQLite_pysqliteTimeStamp,
        }
    )
    
    # Py3K
    #description_encoding = None
    
    driver = 'pysqlite'
    
    def __init__(self, **kwargs):
        SQLiteDialect.__init__(self, **kwargs)

        if self.dbapi is not None:
            sqlite_ver = self.dbapi.version_info
            if sqlite_ver < (2, 1, 3):
                util.warn(
                    ("The installed version of pysqlite2 (%s) is out-dated "
                     "and will cause errors in some cases.  Version 2.1.3 "
                     "or greater is recommended.") %
                    '.'.join([str(subver) for subver in sqlite_ver]))

    @classmethod
    def dbapi(cls):
        try:
            from pysqlite2 import dbapi2 as sqlite
        except ImportError, e:
            try:
                from sqlite3 import dbapi2 as sqlite #try the 2.5+ stdlib name.
            except ImportError:
                raise e
        return sqlite

    def _get_server_version_info(self, connection):
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

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.ProgrammingError) and "Cannot operate on a closed database." in str(e)

dialect = SQLiteDialect_pysqlite
