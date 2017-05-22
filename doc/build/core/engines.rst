.. _engines_toplevel:

====================
Engine Configuration
====================

The :class:`.Engine` is the starting point for any SQLAlchemy application. It's
"home base" for the actual database and its :term:`DBAPI`, delivered to the SQLAlchemy
application through a connection pool and a :class:`.Dialect`, which describes how
to talk to a specific kind of database/DBAPI combination.

The general structure can be illustrated as follows:

.. image:: sqla_engine_arch.png

Where above, an :class:`.Engine` references both a
:class:`.Dialect` and a :class:`.Pool`,
which together interpret the DBAPI's module functions as well as the behavior
of the database.

Creating an engine is just a matter of issuing a single call,
:func:`.create_engine()`::

    from sqlalchemy import create_engine
    engine = create_engine('postgresql://scott:tiger@localhost:5432/mydatabase')

The above engine creates a :class:`.Dialect` object tailored towards
PostgreSQL, as well as a :class:`.Pool` object which will establish a DBAPI
connection at ``localhost:5432`` when a connection request is first received.
Note that the :class:`.Engine` and its underlying :class:`.Pool` do **not**
establish the first actual DBAPI connection until the :meth:`.Engine.connect`
method is called, or an operation which is dependent on this method such as
:meth:`.Engine.execute` is invoked. In this way, :class:`.Engine` and
:class:`.Pool` can be said to have a *lazy initialization* behavior.

The :class:`.Engine`, once created, can either be used directly to interact with the database,
or can be passed to a :class:`.Session` object to work with the ORM.   This section
covers the details of configuring an :class:`.Engine`.   The next section, :ref:`connections_toplevel`,
will detail the usage API of the :class:`.Engine` and similar, typically for non-ORM
applications.

.. _supported_dbapis:

Supported Databases
===================

SQLAlchemy includes many :class:`.Dialect` implementations for various
backends.   Dialects for the most common databases are included with SQLAlchemy; a handful
of others require an additional install of a separate dialect.

See the section :ref:`dialect_toplevel` for information on the various backends available.

.. _database_urls:

Database Urls
=============

The :func:`.create_engine` function produces an :class:`.Engine` object based
on a URL.  These URLs follow `RFC-1738
<http://rfc.net/rfc1738.html>`_, and usually can include username, password,
hostname, database name as well as optional keyword arguments for additional configuration.
In some cases a file path is accepted, and in others a "data source name" replaces
the "host" and "database" portions.  The typical form of a database URL is::

    dialect+driver://username:password@host:port/database

Dialect names include the identifying name of the SQLAlchemy dialect,
a name such as ``sqlite``, ``mysql``, ``postgresql``, ``oracle``, or ``mssql``.
The drivername is the name of the DBAPI to be used to connect to
the database using all lowercase letters. If not specified, a "default" DBAPI
will be imported if available - this default is typically the most widely
known driver available for that backend.

Examples for common connection styles follow below.  For a full index of
detailed information on all included dialects as well as links to third-party dialects, see
:ref:`dialect_toplevel`.

PostgreSQL
----------

The PostgreSQL dialect uses psycopg2 as the default DBAPI.  pg8000 is
also available as a pure-Python substitute::

    # default
    engine = create_engine('postgresql://scott:tiger@localhost/mydatabase')

    # psycopg2
    engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')

    # pg8000
    engine = create_engine('postgresql+pg8000://scott:tiger@localhost/mydatabase')

More notes on connecting to PostgreSQL at :ref:`postgresql_toplevel`.

MySQL
-----

The MySQL dialect uses mysql-python as the default DBAPI.  There are many
MySQL DBAPIs available, including MySQL-connector-python and OurSQL::

    # default
    engine = create_engine('mysql://scott:tiger@localhost/foo')

    # mysql-python
    engine = create_engine('mysql+mysqldb://scott:tiger@localhost/foo')

    # MySQL-connector-python
    engine = create_engine('mysql+mysqlconnector://scott:tiger@localhost/foo')

    # OurSQL
    engine = create_engine('mysql+oursql://scott:tiger@localhost/foo')

More notes on connecting to MySQL at :ref:`mysql_toplevel`.

Oracle
------

The Oracle dialect uses cx_oracle as the default DBAPI::

    engine = create_engine('oracle://scott:tiger@127.0.0.1:1521/sidname')

    engine = create_engine('oracle+cx_oracle://scott:tiger@tnsname')

More notes on connecting to Oracle at :ref:`oracle_toplevel`.

Microsoft SQL Server
--------------------

The SQL Server dialect uses pyodbc as the default DBAPI.  pymssql is
also available::

    # pyodbc
    engine = create_engine('mssql+pyodbc://scott:tiger@mydsn')

    # pymssql
    engine = create_engine('mssql+pymssql://scott:tiger@hostname:port/dbname')

More notes on connecting to SQL Server at :ref:`mssql_toplevel`.

SQLite
------

SQLite connects to file-based databases, using the Python built-in
module ``sqlite3`` by default.

As SQLite connects to local files, the URL format is slightly different.
The "file" portion of the URL is the filename of the database.
For a relative file path, this requires three slashes::

    # sqlite://<nohostname>/<path>
    # where <path> is relative:
    engine = create_engine('sqlite:///foo.db')

And for an absolute file path, the three slashes are followed by the absolute path::

    #Unix/Mac - 4 initial slashes in total
    engine = create_engine('sqlite:////absolute/path/to/foo.db')
    #Windows 
    engine = create_engine('sqlite:///C:\\path\\to\\foo.db')
    #Windows alternative using raw string
    engine = create_engine(r'sqlite:///C:\path\to\foo.db')

To use a SQLite ``:memory:`` database, specify an empty URL::

    engine = create_engine('sqlite://')

More notes on connecting to SQLite at :ref:`sqlite_toplevel`.

Others
------

See :ref:`dialect_toplevel`, the top-level page for all additional dialect
documentation.

.. _create_engine_args:

Engine Creation API
===================

.. autofunction:: sqlalchemy.create_engine

.. autofunction:: sqlalchemy.engine_from_config

.. autofunction:: sqlalchemy.engine.url.make_url


.. autoclass:: sqlalchemy.engine.url.URL
    :members:

Pooling
=======

The :class:`.Engine` will ask the connection pool for a
connection when the ``connect()`` or ``execute()`` methods are called. The
default connection pool, :class:`~.QueuePool`, will open connections to the
database on an as-needed basis. As concurrent statements are executed,
:class:`.QueuePool` will grow its pool of connections to a
default size of five, and will allow a default "overflow" of ten. Since the
:class:`.Engine` is essentially "home base" for the
connection pool, it follows that you should keep a single
:class:`.Engine` per database established within an
application, rather than creating a new one for each connection.

.. note::

   :class:`.QueuePool` is not used by default for SQLite engines.  See
   :ref:`sqlite_toplevel` for details on SQLite connection pool usage.

For more information on connection pooling, see :ref:`pooling_toplevel`.


.. _custom_dbapi_args:

Custom DBAPI connect() arguments
================================

Custom arguments used when issuing the ``connect()`` call to the underlying
DBAPI may be issued in three distinct ways. String-based arguments can be
passed directly from the URL string as query arguments:

.. sourcecode:: python+sql

    db = create_engine('postgresql://scott:tiger@localhost/test?argument1=foo&argument2=bar')

If SQLAlchemy's database connector is aware of a particular query argument, it
may convert its type from string to its proper type.

:func:`~sqlalchemy.create_engine` also takes an argument ``connect_args`` which is an additional dictionary that will be passed to ``connect()``.  This can be used when arguments of a type other than string are required, and SQLAlchemy's database connector has no type conversion logic present for that parameter:

.. sourcecode:: python+sql

    db = create_engine('postgresql://scott:tiger@localhost/test', connect_args = {'argument1':17, 'argument2':'bar'})

The most customizable connection method of all is to pass a ``creator``
argument, which specifies a callable that returns a DBAPI connection:

.. sourcecode:: python+sql

    def connect():
        return psycopg.connect(user='scott', host='localhost')

    db = create_engine('postgresql://', creator=connect)



.. _dbengine_logging:

Configuring Logging
===================

Python's standard `logging
<http://docs.python.org/library/logging.html>`_ module is used to
implement informational and debug log output with SQLAlchemy. This allows
SQLAlchemy's logging to integrate in a standard way with other applications
and libraries. The ``echo`` and ``echo_pool`` flags that are present on
:func:`~sqlalchemy.create_engine`, as well as the ``echo_uow`` flag used on
:class:`~sqlalchemy.orm.session.Session`, all interact with regular loggers.

This section assumes familiarity with the above linked logging module. All
logging performed by SQLAlchemy exists underneath the ``sqlalchemy``
namespace, as used by ``logging.getLogger('sqlalchemy')``. When logging has
been configured (i.e. such as via ``logging.basicConfig()``), the general
namespace of SA loggers that can be turned on is as follows:

* ``sqlalchemy.engine`` - controls SQL echoing.  set to ``logging.INFO`` for SQL query output, ``logging.DEBUG`` for query + result set output.
* ``sqlalchemy.dialects`` - controls custom logging for SQL dialects.  See the documentation of individual dialects for details.
* ``sqlalchemy.pool`` - controls connection pool logging.  set to ``logging.INFO`` or lower to log connection pool checkouts/checkins.
* ``sqlalchemy.orm`` - controls logging of various ORM functions.  set to ``logging.INFO`` for information on mapper configurations.

For example, to log SQL queries using Python logging instead of the ``echo=True`` flag::

    import logging

    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

By default, the log level is set to ``logging.WARN`` within the entire
``sqlalchemy`` namespace so that no log operations occur, even within an
application that has logging enabled otherwise.

The ``echo`` flags present as keyword arguments to
:func:`~sqlalchemy.create_engine` and others as well as the ``echo`` property
on :class:`~sqlalchemy.engine.Engine`, when set to ``True``, will first
attempt to ensure that logging is enabled. Unfortunately, the ``logging``
module provides no way of determining if output has already been configured
(note we are referring to if a logging configuration has been set up, not just
that the logging level is set). For this reason, any ``echo=True`` flags will
result in a call to ``logging.basicConfig()`` using sys.stdout as the
destination. It also sets up a default format using the level name, timestamp,
and logger name. Note that this configuration has the affect of being
configured **in addition** to any existing logger configurations. Therefore,
**when using Python logging, ensure all echo flags are set to False at all
times**, to avoid getting duplicate log lines.

The logger name of instance such as an :class:`~sqlalchemy.engine.Engine`
or :class:`~sqlalchemy.pool.Pool` defaults to using a truncated hex identifier
string. To set this to a specific name, use the "logging_name" and
"pool_logging_name" keyword arguments with :func:`sqlalchemy.create_engine`.

.. note::

   The SQLAlchemy :class:`.Engine` conserves Python function call overhead
   by only emitting log statements when the current logging level is detected
   as ``logging.INFO`` or ``logging.DEBUG``.  It only checks this level when
   a new connection is procured from the connection pool.  Therefore when
   changing the logging configuration for an already-running application, any
   :class:`.Connection` that's currently active, or more commonly a
   :class:`~.orm.session.Session` object that's active in a transaction, won't log any
   SQL according to the new configuration until a new :class:`.Connection`
   is procured (in the case of :class:`~.orm.session.Session`, this is
   after the current transaction ends and a new one begins).
