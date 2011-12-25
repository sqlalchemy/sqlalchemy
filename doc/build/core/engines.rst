.. _engines_toplevel:

====================
Engine Configuration
====================

The **Engine** is the starting point for any SQLAlchemy application. It's
"home base" for the actual database and its DBAPI, delivered to the SQLAlchemy
application through a connection pool and a **Dialect**, which describes how
to talk to a specific kind of database/DBAPI combination.

The general structure can be illustrated as follows:

.. image:: sqla_engine_arch.png

Where above, an :class:`~sqlalchemy.engine.base.Engine` references both a
:class:`~sqlalchemy.engine.base.Dialect` and a :class:`~sqlalchemy.pool.Pool`,
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
====================

SQLAlchemy includes many :class:`~sqlalchemy.engine.base.Dialect` implementations for various 
backends; each is described as its own package in the :ref:`sqlalchemy.dialects_toplevel` package.  A 
SQLAlchemy dialect always requires that an appropriate DBAPI driver is installed.

The table below summarizes the state of DBAPI support in SQLAlchemy 0.7.  The values 
translate as:

* yes / Python platform - The SQLAlchemy dialect is mostly or fully operational on the target platform.
* yes / OS platform - The DBAPI supports that platform.
* no / Python platform - The DBAPI does not support that platform, or there is no SQLAlchemy dialect support.
* no / OS platform - The DBAPI does not support that platform.
* partial - the DBAPI is partially usable on the target platform but has major unresolved issues.
* development - a development version of the dialect exists, but is not yet usable.
* thirdparty - the dialect itself is maintained by a third party, who should be consulted for
  information on current support.
* \* - indicates the given DBAPI is the "default" for SQLAlchemy, i.e. when just the database name is specified

=========================  ===========================  ===========  ===========   ===========  =================  ============
Driver                     Connect string               Py2K         Py3K          Jython       Unix               Windows
=========================  ===========================  ===========  ===========   ===========  =================  ============
**DB2/Informix IDS**
ibm-db_                    thirdparty                   thirdparty   thirdparty    thirdparty   thirdparty         thirdparty
**Drizzle**
mysql-python_              ``drizzle+mysqldb``\*        yes          development   no           yes                yes
**Firebird / Interbase**
kinterbasdb_               ``firebird+kinterbasdb``\*   yes          development   no           yes                yes
**Informix**
informixdb_                ``informix+informixdb``\*    yes          development   no           unknown            unknown
**MaxDB**
sapdb_                     ``maxdb+sapdb``\*            development  development   no           yes                unknown
**Microsoft Access**
pyodbc_                    ``access+pyodbc``\*          development  development   no           unknown            yes
**Microsoft SQL Server**
adodbapi_                  ``mssql+adodbapi``           development  development   no           no                 yes
`jTDS JDBC Driver`_        ``mssql+zxjdbc``             no           no            development  yes                yes
mxodbc_                    ``mssql+mxodbc``             yes          development   no           yes with FreeTDS_  yes
pyodbc_                    ``mssql+pyodbc``\*           yes          development   no           yes with FreeTDS_  yes
pymssql_                   ``mssql+pymssql``            yes          development   no           yes                yes
**MySQL**
`MySQL Connector/J`_       ``mysql+zxjdbc``             no           no            yes          yes                yes
`MySQL Connector/Python`_  ``mysql+mysqlconnector``     yes          yes           no           yes                yes
mysql-python_              ``mysql+mysqldb``\*          yes          development   no           yes                yes
OurSQL_                    ``mysql+oursql``             yes          yes           no           yes                yes
pymysql_                   ``mysql+pymysql``            yes          development   no           yes                yes
**Oracle**
cx_oracle_                 ``oracle+cx_oracle``\*       yes          development   no           yes                yes
`Oracle JDBC Driver`_      ``oracle+zxjdbc``            no           no            yes          yes                yes
**Postgresql**
pg8000_                    ``postgresql+pg8000``        yes          yes           no           yes                yes
`PostgreSQL JDBC Driver`_  ``postgresql+zxjdbc``        no           no            yes          yes                yes
psycopg2_                  ``postgresql+psycopg2``\*    yes          yes           no           yes                yes
pypostgresql_              ``postgresql+pypostgresql``  no           yes           no           yes                yes
**SQLite**
pysqlite_                  ``sqlite+pysqlite``\*        yes          yes           no           yes                yes
sqlite3_                   ``sqlite+pysqlite``\*        yes          yes           no           yes                yes
**Sybase ASE**
mxodbc_                    ``sybase+mxodbc``            development  development   no           yes                yes
pyodbc_                    ``sybase+pyodbc``\*          partial      development   no           unknown            unknown
python-sybase_             ``sybase+pysybase``          yes [1]_     development   no           yes                yes
=========================  ===========================  ===========  ===========   ===========  =================  ============

.. [1] The Sybase dialect currently lacks the ability to reflect tables.
.. _psycopg2: http://www.initd.org/
.. _pg8000: http://pybrary.net/pg8000/
.. _pypostgresql: http://python.projects.postgresql.org/
.. _mysql-python: http://sourceforge.net/projects/mysql-python
.. _MySQL Connector/Python: https://launchpad.net/myconnpy
.. _OurSQL: http://packages.python.org/oursql/
.. _pymysql: http://code.google.com/p/pymysql/
.. _PostgreSQL JDBC Driver: http://jdbc.postgresql.org/
.. _sqlite3: http://docs.python.org/library/sqlite3.html
.. _pysqlite: http://pypi.python.org/pypi/pysqlite/
.. _MySQL Connector/J: http://dev.mysql.com/downloads/connector/j/
.. _cx_Oracle: http://cx-oracle.sourceforge.net/
.. _Oracle JDBC Driver: http://www.oracle.com/technology/software/tech/java/sqlj_jdbc/index.html
.. _kinterbasdb:  http://firebirdsql.org/index.php?op=devel&sub=python
.. _pyodbc: http://code.google.com/p/pyodbc/
.. _mxodbc: http://www.egenix.com/products/python/mxODBC/
.. _FreeTDS: http://www.freetds.org/
.. _adodbapi: http://adodbapi.sourceforge.net/
.. _pymssql: http://code.google.com/p/pymssql/
.. _jTDS JDBC Driver: http://jtds.sourceforge.net/
.. _ibm-db: http://code.google.com/p/ibm-db/
.. _informixdb: http://informixdb.sourceforge.net/
.. _sapdb: http://www.sapdb.org/sapdbapi.html
.. _python-sybase: http://python-sybase.sourceforge.net/

Further detail on dialects is available at :ref:`dialect_toplevel`.


.. _create_engine_args:

Engine Creation API
===================

Keyword options can also be specified to :func:`~sqlalchemy.create_engine`,
following the string URL as follows:

.. sourcecode:: python+sql

    db = create_engine('postgresql://...', encoding='latin1', echo=True)

.. autofunction:: sqlalchemy.create_engine

.. autofunction:: sqlalchemy.engine_from_config

Database Urls
=============

SQLAlchemy indicates the source of an Engine strictly via `RFC-1738
<http://rfc.net/rfc1738.html>`_ style URLs, combined with optional keyword
arguments to specify options for the Engine. The form of the URL is::

    dialect+driver://username:password@host:port/database

Dialect names include the identifying name of the SQLAlchemy dialect which
include ``sqlite``, ``mysql``, ``postgresql``, ``oracle``, ``mssql``, and
``firebird``. The drivername is the name of the DBAPI to be used to connect to
the database using all lowercase letters. If not specified, a "default" DBAPI
will be imported if available - this default is typically the most widely
known driver available for that backend (i.e. cx_oracle, pysqlite/sqlite3,
psycopg2, mysqldb). For Jython connections, specify the `zxjdbc` driver, which
is the JDBC-DBAPI bridge included with Jython.

.. sourcecode:: python+sql

    # postgresql - psycopg2 is the default driver.
    pg_db = create_engine('postgresql://scott:tiger@localhost/mydatabase')
    pg_db = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')
    pg_db = create_engine('postgresql+pg8000://scott:tiger@localhost/mydatabase')
    pg_db = create_engine('postgresql+pypostgresql://scott:tiger@localhost/mydatabase')

    # postgresql on Jython
    pg_db = create_engine('postgresql+zxjdbc://scott:tiger@localhost/mydatabase')

    # mysql - MySQLdb (mysql-python) is the default driver
    mysql_db = create_engine('mysql://scott:tiger@localhost/foo')
    mysql_db = create_engine('mysql+mysqldb://scott:tiger@localhost/foo')

    # mysql on Jython
    mysql_db = create_engine('mysql+zxjdbc://localhost/foo')

    # mysql with pyodbc (buggy)
    mysql_db = create_engine('mysql+pyodbc://scott:tiger@some_dsn')

    # oracle - cx_oracle is the default driver
    oracle_db = create_engine('oracle://scott:tiger@127.0.0.1:1521/sidname')

    # oracle via TNS name
    oracle_db = create_engine('oracle+cx_oracle://scott:tiger@tnsname')

    # mssql using ODBC datasource names.  PyODBC is the default driver.
    mssql_db = create_engine('mssql://mydsn')
    mssql_db = create_engine('mssql+pyodbc://mydsn')
    mssql_db = create_engine('mssql+adodbapi://mydsn')
    mssql_db = create_engine('mssql+pyodbc://username:password@mydsn')

SQLite connects to file based databases. The same URL format is used, omitting
the hostname, and using the "file" portion as the filename of the database.
This has the effect of four slashes being present for an absolute file path::

    # sqlite://<nohostname>/<path>
    # where <path> is relative:
    sqlite_db = create_engine('sqlite:///foo.db')

    # or absolute, starting with a slash:
    sqlite_db = create_engine('sqlite:////absolute/path/to/foo.db')

To use a SQLite ``:memory:`` database, specify an empty URL::

    sqlite_memory_db = create_engine('sqlite://')

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

.. autoclass:: sqlalchemy.engine.url.URL
    :members:

.. _custom_dbapi_args:

Custom DBAPI connect() arguments
=================================

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
====================

Python's standard `logging
<http://www.python.org/doc/lib/module-logging.html>`_ module is used to
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
on :class:`~sqlalchemy.engine.base.Engine`, when set to ``True``, will first
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

The logger name of instance such as an :class:`~sqlalchemy.engine.base.Engine`
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
