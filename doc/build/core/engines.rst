.. _engines_toplevel:

====================
Engine Configuration
====================

The :class:`_engine.Engine` is the starting point for any SQLAlchemy application. It's
"home base" for the actual database and its :term:`DBAPI`, delivered to the SQLAlchemy
application through a connection pool and a :class:`.Dialect`, which describes how
to talk to a specific kind of database/DBAPI combination.

The general structure can be illustrated as follows:

.. image:: sqla_engine_arch.png

Where above, an :class:`_engine.Engine` references both a
:class:`.Dialect` and a :class:`_pool.Pool`,
which together interpret the DBAPI's module functions as well as the behavior
of the database.

Creating an engine is just a matter of issuing a single call,
:func:`_sa.create_engine()`::

    from sqlalchemy import create_engine
    engine = create_engine('postgresql://scott:tiger@localhost:5432/mydatabase')

The above engine creates a :class:`.Dialect` object tailored towards
PostgreSQL, as well as a :class:`_pool.Pool` object which will establish a DBAPI
connection at ``localhost:5432`` when a connection request is first received.
Note that the :class:`_engine.Engine` and its underlying :class:`_pool.Pool` do **not**
establish the first actual DBAPI connection until the :meth:`_engine.Engine.connect`
method is called, or an operation which is dependent on this method such as
:meth:`_engine.Engine.execute` is invoked. In this way, :class:`_engine.Engine` and
:class:`_pool.Pool` can be said to have a *lazy initialization* behavior.

The :class:`_engine.Engine`, once created, can either be used directly to interact with the database,
or can be passed to a :class:`.Session` object to work with the ORM.   This section
covers the details of configuring an :class:`_engine.Engine`.   The next section, :ref:`connections_toplevel`,
will detail the usage API of the :class:`_engine.Engine` and similar, typically for non-ORM
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

The :func:`_sa.create_engine` function produces an :class:`_engine.Engine` object based
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

As the URL is like any other URL, special characters such as those that
may be used in the password need to be URL encoded.   Below is an example
of a URL that includes the password ``"kx%jj5/g"``::

  postgresql+pg8000://dbuser:kx%25jj5%2Fg@pghost10/appdb

The encoding for the above password can be generated using ``urllib``::

  >>> import urllib.parse
  >>> urllib.parse.quote_plus("kx%jj5/g")
  'kx%25jj5%2Fg'

Examples for common connection styles follow below.  For a full index of
detailed information on all included dialects as well as links to third-party
dialects, see :ref:`dialect_toplevel`.

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

    # mysqlclient (a maintained fork of MySQL-Python)
    engine = create_engine('mysql+mysqldb://scott:tiger@localhost/foo')

    # PyMySQL
    engine = create_engine('mysql+pymysql://scott:tiger@localhost/foo')

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

    # Unix/Mac - 4 initial slashes in total
    engine = create_engine('sqlite:////absolute/path/to/foo.db')

    # Windows
    engine = create_engine('sqlite:///C:\\path\\to\\foo.db')

    # Windows alternative using raw string
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

.. autofunction:: sqlalchemy.create_mock_engine

.. autofunction:: sqlalchemy.engine.make_url


.. autoclass:: sqlalchemy.engine.URL
    :members:

    .. py:attribute:: drivername
        :annotation: str

        database backend and driver name, such as
        ``postgresql+psycopg2``

    .. py:attribute::  username
        :annotation: str

        username string

    .. py:attribute::  password
        :annotation: str

        password, which is normally a string but may also be any
        object that has a ``__str__()`` method.

    .. py:attribute::  host
        :annotation: str

        string hostname

    .. py:attribute::  port
        :annotation: int

        integer port number

    .. py:attribute::  database
        :annotation: str

        string database name

    .. py:attribute::  query
        :annotation: Mapping[str, Union[str, Sequence[str]]]

        an immutable mapping representing the query string.  contains strings
        for keys and either strings or tuples of strings for values, e.g.::

            >>> from sqlalchemy.engine import make_url
            >>> url = make_url("postgresql://user:pass@host/dbname?alt_host=host1&alt_host=host2&ssl_cipher=%2Fpath%2Fto%2Fcrt")
            >>> url.query
            immutabledict({'alt_host': ('host1', 'host2'), 'ssl_cipher': '/path/to/crt'})

         To create a mutable copy of this mapping, use the ``dict`` constructor::

            mutable_query_opts = dict(url.query)

        .. seealso::

          :attr:`_engine.URL.normalized_query` - normalizes all values into sequences
          for consistent processing

          Methods for altering the contents of :attr:`_engine.URL.query`:

          :meth:`_engine.URL.update_query_dict`

          :meth:`_engine.URL.update_query_string`

          :meth:`_engine.URL.update_query_pairs`

          :meth:`_engine.URL.difference_update_query`

Pooling
=======

The :class:`_engine.Engine` will ask the connection pool for a
connection when the ``connect()`` or ``execute()`` methods are called. The
default connection pool, :class:`~.QueuePool`, will open connections to the
database on an as-needed basis. As concurrent statements are executed,
:class:`.QueuePool` will grow its pool of connections to a
default size of five, and will allow a default "overflow" of ten. Since the
:class:`_engine.Engine` is essentially "home base" for the
connection pool, it follows that you should keep a single
:class:`_engine.Engine` per database established within an
application, rather than creating a new one for each connection.

.. note::

   :class:`.QueuePool` is not used by default for SQLite engines.  See
   :ref:`sqlite_toplevel` for details on SQLite connection pool usage.

For more information on connection pooling, see :ref:`pooling_toplevel`.


.. _custom_dbapi_args:

Custom DBAPI connect() arguments / on-connect routines
=======================================================

For cases where special connection methods are needed, in the vast majority
of cases, it is most appropriate to use one of several hooks at the
:func:`_sa.create_engine` level in order to customize this process. These
are described in the following sub-sections.

Special Keyword Arguments Passed to dbapi.connect()
---------------------------------------------------

All Python DBAPIs accept additional arguments beyond the basics of connecting.
Common parameters include those to specify character set encodings and timeout
values; more complex data includes special DBAPI constants and objects and SSL
sub-parameters. There are two rudimentary means of passing these arguments
without complexity.

Add Parameters to the URL Query string
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Simple string values, as well as some numeric values and boolean flags, may be
often specified in the query string of the URL directly. A common example of
this is DBAPIs that accept an argument ``encoding`` for character encodings,
such as most MySQL DBAPIs::

    engine = create_engine(
        "mysql+pymysql://user:pass@host/test?charset=utf8mb4"
    )

The advantage of using the query string is that additional DBAPI options may be
specified in configuration files in a manner that's portable to the DBAPI
specified in the URL. The specific parameters passed through at this level vary
by SQLAlchemy dialect. Some dialects pass all arguments through as strings,
while others will parse for specific datatypes and move parameters to different
places, such as into driver-level DSNs and connect strings. As per-dialect
behavior in this area currently varies, the dialect documentation should be
consulted for the specific dialect in use to see if particular parameters are
supported at this level.

.. tip::

  A general technique to display the exact arguments passed to the DBAPI
  for a given URL may be performed using the :meth:`.Dialect.create_connect_args`
  method directly as follows::

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("mysql+pymysql://some_user:some_pass@some_host/test?charset=utf8mb4")
    >>> args, kwargs = engine.dialect.create_connect_args(engine.url)
    >>> args, kwargs
    ([], {'host': 'some_host', 'database': 'test', 'user': 'some_user', 'password': 'some_pass', 'charset': 'utf8mb4', 'client_flag': 2})

  The above ``args, kwargs`` pair is normally passed to the DBAPI as
  ``dbapi.connect(*args, **kwargs)``.

Use the connect_args dictionary parameter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A more general system of passing any parameter to the ``dbapi.connect()``
function that is guaranteed to pass all parameters at all times is the
:paramref:`_sa.create_engine.connect_args` dictionary parameter. This may be
used for parameters that are otherwise not handled by the dialect when added to
the query string, as well as when special sub-structures or objects must be
passed to the DBAPI. Sometimes it's just that a particular flag must be sent as
the ``True`` symbol and the SQLAlchemy dialect is not aware of this keyword
argument to coerce it from its string form as presented in the URL. Below
illustrates the use of a psycopg2 "connection factory" that replaces the
underlying implementation the connection::


    engine = create_engine(
        "postgresql://user:pass@hostname/dbname",
        connect_args={"connection_factory": MyConnectionFactory}
    )

Another example is the pyodbc "timeout" parameter::

    engine = create_engine(
      "mssql+pyodbc://user:pass@sqlsrvr?driver=ODBC+Driver+13+for+SQL+Server",
      connect_args={"timeout": 30}
    )

The above example also illustrates that both URL "query string" parameters as
well as :paramref:`_sa.create_engine.connect_args` may be used at the same
time; in the case of pyodbc, the "driver" keyword has special meaning
within the URL.

Controlling how parameters are passed to the DBAPI connect() function
---------------------------------------------------------------------

Beyond manipulating the parameters passed to ``connect()``, we can further
customize how the DBAPI ``connect()`` function itself is called using the
:meth:`.DialectEvents.do_connect` event hook. This hook is passed the full
``*args, **kwargs`` that the dialect would send to ``connect()``. These
collections can then be modified in place to alter how they are used::

    from sqlalchemy import event

    engine = create_engine("postgresql://user:pass@hostname/dbname")

    @event.listens_for(engine, "do_connect")
    def receive_do_connect(dialect, conn_rec, cargs, cparams):
        cparams['connection_factory'] = MyConnectionFactory

Modifying the DBAPI connection after connect, or running commands after connect
-------------------------------------------------------------------------------

For a DBAPI connection that SQLAlchemy creates without issue, but where we
would like to modify the completed connection before it's actually used, such
as for setting special flags or running certain commands, the
:meth:`.PoolEvents.connect` event hook is the most appropriate hook.  This
hook is called for every new connection created, before it is used by
SQLAlchemy::

    from sqlalchemy import event

    engine = create_engine(
        "postgresql://user:pass@hostname/dbname"
    )

    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("SET some session variables")
        cursor.close()


Fully Replacing the DBAPI ``connect()`` function
------------------------------------------------

Finally, the :meth:`.DialectEvents.do_connect` event hook can also allow us to take
over the connection process entirely by establishing the connection
and returning it::

    from sqlalchemy import event

    engine = create_engine(
        "postgresql://user:pass@hostname/dbname"
    )

    @event.listens_for(engine, "do_connect")
    def receive_do_connect(dialect, conn_rec, cargs, cparams):
        # return the new DBAPI connection with whatever we'd like to
        # do
        return psycopg2.connect(*cargs, **cparams)

The :meth:`.DialectEvents.do_connect` hook supersedes the previous
:paramref:`_sa.create_engine.creator` hook, which remains available.
:meth:`.DialectEvents.do_connect` has the distinct advantage that the
complete arguments parsed from the URL are also passed to the user-defined
function which is not the case with :paramref:`_sa.create_engine.creator`.

.. _dbengine_logging:

Configuring Logging
===================

Python's standard `logging
<http://docs.python.org/library/logging.html>`_ module is used to
implement informational and debug log output with SQLAlchemy. This allows
SQLAlchemy's logging to integrate in a standard way with other applications
and libraries.   There are also two parameters
:paramref:`_sa.create_engine.echo` and :paramref:`_sa.create_engine.echo_pool`
present on :func:`_sa.create_engine` which allow immediate logging to ``sys.stdout``
for the purposes of local development; these parameters ultimately interact
with the regular Python loggers described below.

This section assumes familiarity with the above linked logging module. All
logging performed by SQLAlchemy exists underneath the ``sqlalchemy``
namespace, as used by ``logging.getLogger('sqlalchemy')``. When logging has
been configured (i.e. such as via ``logging.basicConfig()``), the general
namespace of SA loggers that can be turned on is as follows:

* ``sqlalchemy.engine`` - controls SQL echoing.  Set to ``logging.INFO`` for
  SQL query output, ``logging.DEBUG`` for query + result set output.  These
  settings are equivalent to ``echo=True`` and ``echo="debug"`` on
  :paramref:`_sa.create_engine.echo`, respectively.

* ``sqlalchemy.pool`` - controls connection pool logging.  Set to
  ``logging.INFO`` to log connection invalidation and recycle events; set to
  ``logging.DEBUG`` to additionally log all pool checkins and checkouts.
  These settings are equivalent to ``pool_echo=True`` and ``pool_echo="debug"``
  on :paramref:`_sa.create_engine.echo_pool`, respectively.

* ``sqlalchemy.dialects`` - controls custom logging for SQL dialects, to the
  extend that logging is used within specific dialects, which is generally
  minimal.

* ``sqlalchemy.orm`` - controls logging of various ORM functions to the extent
  that logging is used within the ORM, which is generally minimal.  Set to
  ``logging.INFO`` to log some top-level information on mapper configurations.

For example, to log SQL queries using Python logging instead of the
``echo=True`` flag::

    import logging

    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

By default, the log level is set to ``logging.WARN`` within the entire
``sqlalchemy`` namespace so that no log operations occur, even within an
application that has logging enabled otherwise.

.. note::

   The SQLAlchemy :class:`_engine.Engine` conserves Python function call
   overhead by only emitting log statements when the current logging level is
   detected as ``logging.INFO`` or ``logging.DEBUG``.  It only checks this
   level when a new connection is procured from the connection pool.  Therefore
   when changing the logging configuration for an already-running application,
   any :class:`_engine.Connection` that's currently active, or more commonly a
   :class:`~.orm.session.Session` object that's active in a transaction, won't
   log any SQL according to the new configuration until a new
   :class:`_engine.Connection` is procured (in the case of
   :class:`~.orm.session.Session`, this is after the current transaction ends
   and a new one begins).

More on the Echo Flag
---------------------

As mentioned previously, the :paramref:`_sa.create_engine.echo` and :paramref:`_sa.create_engine.echo_pool`
parameters are a shortcut to immediate logging to ``sys.stdout``::


    >>> from sqlalchemy import create_engine, text
    >>> e = create_engine("sqlite://", echo=True, echo_pool='debug')
    >>> with e.connect() as conn:
    ...    print(conn.scalar(text("select 'hi'")))
    ...
    2020-10-24 12:54:57,701 DEBUG sqlalchemy.pool.impl.SingletonThreadPool Created new connection <sqlite3.Connection object at 0x7f287819ac60>
    2020-10-24 12:54:57,701 DEBUG sqlalchemy.pool.impl.SingletonThreadPool Connection <sqlite3.Connection object at 0x7f287819ac60> checked out from pool
    2020-10-24 12:54:57,702 INFO sqlalchemy.engine.Engine select 'hi'
    2020-10-24 12:54:57,702 INFO sqlalchemy.engine.Engine ()
    hi
    2020-10-24 12:54:57,703 DEBUG sqlalchemy.pool.impl.SingletonThreadPool Connection <sqlite3.Connection object at 0x7f287819ac60> being returned to pool
    2020-10-24 12:54:57,704 DEBUG sqlalchemy.pool.impl.SingletonThreadPool Connection <sqlite3.Connection object at 0x7f287819ac60> rollback-on-return

Use of these flags is roughly equivalent to::

    import logging
    logging.basicConfig()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)

It's important to note that these two flags work **independently** of any
existing logging configuration, and will make use of ``logging.basicConfig()``
unconditionally.  This has the effect of being configured **in addition** to
any existing logger configurations. Therefore, **when configuring logging
explicitly, ensure all echo flags are set to False at all times**, to avoid
getting duplicate log lines.

Setting the Logging Name
-------------------------

The logger name of instance such as an :class:`~sqlalchemy.engine.Engine` or
:class:`~sqlalchemy.pool.Pool` defaults to using a truncated hex identifier
string. To set this to a specific name, use the
:paramref:`_sa.create_engine.logging_name` and
:paramref:`_sa.create_engine.pool_logging_name`  with
:func:`sqlalchemy.create_engine`::

    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy import text
    >>> e = create_engine("sqlite://", echo=True, logging_name='myengine')
    >>> with e.connect() as conn:
    ...     conn.execute(text("select 'hi'"))
    ...
    2020-10-24 12:47:04,291 INFO sqlalchemy.engine.Engine.myengine select 'hi'
    2020-10-24 12:47:04,292 INFO sqlalchemy.engine.Engine.myengine ()

.. _dbengine_logging_tokens:

Setting Per-Connection / Sub-Engine Tokens
------------------------------------------

.. versionadded:: 1.4.0b2


While the logging name is appropriate to establish on an
:class:`_engine.Engine` object that is long lived, it's not flexible enough
to accommodate for an arbitrarily large list of names, for the case of
tracking individual connections and/or transactions in log messages.

For this use case, the log message itself generated by the
:class:`_engine.Connection` and :class:`_engine.Result` objects may be
augmented with additional tokens such as transaction or request identifiers.
The :paramref:`_engine.Connection.execution_options.logging_token` parameter
accepts a string argument that may be used to establish per-connection tracking
tokens::

    >>> from sqlalchemy import create_engine
    >>> e = create_engine("sqlite://", echo="debug")
    >>> with e.connect().execution_options(logging_token="track1") as conn:
    ...     conn.execute("select 1").all()
    2021-02-03 11:48:45,754 INFO sqlalchemy.engine.Engine [track1] select 1
    2021-02-03 11:48:45,754 INFO sqlalchemy.engine.Engine [track1] [raw sql] ()
    2021-02-03 11:48:45,754 DEBUG sqlalchemy.engine.Engine [track1] Col ('1',)
    2021-02-03 11:48:45,755 DEBUG sqlalchemy.engine.Engine [track1] Row (1,)

The :paramref:`_engine.Connection.execution_options.logging_token` parameter
may also be established on engines or sub-engines via
:paramref:`_sa.create_engine.execution_options` or :meth:`_engine.Engine.execution_options`.
This may be useful to apply different logging tokens to different components
of an application without creating new engines::

    >>> from sqlalchemy import create_engine
    >>> e = create_engine("sqlite://", echo="debug")
    >>> e1 = e.execution_options(logging_token="track1")
    >>> e2 = e.execution_options(logging_token="track2")
    >>> with e1.connect() as conn:
    ...     conn.execute("select 1").all()
    2021-02-03 11:51:08,960 INFO sqlalchemy.engine.Engine [track1] select 1
    2021-02-03 11:51:08,960 INFO sqlalchemy.engine.Engine [track1] [raw sql] ()
    2021-02-03 11:51:08,960 DEBUG sqlalchemy.engine.Engine [track1] Col ('1',)
    2021-02-03 11:51:08,961 DEBUG sqlalchemy.engine.Engine [track1] Row (1,)

    >>> with e2.connect() as conn:
    ...     conn.execute("select 2").all()
    2021-02-03 11:52:05,518 INFO sqlalchemy.engine.Engine [track2] Select 1
    2021-02-03 11:52:05,519 INFO sqlalchemy.engine.Engine [track2] [raw sql] ()
    2021-02-03 11:52:05,520 DEBUG sqlalchemy.engine.Engine [track2] Col ('1',)
    2021-02-03 11:52:05,520 DEBUG sqlalchemy.engine.Engine [track2] Row (1,)


Hiding Parameters
------------------

The logging emitted by :class:`_engine.Engine` also indicates an excerpt
of the SQL parameters that are present for a particular statement.  To prevent
these parameters from being logged for privacy purposes, enable the
:paramref:`_sa.create_engine.hide_parameters` flag::

    >>> e = create_engine("sqlite://", echo=True, hide_parameters=True)
    >>> with e.connect() as conn:
    ...     conn.execute(text("select :some_private_name"), {"some_private_name": "pii"})
    ...
    2020-10-24 12:48:32,808 INFO sqlalchemy.engine.Engine select ?
    2020-10-24 12:48:32,808 INFO sqlalchemy.engine.Engine [SQL parameters hidden due to hide_parameters=True]

