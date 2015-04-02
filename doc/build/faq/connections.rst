Connections / Engines
=====================

.. contents::
    :local:
    :class: faq
    :backlinks: none


How do I configure logging?
---------------------------

See :ref:`dbengine_logging`.

How do I pool database connections?   Are my connections pooled?
----------------------------------------------------------------

SQLAlchemy performs application-level connection pooling automatically
in most cases.  With the exception of SQLite, a :class:`.Engine` object
refers to a :class:`.QueuePool` as a source of connectivity.

For more detail, see :ref:`engines_toplevel` and :ref:`pooling_toplevel`.

How do I pass custom connect arguments to my database API?
-----------------------------------------------------------

The :func:`.create_engine` call accepts additional arguments either
directly via the ``connect_args`` keyword argument::

    e = create_engine("mysql://scott:tiger@localhost/test",
                        connect_args={"encoding": "utf8"})

Or for basic string and integer arguments, they can usually be specified
in the query string of the URL::

    e = create_engine("mysql://scott:tiger@localhost/test?encoding=utf8")

.. seealso::

    :ref:`custom_dbapi_args`

"MySQL Server has gone away"
----------------------------

There are two major causes for this error:

1. The MySQL client closes connections which have been idle for a set period
of time, defaulting to eight hours.   This can be avoided by using the ``pool_recycle``
setting with :func:`.create_engine`, described at :ref:`mysql_connection_timeouts`.

2. Usage of the MySQLdb :term:`DBAPI`, or a similar DBAPI, in a non-threadsafe manner, or in an otherwise
inappropriate way.   The MySQLdb connection object is not threadsafe - this expands
out to any SQLAlchemy system that links to a single connection, which includes the ORM
:class:`.Session`.  For background
on how :class:`.Session` should be used in a multithreaded environment,
see :ref:`session_faq_threadsafe`.

Why does SQLAlchemy issue so many ROLLBACKs?
---------------------------------------------

SQLAlchemy currently assumes DBAPI connections are in "non-autocommit" mode -
this is the default behavior of the Python database API, meaning it
must be assumed that a transaction is always in progress. The
connection pool issues ``connection.rollback()`` when a connection is returned.
This is so that any transactional resources remaining on the connection are
released. On a database like Postgresql or MSSQL where table resources are
aggressively locked, this is critical so that rows and tables don't remain
locked within connections that are no longer in use. An application can
otherwise hang. It's not just for locks, however, and is equally critical on
any database that has any kind of transaction isolation, including MySQL with
InnoDB. Any connection that is still inside an old transaction will return
stale data, if that data was already queried on that connection within
isolation. For background on why you might see stale data even on MySQL, see
http://dev.mysql.com/doc/refman/5.1/en/innodb-transaction-model.html

I'm on MyISAM - how do I turn it off?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The behavior of the connection pool's connection return behavior can be
configured using ``reset_on_return``::

    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool

    engine = create_engine('mysql://scott:tiger@localhost/myisam_database', pool=QueuePool(reset_on_return=False))

I'm on SQL Server - how do I turn those ROLLBACKs into COMMITs?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``reset_on_return`` accepts the values ``commit``, ``rollback`` in addition
to ``True``, ``False``, and ``None``.   Setting to ``commit`` will cause
a COMMIT as any connection is returned to the pool::

    engine = create_engine('mssql://scott:tiger@mydsn', pool=QueuePool(reset_on_return='commit'))


I am using multiple connections with a SQLite database (typically to test transaction operation), and my test program is not working!
----------------------------------------------------------------------------------------------------------------------------------------------------------

If using a SQLite ``:memory:`` database, or a version of SQLAlchemy prior
to version 0.7, the default connection pool is the :class:`.SingletonThreadPool`,
which maintains exactly one SQLite connection per thread.  So two
connections in use in the same thread will actually be the same SQLite
connection.   Make sure you're not using a :memory: database and
use :class:`.NullPool`, which is the default for non-memory databases in
current SQLAlchemy versions.

.. seealso::

    :ref:`pysqlite_threading_pooling` - info on PySQLite's behavior.

How do I get at the raw DBAPI connection when using an Engine?
--------------------------------------------------------------

With a regular SA engine-level Connection, you can get at a pool-proxied
version of the DBAPI connection via the :attr:`.Connection.connection` attribute on
:class:`.Connection`, and for the really-real DBAPI connection you can call the
:attr:`.ConnectionFairy.connection` attribute on that - but there should never be any need to access
the non-pool-proxied DBAPI connection, as all methods are proxied through::

    engine = create_engine(...)
    conn = engine.connect()
    conn.connection.<do DBAPI things>
    cursor = conn.connection.cursor(<DBAPI specific arguments..>)

You must ensure that you revert any isolation level settings or other
operation-specific settings on the connection back to normal before returning
it to the pool.

As an alternative to reverting settings, you can call the :meth:`.Connection.detach` method on
either :class:`.Connection` or the proxied connection, which will de-associate
the connection from the pool such that it will be closed and discarded
when :meth:`.Connection.close` is called::

    conn = engine.connect()
    conn.detach()  # detaches the DBAPI connection from the connection pool
    conn.connection.<go nuts>
    conn.close()  # connection is closed for real, the pool replaces it with a new connection
