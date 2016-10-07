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
released. On a database like PostgreSQL or MSSQL where table resources are
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

How do I use engines / connections / sessions with Python multiprocessing, or os.fork()?
----------------------------------------------------------------------------------------

The key goal with multiple python processes is to prevent any database connections
from being shared across processes.   Depending on specifics of the driver and OS,
the issues that arise here range from non-working connections to socket connections that
are used by multiple processes concurrently, leading to broken messaging (the latter
case is typically the most common).

The SQLAlchemy :class:`.Engine` object refers to a connection pool of existing
database connections.  So when this object is replicated to a child process,
the goal is to ensure that no database connections are carried over.  There
are three general approaches to this:

1. Disable pooling using :class:`.NullPool`.  This is the most simplistic,
   one shot system that prevents the :class:`.Engine` from using any connection
   more than once.

2. Call :meth:`.Engine.dispose` on any given :class:`.Engine` as soon one is
   within the new process.  In Python multiprocessing, constructs such as
   ``multiprocessing.Pool`` include "initializer" hooks which are a place
   that this can be performed; otherwise at the top of where ``os.fork()``
   or where the ``Process`` object begins the child fork, a single call
   to :meth:`.Engine.dispose` will ensure any remaining connections are flushed.

3. An event handler can be applied to the connection pool that tests for connections
   being shared across process boundaries, and invalidates them.  This looks like
   the following::

        import os
        import warnings

        from sqlalchemy import event
        from sqlalchemy import exc

        def add_engine_pidguard(engine):
            """Add multiprocessing guards.

            Forces a connection to be reconnected if it is detected
            as having been shared to a sub-process.

            """

            @event.listens_for(engine, "connect")
            def connect(dbapi_connection, connection_record):
                connection_record.info['pid'] = os.getpid()

            @event.listens_for(engine, "checkout")
            def checkout(dbapi_connection, connection_record, connection_proxy):
                pid = os.getpid()
                if connection_record.info['pid'] != pid:
                    # substitute log.debug() or similar here as desired
                    warnings.warn(
                        "Parent process %(orig)s forked (%(newproc)s) with an open "
                        "database connection, "
                        "which is being discarded and recreated." %
                        {"newproc": pid, "orig": connection_record.info['pid']})
                    connection_record.connection = connection_proxy.connection = None
                    raise exc.DisconnectionError(
                        "Connection record belongs to pid %s, "
                        "attempting to check out in pid %s" %
                        (connection_record.info['pid'], pid)
                    )

   These events are applied to an :class:`.Engine` as soon as its created::

        engine = create_engine("...")

        add_engine_pidguard(engine)

The above strategies will accommodate the case of an :class:`.Engine`
being shared among processes.  However, for the case of a transaction-active
:class:`.Session` or :class:`.Connection` being shared, there's no automatic
fix for this; an application needs to ensure a new child process only
initiate new :class:`.Connection` objects and transactions, as well as ORM
:class:`.Session` objects.  For a :class:`.Session` object, technically
this is only needed if the session is currently transaction-bound, however
the scope of a single :class:`.Session` is in any case intended to be
kept within a single call stack in any case (e.g. not a global object, not
shared between processes or threads).
