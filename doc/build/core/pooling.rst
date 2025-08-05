.. _pooling_toplevel:

Connection Pooling
==================

.. module:: sqlalchemy.pool

A connection pool is a standard technique used to maintain
long running connections in memory for efficient re-use,
as well as to provide
management for the total number of connections an application
might use simultaneously.

Particularly for
server-side web applications, a connection pool is the standard way to
maintain a "pool" of active database connections in memory which are
reused across requests.

SQLAlchemy includes several connection pool implementations
which integrate with the :class:`_engine.Engine`.  They can also be used
directly for applications that want to add pooling to an otherwise
plain DBAPI approach.

Connection Pool Configuration
-----------------------------

The :class:`_engine.Engine` returned by the
:func:`~sqlalchemy.create_engine` function in most cases has a :class:`.QueuePool`
integrated, pre-configured with reasonable pooling defaults.  If
you're reading this section only to learn how to enable pooling - congratulations!
You're already done.

The most common :class:`.QueuePool` tuning parameters can be passed
directly to :func:`~sqlalchemy.create_engine` as keyword arguments:
``pool_size``, ``max_overflow``, ``pool_recycle`` and
``pool_timeout``.  For example::

    engine = create_engine(
        "postgresql+psycopg2://me@localhost/mydb", pool_size=20, max_overflow=0
    )

All SQLAlchemy pool implementations have in common
that none of them "pre create" connections - all implementations wait
until first use before creating a connection.   At that point, if
no additional concurrent checkout requests for more connections
are made, no additional connections are created.   This is why it's perfectly
fine for :func:`_sa.create_engine` to default to using a :class:`.QueuePool`
of size five without regard to whether or not the application really needs five connections
queued up - the pool would only grow to that size if the application
actually used five connections concurrently, in which case the usage of a
small pool is an entirely appropriate default behavior.

.. note:: The :class:`.QueuePool` class is **not compatible with asyncio**.
   When using :class:`_asyncio.create_async_engine` to create an instance of
   :class:`.AsyncEngine`, the :class:`_pool.AsyncAdaptedQueuePool` class,
   which makes use of an asyncio-compatible queue implementation, is used
   instead.


.. _pool_switching:

Switching Pool Implementations
------------------------------

The usual way to use a different kind of pool with :func:`_sa.create_engine`
is to use the ``poolclass`` argument.   This argument accepts a class
imported from the ``sqlalchemy.pool`` module, and handles the details
of building the pool for you.   A common use case here is when
connection pooling is to be disabled, which can be achieved by using
the :class:`.NullPool` implementation::

    from sqlalchemy.pool import NullPool

    engine = create_engine(
        "postgresql+psycopg2://scott:tiger@localhost/test", poolclass=NullPool
    )

Using a Custom Connection Function
----------------------------------

See the section :ref:`custom_dbapi_args` for a rundown of the various
connection customization routines.



Constructing a Pool
-------------------

To use a :class:`_pool.Pool` by itself, the ``creator`` function is
the only argument that's required and is passed first, followed
by any additional options::

    import sqlalchemy.pool as pool
    import psycopg2


    def getconn():
        c = psycopg2.connect(user="ed", host="127.0.0.1", dbname="test")
        return c


    mypool = pool.QueuePool(getconn, max_overflow=10, pool_size=5)

DBAPI connections can then be procured from the pool using the
:meth:`_pool.Pool.connect` function. The return value of this method is a DBAPI
connection that's contained within a transparent proxy::

    # get a connection
    conn = mypool.connect()

    # use it
    cursor_obj = conn.cursor()
    cursor_obj.execute("select foo")

The purpose of the transparent proxy is to intercept the ``close()`` call,
such that instead of the DBAPI connection being closed, it is returned to the
pool::

    # "close" the connection.  Returns
    # it to the pool.
    conn.close()

The proxy also returns its contained DBAPI connection to the pool when it is
garbage collected, though it's not deterministic in Python that this occurs
immediately (though it is typical with cPython). This usage is not recommended
however and in particular is not supported with asyncio DBAPI drivers.

.. _pool_reset_on_return:

Reset On Return
---------------

The pool includes "reset on return" behavior which will call the ``rollback()``
method of the DBAPI connection when the connection is returned to the pool.
This is so that any existing transactional state is removed from the
connection, which includes not just uncommitted data but table and row locks as
well. For most DBAPIs, the call to ``rollback()`` is relatively inexpensive.

The "reset on return" feature takes place when a connection is :term:`released`
back to the connection pool.  In modern SQLAlchemy, this reset on return
behavior is shared between the :class:`.Connection` and the :class:`.Pool`,
where the :class:`.Connection` itself, if it releases its transaction upon close,
considers ``.rollback()`` to have been called, and instructs the pool to skip
this step.


Disabling Reset on Return for non-transactional connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For very specific cases where this ``rollback()`` is not useful, such as when
using a connection that is configured for
:ref:`autocommit <dbapi_autocommit_understanding>` or when using a database
that has no ACID capabilities such as the MyISAM engine of MySQL, the
reset-on-return behavior can be disabled, which is typically done for
performance reasons.

As of SQLAlchemy 2.0.43, the :paramref:`.create_engine.skip_autocommit_rollback`
parameter of :func:`.create_engine` provides the most complete means of
preventing ROLLBACK from being emitted while under autocommit mode, as it
blocks the DBAPI ``.rollback()`` method from being called by the dialect
completely::

    autocommit_engine = create_engine(
        "mysql+mysqldb://scott:tiger@mysql80/test",
        skip_autocommit_rollback=True,
        isolation_level="AUTOCOMMIT",
    )

Detail on this pattern is at :ref:`dbapi_autocommit_skip_rollback`.

The :class:`_pool.Pool` itself also has a parameter that can control its
"reset on return" behavior, noting that in modern SQLAlchemy this is not
the only path by which the DBAPI transaction is released, which is the
:paramref:`_pool.Pool.reset_on_return` parameter of :class:`_pool.Pool`, which
is also available from :func:`_sa.create_engine` as
:paramref:`_sa.create_engine.pool_reset_on_return`, passing a value of ``None``.
This pattern looks as below::

    autocommit_engine = create_engine(
        "mysql+mysqldb://scott:tiger@mysql80/test",
        pool_reset_on_return=None,
        isolation_level="AUTOCOMMIT",
    )

The above pattern will still see ROLLBACKs occur however as the :class:`.Connection`
object implicitly starts transaction blocks in the SQLAlchemy 2.0 series,
which still emit ROLLBACK independently of the pool's reset sequence.

Custom Reset-on-Return Schemes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

"reset on return" consisting of a single ``rollback()`` may not be sufficient
for some use cases; in particular, applications which make use of temporary
tables may wish for these tables to be automatically removed on connection
checkin. Some (but notably not all) backends include features that can "reset"
such tables within the scope of a database connection, which may be a desirable
behavior for connection pool reset. Other server resources such as prepared
statement handles and server-side statement caches may persist beyond the
checkin process, which may or may not be desirable, depending on specifics.
Again, some (but again not all) backends may provide for a means of resetting
this state.  The two SQLAlchemy included dialects which are known to have
such reset schemes include Microsoft SQL Server, where an undocumented but
widely known stored procedure called ``sp_reset_connection`` is often used,
and PostgreSQL, which has a well-documented series of commands including
``DISCARD`` ``RESET``, ``DEALLOCATE``, and ``UNLISTEN``.

.. note: next paragraph + example should match mssql/base.py example

The following example illustrates how to replace reset on return with the
Microsoft SQL Server ``sp_reset_connection`` stored procedure, using the
:meth:`.PoolEvents.reset` event hook. The
:paramref:`_sa.create_engine.pool_reset_on_return` parameter is set to ``None``
so that the custom scheme can replace the default behavior completely. The
custom hook implementation calls ``.rollback()`` in any case, as it's usually
important that the DBAPI's own tracking of commit/rollback will remain
consistent with the state of the transaction::

    from sqlalchemy import create_engine
    from sqlalchemy import event

    mssql_engine = create_engine(
        "mssql+pyodbc://scott:tiger^5HHH@mssql2017:1433/test?driver=ODBC+Driver+17+for+SQL+Server",
        # disable default reset-on-return scheme
        pool_reset_on_return=None,
    )


    @event.listens_for(mssql_engine, "reset")
    def _reset_mssql(dbapi_connection, connection_record, reset_state):
        if not reset_state.terminate_only:
            dbapi_connection.execute("{call sys.sp_reset_connection}")

        # so that the DBAPI itself knows that the connection has been
        # reset
        dbapi_connection.rollback()

.. versionchanged:: 2.0.0b3  Added additional state arguments to
   the :meth:`.PoolEvents.reset` event and additionally ensured the event
   is invoked for all "reset" occurrences, so that it's appropriate
   as a place for custom "reset" handlers.   Previous schemes which
   use the :meth:`.PoolEvents.checkin` handler remain usable as well.

.. seealso::

    * :ref:`mssql_reset_on_return` - in the :ref:`mssql_toplevel` documentation
    * :ref:`postgresql_reset_on_return` in the :ref:`postgresql_toplevel` documentation




Logging reset-on-return events
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Logging for pool events including reset on return can be set
``logging.DEBUG``
log level along with the ``sqlalchemy.pool`` logger, or by setting
:paramref:`_sa.create_engine.echo_pool` to ``"debug"`` when using
:func:`_sa.create_engine`::

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("postgresql://scott:tiger@localhost/test", echo_pool="debug")

The above pool will show verbose logging including reset on return::

    >>> c1 = engine.connect()
    DEBUG sqlalchemy.pool.impl.QueuePool Created new connection <connection object ...>
    DEBUG sqlalchemy.pool.impl.QueuePool Connection <connection object ...> checked out from pool
    >>> c1.close()
    DEBUG sqlalchemy.pool.impl.QueuePool Connection <connection object ...> being returned to pool
    DEBUG sqlalchemy.pool.impl.QueuePool Connection <connection object ...> rollback-on-return


Pool Events
-----------

Connection pools support an event interface that allows hooks to execute
upon first connect, upon each new connection, and upon checkout and
checkin of connections.   See :class:`_events.PoolEvents` for details.

.. _pool_disconnects:

Dealing with Disconnects
------------------------

The connection pool has the ability to refresh individual connections as well as
its entire set of connections, setting the previously pooled connections as
"invalid".   A common use case is allow the connection pool to gracefully recover
when the database server has been restarted, and all previously established connections
are no longer functional.   There are two approaches to this.

.. _pool_disconnects_pessimistic:

Disconnect Handling - Pessimistic
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The pessimistic approach refers to emitting a test statement on the SQL
connection at the start of each connection pool checkout, to test
that the database connection is still viable.   The implementation is
dialect-specific, and makes use of either a DBAPI-specific ping method,
or by using a simple SQL statement like "SELECT 1", in order to test the
connection for liveness.

The approach adds a small bit of overhead to the connection checkout process,
however is otherwise the most simple and reliable approach to completely
eliminating database errors due to stale pooled connections.   The calling
application does not need to be concerned about organizing operations
to be able to recover from stale connections checked out from the pool.

Pessimistic testing of connections upon checkout is achievable by
using the :paramref:`_pool.Pool.pre_ping` argument, available from :func:`_sa.create_engine`
via the :paramref:`_sa.create_engine.pool_pre_ping` argument::

    engine = create_engine("mysql+pymysql://user:pw@host/db", pool_pre_ping=True)

The "pre ping" feature operates on a per-dialect basis either by invoking a
DBAPI-specific "ping" method, or if not available will emit SQL equivalent to
"SELECT 1", catching any errors and detecting the error as a "disconnect"
situation. If the ping / error check determines that the connection is not
usable, the connection will be immediately recycled, and all other pooled
connections older than the current time are invalidated, so that the next time
they are checked out, they will also be recycled before use.

If the database is still not available when "pre ping" runs, then the initial
connect will fail and the error for failure to connect will be propagated
normally.  In the uncommon situation that the database is available for
connections, but is not able to respond to a "ping", the "pre_ping" will try up
to three times before giving up, propagating the database error last received.

It is critical to note that the pre-ping approach **does not accommodate for
connections dropped in the middle of transactions or other SQL operations**. If
the database becomes unavailable while a transaction is in progress, the
transaction will be lost and the database error will be raised.   While the
:class:`_engine.Connection` object will detect a "disconnect" situation and
recycle the connection as well as invalidate the rest of the connection pool
when this condition occurs, the individual operation where the exception was
raised will be lost, and it's up to the application to either abandon the
operation, or retry the whole transaction again.  If the engine is
configured using DBAPI-level autocommit connections, as described at
:ref:`dbapi_autocommit`, a connection **may** be reconnected transparently
mid-operation using events.  See the section :ref:`faq_execute_retry` for
an example.

For dialects that make use of "SELECT 1" and catch errors in order to detect
disconnects, the disconnection test may be augmented for new backend-specific
error messages using the :meth:`_events.DialectEvents.handle_error` hook.

.. _pool_disconnects_pessimistic_custom:

Custom / Legacy Pessimistic Ping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before :paramref:`_sa.create_engine.pool_pre_ping` was added, the "pre-ping"
approach historically has been performed manually using
the :meth:`_events.ConnectionEvents.engine_connect` engine event.
The most common recipe for this is below, for reference
purposes in case an application is already using such a recipe, or special
behaviors are needed::

    from sqlalchemy import exc
    from sqlalchemy import event
    from sqlalchemy import select

    some_engine = create_engine(...)


    @event.listens_for(some_engine, "engine_connect")
    def ping_connection(connection, branch):
        if branch:
            # this parameter is always False as of SQLAlchemy 2.0,
            # but is still accepted by the event hook.  In 1.x versions
            # of SQLAlchemy, "branched" connections should be skipped.
            return

        try:
            # run a SELECT 1.   use a core select() so that
            # the SELECT of a scalar value without a table is
            # appropriately formatted for the backend
            connection.scalar(select(1))
        except exc.DBAPIError as err:
            # catch SQLAlchemy's DBAPIError, which is a wrapper
            # for the DBAPI's exception.  It includes a .connection_invalidated
            # attribute which specifies if this connection is a "disconnect"
            # condition, which is based on inspection of the original exception
            # by the dialect in use.
            if err.connection_invalidated:
                # run the same SELECT again - the connection will re-validate
                # itself and establish a new connection.  The disconnect detection
                # here also causes the whole connection pool to be invalidated
                # so that all stale connections are discarded.
                connection.scalar(select(1))
            else:
                raise

The above recipe has the advantage that we are making use of SQLAlchemy's
facilities for detecting those DBAPI exceptions that are known to indicate
a "disconnect" situation, as well as the :class:`_engine.Engine` object's ability
to correctly invalidate the current connection pool when this condition
occurs and allowing the current :class:`_engine.Connection` to re-validate onto
a new DBAPI connection.


Disconnect Handling - Optimistic
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When pessimistic handling is not employed, as well as when the database is
shutdown and/or restarted in the middle of a connection's period of use within
a transaction, the other approach to dealing with stale / closed connections is
to let SQLAlchemy handle disconnects as  they occur, at which point all
connections in the pool are invalidated, meaning they are assumed to be
stale and will be refreshed upon next checkout.  This behavior assumes the
:class:`_pool.Pool` is used in conjunction with a :class:`_engine.Engine`.
The :class:`_engine.Engine` has logic which can detect
disconnection events and refresh the pool automatically.

When the :class:`_engine.Connection` attempts to use a DBAPI connection, and an
exception is raised that corresponds to a "disconnect" event, the connection
is invalidated. The :class:`_engine.Connection` then calls the :meth:`_pool.Pool.recreate`
method, effectively invalidating all connections not currently checked out so
that they are replaced with new ones upon next checkout.  This flow is
illustrated by the code example below::

    from sqlalchemy import create_engine, exc

    e = create_engine(...)
    c = e.connect()

    try:
        # suppose the database has been restarted.
        c.execute(text("SELECT * FROM table"))
        c.close()
    except exc.DBAPIError as e:
        # an exception is raised, Connection is invalidated.
        if e.connection_invalidated:
            print("Connection was invalidated!")

    # after the invalidate event, a new connection
    # starts with a new Pool
    c = e.connect()
    c.execute(text("SELECT * FROM table"))

The above example illustrates that no special intervention is needed to
refresh the pool, which continues normally after a disconnection event is
detected.   However, one database exception is raised, per each connection
that is in use while the database unavailability event occurred.
In a typical web application using an ORM Session, the above condition would
correspond to a single request failing with a 500 error, then the web application
continuing normally beyond that.   Hence the approach is "optimistic" in that frequent
database restarts are not anticipated.


.. _pool_setting_recycle:

Setting Pool Recycle
~~~~~~~~~~~~~~~~~~~~

An additional setting that can augment the "optimistic" approach is to set the
pool recycle parameter.   This parameter prevents the pool from using a particular
connection that has passed a certain age, and is appropriate for database backends
such as MySQL that automatically close connections that have been stale after a particular
period of time::

    from sqlalchemy import create_engine

    e = create_engine("mysql+mysqldb://scott:tiger@localhost/test", pool_recycle=3600)

Above, any DBAPI connection that has been open for more than one hour will be invalidated and replaced,
upon next checkout.   Note that the invalidation **only** occurs during checkout - not on
any connections that are held in a checked out state.     ``pool_recycle`` is a function
of the :class:`_pool.Pool` itself, independent of whether or not an :class:`_engine.Engine` is in use.


.. _pool_connection_invalidation:

More on Invalidation
^^^^^^^^^^^^^^^^^^^^

The :class:`_pool.Pool` provides "connection invalidation" services which allow
both explicit invalidation of a connection as well as automatic invalidation
in response to conditions that are determined to render a connection unusable.

"Invalidation" means that a particular DBAPI connection is removed from the
pool and discarded.  The ``.close()`` method is called on this connection
if it is not clear that the connection itself might not be closed, however
if this method fails, the exception is logged but the operation still proceeds.

When using a :class:`_engine.Engine`, the :meth:`_engine.Connection.invalidate` method is
the usual entrypoint to explicit invalidation.   Other conditions by which
a DBAPI connection might be invalidated include:

* a DBAPI exception such as :class:`.OperationalError`, raised when a
  method like ``connection.execute()`` is called, is detected as indicating
  a so-called "disconnect" condition.   As the Python DBAPI provides no
  standard system for determining the nature of an exception, all SQLAlchemy
  dialects include a system called ``is_disconnect()`` which will examine
  the contents of an exception object, including the string message and
  any potential error codes included with it, in order to determine if this
  exception indicates that the connection is no longer usable.  If this is the
  case, the :meth:`._ConnectionFairy.invalidate` method is called and the
  DBAPI connection is then discarded.

* When the connection is returned to the pool, and
  calling the ``connection.rollback()`` or ``connection.commit()`` methods,
  as dictated by the pool's "reset on return" behavior, throws an exception.
  A final attempt at calling ``.close()`` on the connection will be made,
  and it is then discarded.

* When a listener implementing :meth:`_events.PoolEvents.checkout` raises the
  :class:`~sqlalchemy.exc.DisconnectionError` exception, indicating that the connection
  won't be usable and a new connection attempt needs to be made.

All invalidations which occur will invoke the :meth:`_events.PoolEvents.invalidate`
event.

.. _pool_new_disconnect_codes:

Supporting new database error codes for disconnect scenarios
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

SQLAlchemy dialects each include a routine called ``is_disconnect()`` that is
invoked whenever a DBAPI exception is encountered. The DBAPI exception object
is passed to this method, where dialect-specific heuristics will then determine
if the error code received indicates that the database connection has been
"disconnected", or is in an otherwise unusable state which indicates it should
be recycled. The heuristics applied here may be customized using the
:meth:`_events.DialectEvents.handle_error` event hook, which is typically
established via the owning :class:`_engine.Engine` object. Using this hook, all
errors which occur are delivered passing along a contextual object known as
:class:`.ExceptionContext`. Custom event hooks may control whether or not a
particular error should be considered a "disconnect" situation or not, as well
as if this disconnect should cause the entire connection pool to be invalidated
or not.

For example, to add support to consider the Oracle Database driver error codes
``DPY-1001`` and ``DPY-4011`` to be handled as disconnect codes, apply an event
handler to the engine after creation::

    import re

    from sqlalchemy import create_engine

    engine = create_engine(
        "oracle+oracledb://scott:tiger@localhost:1521?service_name=freepdb1"
    )


    @event.listens_for(engine, "handle_error")
    def handle_exception(context: ExceptionContext) -> None:
        if not context.is_disconnect and re.match(
            r"^(?:DPY-1001|DPY-4011)", str(context.original_exception)
        ):
            context.is_disconnect = True

        return None

The above error processing function will be invoked for all Oracle Database
errors raised, including those caught when using the :ref:`pool pre ping
<pool_disconnects_pessimistic>` feature for those backends that rely upon
disconnect error handling (new in 2.0).

.. seealso::

    :meth:`_events.DialectEvents.handle_error`

.. _pool_use_lifo:

Using FIFO vs. LIFO
-------------------

The :class:`.QueuePool` class features a flag called
:paramref:`.QueuePool.use_lifo`, which can also be accessed from
:func:`_sa.create_engine` via the flag :paramref:`_sa.create_engine.pool_use_lifo`.
Setting this flag to ``True`` causes the pool's "queue" behavior to instead be
that of a "stack", e.g. the last connection to be returned to the pool is the
first one to be used on the next request. In contrast to the pool's long-
standing behavior of first-in-first-out, which produces a round-robin effect of
using each connection in the pool in series, lifo mode allows excess
connections to remain idle in the pool, allowing server-side timeout schemes to
close these connections out.   The difference between FIFO and LIFO is
basically whether or not its desirable for the pool to keep a full set of
connections ready to go even during idle periods::

    engine = create_engine("postgresql://", pool_use_lifo=True, pool_pre_ping=True)

Above, we also make use of the :paramref:`_sa.create_engine.pool_pre_ping` flag
so that connections which are closed from the server side are gracefully
handled by the connection pool and replaced with a new connection.

Note that the flag only applies to :class:`.QueuePool` use.

.. seealso::

    :ref:`pool_disconnects`


.. _pooling_multiprocessing:

Using Connection Pools with Multiprocessing or os.fork()
--------------------------------------------------------

It's critical that when using a connection pool, and by extension when
using an :class:`_engine.Engine` created via :func:`_sa.create_engine`, that
the pooled connections **are not shared to a forked process**.  TCP connections
are represented as file descriptors, which usually work across process
boundaries, meaning this will cause concurrent access to the file descriptor
on behalf of two or more entirely independent Python interpreter states.

Depending on specifics of the driver and OS, the issues that arise here range
from non-working connections to socket connections that are used by multiple
processes concurrently, leading to broken messaging (the latter case is
typically the most common).

The SQLAlchemy :class:`_engine.Engine` object refers to a connection pool of existing
database connections.  So when this object is replicated to a child process,
the goal is to ensure that no database connections are carried over.  There
are four general approaches to this:

1. Disable pooling using :class:`.NullPool`.  This is the most simplistic,
   one shot system that prevents the :class:`_engine.Engine` from using any connection
   more than once::

    from sqlalchemy.pool import NullPool

    engine = create_engine("mysql+mysqldb://user:pass@host/dbname", poolclass=NullPool)

2. Call :meth:`_engine.Engine.dispose` on any given :class:`_engine.Engine`,
   passing the :paramref:`.Engine.dispose.close` parameter with a value of
   ``False``, within the initialize phase of the child process.  This is
   so that the new process will not touch any of the parent process' connections
   and will instead start with new connections.
   **This is the recommended approach**::

        from multiprocessing import Pool

        engine = create_engine("mysql+mysqldb://user:pass@host/dbname")


        def run_in_process(some_data_record):
            with engine.connect() as conn:
                conn.execute(text("..."))


        def initializer():
            """ensure the parent proc's database connections are not touched
            in the new connection pool"""
            engine.dispose(close=False)


        with Pool(10, initializer=initializer) as p:
            p.map(run_in_process, data)

   .. versionadded:: 1.4.33  Added the :paramref:`.Engine.dispose.close`
      parameter to allow the replacement of a connection pool in a child
      process without interfering with the connections used by the parent
      process.

3. Call :meth:`.Engine.dispose` **directly before** the child process is
   created.  This will also cause the child process to start with a new
   connection pool, while ensuring the parent connections are not transferred
   to the child process::

        engine = create_engine("mysql://user:pass@host/dbname")


        def run_in_process():
            with engine.connect() as conn:
                conn.execute(text("..."))


        # before process starts, ensure engine.dispose() is called
        engine.dispose()
        p = Process(target=run_in_process)
        p.start()

4. An event handler can be applied to the connection pool that tests for
   connections being shared across process boundaries, and invalidates them::

    from sqlalchemy import event
    from sqlalchemy import exc
    import os

    engine = create_engine("...")


    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info["pid"] = os.getpid()


    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info["pid"] != pid:
            connection_record.dbapi_connection = connection_proxy.dbapi_connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" % (connection_record.info["pid"], pid)
            )

   Above, we use an approach similar to that described in
   :ref:`pool_disconnects_pessimistic` to treat a DBAPI connection that
   originated in a different parent process as an "invalid" connection,
   coercing the pool to recycle the connection record to make a new connection.

The above strategies will accommodate the case of an :class:`_engine.Engine`
being shared among processes. The above steps alone are not sufficient for the
case of sharing a specific :class:`_engine.Connection` over a process boundary;
prefer to keep the scope of a particular :class:`_engine.Connection` local to a
single process (and thread). It's additionally not supported to share any kind
of ongoing transactional state directly across a process boundary, such as an
ORM :class:`_orm.Session` object that's begun a transaction and references
active :class:`_orm.Connection` instances; again prefer to create new
:class:`_orm.Session` objects in new processes.

Using a pool instance directly
------------------------------

A pool implementation can be used directly without an engine. This could be used
in applications that just wish to use the pool behavior without all other
SQLAlchemy features.
In the example below the default pool for the ``MySQLdb`` dialect is obtained using
:func:`_sa.create_pool_from_url`::

    from sqlalchemy import create_pool_from_url

    my_pool = create_pool_from_url(
        "mysql+mysqldb://", max_overflow=5, pool_size=5, pre_ping=True
    )

    con = my_pool.connect()
    # use the connection
    ...
    # then close it
    con.close()

If the type of pool to create is not specified, the default one for the dialect
will be used. To specify it directly the ``poolclass`` argument can be used,
like in the following example::

    from sqlalchemy import create_pool_from_url
    from sqlalchemy import NullPool

    my_pool = create_pool_from_url("mysql+mysqldb://", poolclass=NullPool)

.. _pool_api:

API Documentation - Available Pool Implementations
--------------------------------------------------

.. autoclass:: sqlalchemy.pool.Pool
    :members:

.. autoclass:: sqlalchemy.pool.QueuePool
    :members:

.. autoclass:: sqlalchemy.pool.AsyncAdaptedQueuePool
    :members:

.. autoclass:: SingletonThreadPool
    :members:

.. autoclass:: AssertionPool
    :members:

.. autoclass:: NullPool
    :members:

.. autoclass:: StaticPool
    :members:

.. autoclass:: ManagesConnection
    :members:

.. autoclass:: ConnectionPoolEntry
    :members:
    :inherited-members:

.. autoclass:: PoolProxiedConnection
    :members:
    :inherited-members:

.. autoclass:: _ConnectionFairy

.. autoclass:: _ConnectionRecord
