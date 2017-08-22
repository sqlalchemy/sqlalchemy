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
which integrate with the :class:`.Engine`.  They can also be used
directly for applications that want to add pooling to an otherwise
plain DBAPI approach.

Connection Pool Configuration
-----------------------------

The :class:`~.engine.Engine` returned by the
:func:`~sqlalchemy.create_engine` function in most cases has a :class:`.QueuePool`
integrated, pre-configured with reasonable pooling defaults.  If
you're reading this section only to learn how to enable pooling - congratulations!
You're already done.

The most common :class:`.QueuePool` tuning parameters can be passed
directly to :func:`~sqlalchemy.create_engine` as keyword arguments:
``pool_size``, ``max_overflow``, ``pool_recycle`` and
``pool_timeout``.  For example::

  engine = create_engine('postgresql://me@localhost/mydb',
                         pool_size=20, max_overflow=0)

In the case of SQLite, the :class:`.SingletonThreadPool` or
:class:`.NullPool` are selected by the dialect to provide
greater compatibility with SQLite's threading and locking
model, as well as to provide a reasonable default behavior
to SQLite "memory" databases, which maintain their entire
dataset within the scope of a single connection.

All SQLAlchemy pool implementations have in common
that none of them "pre create" connections - all implementations wait
until first use before creating a connection.   At that point, if
no additional concurrent checkout requests for more connections
are made, no additional connections are created.   This is why it's perfectly
fine for :func:`.create_engine` to default to using a :class:`.QueuePool`
of size five without regard to whether or not the application really needs five connections
queued up - the pool would only grow to that size if the application
actually used five connections concurrently, in which case the usage of a
small pool is an entirely appropriate default behavior.

.. _pool_switching:

Switching Pool Implementations
------------------------------

The usual way to use a different kind of pool with :func:`.create_engine`
is to use the ``poolclass`` argument.   This argument accepts a class
imported from the ``sqlalchemy.pool`` module, and handles the details
of building the pool for you.   Common options include specifying
:class:`.QueuePool` with SQLite::

    from sqlalchemy.pool import QueuePool
    engine = create_engine('sqlite:///file.db', poolclass=QueuePool)

Disabling pooling using :class:`.NullPool`::

    from sqlalchemy.pool import NullPool
    engine = create_engine(
              'postgresql+psycopg2://scott:tiger@localhost/test',
              poolclass=NullPool)

Using a Custom Connection Function
----------------------------------

All :class:`.Pool` classes accept an argument ``creator`` which is
a callable that creates a new connection.  :func:`.create_engine`
accepts this function to pass onto the pool via an argument of
the same name::

    import sqlalchemy.pool as pool
    import psycopg2

    def getconn():
        c = psycopg2.connect(username='ed', host='127.0.0.1', dbname='test')
        # do things with 'c' to set up
        return c

    engine = create_engine('postgresql+psycopg2://', creator=getconn)

For most "initialize on connection" routines, it's more convenient
to use the :class:`.PoolEvents` event hooks, so that the usual URL argument to
:func:`.create_engine` is still usable.  ``creator`` is there as
a last resort for when a DBAPI has some form of ``connect``
that is not at all supported by SQLAlchemy.

Constructing a Pool
-------------------

To use a :class:`.Pool` by itself, the ``creator`` function is
the only argument that's required and is passed first, followed
by any additional options::

    import sqlalchemy.pool as pool
    import psycopg2

    def getconn():
        c = psycopg2.connect(username='ed', host='127.0.0.1', dbname='test')
        return c

    mypool = pool.QueuePool(getconn, max_overflow=10, pool_size=5)

DBAPI connections can then be procured from the pool using the :meth:`.Pool.connect`
function.  The return value of this method is a DBAPI connection that's contained
within a transparent proxy::

    # get a connection
    conn = mypool.connect()

    # use it
    cursor = conn.cursor()
    cursor.execute("select foo")

The purpose of the transparent proxy is to intercept the ``close()`` call,
such that instead of the DBAPI connection being closed, it is returned to the
pool::

    # "close" the connection.  Returns
    # it to the pool.
    conn.close()

The proxy also returns its contained DBAPI connection to the pool
when it is garbage collected,
though it's not deterministic in Python that this occurs immediately (though
it is typical with cPython).

The ``close()`` step also performs the important step of calling the
``rollback()`` method of the DBAPI connection.   This is so that any
existing transaction on the connection is removed, not only ensuring
that no existing state remains on next usage, but also so that table
and row locks are released as well as that any isolated data snapshots
are removed.   This behavior can be disabled using the ``reset_on_return``
option of :class:`.Pool`.

A particular pre-created :class:`.Pool` can be shared with one or more
engines by passing it to the ``pool`` argument of :func:`.create_engine`::

    e = create_engine('postgresql://', pool=mypool)

Pool Events
-----------

Connection pools support an event interface that allows hooks to execute
upon first connect, upon each new connection, and upon checkout and
checkin of connections.   See :class:`.PoolEvents` for details.

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
that the database connection is still viable.   Typically, this
is a simple statement like "SELECT 1", but may also make use of some
DBAPI-specific method to test the connection for liveness.

The approach adds a small bit of overhead to the connection checkout process,
however is otherwise the most simple and reliable approach to completely
eliminating database errors due to stale pooled connections.   The calling
application does not need to be concerned about organizing operations
to be able to recover from stale connections checked out from the pool.

It is critical to note that the pre-ping approach **does not accommodate for
connections dropped in the middle of transactions or other SQL operations**.
If the database becomes unavailable while a transaction is in progress, the
transaction will be lost and the database error will be raised.   While
the :class:`.Connection` object will detect a "disconnect" situation and
recycle the connection as well as invalidate the rest of the connection pool
when this condition occurs,
the individual operation where the exception was raised will be lost, and it's
up to the application to either abandon
the operation, or retry the whole transaction again.

Pessimistic testing of connections upon checkout is achievable by
using the :paramref:`.Pool.pre_ping` argument, available from :func:`.create_engine`
via the :paramref:`.create_engine.pool_pre_ping` argument::

    engine = create_engine("mysql+pymysql://user:pw@host/db", pool_pre_ping=True)

The "pre ping" feature will normally emit SQL equivalent to "SELECT 1" each time a
connection is checked out from the pool; if an error is raised that is detected
as a "disconnect" situation, the connection will be immediately recycled, and
all other pooled connections older than the current time are invalidated, so
that the next time they are checked out, they will also be recycled before use.

If the database is still not available when "pre ping" runs, then the initial
connect will fail and the error for failure to connect will be propagated
normally.  In the uncommon situation that the database is available for
connections, but is not able to respond to a "ping", the "pre_ping" will try up
to three times before giving up, propagating the database error last received.

.. note::

    the "SELECT 1" emitted by "pre-ping" is invoked within the scope
    of the connection pool / dialect, using a very short codepath for minimal
    Python latency.   As such, this statement is **not logged in the SQL
    echo output**, and will not show up in SQLAlchemy's engine logging.

.. versionadded:: 1.2 Added "pre-ping" capability to the :class:`.Pool`
   class.

Custom / Legacy Pessimistic Ping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before :paramref:`.create_engine.pool_pre_ping` was added, the "pre-ping"
approach historically has been performed manually using
the :meth:`.ConnectionEvents.engine_connect` engine event.
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
            # "branch" refers to a sub-connection of a connection,
            # we don't want to bother pinging on these.
            return

        # turn off "close with result".  This flag is only used with
        # "connectionless" execution, otherwise will be False in any case
        save_should_close_with_result = connection.should_close_with_result
        connection.should_close_with_result = False

        try:
            # run a SELECT 1.   use a core select() so that
            # the SELECT of a scalar value without a table is
            # appropriately formatted for the backend
            connection.scalar(select([1]))
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
                connection.scalar(select([1]))
            else:
                raise
        finally:
            # restore "close with result"
            connection.should_close_with_result = save_should_close_with_result

The above recipe has the advantage that we are making use of SQLAlchemy's
facilities for detecting those DBAPI exceptions that are known to indicate
a "disconnect" situation, as well as the :class:`.Engine` object's ability
to correctly invalidate the current connection pool when this condition
occurs and allowing the current :class:`.Connection` to re-validate onto
a new DBAPI connection.


Disconnect Handling - Optimistic
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When pessimistic handling is not employed, as well as when the database is
shutdown and/or restarted in the middle of a connection's period of use within
a transaction, the other approach to dealing with stale / closed connections is
to let SQLAlchemy handle disconnects as  they occur, at which point all
connections in the pool are invalidated, meaning they are assumed to be
stale and will be refreshed upon next checkout.  This behavior assumes the
:class:`.Pool` is used in conjunction with a :class:`.Engine`.
The :class:`.Engine` has logic which can detect
disconnection events and refresh the pool automatically.

When the :class:`.Connection` attempts to use a DBAPI connection, and an
exception is raised that corresponds to a "disconnect" event, the connection
is invalidated. The :class:`.Connection` then calls the :meth:`.Pool.recreate`
method, effectively invalidating all connections not currently checked out so
that they are replaced with new ones upon next checkout.  This flow is
illustrated by the code example below::

    from sqlalchemy import create_engine, exc
    e = create_engine(...)
    c = e.connect()

    try:
        # suppose the database has been restarted.
        c.execute("SELECT * FROM table")
        c.close()
    except exc.DBAPIError, e:
        # an exception is raised, Connection is invalidated.
        if e.connection_invalidated:
            print("Connection was invalidated!")

    # after the invalidate event, a new connection
    # starts with a new Pool
    c = e.connect()
    c.execute("SELECT * FROM table")

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
    e = create_engine("mysql://scott:tiger@localhost/test", pool_recycle=3600)

Above, any DBAPI connection that has been open for more than one hour will be invalidated and replaced,
upon next checkout.   Note that the invalidation **only** occurs during checkout - not on
any connections that are held in a checked out state.     ``pool_recycle`` is a function
of the :class:`.Pool` itself, independent of whether or not an :class:`.Engine` is in use.


.. _pool_connection_invalidation:

More on Invalidation
^^^^^^^^^^^^^^^^^^^^

The :class:`.Pool` provides "connection invalidation" services which allow
both explicit invalidation of a connection as well as automatic invalidation
in response to conditions that are determined to render a connection unusable.

"Invalidation" means that a particular DBAPI connection is removed from the
pool and discarded.  The ``.close()`` method is called on this connection
if it is not clear that the connection itself might not be closed, however
if this method fails, the exception is logged but the operation still proceeds.

When using a :class:`.Engine`, the :meth:`.Connection.invalidate` method is
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

* When a listener implementing :meth:`.PoolEvents.checkout` raises the
  :class:`~sqlalchemy.exc.DisconnectionError` exception, indicating that the connection
  won't be usable and a new connection attempt needs to be made.

All invalidations which occur will invoke the :meth:`.PoolEvents.invalidate`
event.

Using Connection Pools with Multiprocessing
-------------------------------------------

It's critical that when using a connection pool, and by extension when
using an :class:`.Engine` created via :func:`.create_engine`, that
the pooled connections **are not shared to a forked process**.  TCP connections
are represented as file descriptors, which usually work across process
boundaries, meaning this will cause concurrent access to the file descriptor
on behalf of two or more entirely independent Python interpreter states.

There are two approaches to dealing with this.

The first is, either create a new :class:`.Engine` within the child
process, or upon an existing :class:`.Engine`, call :meth:`.Engine.dispose`
before the child process uses any connections.  This will remove all existing
connections from the pool so that it makes all new ones.  Below is
a simple version using ``multiprocessing.Process``, but this idea
should be adapted to the style of forking in use::

    eng = create_engine("...")

    def run_in_process():
      eng.dispose()

      with eng.connect() as conn:
          conn.execute("...")

    p = Process(target=run_in_process)

The next approach is to instrument the :class:`.Pool` itself with events
so that connections are automatically invalidated in the subprocess.
This is a little more magical but probably more foolproof::

    from sqlalchemy import event
    from sqlalchemy import exc
    import os

    eng = create_engine("...")

    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                    "Connection record belongs to pid %s, "
                    "attempting to check out in pid %s" %
                    (connection_record.info['pid'], pid)
            )

Above, we use an approach similar to that described in
:ref:`pool_disconnects_pessimistic` to treat a DBAPI connection that
originated in a different parent process as an "invalid" connection,
coercing the pool to recycle the connection record to make a new connection.



API Documentation - Available Pool Implementations
--------------------------------------------------

.. autoclass:: sqlalchemy.pool.Pool

   .. automethod:: __init__
   .. automethod:: connect
   .. automethod:: dispose
   .. automethod:: recreate
   .. automethod:: unique_connection

.. autoclass:: sqlalchemy.pool.QueuePool

   .. automethod:: __init__
   .. automethod:: connect
   .. automethod:: unique_connection

.. autoclass:: SingletonThreadPool

   .. automethod:: __init__

.. autoclass:: AssertionPool


.. autoclass:: NullPool


.. autoclass:: StaticPool

.. autoclass:: _ConnectionFairy
    :members:

    .. autoattribute:: _connection_record

.. autoclass:: _ConnectionRecord
    :members:


Pooling Plain DB-API Connections
--------------------------------

Any :pep:`249` DB-API module can be "proxied" through the connection
pool transparently.  Usage of the DB-API is exactly as before, except
the ``connect()`` method will consult the pool.  Below we illustrate
this with ``psycopg2``::

    import sqlalchemy.pool as pool
    import psycopg2 as psycopg

    psycopg = pool.manage(psycopg)

    # then connect normally
    connection = psycopg.connect(database='test', username='scott',
                                 password='tiger')

This produces a :class:`_DBProxy` object which supports the same
``connect()`` function as the original DB-API module.  Upon
connection, a connection proxy object is returned, which delegates its
calls to a real DB-API connection object.  This connection object is
stored persistently within a connection pool (an instance of
:class:`.Pool`) that corresponds to the exact connection arguments sent
to the ``connect()`` function.

The connection proxy supports all of the methods on the original
connection object, most of which are proxied via ``__getattr__()``.
The ``close()`` method will return the connection to the pool, and the
``cursor()`` method will return a proxied cursor object.  Both the
connection proxy and the cursor proxy will also return the underlying
connection to the pool after they have both been garbage collected,
which is detected via weakref callbacks  (``__del__`` is not used).

Additionally, when connections are returned to the pool, a
``rollback()`` is issued on the connection unconditionally.  This is
to release any locks still held by the connection that may have
resulted from normal activity.

By default, the ``connect()`` method will return the same connection
that is already checked out in the current thread.  This allows a
particular connection to be used in a given thread without needing to
pass it around between functions.  To disable this behavior, specify
``use_threadlocal=False`` to the ``manage()`` function.

.. autofunction:: sqlalchemy.pool.manage

.. autofunction:: sqlalchemy.pool.clear_managers

