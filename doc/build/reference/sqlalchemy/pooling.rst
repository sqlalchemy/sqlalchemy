.. _pooling_toplevel:

Connection Pooling
==================

.. module:: sqlalchemy.pool

SQLAlchemy ships with a connection pooling framework that integrates
with the Engine system and can also be used on its own to manage plain
DB-API connections.

At the base of any database helper library is a system for efficiently
acquiring connections to the database.  Since the establishment of a
database connection is typically a somewhat expensive operation, an
application needs a way to get at database connections repeatedly
without incurring the full overhead each time.  Particularly for
server-side web applications, a connection pool is the standard way to
maintain a group or "pool" of active database connections which are
reused from request to request in a single server process.

Connection Pool Configuration
-----------------------------

The :class:`~sqlalchemy.engine.Engine` returned by the
:func:`~sqlalchemy.create_engine` function in most cases has a :class:`QueuePool`
integrated, pre-configured with reasonable pooling defaults.  If
you're reading this section to simply enable pooling- congratulations!
You're already done.

The most common :class:`QueuePool` tuning parameters can be passed
directly to :func:`~sqlalchemy.create_engine` as keyword arguments:
``pool_size``, ``max_overflow``, ``pool_recycle`` and
``pool_timeout``.  For example::

  engine = create_engine('postgresql://me@localhost/mydb',
                         pool_size=20, max_overflow=0)

In the case of SQLite, a :class:`SingletonThreadPool` is provided instead,
to provide compatibility with SQLite's restricted threading model.


Custom Pool Construction
------------------------

:class:`Pool` instances may be created directly for your own use or to
supply to :func:`sqlalchemy.create_engine` via the ``pool=``
keyword argument.

Constructing your own pool requires supplying a callable function the
Pool can use to create new connections.  The function will be called
with no arguments.

Through this method, custom connection schemes can be made, such as a
using connections from another library's pool, or making a new
connection that automatically executes some initialization commands::

    import sqlalchemy.pool as pool
    import psycopg2

    def getconn():
        c = psycopg2.connect(username='ed', host='127.0.0.1', dbname='test')
        # execute an initialization function on the connection before returning
        c.cursor.execute("setup_encodings()")
        return c

    p = pool.QueuePool(getconn, max_overflow=10, pool_size=5)

Or with SingletonThreadPool::

    import sqlalchemy.pool as pool
    import sqlite

    p = pool.SingletonThreadPool(lambda: sqlite.connect(filename='myfile.db'))


Builtin Pool Implementations
----------------------------

.. autoclass:: AssertionPool
   :members:
   :show-inheritance:

.. autoclass:: NullPool
   :members:
   :show-inheritance:

.. autoclass:: sqlalchemy.pool.Pool
   :members:
   :show-inheritance:
   :undoc-members:
   :inherited-members:

.. autoclass:: sqlalchemy.pool.QueuePool
   :members:
   :show-inheritance:

.. autoclass:: SingletonThreadPool
   :members:
   :show-inheritance:

.. autoclass:: StaticPool
   :members:
   :show-inheritance:


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
:class:`Pool`) that corresponds to the exact connection arguments sent
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

