:orphan:

.. _errors:

==============
Error Messages
==============

This section lists descriptions and background for common error messages
and warnings raised or emitted by SQLAlchemy.

SQLAlchemy normally raises errors within the context of a SQLAlchemy-specific
exception class.  For details on these classes, see
:ref:`core_exceptions_toplevel` and :ref:`orm_exceptions_toplevel`.

SQLAlchemy errors can roughly be separated into two categories, the
**programming-time error** and the **runtime error**.     Programming-time
errors are raised as a result of functions or methods being called with
incorrect arguments, or from other configuration-oriented methods such  as
mapper configurations that can't be resolved.   The programming-time error is
typically immediate and deterministic.    The runtime error on the other hand
represents a failure that occurs as a program runs in response to some
condition that occurs arbitrarily, such as database connections being
exhausted or some data-related issue occurring.   Runtime errors are more
likely to be seen in the logs of a running application as the program
encounters these states in response to load and data being encountered.

Since runtime errors are not as easy to reproduce and often occur in response
to some arbitrary condition as the program runs, they are more difficult to
debug and also affect programs that have already been put into production.

Within this section, the goal is to try to provide background on some of the
most common runtime errors as well as programming time errors.


Connections and Transactions
============================

.. _error_3o7r:

QueuePool limit of size <x> overflow <y> reached, connection timed out, timeout <z>
-----------------------------------------------------------------------------------

This is possibly the most common runtime error experienced, as it directly
involves the work load of the application surpassing a configured limit, one
which typically applies to nearly all SQLAlchemy applications.

The following points summarize what this error means, beginning with the
most fundamental points that most SQLAlchemy users should already be
familiar with.

* **The SQLAlchemy Engine object uses a pool of connections by default** - What
  this means is that when one makes use of a SQL database connection resource
  of an :class:`.Engine` object, and then :term:`releases` that resource,
  the database connection itself remains connected to the database and
  is returned to an internal queue where it can be used again.  Even though
  the code may appear to be ending its conversation with the database, in many
  cases the application will still maintain a fixed number of database connections
  that persist until the application ends or the pool is explicitly disposed.

* Because of the pool, when an application makes use of a SQL database
  connection, most typically from either making use of :meth:`.Engine.connect`
  or when making queries using an ORM :class:`.Session`, this activity
  does not necessarily establish a new connection to the database at the
  moment the connection object is acquired; it instead consults the
  connection pool for a connection, which will often retrieve an existing
  connection from the pool to be re-used.  If no connections are available,
  the pool will create a new database connection, but only if the
  pool has not surpassed a configured capacity.

* The default pool used in most cases is called :class:`.QueuePool`.  When
  you ask this pool to give you a connection and none are available, it
  will create a new connection **if the total number of connections in play
  are less than a configured value**.  This value is equal to the
  **pool size plus the max overflow**.     That means if you have configured
  your engine as::

   engine = create_engine("mysql://u:p@host/db", pool_size=10, max_overflow=20)

  The above :class:`.Engine` will allow **at most 30 connections** to be in
  play at any time, not including connections that were detached from the
  engine or invalidated.  If a request for a new connection arrives and
  30 connections are already in use by other parts of the application,
  the connection pool will block for a fixed period of time,
  before timing out and raising this error message.

  In order to allow for a higher number of connections be in use at once,
  the pool can be adjusted using the
  :paramref:`.create_engine.pool_size` and :paramref:`.create_engine.max_overflow`
  parameters as passed to the :func:`.create_engine` function.      The timeout
  to wait for a connection to be available is configured using the
  :paramref:`.create_engine.pool_timeout` parameter.

* The pool can be configured to have unlimited overflow by setting
  :paramref:`.create_engine.max_overflow` to the value "-1".  With this setting,
  the pool will still maintain a fixed pool of connections, however it will
  never block upon a new connection being requested; it will instead unconditionally
  make a new connection if none are available.

  However, when running in this way, if the application has an issue where it
  is using up all available connectivity resources, it will eventually hit the
  configured limit of available connections on the database itself, which will
  again return an error.  More seriously, when the application exhausts the
  database of connections, it usually will have caused a great
  amount of  resources to be used up before failing, and can also interfere
  with other applications and database status mechanisms that rely upon being
  able to connect to the database.

  Given the above, the connection pool can be looked at as a **safety valve
  for connection use**, providing a critical layer of protection against
  a rogue application causing the entire database to become unavailable
  to all other applications.   When receiving this error message, it is vastly
  preferable to repair the issue using up too many connections and/or
  configure the limits appropriately, rather than allowing for unlimited
  overflow which does not actually solve the underlying issue.

What causes an application to use up all the connections that it has available?

* **The application is fielding too many concurrent requests to do work based
  on the configured value for the pool** - This is the most straightforward
  cause.  If you have
  an application that runs in a thread pool that allows for 30 concurrent
  threads, with one connection in use per thread, if your pool is not configured
  to allow at least 30 connections checked out at once, you will get this
  error once your application receives enough concurrent requests. Solution
  is to raise the limits on the pool or lower the number of concurrent threads.

* **The application is not returning connections to the pool** - This is the
  next most common reason, which is that the application is making use of the
  connection pool, but the program is failing to :term:`release` these
  connections and is instead leaving them open.   The connection pool as well
  as the ORM :class:`.Session` do have logic such that when the session and/or
  connection object is garbage collected, it results in the underlying
  connection resources being released, however this behavior cannot be relied
  upon to release resources in a timely manner.

  A common reason this can occur is that the application uses ORM sessions and
  does not call :meth:`.Session.close` upon them one the work involving that
  session is complete. Solution is to make sure ORM sessions if using the ORM,
  or engine-bound :class:`.Connection` objects if using Core, are explicitly
  closed at the end of the work being done, either via the appropriate
  ``.close()`` method, or by using one of the available context managers (e.g.
  "with:" statement) to properly release the resource.

* **The application is attempting to run long-running transactions** - A
  database transaction is a very expensive resource, and should **never be
  left idle waiting for some event to occur**.  If an application is waiting
  for a user to push a button, or a result to come off of a long running job
  queue, or is holding a persistent connection open to a browser, **don't
  keep a database transaction open for the whole time**.  As the application
  needs to work with the database and interact with an event, open a short-lived
  transaction at that point and then close it.

* **The application is deadlocking** - Also a common cause of this error and
  more difficult to grasp, if an application is not able to complete its use
  of a connection either due to an application-side or database-side deadlock,
  the application can use up all the available connections which then leads to
  additional requests receiving this error.   Reasons for deadlocks include:

  * Using an implicit async system such as gevent or eventlet without
    properly monkeypatching all socket libraries and drivers, or which
    has bugs in not fully covering for all monkeypatched driver methods,
    or less commonly when the async system is being used against CPU-bound
    workloads and greenlets making use of database resources are simply waiting
    too long to attend to them.  Neither implicit nor explicit async
    programming frameworks are typically
    necessary or appropriate for the vast majority of relational database
    operations; if an application must use an async system for some area
    of functionality, it's best that database-oriented business methods
    run within traditional threads that pass messages to the async part
    of the application.

  * A database side deadlock, e.g. rows are mutually deadlocked

  * Threading errors, such as mutexes in a mutual deadlock, or calling
    upon an already locked mutex in the same thread

Keep in mind an alternative to using pooling is to turn off pooling entirely.
See the section :ref:`pool_switching` for background on this.  However, note
that when this error message is occurring, it is **always** due to a bigger
problem in the application itself; the pool just helps to reveal the problem
sooner.

.. seealso::

 :ref:`pooling_toplevel`

 :ref:`connections_toplevel`


.. _error_dbapi:

DBAPI Errors
============

The Python database API, or DBAPI, is a specification for database drivers
which can be located at `Pep-249 <https://www.python.org/dev/peps/pep-0249/>`_.
This API specifies a set of exception classes that accommodate the full range
of failure modes of the database.

SQLAlchemy does not generate these exceptions directly.  Instead, they are
intercepted from the database driver and wrapped by the SQLAlchemy-provided
exception :class:`.DBAPIError`, however the messaging within the exception is
**generated by the driver, not SQLAlchemy**.

.. _error_rvf5:

InterfaceError
--------------

Exception raised for errors that are related to the database interface rather
than the database itself.

This error is a :ref:`DBAPI Error <error_dbapi>` and originates from
the database driver (DBAPI), not SQLAlchemy itself.

The ``InterfaceError`` is sometimes raised by drivers in the context
of the database connection being dropped, or not being able to connect
to the database.   For tips on how to deal with this, see the section
:ref:`pool_disconnects`.

.. _error_4xp6:

DatabaseError
--------------

Exception raised for errors that are related to the database itself, and not
the interface or data being passed.

This error is a :ref:`DBAPI Error <error_dbapi>` and originates from
the database driver (DBAPI), not SQLAlchemy itself.

.. _error_9h9h:

DataError
---------

Exception raised for errors that are due to problems with the processed data
like division by zero, numeric value out of range, etc.

This error is a :ref:`DBAPI Error <error_dbapi>` and originates from
the database driver (DBAPI), not SQLAlchemy itself.

.. _error_e3q8:

OperationalError
-----------------

Exception raised for errors that are related to the database's operation and
not necessarily under the control of the programmer, e.g. an unexpected
disconnect occurs, the data source name is not found, a transaction could not
be processed, a memory allocation error occurred during processing, etc.

This error is a :ref:`DBAPI Error <error_dbapi>` and originates from
the database driver (DBAPI), not SQLAlchemy itself.

The ``OperationalError`` is the most common (but not the only) error class used
by drivers in the context of the database connection being dropped, or not
being able to connect to the database.   For tips on how to deal with this, see
the section :ref:`pool_disconnects`.

.. _error_gkpj:

IntegrityError
--------------

Exception raised when the relational integrity of the database is affected,
e.g. a foreign key check fails.

This error is a :ref:`DBAPI Error <error_dbapi>` and originates from
the database driver (DBAPI), not SQLAlchemy itself.

.. _error_2j85:

InternalError
-------------

Exception raised when the database encounters an internal error, e.g. the
cursor is not valid anymore, the transaction is out of sync, etc.

This error is a :ref:`DBAPI Error <error_dbapi>` and originates from
the database driver (DBAPI), not SQLAlchemy itself.

The ``InternalError`` is sometimes raised by drivers in the context
of the database connection being dropped, or not being able to connect
to the database.   For tips on how to deal with this, see the section
:ref:`pool_disconnects`.

.. _error_f405:

ProgrammingError
----------------

Exception raised for programming errors, e.g. table not found or already
exists, syntax error in the SQL statement, wrong number of parameters
specified, etc.

This error is a :ref:`DBAPI Error <error_dbapi>` and originates from
the database driver (DBAPI), not SQLAlchemy itself.

The ``ProgrammingError`` is sometimes raised by drivers in the context
of the database connection being dropped, or not being able to connect
to the database.   For tips on how to deal with this, see the section
:ref:`pool_disconnects`.

.. _error_tw8g:

NotSupportedError
------------------

Exception raised in case a method or database API was used which is not
supported by the database, e.g. requesting a .rollback() on a connection that
does not support transaction or has transactions turned off.

This error is a :ref:`DBAPI Error <error_dbapi>` and originates from
the database driver (DBAPI), not SQLAlchemy itself.

SQL Expression Language
=======================

.. _error_2afi:

This Compiled object is not bound to any Engine or Connection
-------------------------------------------------------------

This error refers to the concept of "bound metadata", described at
:ref:`dbengine_implicit`.   The issue occurs when one invokes the
:meth:`.Executable.execute` method directly off of a Core expression object
that is not associated with any :class:`.Engine`::

 metadata = MetaData()
 table = Table('t', metadata, Column('q', Integer))

 stmt = select([table])
 result = stmt.execute()   # <--- raises

What the logic is expecting is that the :class:`.MetaData` object has
been **bound** to a :class:`.Engine`::

 engine = create_engine("mysql+pymysql://user:pass@host/db")
 metadata = MetaData(bind=engine)

Where above, any statement that derives from a :class:`.Table` which
in turn derives from that :class:`.MetaData` will implicitly make use of
the given :class:`.Engine` in order to invoke the statement.

Note that the concept of bound metadata is a **legacy pattern** and in most
cases is **highly discouraged**.   The best way to invoke the statement is
to pass it to the :meth:`.Connection.execute` method of a :class:`.Connection`::

 with engine.connect() as conn:
   result = conn.execute(stmt)

When using the ORM, a similar facility is available via the :class:`.Session`::

 result = session.exxecute(stmt)

.. seealso::

 :ref:`dbengine_implicit`


.. _error_cd3x:

A value is required for bind parameter <x> (in parameter group <y>)
-------------------------------------------------------------------

This error occurs when a statement makes use of :func:`.bindparam` either
implicitly or explicitly and does not provide a value when the statement
is executed::

 stmt = select([table.c.column]).where(table.c.id == bindparam('my_param'))

 result = conn.execute(stmt)

Above, no value has been provided for the parameter "my_param".  The correct
approach is to provide a value::

 result = conn.execute(stmt, my_param=12)

When the message takes the form "a value is required for bind parameter <x>
in parameter group <y>", the message is referring to the "executemany" stye
of execution.  In this case, the statement is typically an INSERT, UPDATE,
or DELETE and a list of parameters is being passed.   In this format, the
statement may be generated dynamically to include parameter positions for
every parameter given in the argument list, where it will use the
**first set of parameters** to determine what these should be.

For example, the statement below is calculated based on the first parameter
set to require the parameters, "a", "b", and "c" - these names determine
the final string format of the statement which will be used for each
set of parameters in the list.  As the second entry does not contain "b",
this error is generated::

 m = MetaData()
 t = Table(
     't', m,
     Column('a', Integer),
     Column('b', Integer),
     Column('c', Integer)
 )

 e.execute(
     t.insert(), [
         {"a": 1, "b": 2, "c": 3},
         {"a": 2, "c": 4},
         {"a": 3, "b": 4, "c": 5},
     ]
 )

 sqlalchemy.exc.StatementError: (sqlalchemy.exc.InvalidRequestError)
 A value is required for bind parameter 'b', in parameter group 1
 [SQL: u'INSERT INTO t (a, b, c) VALUES (?, ?, ?)']
 [parameters: [{'a': 1, 'c': 3, 'b': 2}, {'a': 2, 'c': 4}, {'a': 3, 'c': 5, 'b': 4}]]

Since "b" is required, pass it as ``None`` so that the INSERT may proceed::

 e.execute(
     t.insert(), [
         {"a": 1, "b": 2, "c": 3},
         {"a": 2, "b": None, "c": 4},
         {"a": 3, "b": 4, "c": 5},
     ]
 )

.. seealso::

 :ref:`coretutorial_bind_param`

 :ref:`execute_multiple`

Object Relational Mapping
=========================

.. _error_bhk3:

Parent instance <x> is not bound to a Session; (lazy load/deferred load/refresh/etc.) operation cannot proceed
--------------------------------------------------------------------------------------------------------------

This is likely the most common error message when dealing with the ORM, and it
occurs as a result of the nature of a technique the ORM makes wide use of known
as :term:`lazy loading`.   Lazy loading is a common object-relational pattern
whereby an object that's persisted by the ORM maintains a proxy to the database
itself, such that when various attributes upon the object are accessed, their
value may be retrieved from the database *lazily*.   The advantage to this
approach is that objects can be retrieved from the database without having
to load all of their attributes or related data at once, and instead only that
data which is requested can be delivered at that time.   The major disadvantage
is basically a mirror image of the advantage, which is that if lots of objects
are being loaded which are known to require a certain set of data in all cases,
it is wasteful to load that additional data piecemeal.

Another caveat of lazy loading beyond the usual efficiency concerns is that
in order for lazy loading to proceed, the object has to **remain associated
with a Session** in order to be able to retrieve its state.  This error message
means that an object has become de-associated with its :class:`.Session` and
is being asked to lazy load data from the database.

The most common reason that objects become detached from their :class:`.Session`
is that the session itself was closed, typically via the :meth:`.Session.close`
method.   The objects will then live on to be accessed further, very often
within web applications where they are delivered to a server-side templating
engine and are asked for further attributes which they cannot load.

Mitigation of this error is via two general techniques:

* **Don't close the session prematurely** - Often, applications will close
  out a transaction before passing off related objects to some other system
  which then fails due to this error.   Sometimes the transaction doesn't need
  to be closed so soon; an example is the web application closes out
  the transaction before the view is rendered.  This is often done in the name
  of "correctness", but may be seen as a mis-application of "encapsulation",
  as this term refers to code organization, not actual actions. The template that
  uses an ORM object is making use of the `proxy pattern <https://en.wikipedia.org/wiki/Proxy_pattern>`_
  which keeps database logic encapsulated from the caller.   If the
  :class:`.Session` can be held open until the lifespan of the objects are done,
  this is the best approach.

* **Load everything that's needed up front** - It is very often impossible to
  keep the transaction open, especially in more complex applications that need
  to pass objects off to other systems that can't run in the same context
  even though they're in the same process.  In this case, the application
  should try to make appropriate use of :term:`eager loading` to ensure
  that objects have what they need up front.   As an additional measure,
  special directives like the :func:`.raiseload` option can ensure that
  systems don't call upon lazy loading when its not expected.

  .. seealso::

    :ref:`loading_toplevel` - detailed documentation on eager loading and other
    relationship-oriented loading techniques


Core Exception Classes
======================

See :ref:`core_exceptions_toplevel` for Core exception classes.


ORM Exception Classes
======================

See :ref:`orm_exceptions_toplevel` for ORM exception classes.



