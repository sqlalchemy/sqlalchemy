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


Legacy API Features
===================

.. the reason we need this section here distinct from the migration notes
   is because this is actually an ArgumentError that's raised by select()
   when the "legacy" and "future" mode styles are used together.

.. _error_c9ae:

select() construct created in "legacy" mode; keyword arguments, etc.
--------------------------------------------------------------------

The :func:`_expression.select` construct has been updated as of SQLAlchemy
1.4 to support the newer calling style that will be standard in
:ref:`SQLAlchemy 2.0 <error_b8d9>`.   For backwards compatibility in the
interm, the construct accepts arguments in both the "legacy" style as well
as the "new" style.

The "new" style features that column and table expressions are passed
positionally to the :func:`_expression.select` construct only; any other
modifiers to the object must be passed using subsequent method chaining::

    # this is the way to do it going forward
    stmt = select(table1.c.myid).where(table1.c.myid == table2.c.otherid)

For comparison, a :func:`_expression.select` in legacy forms of SQLAlchemy,
before methods like :meth:`.Select.where` were even added, would like::

    # this is how it was documented in original SQLAlchemy versions
    # many years ago
    stmt = select([table1.c.myid], whereclause=table1.c.myid == table2.c.otherid)

Or even that the "whereclause" would be passed positionally::

    # this is also how it was documented in original SQLAlchemy versions
    # many years ago
    stmt = select([table1.c.myid], table1.c.myid == table2.c.otherid)

For some years now, the additional "whereclause" and other arguments that are
accepted have been removed from most narrative documentation, leading to a
calling style that is most familiar as the list of column arguments passed
as a list, but no further arguments::

    # this is how it's been documented since around version 1.0 or so
    stmt = select([table1.c.myid]).where(table1.c.myid == table2.c.otherid)

The document at :ref:`migration_20_5284` describes this change in terms
of :ref:`2.0 Migration <migration_20_toplevel>`.

.. seealso::

    :ref:`migration_20_5284`

    :ref:`migration_20_toplevel`



.. _error_b8d9:

The <some function> in SQLAlchemy 2.0 will no longer <something>
--------------------------------------------------------------------------------------------

SQLAlchemy 2.0 is expected to be a major shift for a wide variety of key
SQLAlchemy usage patterns in both the Core and ORM components.   The goal
of this release is to make a slight readjustment in some of the most
fundamental assumptions of SQLAlchemy since its early beginnings, and
to deliver a newly streamlined usage model that is hoped to be significantly
more minimalist and consistent between the Core and ORM components, as well as
more capable.

Introduced at :ref:`migration_20_toplevel`, the SQLAlchemy 2.0 project includes
a comprehensive future compatibility system that is to be integrated into the
1.4 series of SQLAlchemy, such that applications will have a clear,
unambiguous, and incremental upgrade path in order to migrate applications to
being fully 2.0 compatible.   The :class:`.exc.RemovedIn20Warning` deprecation
warning is at the base of this system to provide guidance on what behaviors in
an existing codebase will need to be modified.  An overview of how to enable
this warning is at :ref:`deprecation_20_mode`.

.. seealso::

    :ref:`migration_20_toplevel`  - An overview of the upgrade process from
    the 1.x series, as well as the current goals and progress of SQLAlchemy
    2.0.


    :ref:`deprecation_20_mode` - specific guidelines on how to use
    "2.0 deprecations mode" in SQLAlchemy 1.4.

.. _error_c9bf:

A bind was located via legacy bound metadata, but since future=True is set on this Session, this bind is ignored.
-------------------------------------------------------------------------------------------------------------------

The concept of "bound metadata" is being removed in SQLAlchemy 2.0.  This
refers to the :paramref:`_schema.MetaData.bind` parameter on the
:class:`_schema.MetaData` object that in turn allows objects like the ORM
:class:`_orm.Session` to associate a particular mapped class with an
:class:`_orm.Engine`.   In SQLAlchemy 2.0, the :class:`_orm.Session` must be
linked to each :class:`_orm.Engine` directly. That is, instead of instantiating
the :class:`_orm.Session` or
:class:`_orm.sessionmaker` without any arguments, and associating the
:class:`_engine.Engine` with the :class:`_schema.MetaData`::

    engine = create_engine("sqlite://")
    Session = sessionmaker()
    metadata = MetaData(bind=engine)
    Base = declarative_base(metadata=metadata)

    class MyClass(Base):
        # ...


    session = Session()
    session.add(MyClass())
    session.commit()

The :class:`_engine.Engine` must instead be associated directly with the
:class:`_orm.sessionmaker` or :class:`_orm.Session`.  The
:class:`_schema.MetaData` object should no longer be associated with any
engine::


    engine = create_engine("sqlite://")
    Session = sessionmaker(engine)
    Base = declarative_base()

    class MyClass(Base):
        # ...


    session = Session()
    session.add(MyClass())
    session.commit()

In SQLAlchemy 1.4, this :term:`2.0 style` behavior is enabled when the
:paramref:`_orm.Session.future` flag is set on :class:`_orm.sessionmaker`
or :class:`_orm.Session`.

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
  of an :class:`_engine.Engine` object, and then :term:`releases` that resource,
  the database connection itself remains connected to the database and
  is returned to an internal queue where it can be used again.  Even though
  the code may appear to be ending its conversation with the database, in many
  cases the application will still maintain a fixed number of database connections
  that persist until the application ends or the pool is explicitly disposed.

* Because of the pool, when an application makes use of a SQL database
  connection, most typically from either making use of :meth:`_engine.Engine.connect`
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

  The above :class:`_engine.Engine` will allow **at most 30 connections** to be in
  play at any time, not including connections that were detached from the
  engine or invalidated.  If a request for a new connection arrives and
  30 connections are already in use by other parts of the application,
  the connection pool will block for a fixed period of time,
  before timing out and raising this error message.

  In order to allow for a higher number of connections be in use at once,
  the pool can be adjusted using the
  :paramref:`_sa.create_engine.pool_size` and :paramref:`_sa.create_engine.max_overflow`
  parameters as passed to the :func:`_sa.create_engine` function.      The timeout
  to wait for a connection to be available is configured using the
  :paramref:`_sa.create_engine.pool_timeout` parameter.

* The pool can be configured to have unlimited overflow by setting
  :paramref:`_sa.create_engine.max_overflow` to the value "-1".  With this setting,
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
  or engine-bound :class:`_engine.Connection` objects if using Core, are explicitly
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


.. _error_8s2b:

Can't reconnect until invalid transaction is rolled back
----------------------------------------------------------

This error condition refers to the case where a :class:`_engine.Connection` was
invalidated, either due to a database disconnect detection or due to an
explicit call to :meth:`_engine.Connection.invalidate`, but there is still a
transaction present that was initiated by the :meth:`_engine.Connection.begin`
method.  When a connection is invalidated, any :class:`_engine.Transaction`
that was in progress is now in an invalid state, and must be explicitly rolled
back in order to remove it from the :class:`_engine.Connection`.

.. _error_8s2a:

This connection is on an inactive transaction.  Please rollback() fully before proceeding
------------------------------------------------------------------------------------------

This error condition was added to SQLAlchemy as of version 1.4.    The error
refers to the state where a :class:`_engine.Connection` is placed into a
transaction using a method like :meth:`_engine.Connection.begin`, and then a
further "marker" transaction is created within that scope; the "marker"
transaction is then rolled back using :meth:`.Transaction.rollback` or closed
using :meth:`.Transaction.close`, however the outer transaction is still
present in an "inactive" state and must be rolled back.

The pattern looks like::

    engine = create_engine(...)

    connection = engine.connect()
    transaction1 = connection.begin()

    # this is a "sub" or "marker" transaction, a logical nesting
    # structure based on "real" transaction transaction1
    transaction2 = connection.begin()
    transaction2.rollback()

    # transaction1 is still present and needs explicit rollback,
    # so this will raise
    connection.execute(text("select 1"))

Above, ``transaction2`` is a "marker" transaction, which indicates a logical
nesting of transactions within an outer one; while the inner transaction
can roll back the whole transaction via its rollback() method, its commit()
method has no effect except to close the scope of the "marker" transaction
itself.   The call to ``transaction2.rollback()`` has the effect of
**deactivating** transaction1 which means it is essentially rolled back
at the database level, however is still present in order to accommodate
a consistent nesting pattern of transactions.

The correct resolution is to ensure the outer transaction is also
rolled back::

    transaction1.rollback()

This pattern is not commonly used in Core.  Within the ORM, a similar issue can
occur which is the product of the ORM's "logical" transaction structure; this
is described in the FAQ entry at :ref:`faq_session_rollback`.

The "subtransaction" pattern is to be removed in SQLAlchemy 2.0 so that this
particular programming pattern will no longer be available and this
error message will no longer occur in Core.

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

.. _error_l7de:

Compiler StrSQLCompiler can't render element of type <element type>
-------------------------------------------------------------------

This error usually occurs when attempting to stringify a SQL expression
construct that includes elements which are not part of the default compilation;
in this case, the error will be against the :class:`.StrSQLCompiler` class.
In less common cases, it can also occur when the wrong kind of SQL expression
is used with a particular type of database backend; in those cases, other
kinds of SQL compiler classes will be named, such as ``SQLCompiler`` or
``sqlalchemy.dialects.postgresql.PGCompiler``.  The guidance below is
more specific to the "stringification" use case but describes the general
background as well.

Normally, a Core SQL construct or ORM :class:`_query.Query` object can be stringified
directly, such as when we use ``print()``::

  >>> from sqlalchemy import column
  >>> print(column('x') == 5)
  x = :x_1

When the above SQL expression is stringified, the :class:`.StrSQLCompiler`
compiler class is used, which is a special statement compiler that is invoked
when a construct is stringified without any dialect-specific information.

However, there are many constructs that are specific to some particular kind
of database dialect, for which the :class:`.StrSQLCompiler` doesn't know how
to turn into a string, such as the PostgreSQL
`"insert on conflict" <postgresql_insert_on_conflict>`_ construct::

  >>> from sqlalchemy.dialects.postgresql import insert
  >>> from sqlalchemy import table, column
  >>> my_table = table('my_table', column('x'), column('y'))
  >>> insert_stmt = insert(my_table).values(x='foo')
  >>> insert_stmt = insert_stmt.on_conflict_do_nothing(
  ...     index_elements=['y']
  ... )
  >>> print(insert_stmt)
  Traceback (most recent call last):

  ...

  sqlalchemy.exc.UnsupportedCompilationError:
  Compiler <sqlalchemy.sql.compiler.StrSQLCompiler object at 0x7f04fc17e320>
  can't render element of type
  <class 'sqlalchemy.dialects.postgresql.dml.OnConflictDoNothing'>

In order to stringify constructs that are specific to particular backend,
the :meth:`_expression.ClauseElement.compile` method must be used, passing either an
:class:`_engine.Engine` or a :class:`.Dialect` object which will invoke the correct
compiler.   Below we use a PostgreSQL dialect::

  >>> from sqlalchemy.dialects import postgresql
  >>> print(insert_stmt.compile(dialect=postgresql.dialect()))
  INSERT INTO my_table (x) VALUES (%(x)s) ON CONFLICT (y) DO NOTHING

For an ORM :class:`_query.Query` object, the statement can be accessed using the
:attr:`~.orm.query.Query.statement` accessor::

    statement = query.statement
    print(statement.compile(dialect=postgresql.dialect()))

See the FAQ link below for additional detail on direct stringification /
compilation of SQL elements.

.. seealso::

  :ref:`faq_sql_expression_string`


TypeError: <operator> not supported between instances of 'ColumnProperty' and <something>
-----------------------------------------------------------------------------------------

This often occurs when attempting to use a :func:`.column_property` or
:func:`.deferred` object in the context of a SQL expression, usually within
declarative such as::

    class Bar(Base):
        __tablename__ = 'bar'

        id = Column(Integer, primary_key=True)
        cprop = deferred(Column(Integer))

        __table_args__ = (
            CheckConstraint(cprop > 5),
        )

Above, the ``cprop`` attribute is used inline before it has been mapped,
however this ``cprop`` attribute is not a :class:`_schema.Column`,
it's a :class:`.ColumnProperty`, which is an interim object and therefore
does not have the full functionality of either the :class:`_schema.Column` object
or the :class:`.InstrumentedAttribute` object that will be mapped onto the
``Bar`` class once the declarative process is complete.

While the :class:`.ColumnProperty` does have a ``__clause_element__()`` method,
which allows it to work in some column-oriented contexts, it can't work in an
open-ended comparison context as illustrated above, since it has no Python
``__eq__()`` method that would allow it to interpret the comparison to the
number "5" as a SQL expression and not a regular Python comparison.

The solution is to access the :class:`_schema.Column` directly using the
:attr:`.ColumnProperty.expression` attribute::

    class Bar(Base):
        __tablename__ = 'bar'

        id = Column(Integer, primary_key=True)
        cprop = deferred(Column(Integer))

        __table_args__ = (
            CheckConstraint(cprop.expression > 5),
        )

.. _error_2afi:

This Compiled object is not bound to any Engine or Connection
-------------------------------------------------------------

This error refers to the concept of "bound metadata", described at
:ref:`dbengine_implicit`.   The issue occurs when one invokes the
:meth:`.Executable.execute` method directly off of a Core expression object
that is not associated with any :class:`_engine.Engine`::

 metadata = MetaData()
 table = Table('t', metadata, Column('q', Integer))

 stmt = select(table)
 result = stmt.execute()   # <--- raises

What the logic is expecting is that the :class:`_schema.MetaData` object has
been **bound** to a :class:`_engine.Engine`::

 engine = create_engine("mysql+pymysql://user:pass@host/db")
 metadata = MetaData(bind=engine)

Where above, any statement that derives from a :class:`_schema.Table` which
in turn derives from that :class:`_schema.MetaData` will implicitly make use of
the given :class:`_engine.Engine` in order to invoke the statement.

Note that the concept of bound metadata is a **legacy pattern** and in most
cases is **highly discouraged**.   The best way to invoke the statement is
to pass it to the :meth:`_engine.Connection.execute` method of a :class:`_engine.Connection`::

 with engine.connect() as conn:
   result = conn.execute(stmt)

When using the ORM, a similar facility is available via the :class:`.Session`::

 result = session.execute(stmt)

.. seealso::

 :ref:`dbengine_implicit`


.. _error_cd3x:

A value is required for bind parameter <x> (in parameter group <y>)
-------------------------------------------------------------------

This error occurs when a statement makes use of :func:`.bindparam` either
implicitly or explicitly and does not provide a value when the statement
is executed::

 stmt = select(table.c.column).where(table.c.id == bindparam('my_param'))

 result = conn.execute(stmt)

Above, no value has been provided for the parameter "my_param".  The correct
approach is to provide a value::

 result = conn.execute(stmt, my_param=12)

When the message takes the form "a value is required for bind parameter <x>
in parameter group <y>", the message is referring to the "executemany" style
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

.. _error_89ve:

Expected FROM clause, got Select.  To create a FROM clause, use the .subquery() method
--------------------------------------------------------------------------------------

This refers to a change made as of SQLAlchemy 1.4 where a SELECT statement as generated
by a function such as :func:`_expression.select`, but also including things like unions and textual
SELECT expressions are no longer considered to be :class:`_expression.FromClause` objects and
can't be placed directly in the FROM clause of another SELECT statement without them
being wrapped in a :class:`.Subquery` first.   This is a major conceptual change in the
Core and the full rationale is discussed at :ref:`change_4617`.

Given an example as::

    m = MetaData()
    t = Table(
       't', m,
       Column('a', Integer),
       Column('b', Integer),
       Column('c', Integer)
    )
    stmt = select(t)

Above, ``stmt`` represents a SELECT statement.  The error is produced when we want
to use ``stmt`` directly as a FROM clause in another SELECT, such as if we
attempted to select from it::

    new_stmt_1 = select(stmt)

Or if we wanted to use it in a FROM clause such as in a JOIN::

    new_stmt_2 = select(some_table).select_from(some_table.join(stmt))

In previous versions of SQLAlchemy, using a SELECT inside of another SELECT
would produce a parenthesized, unnamed subquery.   In most cases, this form of
SQL is not very useful as databases like MySQL and PostgreSQL require that
subqueries in FROM clauses have named aliases, which means using the
:meth:`_expression.SelectBase.alias` method or as of 1.4 using the
:meth:`_expression.SelectBase.subquery` method to produce this.   On other databases, it
is still much clearer for the subquery to have a name to resolve any ambiguity
on future references to column  names inside the subquery.

Beyond the above practical reasons, there are a lot of other SQLAlchemy-oriented
reasons the change is being made.  The correct form of the above two statements
therefore requires that :meth:`_expression.SelectBase.subquery` is used::

    subq = stmt.subquery()

    new_stmt_1 = select(subq)

    new_stmt_2 = select(some_table).select_from(some_table.join(subq))

.. seealso::

  :ref:`change_4617`


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
  that objects have what they need up front.

  When using this approach, it is usually necessary that the
  :paramref:`_orm.Session.expire_on_commit` parameter be set to ``False``, so
  that after a :meth:`_orm.Session.commit` operation, the objects within the
  session aren't :term:`expired`, which would incur a lazy load if their
  attributes were subsequently accessed.  Additionally, the
  :meth:`_orm.Session.rollback` method unconditionally expires all contents in
  the :class:`_orm.Session` and should also be avoided in non-error scenarios.

  .. seealso::

    :ref:`loading_toplevel` - detailed documentation on eager loading and other
    relationship-oriented loading techniques

    :ref:`session_committing` - background on session commit

    :ref:`session_expire` - background on attribute expiry


.. _error_7s2a:

This Session's transaction has been rolled back due to a previous exception during flush
----------------------------------------------------------------------------------------

The flush process of the :class:`.Session`, described at
:ref:`session_flushing`, will roll back the database transaction if an error is
encountered, in order to maintain internal consistency.  However, once this
occurs, the session's transaction is now "inactive" and must be explicitly
rolled back by the calling application, in the same way that it would otherwise
need to be explicitly committed if a failure had not occurred.

This is a common error when using the ORM and typically applies to an
application that doesn't yet have correct "framing" around its
:class:`.Session` operations. Further detail is described in the FAQ at
:ref:`faq_session_rollback`.

.. _error_bbf0:

For relationship <relationship>, delete-orphan cascade is normally configured only on the "one" side of a one-to-many relationship, and not on the "many" side of a many-to-one or many-to-many relationship.
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


This error arises when the "delete-orphan" :ref:`cascade <unitofwork_cascades>`
is set on a many-to-one or many-to-many relationship, such as::


    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)

        bs = relationship("B", back_populates="a")


    class B(Base):
        __tablename__ = "b"
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey("a.id"))

        # this will emit the error message when the mapper
        # configuration step occurs
        a = relationship("A", back_populates="bs", cascade="all, delete-orphan")

    configure_mappers()

Above, the "delete-orphan" setting on ``B.a`` indicates the intent that
when every ``B`` object that refers to a particular ``A`` is deleted, that the
``A`` should then be deleted as well.   That is, it expresses that the "orphan"
which is being deleted would be an ``A`` object, and it becomes an "orphan"
when every ``B`` that refers to it is deleted.

The "delete-orphan" cascade model does not support this functionality.   The
"orphan" consideration is only made in terms of the deletion of a single object
which would then refer to zero or more objects that are now "orphaned" by
this single deletion, which would result in those objects being deleted as
well.  In other words, it is designed only to track the creation of "orphans"
based on the removal of one and only one "parent" object per orphan,  which is
the natural case in a one-to-many relationship where a deletion of the
object on the "one" side results in the subsequent deletion of the related
items on the "many" side.

The above mapping in support of this functionality would instead place the
cascade setting on the one-to-many side, which looks like::

    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)

        bs = relationship("B", back_populates="a", cascade="all, delete-orphan")


    class B(Base):
        __tablename__ = "b"
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey("a.id"))

        a = relationship("A", back_populates="bs")

Where the intent is expressed that when an ``A`` is deleted, all of the
``B`` objects to which it refers are also deleted.

The error message then goes on to suggest the usage of the
:paramref:`_orm.relationship.single_parent` flag.    This flag may be used
to enforce that a relationship which is capable of having many objects
refer to a particular object will in fact have only **one** object referring
to it at a time.   It is used for legacy or other less ideal
database schemas where the foreign key relationships suggest a "many"
collection, however in practice only one object would actually refer
to a given target object at at time.  This uncommon scenario
can be demonstrated in terms of the above example as follows::

    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)

        bs = relationship("B", back_populates="a")


    class B(Base):
        __tablename__ = "b"
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey("a.id"))

        a = relationship(
            "A",
            back_populates="bs",
            single_parent=True,
            cascade="all, delete-orphan",
        )

The above configuration will then install a validator which will enforce
that only one ``B`` may be associated with an ``A`` at at time, within
the scope of the ``B.a`` relationship::

    >>> b1 = B()
    >>> b2 = B()
    >>> a1 = A()
    >>> b1.a = a1
    >>> b2.a = a1
    sqlalchemy.exc.InvalidRequestError: Instance <A at 0x7eff44359350> is
    already associated with an instance of <class '__main__.B'> via its
    B.a attribute, and is only allowed a single parent.

Note that this validator is of limited scope and will not prevent multiple
"parents" from being created via the other direction.  For example, it will
not detect the same setting in terms of ``A.bs``:

.. sourcecode:: pycon+sql

    >>> a1.bs = [b1, b2]
    >>> session.add_all([a1, b1, b2])
    >>> session.commit()
    {opensql}
    INSERT INTO a DEFAULT VALUES
    ()
    INSERT INTO b (a_id) VALUES (?)
    (1,)
    INSERT INTO b (a_id) VALUES (?)
    (1,)

However, things will not go as expected later on, as the "delete-orphan" cascade
will continue to work in terms of a **single** lead object, meaning if we
delete **either** of the ``B`` objects, the ``A`` is deleted.   The other ``B`` stays
around, where the ORM will usually be smart enough to set the foreign key attribute
to NULL, but this is usually not what's desired:

.. sourcecode:: pycon+sql

    >>> session.delete(b1)
    >>> session.commit()
    {opensql}
    UPDATE b SET a_id=? WHERE b.id = ?
    (None, 2)
    DELETE FROM b WHERE b.id = ?
    (1,)
    DELETE FROM a WHERE a.id = ?
    (1,)
    COMMIT

For all the above examples, similar logic applies to the calculus of a
many-to-many relationship; if a many-to-many relationship sets single_parent=True
on one side, that side can use the "delete-orphan" cascade, however this is
very unlikely to be what someone actually wants as the point of a many-to-many
relationship is so that there can be many objects referring to an object
in either direction.

Overall, "delete-orphan" cascade is usually applied
on the "one" side of a one-to-many relationship so that it deletes objects
in the "many" side, and not the other way around.

.. versionchanged:: 1.3.18  The text of the "delete-orphan" error message
   when used on a many-to-one or many-to-many relationship has been updated
   to be more descriptive.


.. seealso::

    :ref:`unitofwork_cascades`

    :ref:`cascade_delete_orphan`

    :ref:`error_bbf1`



.. _error_bbf1:

Instance <instance> is already associated with an instance of <instance> via its <attribute> attribute, and is only allowed a single parent.
---------------------------------------------------------------------------------------------------------------------------------------------


This error is emitted when the :paramref:`_orm.relationship.single_parent` flag
is used, and more than one object is assigned as the "parent" of an object at
once.

Given the following mapping::

    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)


    class B(Base):
        __tablename__ = "b"
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey("a.id"))

        a = relationship(
            "A",
            single_parent=True,
            cascade="all, delete-orphan",
        )

The intent indicates that no more than a single ``B`` object may refer
to a particular ``A`` object at once::

    >>> b1 = B()
    >>> b2 = B()
    >>> a1 = A()
    >>> b1.a = a1
    >>> b2.a = a1
    sqlalchemy.exc.InvalidRequestError: Instance <A at 0x7eff44359350> is
    already associated with an instance of <class '__main__.B'> via its
    B.a attribute, and is only allowed a single parent.

When this error occurs unexpectedly, it is usually because the
:paramref:`_orm.relationship.single_parent` flag was applied in response
to the error message described at :ref:`error_bbf0`, and the issue is in
fact a misunderstanding of the "delete-orphan" cascade setting.  See that
message for details.


.. seealso::

    :ref:`error_bbf0`


.. _error_qzyx:

relationship X will copy column Q to column P, which conflicts with relationship(s): 'Y'
----------------------------------------------------------------------------------------

This warning refers to the case when two or more relationships will write data to the
same columns on flush, but the ORM does not have any kind of back population configuration
between the two relationships.  The fix is usually to install the correct
:paramref:`_orm.back_populates` configuration.   Given the following mapping::

  class Parent(Base):
      __tablename__ = "parent"
      id = Column(Integer, primary_key=True)
      children = relationship("Child")


  class Child(Base):
      __tablename__ = "child"
      id = Column(Integer, primary_key=True)
      parent_id = Column(ForeignKey("parent.id"))
      parent = relationship("Parent")

The above mapping will generate warnings::

  SAWarning: relationship 'Child.parent' will copy column parent.id to column child.parent_id,
  which conflicts with relationship(s): 'Parent.children' (copies parent.id to child.parent_id).

The relationships ``Child.parent`` and ``Parent.children`` appear to be in conflict.
The solution is to apply :paramref:`_orm.relationship.back_populates`::

  class Parent(Base):
      __tablename__ = "parent"
      id = Column(Integer, primary_key=True)
      children = relationship("Child", back_populates="parent")


  class Child(Base):
      __tablename__ = "child"
      id = Column(Integer, primary_key=True)
      parent_id = Column(ForeignKey("parent.id"))
      parent = relationship("Parent", back_populates="children")

For more customized relationships where an "overlap" situation may be
intentional and cannot be resolved, the :paramref:`_orm.relationship.overlaps`
parameter may specify the names of relationships for which the warning should
not take effect. This typically occurs for two or more relationships to the
same underlying table that include custom
:paramref:`_orm.relationship.primaryjoin` conditions that limit the related
items in each case::

  class Parent(Base):
      __tablename__ = "parent"
      id = Column(Integer, primary_key=True)
      c1 = relationship(
          "Child",
          primaryjoin="and_(Parent.id == Child.parent_id, Child.flag == 0)",
          backref="parent",
          overlaps="c2, parent"
      )
      c2 = relationship(
          "Child",
          primaryjoin="and_(Parent.id == Child.parent_id, Child.flag == 1)",
          overlaps="c1, parent"
      )


  class Child(Base):
      __tablename__ = "child"
      id = Column(Integer, primary_key=True)
      parent_id = Column(ForeignKey("parent.id"))

      flag = Column(Integer)


Above, the ORM will know that the overlap between ``Parent.c1``,
``Parent.c2`` and ``Child.parent`` is intentional.

AsyncIO Exceptions
==================

.. _error_xd1r:

AwaitRequired
-------------

The SQLAlchemy async mode requires an async driver to be used to connect to the db.
This error is usually raised when trying to use the async version of SQLAlchemy
with a non compatible :term:`DBAPI`.

.. seealso::

    :ref:`asyncio_toplevel`

.. _error_xd2s:

MissingGreenlet
---------------

A call to the async :term:`DBAPI` was initiated outside the greenlet spawn
context usually setup by the SQLAlchemy AsyncIO proxy classes. Usually this
error happens when an IO was attempted in an unexpected place, without using
the provided async api. When using the ORM this may be due to a lazy loading
attempt, which is unsupported when using SQLAlchemy with AsyncIO dialects.

.. seealso::

    :ref:`asyncio_orm_avoid_lazyloads` - covers most ORM scenarios where
    this problem can occur and how to mitigate.


Core Exception Classes
======================

See :ref:`core_exceptions_toplevel` for Core exception classes.


ORM Exception Classes
======================

See :ref:`orm_exceptions_toplevel` for ORM exception classes.



