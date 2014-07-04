.. _connections_toplevel:

=====================================
Working with Engines and Connections
=====================================

.. module:: sqlalchemy.engine

This section details direct usage of the :class:`.Engine`,
:class:`.Connection`, and related objects. Its important to note that when
using the SQLAlchemy ORM, these objects are not generally accessed; instead,
the :class:`.Session` object is used as the interface to the database.
However, for applications that are built around direct usage of textual SQL
statements and/or SQL expression constructs without involvement by the ORM's
higher level management services, the :class:`.Engine` and
:class:`.Connection` are king (and queen?) - read on.

Basic Usage
===========

Recall from :doc:`/core/engines` that an :class:`.Engine` is created via
the :func:`.create_engine` call::

    engine = create_engine('mysql://scott:tiger@localhost/test')

The typical usage of :func:`.create_engine()` is once per particular database
URL, held globally for the lifetime of a single application process. A single
:class:`.Engine` manages many individual DBAPI connections on behalf of the
process and is intended to be called upon in a concurrent fashion. The
:class:`.Engine` is **not** synonymous to the DBAPI ``connect`` function,
which represents just one connection resource - the :class:`.Engine` is most
efficient when created just once at the module level of an application, not
per-object or per-function call.

For a multiple-process application that uses the ``os.fork`` system call, or
for example the Python ``multiprocessing`` module, it's usually required that a
separate :class:`.Engine` be used for each child process. This is because the
:class:`.Engine` maintains a reference to a connection pool that ultimately
references DBAPI connections - these tend to not be portable across process
boundaries. An :class:`.Engine` that is configured not to use pooling (which
is achieved via the usage of :class:`.NullPool`) does not have this
requirement.

The engine can be used directly to issue SQL to the database. The most generic
way is first procure a connection resource, which you get via the
:meth:`.Engine.connect` method::

    connection = engine.connect()
    result = connection.execute("select username from users")
    for row in result:
        print "username:", row['username']
    connection.close()

The connection is an instance of :class:`.Connection`,
which is a **proxy** object for an actual DBAPI connection.  The DBAPI
connection is retrieved from the connection pool at the point at which
:class:`.Connection` is created.

The returned result is an instance of :class:`.ResultProxy`, which
references a DBAPI cursor and provides a largely compatible interface
with that of the DBAPI cursor.   The DBAPI cursor will be closed
by the :class:`.ResultProxy` when all of its result rows (if any) are
exhausted.  A :class:`.ResultProxy` that returns no rows, such as that of
an UPDATE statement (without any returned rows),
releases cursor resources immediately upon construction.

When the :meth:`~.Connection.close` method is called, the referenced DBAPI
connection is :term:`released` to the connection pool.   From the perspective
of the database itself, nothing is actually "closed", assuming pooling is
in use.  The pooling mechanism issues a ``rollback()`` call on the DBAPI
connection so that any transactional state or locks are removed, and
the connection is ready for its next usage.

The above procedure can be performed in a shorthand way by using the
:meth:`~.Engine.execute` method of :class:`.Engine` itself::

    result = engine.execute("select username from users")
    for row in result:
        print "username:", row['username']

Where above, the :meth:`~.Engine.execute` method acquires a new
:class:`.Connection` on its own, executes the statement with that object,
and returns the :class:`.ResultProxy`.  In this case, the :class:`.ResultProxy`
contains a special flag known as ``close_with_result``, which indicates
that when its underlying DBAPI cursor is closed, the :class:`.Connection`
object itself is also closed, which again returns the DBAPI connection
to the connection pool, releasing transactional resources.

If the :class:`.ResultProxy` potentially has rows remaining, it can be
instructed to close out its resources explicitly::

    result.close()

If the :class:`.ResultProxy` has pending rows remaining and is dereferenced by
the application without being closed, Python garbage collection will
ultimately close out the cursor as well as trigger a return of the pooled
DBAPI connection resource to the pool (SQLAlchemy achieves this by the usage
of weakref callbacks - *never* the ``__del__`` method) - however it's never a
good idea to rely upon Python garbage collection to manage resources.

Our example above illustrated the execution of a textual SQL string.
The :meth:`~.Connection.execute` method can of course accommodate more than
that, including the variety of SQL expression constructs described
in :ref:`sqlexpression_toplevel`.

Using Transactions
==================

.. note::

  This section describes how to use transactions when working directly
  with :class:`.Engine` and :class:`.Connection` objects. When using the
  SQLAlchemy ORM, the public API for transaction control is via the
  :class:`.Session` object, which makes usage of the :class:`.Transaction`
  object internally. See :ref:`unitofwork_transaction` for further
  information.

The :class:`~sqlalchemy.engine.Connection` object provides a :meth:`~.Connection.begin`
method which returns a :class:`.Transaction` object.
This object is usually used within a try/except clause so that it is
guaranteed to invoke :meth:`.Transaction.rollback` or :meth:`.Transaction.commit`::

    connection = engine.connect()
    trans = connection.begin()
    try:
        r1 = connection.execute(table1.select())
        connection.execute(table1.insert(), col1=7, col2='this is some data')
        trans.commit()
    except:
        trans.rollback()
        raise

The above block can be created more succinctly using context
managers, either given an :class:`.Engine`::

    # runs a transaction
    with engine.begin() as connection:
        r1 = connection.execute(table1.select())
        connection.execute(table1.insert(), col1=7, col2='this is some data')

Or from the :class:`.Connection`, in which case the :class:`.Transaction` object
is available as well::

    with connection.begin() as trans:
        r1 = connection.execute(table1.select())
        connection.execute(table1.insert(), col1=7, col2='this is some data')

.. _connections_nested_transactions:

Nesting of Transaction Blocks
------------------------------

The :class:`.Transaction` object also handles "nested"
behavior by keeping track of the outermost begin/commit pair. In this example,
two functions both issue a transaction on a :class:`.Connection`, but only the outermost
:class:`.Transaction` object actually takes effect when it is committed.

.. sourcecode:: python+sql

    # method_a starts a transaction and calls method_b
    def method_a(connection):
        trans = connection.begin() # open a transaction
        try:
            method_b(connection)
            trans.commit()  # transaction is committed here
        except:
            trans.rollback() # this rolls back the transaction unconditionally
            raise

    # method_b also starts a transaction
    def method_b(connection):
        trans = connection.begin() # open a transaction - this runs in the context of method_a's transaction
        try:
            connection.execute("insert into mytable values ('bat', 'lala')")
            connection.execute(mytable.insert(), col1='bat', col2='lala')
            trans.commit()  # transaction is not committed yet
        except:
            trans.rollback() # this rolls back the transaction unconditionally
            raise

    # open a Connection and call method_a
    conn = engine.connect()
    method_a(conn)
    conn.close()

Above, ``method_a`` is called first, which calls ``connection.begin()``. Then
it calls ``method_b``. When ``method_b`` calls ``connection.begin()``, it just
increments a counter that is decremented when it calls ``commit()``. If either
``method_a`` or ``method_b`` calls ``rollback()``, the whole transaction is
rolled back. The transaction is not committed until ``method_a`` calls the
``commit()`` method. This "nesting" behavior allows the creation of functions
which "guarantee" that a transaction will be used if one was not already
available, but will automatically participate in an enclosing transaction if
one exists.

.. index::
   single: thread safety; transactions

.. _autocommit:

Understanding Autocommit
========================

The previous transaction example illustrates how to use :class:`.Transaction`
so that several executions can take part in the same transaction. What happens
when we issue an INSERT, UPDATE or DELETE call without using
:class:`.Transaction`?  While some DBAPI
implementations provide various special "non-transactional" modes, the core
behavior of DBAPI per PEP-0249 is that a *transaction is always in progress*,
providing only ``rollback()`` and ``commit()`` methods but no ``begin()``.
SQLAlchemy assumes this is the case for any given DBAPI.

Given this requirement, SQLAlchemy implements its own "autocommit" feature which
works completely consistently across all backends. This is achieved by
detecting statements which represent data-changing operations, i.e. INSERT,
UPDATE, DELETE, as well as data definition language (DDL) statements such as
CREATE TABLE, ALTER TABLE, and then issuing a COMMIT automatically if no
transaction is in progress. The detection is based on the presence of the
``autocommit=True`` execution option on the statement.   If the statement
is a text-only statement and the flag is not set, a regular expression is used
to detect INSERT, UPDATE, DELETE, as well as a variety of other commands
for a particular backend::

    conn = engine.connect()
    conn.execute("INSERT INTO users VALUES (1, 'john')")  # autocommits

The "autocommit" feature is only in effect when no :class:`.Transaction` has
otherwise been declared.   This means the feature is not generally used with
the ORM, as the :class:`.Session` object by default always maintains an
ongoing :class:`.Transaction`.

Full control of the "autocommit" behavior is available using the generative
:meth:`.Connection.execution_options` method provided on :class:`.Connection`,
:class:`.Engine`, :class:`.Executable`, using the "autocommit" flag which will
turn on or off the autocommit for the selected scope. For example, a
:func:`.text` construct representing a stored procedure that commits might use
it so that a SELECT statement will issue a COMMIT::

    engine.execute(text("SELECT my_mutating_procedure()").execution_options(autocommit=True))

.. _dbengine_implicit:

Connectionless Execution, Implicit Execution
=============================================

Recall from the first section we mentioned executing with and without explicit
usage of :class:`.Connection`. "Connectionless" execution
refers to the usage of the ``execute()`` method on an object which is not a
:class:`.Connection`.  This was illustrated using the :meth:`~.Engine.execute` method
of :class:`.Engine`::

    result = engine.execute("select username from users")
    for row in result:
        print "username:", row['username']

In addition to "connectionless" execution, it is also possible
to use the :meth:`~.Executable.execute` method of
any :class:`.Executable` construct, which is a marker for SQL expression objects
that support execution.   The SQL expression object itself references an
:class:`.Engine` or :class:`.Connection` known as the **bind**, which it uses
in order to provide so-called "implicit" execution services.

Given a table as below::

    from sqlalchemy import MetaData, Table, Column, Integer

    meta = MetaData()
    users_table = Table('users', meta,
        Column('id', Integer, primary_key=True),
        Column('name', String(50))
    )

Explicit execution delivers the SQL text or constructed SQL expression to the
:meth:`~.Connection.execute` method of :class:`~sqlalchemy.engine.Connection`:

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///file.db')
    connection = engine.connect()
    result = connection.execute(users_table.select())
    for row in result:
        # ....
    connection.close()

Explicit, connectionless execution delivers the expression to the
:meth:`~.Engine.execute` method of :class:`~sqlalchemy.engine.Engine`:

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///file.db')
    result = engine.execute(users_table.select())
    for row in result:
        # ....
    result.close()

Implicit execution is also connectionless, and makes usage of the :meth:`~.Executable.execute` method
on the expression itself.   This method is provided as part of the
:class:`.Executable` class, which refers to a SQL statement that is sufficient
for being invoked against the database.    The method makes usage of
the assumption that either an
:class:`~sqlalchemy.engine.Engine` or
:class:`~sqlalchemy.engine.Connection` has been **bound** to the expression
object.   By "bound" we mean that the special attribute :attr:`.MetaData.bind`
has been used to associate a series of
:class:`.Table` objects and all SQL constructs derived from them with a specific
engine::

    engine = create_engine('sqlite:///file.db')
    meta.bind = engine
    result = users_table.select().execute()
    for row in result:
        # ....
    result.close()

Above, we associate an :class:`.Engine` with a :class:`.MetaData` object using
the special attribute :attr:`.MetaData.bind`.  The :func:`.select` construct produced
from the :class:`.Table` object has a method :meth:`~.Executable.execute`, which will
search for an :class:`.Engine` that's "bound" to the :class:`.Table`.

Overall, the usage of "bound metadata" has three general effects:

* SQL statement objects gain an :meth:`.Executable.execute` method which automatically
  locates a "bind" with which to execute themselves.
* The ORM :class:`.Session` object supports using "bound metadata" in order
  to establish which :class:`.Engine` should be used to invoke SQL statements
  on behalf of a particular mapped class, though the :class:`.Session`
  also features its own explicit system of establishing complex :class:`.Engine`/
  mapped class configurations.
* The :meth:`.MetaData.create_all`, :meth:`.MetaData.drop_all`, :meth:`.Table.create`,
  :meth:`.Table.drop`, and "autoload" features all make usage of the bound
  :class:`.Engine` automatically without the need to pass it explicitly.

.. note::

    The concepts of "bound metadata" and "implicit execution" are not emphasized in modern SQLAlchemy.
    While they offer some convenience, they are no longer required by any API and
    are never necessary.

    In applications where multiple :class:`.Engine` objects are present, each one logically associated
    with a certain set of tables (i.e. *vertical sharding*), the "bound metadata" technique can be used
    so that individual :class:`.Table` can refer to the appropriate :class:`.Engine` automatically;
    in particular this is supported within the ORM via the :class:`.Session` object
    as a means to associate :class:`.Table` objects with an appropriate :class:`.Engine`,
    as an alternative to using the bind arguments accepted directly by the :class:`.Session`.

    However, the "implicit execution" technique is not at all appropriate for use with the
    ORM, as it bypasses the transactional context maintained by the :class:`.Session`.

    Overall, in the *vast majority* of cases, "bound metadata" and "implicit execution"
    are **not useful**.   While "bound metadata" has a marginal level of usefulness with regards to
    ORM configuration, "implicit execution" is a very old usage pattern that in most
    cases is more confusing than it is helpful, and its usage is discouraged.
    Both patterns seem to encourage the overuse of expedient "short cuts" in application design
    which lead to problems later on.

    Modern SQLAlchemy usage, especially the ORM, places a heavy stress on working within the context
    of a transaction at all times; the "implicit execution" concept makes the job of
    associating statement execution with a particular transaction much more difficult.
    The :meth:`.Executable.execute` method on a particular SQL statement
    usually implies that the execution is not part of any particular transaction, which is
    usually not the desired effect.

In both "connectionless" examples, the
:class:`~sqlalchemy.engine.Connection` is created behind the scenes; the
:class:`~sqlalchemy.engine.ResultProxy` returned by the ``execute()``
call references the :class:`~sqlalchemy.engine.Connection` used to issue
the SQL statement. When the :class:`.ResultProxy` is closed, the underlying
:class:`.Connection` is closed for us, resulting in the
DBAPI connection being returned to the pool with transactional resources removed.

.. _threadlocal_strategy:

Using the Threadlocal Execution Strategy
========================================

The "threadlocal" engine strategy is an optional feature which
can be used by non-ORM applications to associate transactions
with the current thread, such that all parts of the
application can participate in that transaction implicitly without the need to
explicitly reference a :class:`.Connection`.

.. note::

    The "threadlocal" feature is generally discouraged.   It's
    designed for a particular pattern of usage which is generally
    considered as a legacy pattern.  It has **no impact** on the "thread safety"
    of SQLAlchemy components
    or one's application. It also should not be used when using an ORM
    :class:`~sqlalchemy.orm.session.Session` object, as the
    :class:`~sqlalchemy.orm.session.Session` itself represents an ongoing
    transaction and itself handles the job of maintaining connection and
    transactional resources.

Enabling ``threadlocal`` is achieved as follows::

    db = create_engine('mysql://localhost/test', strategy='threadlocal')

The above :class:`.Engine` will now acquire a :class:`.Connection` using
connection resources derived from a thread-local variable whenever
:meth:`.Engine.execute` or :meth:`.Engine.contextual_connect` is called. This
connection resource is maintained as long as it is referenced, which allows
multiple points of an application to share a transaction while using
connectionless execution::

    def call_operation1():
        engine.execute("insert into users values (?, ?)", 1, "john")

    def call_operation2():
        users.update(users.c.user_id==5).execute(name='ed')

    db.begin()
    try:
        call_operation1()
        call_operation2()
        db.commit()
    except:
        db.rollback()

Explicit execution can be mixed with connectionless execution by
using the :meth:`.Engine.connect` method to acquire a :class:`.Connection`
that is not part of the threadlocal scope::

    db.begin()
    conn = db.connect()
    try:
        conn.execute(log_table.insert(), message="Operation started")
        call_operation1()
        call_operation2()
        db.commit()
        conn.execute(log_table.insert(), message="Operation succeeded")
    except:
        db.rollback()
        conn.execute(log_table.insert(), message="Operation failed")
    finally:
        conn.close()

To access the :class:`.Connection` that is bound to the threadlocal scope,
call :meth:`.Engine.contextual_connect`::

    conn = db.contextual_connect()
    call_operation3(conn)
    conn.close()

Calling :meth:`~.Connection.close` on the "contextual" connection does not :term:`release`
its resources until all other usages of that resource are closed as well, including
that any ongoing transactions are rolled back or committed.

.. _dbapi_connections:

Working with Raw DBAPI Connections
==================================

There are some cases where SQLAlchemy does not provide a genericized way
at accessing some :term:`DBAPI` functions, such as calling stored procedures as well
as dealing with multiple result sets.  In these cases, it's just as expedient
to deal with the raw DBAPI connection directly.   This is accessible from
a :class:`.Engine` using the :meth:`.Engine.raw_connection` method::

    dbapi_conn = engine.raw_connection()

The instance returned is a "wrapped" form of DBAPI connection. When its
``.close()`` method is called, the connection is :term:`released` back to the
engine's connection pool::

    dbapi_conn.close()

While SQLAlchemy may in the future add built-in patterns for more DBAPI
use cases, there are diminishing returns as these cases tend to be rarely
needed and they also vary highly dependent on the type of DBAPI in use,
so in any case the direct DBAPI calling pattern is always there for those
cases where it is needed.

Some recipes for DBAPI connection use follow.

.. _stored_procedures:

Calling Stored Procedures
-------------------------

For stored procedures with special syntactical or parameter concerns,
DBAPI-level `callproc <http://legacy.python.org/dev/peps/pep-0249/#callproc>`_
may be used::

    connection = engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.callproc("my_procedure", ['x', 'y', 'z'])
        results = list(cursor.fetchall())
        cursor.close()
        connection.commit()
    finally:
        connection.close()

Multiple Result Sets
--------------------

Multiple result set support is available from a raw DBAPI cursor using the
`nextset <http://legacy.python.org/dev/peps/pep-0249/#nextset>`_ method::

    connection = engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.execute("select * from table1; select * from table2")
        results_one = cursor.fetchall()
        cursor.nextset()
        results_two = cursor.fetchall()
        cursor.close()
    finally:
        connection.close()



Registering New Dialects
========================

The :func:`.create_engine` function call locates the given dialect
using setuptools entrypoints.   These entry points can be established
for third party dialects within the setup.py script.  For example,
to create a new dialect "foodialect://", the steps are as follows:

1. Create a package called ``foodialect``.
2. The package should have a module containing the dialect class,
   which is typically a subclass of :class:`sqlalchemy.engine.default.DefaultDialect`.
   In this example let's say it's called ``FooDialect`` and its module is accessed
   via ``foodialect.dialect``.
3. The entry point can be established in setup.py as follows::

      entry_points="""
      [sqlalchemy.dialects]
      foodialect = foodialect.dialect:FooDialect
      """

If the dialect is providing support for a particular DBAPI on top of
an existing SQLAlchemy-supported database, the name can be given
including a database-qualification.  For example, if ``FooDialect``
were in fact a MySQL dialect, the entry point could be established like this::

      entry_points="""
      [sqlalchemy.dialects]
      mysql.foodialect = foodialect.dialect:FooDialect
      """

The above entrypoint would then be accessed as ``create_engine("mysql+foodialect://")``.

Registering Dialects In-Process
-------------------------------

SQLAlchemy also allows a dialect to be registered within the current process, bypassing
the need for separate installation.   Use the ``register()`` function as follows::

    from sqlalchemy.dialects import registry
    registry.register("mysql.foodialect", "myapp.dialect", "MyMySQLDialect")

The above will respond to ``create_engine("mysql+foodialect://")`` and load the
``MyMySQLDialect`` class from the ``myapp.dialect`` module.

.. versionadded:: 0.8

Connection / Engine API
=======================

.. autoclass:: Connection
   :members:

.. autoclass:: Connectable
   :members:

.. autoclass:: Engine
   :members:

.. autoclass:: sqlalchemy.engine.ExceptionContext
   :members:

.. autoclass:: NestedTransaction
    :members:

.. autoclass:: sqlalchemy.engine.ResultProxy
    :members:

.. autoclass:: sqlalchemy.engine.RowProxy
    :members:

.. autoclass:: Transaction
    :members:

.. autoclass:: TwoPhaseTransaction
    :members:

