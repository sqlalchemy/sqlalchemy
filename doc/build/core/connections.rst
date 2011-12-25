.. _connections_toplevel:

=====================================
Working with Engines and Connections
=====================================

.. module:: sqlalchemy.engine.base

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

Recall from :ref:`engines_toplevel` that an :class:`.Engine` is created via
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
way is first procure a connection resource, which you get via the :class:`connect` method::

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
connection is returned to the connection pool.   From the perspective
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

The :class:`~sqlalchemy.engine.base.Connection` object provides a ``begin()``
method which returns a :class:`~sqlalchemy.engine.base.Transaction` object.
This object is usually used within a try/except clause so that it is
guaranteed to ``rollback()`` or ``commit()``::

    trans = connection.begin()
    try:
        r1 = connection.execute(table1.select())
        connection.execute(table1.insert(), col1=7, col2='this is some data')
        trans.commit()
    except:
        trans.rollback()
        raise

.. _connections_nested_transactions:

Nesting of Transaction Blocks
------------------------------

The :class:`~sqlalchemy.engine.base.Transaction` object also handles "nested"
behavior by keeping track of the outermost begin/commit pair. In this example,
two functions both issue a transaction on a Connection, but only the outermost
Transaction object actually takes effect when it is committed.

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
of :class:`.Engine`.

In addition to "connectionless" execution, it is also possible 
to use the :meth:`~.Executable.execute` method of 
any :class:`.Executable` construct, which is a marker for SQL expression objects
that support execution.   The SQL expression object itself references an
:class:`.Engine` or :class:`.Connection` known as the **bind**, which it uses
in order to provide so-called "implicit" execution services.

Given a table as below::

    meta = MetaData()
    users_table = Table('users', meta,
        Column('id', Integer, primary_key=True),
        Column('name', String(50))
    )

Explicit execution delivers the SQL text or constructed SQL expression to the
``execute()`` method of :class:`~sqlalchemy.engine.base.Connection`:

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///file.db')
    connection = engine.connect()
    result = connection.execute(users_table.select())
    for row in result:
        # ....
    connection.close()

Explicit, connectionless execution delivers the expression to the
``execute()`` method of :class:`~sqlalchemy.engine.base.Engine`:

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///file.db')
    result = engine.execute(users_table.select())
    for row in result:
        # ....
    result.close()

Implicit execution is also connectionless, and calls the ``execute()`` method
on the expression itself, utilizing the fact that either an
:class:`~sqlalchemy.engine.base.Engine` or
:class:`~sqlalchemy.engine.base.Connection` has been *bound* to the expression
object (binding is discussed further in 
:ref:`metadata_toplevel`):

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///file.db')
    meta.bind = engine
    result = users_table.select().execute()
    for row in result:
        # ....
    result.close()

In both "connectionless" examples, the
:class:`~sqlalchemy.engine.base.Connection` is created behind the scenes; the
:class:`~sqlalchemy.engine.base.ResultProxy` returned by the ``execute()``
call references the :class:`~sqlalchemy.engine.base.Connection` used to issue
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
"threadlocal" is designed for a very specific pattern of use, and is not
appropriate unless this very specfic pattern, described below, is what's
desired. It has **no impact** on the "thread safety" of SQLAlchemy components
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
using the :class:`.Engine.connect` method to acquire a :class:`.Connection`
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

Calling :meth:`~.Connection.close` on the "contextual" connection does not release 
its resources until all other usages of that resource are closed as well, including
that any ongoing transactions are rolled back or committed.

Connection / Engine API
=======================

.. autoclass:: Connection
   :show-inheritance:
   :members:

.. autoclass:: Connectable
   :show-inheritance:
   :members:

.. autoclass:: Engine
   :show-inheritance:
   :members:

.. autoclass:: NestedTransaction
    :show-inheritance:
    :members:

.. autoclass:: sqlalchemy.engine.base.ResultProxy
    :members:

.. autoclass:: sqlalchemy.engine.base.RowProxy
    :members:

.. autoclass:: Transaction
    :show-inheritance:
    :members:

.. autoclass:: TwoPhaseTransaction
    :show-inheritance:
    :members:

