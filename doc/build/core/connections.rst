=====================================
Working with Engines and Connections
=====================================

.. module:: sqlalchemy.engine.base

Recall from the beginning of :ref:`engines_toplevel` that the :class:`.Engine` provides a
``connect()`` method which returns a
:class:`~sqlalchemy.engine.base.Connection` object.
:class:`~sqlalchemy.engine.base.Connection` is a *proxy* object which
maintains a reference to a DBAPI connection instance. The ``close()`` method
on :class:`~sqlalchemy.engine.base.Connection` does not actually close the
DBAPI connection, but instead returns it to the connection pool referenced by
the :class:`~sqlalchemy.engine.base.Engine`.
:class:`~sqlalchemy.engine.base.Connection` will also automatically return its
resources to the connection pool when the object is garbage collected, i.e.
its ``__del__()`` method is called. When using the standard C implementation
of Python, this method is usually called immediately as soon as the object is
dereferenced. With other Python implementations such as Jython, this is not so
guaranteed.

The ``execute()`` methods on both :class:`~sqlalchemy.engine.base.Engine` and
:class:`~sqlalchemy.engine.base.Connection` can also receive SQL clause
constructs as well::

    connection = engine.connect()
    result = connection.execute(select([table1], table1.c.col1==5))
    for row in result:
        print row['col1'], row['col2']
    connection.close()

The above SQL construct is known as a ``select()``. The full range of SQL
constructs available are described in :ref:`sqlexpression_toplevel`.

Both :class:`~sqlalchemy.engine.base.Connection` and
:class:`~sqlalchemy.engine.base.Engine` fulfill an interface known as
:class:`~sqlalchemy.engine.base.Connectable` which specifies common
functionality between the two objects, namely being able to call ``connect()``
to return a :class:`~sqlalchemy.engine.base.Connection` object
(:class:`~sqlalchemy.engine.base.Connection` just returns itself), and being
able to call ``execute()`` to get a result set. Following this, most
SQLAlchemy functions and objects which accept an
:class:`~sqlalchemy.engine.base.Engine` as a parameter or attribute with which
to execute SQL will also accept a :class:`~sqlalchemy.engine.base.Connection`.
This argument is named ``bind``::

    engine = create_engine('sqlite:///:memory:')

    # specify some Table metadata
    metadata = MetaData()
    table = Table('sometable', metadata, Column('col1', Integer))

    # create the table with the Engine
    table.create(bind=engine)

    # drop the table with a Connection off the Engine
    connection = engine.connect()
    table.drop(bind=connection)

.. index::
   single: thread safety; connections

Connection API
===============

.. autoclass:: Connection
   :show-inheritance:
   :members:

.. autoclass:: Connectable
   :show-inheritance:
   :members:

Engine API
===========

.. autoclass:: Engine
   :show-inheritance:
   :members:

Result Object API
=================

.. autoclass:: sqlalchemy.engine.base.ResultProxy
    :members:
    
.. autoclass:: sqlalchemy.engine.base.RowProxy
    :members:

Using Transactions
==================

.. note:: This section describes how to use transactions when working directly 
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

.. autoclass:: Transaction
    :members:


Understanding Autocommit
========================

The previous transaction example illustrates how to use
:class:`~sqlalchemy.engine.base.Transaction` so that several executions can
take part in the same transaction. What happens when we issue an INSERT,
UPDATE or DELETE call without using
:class:`~sqlalchemy.engine.base.Transaction`? The answer is **autocommit**.
While many DBAPIs implement a flag called ``autocommit``, the current
SQLAlchemy behavior is such that it implements its own autocommit. This is
achieved by detecting statements which represent data-changing operations,
i.e. INSERT, UPDATE, DELETE, etc., and then issuing a COMMIT automatically if
no transaction is in progress. The detection is based on compiled statement
attributes, or in the case of a text-only statement via regular expressions.

.. sourcecode:: python+sql

    conn = engine.connect()
    conn.execute("INSERT INTO users VALUES (1, 'john')")  # autocommits

.. _dbengine_implicit:

Connectionless Execution, Implicit Execution
=============================================

Recall from the first section we mentioned executing with and without a
:class:`~sqlalchemy.engine.base.Connection`. ``Connectionless`` execution
refers to calling the ``execute()`` method on an object which is not a
:class:`~sqlalchemy.engine.base.Connection`, which could be on the
:class:`~sqlalchemy.engine.base.Engine` itself, or could be a constructed SQL
object. When we say "implicit", we mean that we are calling the ``execute()``
method on an object which is neither a
:class:`~sqlalchemy.engine.base.Connection` nor an
:class:`~sqlalchemy.engine.base.Engine` object; this can only be used with
constructed SQL objects which have their own ``execute()`` method, and can be
"bound" to an :class:`~sqlalchemy.engine.base.Engine`. A description of
"constructed SQL objects" may be found in :ref:`sqlexpression_toplevel`.

A summary of all three methods follows below. First, assume the usage of the
following :class:`~sqlalchemy.schema.MetaData` and
:class:`~sqlalchemy.schema.Table` objects; while we haven't yet introduced
these concepts, for now you only need to know that we are representing a
database table, and are creating an "executable" SQL construct which issues a
statement to the database. These objects are described in
:ref:`metadata_toplevel`.

.. sourcecode:: python+sql

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
object (binding is discussed further in the next section,
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
the SQL statement. When we issue ``close()`` on the
:class:`~sqlalchemy.engine.base.ResultProxy`, or if the result set object
falls out of scope and is garbage collected, the underlying
:class:`~sqlalchemy.engine.base.Connection` is closed for us, resulting in the
DBAPI connection being returned to the pool.

.. _threadlocal_strategy:

Using the Threadlocal Execution Strategy
-----------------------------------------

The "threadlocal" engine strategy is used by non-ORM applications which wish
to bind a transaction to the current thread, such that all parts of the
application can participate in that transaction implicitly without the need to
explicitly reference a :class:`~sqlalchemy.engine.base.Connection`.
"threadlocal" is designed for a very specific pattern of use, and is not
appropriate unless this very specfic pattern, described below, is what's
desired. It has **no impact** on the "thread safety" of SQLAlchemy components
or one's application. It also should not be used when using an ORM
:class:`~sqlalchemy.orm.session.Session` object, as the
:class:`~sqlalchemy.orm.session.Session` itself represents an ongoing
transaction and itself handles the job of maintaining connection and
transactional resources.

Enabling ``threadlocal`` is achieved as follows:

.. sourcecode:: python+sql

    db = create_engine('mysql://localhost/test', strategy='threadlocal')

When the engine above is used in a "connectionless" style, meaning
``engine.execute()`` is called, a DBAPI connection is retrieved from the
connection pool and then associated with the current thread. Subsequent
operations on the :class:`~sqlalchemy.engine.base.Engine` while the DBAPI
connection remains checked out will make use of the *same* DBAPI connection
object. The connection stays allocated until all returned
:class:`~sqlalchemy.engine.base.ResultProxy` objects are closed, which occurs
for a particular :class:`~sqlalchemy.engine.base.ResultProxy` after all
pending results are fetched, or immediately for an operation which returns no
rows (such as an INSERT).

.. sourcecode:: python+sql

    # execute one statement and receive results.  r1 now references a DBAPI connection resource.
    r1 = db.execute("select * from table1")

    # execute a second statement and receive results.  r2 now references the *same* resource as r1
    r2 = db.execute("select * from table2")

    # fetch a row on r1 (assume more results are pending)
    row1 = r1.fetchone()

    # fetch a row on r2 (same)
    row2 = r2.fetchone()

    # close r1.  the connection is still held by r2.
    r1.close()

    # close r2.  with no more references to the underlying connection resources, they
    # are returned to the pool.
    r2.close()

The above example does not illustrate any pattern that is particularly useful,
as it is not a frequent occurence that two execute/result fetching operations
"leapfrog" one another. There is a slight savings of connection pool checkout
overhead between the two operations, and an implicit sharing of the same
transactional context, but since there is no explicitly declared transaction,
this association is short lived.

The real usage of "threadlocal" comes when we want several operations to occur
within the scope of a shared transaction. The
:class:`~sqlalchemy.engine.base.Engine` now has ``begin()``, ``commit()`` and
``rollback()`` methods which will retrieve a connection resource from the pool
and establish a new transaction, maintaining the connection against the
current thread until the transaction is committed or rolled back:

.. sourcecode:: python+sql

    db.begin()
    try:
        call_operation1()
        call_operation2()
        db.commit()
    except:
        db.rollback()

``call_operation1()`` and ``call_operation2()`` can make use of the
:class:`~sqlalchemy.engine.base.Engine` as a global variable, using the
"connectionless" execution style, and their operations will participate in the
same transaction:

.. sourcecode:: python+sql

    def call_operation1():
        engine.execute("insert into users values (?, ?)", 1, "john")

    def call_operation2():
        users.update(users.c.user_id==5).execute(name='ed')

When using threadlocal, operations that do call upon the ``engine.connect()``
method will receive a :class:`~sqlalchemy.engine.base.Connection` that is
**outside** the scope of the transaction. This can be used for operations such
as logging the status of an operation regardless of transaction success:

.. sourcecode:: python+sql

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

Functions which are written to use an explicit
:class:`~sqlalchemy.engine.base.Connection` object, but wish to participate in
the threadlocal transaction, can receive their
:class:`~sqlalchemy.engine.base.Connection` object from the
``contextual_connect()`` method, which returns a
:class:`~sqlalchemy.engine.base.Connection` that is **inside** the scope of
the transaction:

.. sourcecode:: python+sql

    conn = db.contextual_connect()
    call_operation3(conn)
    conn.close()

Calling ``close()`` on the "contextual" connection does not release the
connection resources to the pool if other resources are making use of it. A
resource-counting mechanism is employed so that the connection is released
back to the pool only when all users of that connection, including the
transaction established by ``engine.begin()``, have been completed.

So remember - if you're not sure if you need to use ``strategy="threadlocal"``
or not, the answer is **no** ! It's driven by a specific programming pattern
that is generally not the norm.

