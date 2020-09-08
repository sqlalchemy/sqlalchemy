.. _connections_toplevel:

====================================
Working with Engines and Connections
====================================

.. module:: sqlalchemy.engine

This section details direct usage of the :class:`_engine.Engine`,
:class:`_engine.Connection`, and related objects. Its important to note that when
using the SQLAlchemy ORM, these objects are not generally accessed; instead,
the :class:`.Session` object is used as the interface to the database.
However, for applications that are built around direct usage of textual SQL
statements and/or SQL expression constructs without involvement by the ORM's
higher level management services, the :class:`_engine.Engine` and
:class:`_engine.Connection` are king (and queen?) - read on.

Basic Usage
===========

Recall from :doc:`/core/engines` that an :class:`_engine.Engine` is created via
the :func:`_sa.create_engine` call::

    engine = create_engine('mysql://scott:tiger@localhost/test')

The typical usage of :func:`_sa.create_engine` is once per particular database
URL, held globally for the lifetime of a single application process. A single
:class:`_engine.Engine` manages many individual :term:`DBAPI` connections on behalf of
the process and is intended to be called upon in a concurrent fashion. The
:class:`_engine.Engine` is **not** synonymous to the DBAPI ``connect`` function, which
represents just one connection resource - the :class:`_engine.Engine` is most
efficient when created just once at the module level of an application, not
per-object or per-function call.

.. sidebar:: tip

    When using an :class:`_engine.Engine` with multiple Python processes, such as when
    using ``os.fork`` or Python ``multiprocessing``, it's important that the
    engine is initialized per process.  See :ref:`pooling_multiprocessing` for
    details.

The most basic function of the :class:`_engine.Engine` is to provide access to a
:class:`_engine.Connection`, which can then invoke SQL statements.   To emit
a textual statement to the database looks like::

    with engine.connect() as connection:
        result = connection.execute("select username from users")
        for row in result:
            print("username:", row['username'])

Above, the :meth:`_engine.Engine.connect` method returns a :class:`_engine.Connection`
object, and by using it in a Python context manager (e.g. the ``with:``
statement) the :meth:`_engine.Connection.close` method is automatically invoked at the
end of the block.  The :class:`_engine.Connection`, is a **proxy** object for an
actual DBAPI connection. The DBAPI connection is retrieved from the connection
pool at the point at which :class:`_engine.Connection` is created.

The object returned is known as :class:`_engine.ResultProxy`, which
references a DBAPI cursor and provides methods for fetching rows
similar to that of the DBAPI cursor.   The DBAPI cursor will be closed
by the :class:`_engine.ResultProxy` when all of its result rows (if any) are
exhausted.  A :class:`_engine.ResultProxy` that returns no rows, such as that of
an UPDATE statement (without any returned rows),
releases cursor resources immediately upon construction.

When the :class:`_engine.Connection` is closed at the end of the ``with:`` block, the
referenced DBAPI connection is :term:`released` to the connection pool.   From
the perspective of the database itself, the connection pool will not actually
"close" the connection assuming the pool has room to store this connection  for
the next use.  When the connection is returned to the pool for re-use, the
pooling mechanism issues a ``rollback()`` call on the DBAPI connection so that
any transactional state or locks are removed, and the connection is ready for
its next use.

Our example above illustrated the execution of a textual SQL string.
The :meth:`_engine.Connection.execute` method can of course accommodate more than
that, including the variety of SQL expression constructs described
in :ref:`sqlexpression_toplevel`.


Using Transactions
==================

.. note::

  This section describes how to use transactions when working directly
  with :class:`_engine.Engine` and :class:`_engine.Connection` objects. When using the
  SQLAlchemy ORM, the public API for transaction control is via the
  :class:`.Session` object, which makes usage of the :class:`.Transaction`
  object internally. See :ref:`unitofwork_transaction` for further
  information.

The :class:`~sqlalchemy.engine.Connection` object provides a :meth:`_engine.Connection.begin`
method which returns a :class:`.Transaction` object.  Like the :class:`_engine.Connection`
itself, this object is usually used within a Python ``with:`` block so
that its scope is managed::

    with engine.connect() as connection:
        with connection.begin():
            r1 = connection.execute(table1.select())
            connection.execute(table1.insert(), {"col1": 7, "col2": "this is some data"})

The above block can be stated more simply by using the :meth:`_engine.Engine.begin`
method of :class:`_engine.Engine`::

    # runs a transaction
    with engine.begin() as connection:
        r1 = connection.execute(table1.select())
        connection.execute(table1.insert(), {"col1": 7, "col2": "this is some data"})

The block managed by each ``.begin()`` method has the behavior such that
the transaction is committed when the block completes.   If an exception is
raised, the transaction is instead rolled back, and the exception propagated
outwards.

The underlying object used to represent the transaction is the
:class:`.Transaction` object.  This object is returned by the
:meth:`_engine.Connection.begin` method and includes the methods
:meth:`.Transaction.commit` and :meth:`.Transaction.rollback`.   The context
manager calling form, which invokes these methods automatically, is recommended
as a best practice.

.. _connections_nested_transactions:

Nesting of Transaction Blocks
-----------------------------

.. deprecated:: 1.4 The "transaction nesting" feature of SQLAlchemy is a legacy feature
   that will be deprecated in the 1.4 release and no longer part of the 2.0
   series of SQLAlchemy.   The pattern has proven to be a little too awkward
   and complicated, unless an application makes more of a first-class framework
   around the behavior.  See the following subsection
   :ref:`connections_avoid_nesting`.

The :class:`.Transaction` object also handles "nested" behavior by keeping
track of the outermost begin/commit pair. In this example, two functions both
issue a transaction on a :class:`_engine.Connection`, but only the outermost
:class:`.Transaction` object actually takes effect when it is committed.

.. sourcecode:: python+sql

    # method_a starts a transaction and calls method_b
    def method_a(connection):
        with connection.begin():  # open a transaction
            method_b(connection)

    # method_b also starts a transaction
    def method_b(connection):
        with connection.begin(): # open a transaction - this runs in the
                                 # context of method_a's transaction
            connection.execute("insert into mytable values ('bat', 'lala')")
            connection.execute(mytable.insert(), {"col1": "bat", "col2": "lala"})

    # open a Connection and call method_a
    with engine.connect() as conn:
        method_a(conn)

Above, ``method_a`` is called first, which calls ``connection.begin()``. Then
it calls ``method_b``. When ``method_b`` calls ``connection.begin()``, it just
increments a counter that is decremented when it calls ``commit()``. If either
``method_a`` or ``method_b`` calls ``rollback()``, the whole transaction is
rolled back. The transaction is not committed until ``method_a`` calls the
``commit()`` method. This "nesting" behavior allows the creation of functions
which "guarantee" that a transaction will be used if one was not already
available, but will automatically participate in an enclosing transaction if
one exists.

.. _connections_avoid_nesting:

Arbitrary Transaction Nesting as an Antipattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With many years of experience, the above "nesting" pattern has not proven to
be very popular, and where it has been observed in large projects such
as Openstack, it tends to be complicated.

The most ideal way to organize an application would have a single, or at
least very few, points at which the "beginning" and "commit" of all
database transactions is demarcated.   This is also the general
idea discussed in terms of the ORM at :ref:`session_faq_whentocreate`.  To
adapt the example from the previous section to this practice looks like::


    # method_a calls method_b
    def method_a(connection):
        method_b(connection)

    # method_b uses the connection and assumes the transaction
    # is external
    def method_b(connection):
        connection.execute(text("insert into mytable values ('bat', 'lala')"))
        connection.execute(mytable.insert(), {"col1": "bat", "col2": "lala"})

    # open a Connection inside of a transaction and call method_a
    with engine.begin() as conn:
        method_a(conn)

That is, ``method_a()`` and ``method_b()`` do not deal with the details
of the transaction at all; the transactional scope of the connection is
defined **externally** to the functions that have a SQL dialogue with the
connection.

It may be observed that the above code has fewer lines, and less indentation
which tends to correlate with lower :term:`cyclomatic complexity`.   The
above code is organized such that ``method_a()`` and ``method_b()`` are always
invoked from a point at which a transaction is begun.  The previous
version of the example features a ``method_a()`` and a ``method_b()`` that are
trying to be agnostic of this fact, which suggests they are prepared for
at least twice as many potential codepaths through them.

.. _connections_subtransactions:


Migrating from the "nesting" pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As SQLAlchemy's intrinsic-nested pattern is considered legacy, an application
that for either legacy or novel reasons still seeks to have a context that
automatically frames transactions should seek to maintain this functionality
through the use of a custom Python context manager.  A similar example is also
provided in terms of the ORM in the "seealso" section below.

To provide backwards compatibility for applications that make use of this
pattern, the following context manager or a similar implementation based on
a decorator may be used::

    import contextlib

    @contextlib.contextmanager
    def transaction(connection):
        if not connection.in_transaction():
            with connection.begin():
                yield connection
        else:
            yield connection

The above contextmanager would be used as::

    # method_a starts a transaction and calls method_b
    def method_a(connection):
        with transaction(connection):  # open a transaction
            method_b(connection)

    # method_b either starts a transaction, or uses the one already
    # present
    def method_b(connection):
        with transaction(connection):  # open a transaction
            connection.execute(text("insert into mytable values ('bat', 'lala')"))
            connection.execute(mytable.insert(), {"col1": "bat", "col2": "lala"})

    # open a Connection and call method_a
    with engine.connect() as conn:
        method_a(conn)

A similar approach may be taken such that connectivity is established
on demand as well; the below approach features a single-use context manager
that accesses an enclosing state in order to test if connectivity is already
present::

    import contextlib

    def connectivity(engine):
        connection = None

        @contextlib.contextmanager
        def connect():
            nonlocal connection

            if connection is None:
                connection = engine.connect()
                with connection:
                    with connection.begin():
                        yield connection
            else:
                yield connection

        return connect

Using the above would look like::

    # method_a passes along connectivity context, at the same time
    # it chooses to establish a connection by calling "with"
    def method_a(connectivity):
        with connectivity():
            method_b(connectivity)

    # method_b also wants to use a connection from the context, so it
    # also calls "with:", but also it actually uses the connection.
    def method_b(connectivity):
        with connectivity() as connection:
            connection.execute(text("insert into mytable values ('bat', 'lala')"))
            connection.execute(mytable.insert(), {"col1": "bat", "col2": "lala"})

    # create a new connection/transaction context object and call
    # method_a
    method_a(connectivity(engine))

The above context manager acts not only as a "transaction" context but also
as a context that manages having an open connection against a particular
:class:`_engine.Engine`.   When using the ORM :class:`_orm.Session`, this
connectivty management is provided by the :class:`_orm.Session` itself.
An overview of ORM connectivity patterns is at :ref:`unitofwork_transaction`.

.. seealso::

  :ref:`session_subtransactions_migrating` - ORM version

.. _autocommit:

Library Level (e.g. emulated) Autocommit
==========================================

.. note:: The "autocommit" feature of SQLAlchemy is a legacy feature that will
   be deprecated in an upcoming release.  New usage paradigms will eliminate
   the need for it to be present.

.. note:: This section discusses the feature within SQLAlchemy that automatically
   invokes the ``.commit()`` method on a DBAPI connection, however this is against
   a DBAPI connection that **is itself transactional**.  For true AUTOCOMMIT,
   see the next section :ref:`dbapi_autocommit`.

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
:meth:`_engine.Connection.execution_options` method provided on :class:`_engine.Connection`
and :class:`_engine.Engine`, using the "autocommit" flag which will
turn on or off the autocommit for the selected scope. For example, a
:func:`_expression.text` construct representing a stored procedure that commits might use
it so that a SELECT statement will issue a COMMIT::

    with engine.connect().execution_options(autocommit=True) as conn:
        conn.execute(text("SELECT my_mutating_procedure()"))

.. _dbapi_autocommit:

Setting Transaction Isolation Levels including DBAPI Autocommit
=================================================================

Most DBAPIs support the concept of configurable transaction :term:`isolation` levels.
These are traditionally the four levels "READ UNCOMMITTED", "READ COMMITTED",
"REPEATABLE READ" and "SERIALIZABLE".  These are usually applied to a
DBAPI connection before it begins a new transaction, noting that most
DBAPIs will begin this transaction implicitly when SQL statements are first
emitted.

DBAPIs that support isolation levels also usually support the concept of true
"autocommit", which means that the DBAPI connection itself will be placed into
a non-transactional autocommit mode.   This usually means that the typical
DBAPI behavior of emitting "BEGIN" to the database automatically no longer
occurs, but it may also include other directives.   When using this mode,
**the DBAPI does not use a transaction under any circumstances**.  SQLAlchemy
methods like ``.begin()``, ``.commit()`` and ``.rollback()`` pass silently
and have no effect.

Instead, each statement invoked upon the connection will commit any changes
automatically; it sometimes also means that the connection itself will use
fewer server-side database resources. For this reason and others, "autocommit"
mode is often desirable for non-transactional applications that need to read
individual tables or rows outside the scope of a true ACID transaction.

SQLAlchemy dialects should support these isolation levels as well as autocommit
to as great a degree as possible.   The levels are set via family of
"execution_options" parameters and methods that are throughout the Core, such
as the :meth:`_engine.Connection.execution_options` method.   The parameter is
known as :paramref:`_engine.Connection.execution_options.isolation_level` and
the values are strings which are typically a subset of the following names::

    # possible values for Connection.execution_options(isolation_level="<value>")

    "AUTOCOMMIT"
    "READ COMMITTED"
    "READ UNCOMMITTED"
    "REPEATABLE READ"
    "SERIALIZABLE"

Not every DBAPI supports every value; if an unsupported value is used for a
certain backend, an error is raised.

For example, to force REPEATABLE READ on a specific connection, then
begin a transaction::

  with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
      with connection.begin():
          connection.execute(<statement>)

The :paramref:`_engine.Connection.execution_options.isolation_level` option may
also be set engine wide, as is often preferable.  This is achieved by
passing it within the :paramref:`_sa.create_engine.execution_options`
parameter to :func:`_sa.create_engine`::


    from sqlalchemy import create_engine

    eng = create_engine(
        "postgresql://scott:tiger@localhost/test",
        execution_options={
            "isolation_level": "REPEATABLE READ"
        }
    )

With the above setting, the DBAPI connection will be set to use a
``"REPEATABLE READ"`` isolation level setting for each new transaction
begun.

An application that frequently chooses to run operations within different
isolation levels may wish to create multiple "sub-engines" of a lead
:class:`_engine.Engine`, each of which will be configured to a different
isolation level.  One such use case is an application that has operations
that break into "transactional" and "read-only" operations, a separate
:class:`_engine.Engine` that makes use of ``"AUTOCOMMIT"`` may be
separated off from the main engine::

    from sqlalchemy import create_engine

    eng = create_engine("postgresql://scott:tiger@localhost/test")

    autocommit_engine = eng.execution_options(isolation_level="AUTOCOMMIT")


Above, the :meth:`_engine.Engine.execution_options` method creates a shallow
copy of the original :class:`_engine.Engine`.  Both ``eng`` and
``autocommit_engine`` share the same dialect and connection pool.  However, the
"AUTOCOMMIT" mode will be set upon connections when they are acquired from the
``autocommit_engine``.

The isolation level setting, regardless of which one it is, is unconditionally
reverted when a connection is returned to the connection pool.


.. note:: The :paramref:`_engine.Connection.execution_options.isolation_level`
   parameter necessarily does not apply to statement level options, such as
   that of :meth:`_sql.Executable.execution_options`.  This because the option
   must be set on a DBAPI connection on a per-transaction basis.

.. seealso::

      :ref:`SQLite Transaction Isolation <sqlite_isolation_level>`

      :ref:`PostgreSQL Transaction Isolation <postgresql_isolation_level>`

      :ref:`MySQL Transaction Isolation <mysql_isolation_level>`

      :ref:`SQL Server Transaction Isolation <mssql_isolation_level>`

      :ref:`session_transaction_isolation` - for the ORM

.. _dbengine_implicit:


Connectionless Execution, Implicit Execution
============================================

.. note:: "Connectionless" and "implicit" execution are legacy SQLAlchemy
   features that will be deprecated in an upcoming release.

Recall from the first section we mentioned executing with and without explicit
usage of :class:`_engine.Connection`. "Connectionless" execution
refers to the usage of the ``execute()`` method on an object which is not a
:class:`_engine.Connection`.  This was illustrated using the
:meth:`_engine.Engine.execute` method of :class:`_engine.Engine`::

    result = engine.execute("select username from users")
    for row in result:
        print("username:", row['username'])

In addition to "connectionless" execution, it is also possible
to use the :meth:`~.Executable.execute` method of
any :class:`.Executable` construct, which is a marker for SQL expression objects
that support execution.   The SQL expression object itself references an
:class:`_engine.Engine` or :class:`_engine.Connection` known as the **bind**, which it uses
in order to provide so-called "implicit" execution services.

Given a table as below::

    from sqlalchemy import MetaData, Table, Column, Integer

    meta = MetaData()
    users_table = Table('users', meta,
        Column('id', Integer, primary_key=True),
        Column('name', String(50))
    )

Explicit execution delivers the SQL text or constructed SQL expression to the
:meth:`_engine.Connection.execute` method of :class:`~sqlalchemy.engine.Connection`:

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///file.db')
    with engine.connect() as connection:
        result = connection.execute(users_table.select())
        for row in result:
            # ....

Explicit, connectionless execution delivers the expression to the
:meth:`_engine.Engine.execute` method of :class:`~sqlalchemy.engine.Engine`:

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
object.   By "bound" we mean that the special attribute :attr:`_schema.MetaData.bind`
has been used to associate a series of
:class:`_schema.Table` objects and all SQL constructs derived from them with a specific
engine::

    engine = create_engine('sqlite:///file.db')
    meta.bind = engine
    result = users_table.select().execute()
    for row in result:
        # ....
    result.close()

Above, we associate an :class:`_engine.Engine` with a :class:`_schema.MetaData` object using
the special attribute :attr:`_schema.MetaData.bind`.  The :func:`_expression.select` construct produced
from the :class:`_schema.Table` object has a method :meth:`~.Executable.execute`, which will
search for an :class:`_engine.Engine` that's "bound" to the :class:`_schema.Table`.

Overall, the usage of "bound metadata" has three general effects:

* SQL statement objects gain an :meth:`.Executable.execute` method which automatically
  locates a "bind" with which to execute themselves.
* The ORM :class:`.Session` object supports using "bound metadata" in order
  to establish which :class:`_engine.Engine` should be used to invoke SQL statements
  on behalf of a particular mapped class, though the :class:`.Session`
  also features its own explicit system of establishing complex :class:`_engine.Engine`/
  mapped class configurations.
* The :meth:`_schema.MetaData.create_all`, :meth:`_schema.MetaData.drop_all`, :meth:`_schema.Table.create`,
  :meth:`_schema.Table.drop`, and "autoload" features all make usage of the bound
  :class:`_engine.Engine` automatically without the need to pass it explicitly.

.. note::

    The concepts of "bound metadata" and "implicit execution" are not emphasized in modern SQLAlchemy.
    While they offer some convenience, they are no longer required by any API and
    are never necessary.

    In applications where multiple :class:`_engine.Engine` objects are present, each one logically associated
    with a certain set of tables (i.e. *vertical sharding*), the "bound metadata" technique can be used
    so that individual :class:`_schema.Table` can refer to the appropriate :class:`_engine.Engine` automatically;
    in particular this is supported within the ORM via the :class:`.Session` object
    as a means to associate :class:`_schema.Table` objects with an appropriate :class:`_engine.Engine`,
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
the SQL statement. When the :class:`_engine.ResultProxy` is closed, the underlying
:class:`_engine.Connection` is closed for us, resulting in the
DBAPI connection being returned to the pool with transactional resources removed.

.. _schema_translating:

Translation of Schema Names
===========================

To support multi-tenancy applications that distribute common sets of tables
into multiple schemas, the
:paramref:`.Connection.execution_options.schema_translate_map`
execution option may be used to repurpose a set of :class:`_schema.Table` objects
to render under different schema names without any changes.

Given a table::

    user_table = Table(
        'user', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(50))
    )

The "schema" of this :class:`_schema.Table` as defined by the
:paramref:`_schema.Table.schema` attribute is ``None``.  The
:paramref:`.Connection.execution_options.schema_translate_map` can specify
that all :class:`_schema.Table` objects with a schema of ``None`` would instead
render the schema as ``user_schema_one``::

    connection = engine.connect().execution_options(
        schema_translate_map={None: "user_schema_one"})

    result = connection.execute(user_table.select())

The above code will invoke SQL on the database of the form::

    SELECT user_schema_one.user.id, user_schema_one.user.name FROM
    user_schema_one.user

That is, the schema name is substituted with our translated name.  The
map can specify any number of target->destination schemas::

    connection = engine.connect().execution_options(
        schema_translate_map={
            None: "user_schema_one",     # no schema name -> "user_schema_one"
            "special": "special_schema", # schema="special" becomes "special_schema"
            "public": None               # Table objects with schema="public" will render with no schema
        })

The :paramref:`.Connection.execution_options.schema_translate_map` parameter
affects all DDL and SQL constructs generated from the SQL expression language,
as derived from the :class:`_schema.Table` or :class:`.Sequence` objects.
It does **not** impact literal string SQL used via the :func:`_expression.text`
construct nor via plain strings passed to :meth:`_engine.Connection.execute`.

The feature takes effect **only** in those cases where the name of the
schema is derived directly from that of a :class:`_schema.Table` or :class:`.Sequence`;
it does not impact methods where a string schema name is passed directly.
By this pattern, it takes effect within the "can create" / "can drop" checks
performed by methods such as :meth:`_schema.MetaData.create_all` or
:meth:`_schema.MetaData.drop_all` are called, and it takes effect when
using table reflection given a :class:`_schema.Table` object.  However it does
**not** affect the operations present on the :class:`_reflection.Inspector` object,
as the schema name is passed to these methods explicitly.

.. versionadded:: 1.1

.. _engine_disposal:

Engine Disposal
===============

The :class:`_engine.Engine` refers to a connection pool, which means under normal
circumstances, there are open database connections present while the
:class:`_engine.Engine` object is still resident in memory.   When an :class:`_engine.Engine`
is garbage collected, its connection pool is no longer referred to by
that :class:`_engine.Engine`, and assuming none of its connections are still checked
out, the pool and its connections will also be garbage collected, which has the
effect of closing out the actual database connections as well.   But otherwise,
the :class:`_engine.Engine` will hold onto open database connections assuming
it uses the normally default pool implementation of :class:`.QueuePool`.

The :class:`_engine.Engine` is intended to normally be a permanent
fixture established up-front and maintained throughout the lifespan of an
application.  It is **not** intended to be created and disposed on a
per-connection basis; it is instead a registry that maintains both a pool
of connections as well as configurational information about the database
and DBAPI in use, as well as some degree of internal caching of per-database
resources.

However, there are many cases where it is desirable that all connection resources
referred to by the :class:`_engine.Engine` be completely closed out.  It's
generally not a good idea to rely on Python garbage collection for this
to occur for these cases; instead, the :class:`_engine.Engine` can be explicitly disposed using
the :meth:`_engine.Engine.dispose` method.   This disposes of the engine's
underlying connection pool and replaces it with a new one that's empty.
Provided that the :class:`_engine.Engine`
is discarded at this point and no longer used, all **checked-in** connections
which it refers to will also be fully closed.

Valid use cases for calling :meth:`_engine.Engine.dispose` include:

* When a program wants to release any remaining checked-in connections
  held by the connection pool and expects to no longer be connected
  to that database at all for any future operations.

* When a program uses multiprocessing or ``fork()``, and an
  :class:`_engine.Engine` object is copied to the child process,
  :meth:`_engine.Engine.dispose` should be called so that the engine creates
  brand new database connections local to that fork.   Database connections
  generally do **not** travel across process boundaries.

* Within test suites or multitenancy scenarios where many
  ad-hoc, short-lived :class:`_engine.Engine` objects may be created and disposed.


Connections that are **checked out** are **not** discarded when the
engine is disposed or garbage collected, as these connections are still
strongly referenced elsewhere by the application.
However, after :meth:`_engine.Engine.dispose` is called, those
connections are no longer associated with that :class:`_engine.Engine`; when they
are closed, they will be returned to their now-orphaned connection pool
which will ultimately be garbage collected, once all connections which refer
to it are also no longer referenced anywhere.
Since this process is not easy to control, it is strongly recommended that
:meth:`_engine.Engine.dispose` is called only after all checked out connections
are checked in or otherwise de-associated from their pool.

An alternative for applications that are negatively impacted by the
:class:`_engine.Engine` object's use of connection pooling is to disable pooling
entirely.  This typically incurs only a modest performance impact upon the
use of new connections, and means that when a connection is checked in,
it is entirely closed out and is not held in memory.  See :ref:`pool_switching`
for guidelines on how to disable pooling.

.. _threadlocal_strategy:

Using the Threadlocal Execution Strategy
========================================

The "threadlocal" engine strategy is an optional feature which
can be used by non-ORM applications to associate transactions
with the current thread, such that all parts of the
application can participate in that transaction implicitly without the need to
explicitly reference a :class:`_engine.Connection`.

.. deprecated:: 1.3

    The "threadlocal" engine strategy is deprecated, and will be removed
    in a future release.

    This strategy is designed for a particular pattern of usage which is
    generally considered as a legacy pattern.  It has **no impact** on the
    "thread safety" of SQLAlchemy components or one's application. It also
    should not be used when using an ORM
    :class:`~sqlalchemy.orm.session.Session` object, as the
    :class:`~sqlalchemy.orm.session.Session` itself represents an ongoing
    transaction and itself handles the job of maintaining connection and
    transactional resources.

    .. seealso::

        :ref:`change_4393_threadlocal`

Enabling ``threadlocal`` is achieved as follows::

    db = create_engine('mysql://localhost/test', strategy='threadlocal')

The above :class:`_engine.Engine` will now acquire a :class:`_engine.Connection` using
connection resources derived from a thread-local variable whenever
:meth:`_engine.Engine.execute` or :meth:`_engine.Engine.contextual_connect` is called. This
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
using the :meth:`_engine.Engine.connect` method to acquire a :class:`_engine.Connection`
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

To access the :class:`_engine.Connection` that is bound to the threadlocal scope,
call :meth:`_engine.Engine.contextual_connect`::

    conn = db.contextual_connect()
    call_operation3(conn)
    conn.close()

Calling :meth:`_engine.Connection.close` on the "contextual" connection does not :term:`release`
its resources until all other usages of that resource are closed as well, including
that any ongoing transactions are rolled back or committed.

.. _dbapi_connections:

Working with Raw DBAPI Connections
==================================

There are some cases where SQLAlchemy does not provide a genericized way
at accessing some :term:`DBAPI` functions, such as calling stored procedures as well
as dealing with multiple result sets.  In these cases, it's just as expedient
to deal with the raw DBAPI connection directly.

The most common way to access the raw DBAPI connection is to get it
from an already present :class:`_engine.Connection` object directly.  It is
present using the :attr:`_engine.Connection.connection` attribute::

    connection = engine.connect()
    dbapi_conn = connection.connection

The DBAPI connection here is actually a "proxied" in terms of the
originating connection pool, however this is an implementation detail
that in most cases can be ignored.    As this DBAPI connection is still
contained within the scope of an owning :class:`_engine.Connection` object, it is
best to make use of the :class:`_engine.Connection` object for most features such
as transaction control as well as calling the :meth:`_engine.Connection.close`
method; if these operations are performed on the DBAPI connection directly,
the owning :class:`_engine.Connection` will not be aware of these changes in state.

To overcome the limitations imposed by the DBAPI connection that is
maintained by an owning :class:`_engine.Connection`, a DBAPI connection is also
available without the need to procure a
:class:`_engine.Connection` first, using the :meth:`_engine.Engine.raw_connection` method
of :class:`_engine.Engine`::

    dbapi_conn = engine.raw_connection()

This DBAPI connection is again a "proxied" form as was the case before.
The purpose of this proxying is now apparent, as when we call the ``.close()``
method of this connection, the DBAPI connection is typically not actually
closed, but instead :term:`released` back to the
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

The :func:`_sa.create_engine` function call locates the given dialect
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


Connection / Engine API
=======================

.. autoclass:: Connection
   :members:

.. autoclass:: Connectable
   :members:

.. autoclass:: CreateEnginePlugin
   :members:

.. autoclass:: Engine
   :members:

.. autoclass:: ExceptionContext
   :members:

.. autoclass:: NestedTransaction
    :members:

.. autoclass:: ResultProxy
    :members:

.. autoclass:: RowProxy
    :members:

.. autoclass:: Transaction
    :members:

.. autoclass:: TwoPhaseTransaction
    :members:

