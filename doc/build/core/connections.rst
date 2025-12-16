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
-----------

Recall from :doc:`/core/engines` that an :class:`_engine.Engine` is created via
the :func:`_sa.create_engine` call::

    engine = create_engine("mysql+mysqldb://scott:tiger@localhost/test")

The typical usage of :func:`_sa.create_engine` is once per particular database
URL, held globally for the lifetime of a single application process. A single
:class:`_engine.Engine` manages many individual :term:`DBAPI` connections on behalf of
the process and is intended to be called upon in a concurrent fashion. The
:class:`_engine.Engine` is **not** synonymous to the DBAPI ``connect()`` function, which
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

    from sqlalchemy import text

    with engine.connect() as connection:
        result = connection.execute(text("select username from users"))
        for row in result:
            print("username:", row.username)

Above, the :meth:`_engine.Engine.connect` method returns a :class:`_engine.Connection`
object, and by using it in a Python context manager (e.g. the ``with:``
statement) the :meth:`_engine.Connection.close` method is automatically invoked at the
end of the block.  The :class:`_engine.Connection`, is a **proxy** object for an
actual DBAPI connection. The DBAPI connection is retrieved from the connection
pool at the point at which :class:`_engine.Connection` is created.

The object returned is known as :class:`_engine.CursorResult`, which
references a DBAPI cursor and provides methods for fetching rows
similar to that of the DBAPI cursor.   The DBAPI cursor will be closed
by the :class:`_engine.CursorResult` when all of its result rows (if any) are
exhausted.  A :class:`_engine.CursorResult` that returns no rows, such as that of
an UPDATE statement (without any returned rows),
releases cursor resources immediately upon construction.

When the :class:`_engine.Connection` is closed at the end of the ``with:`` block, the
referenced DBAPI connection is :term:`released` to the connection pool.   From
the perspective of the database itself, the connection pool will not actually
"close" the connection assuming the pool has room to store this connection  for
the next use.  When the connection is returned to the pool for re-use, the
pooling mechanism issues a ``rollback()`` call on the DBAPI connection so that
any transactional state or locks are removed (this is known as
:ref:`pool_reset_on_return`), and the connection is ready for its next use.

Our example above illustrated the execution of a textual SQL string, which
should be invoked by using the :func:`_expression.text` construct to indicate that
we'd like to use textual SQL.  The :meth:`_engine.Connection.execute` method can of
course accommodate more than that; see :ref:`tutorial_working_with_data`
in the :ref:`unified_tutorial` for a tutorial.


Using Transactions
------------------

.. note::

  This section describes how to use transactions when working directly
  with :class:`_engine.Engine` and :class:`_engine.Connection` objects. When using the
  SQLAlchemy ORM, the public API for transaction control is via the
  :class:`.Session` object, which makes usage of the :class:`.Transaction`
  object internally. See :ref:`unitofwork_transaction` for further
  information.

Commit As You Go
~~~~~~~~~~~~~~~~

The :class:`~sqlalchemy.engine.Connection` object always emits SQL statements
within the context of a transaction block.   The first time the
:meth:`_engine.Connection.execute` method is called to execute a SQL
statement, this transaction is begun automatically, using a behavior known
as **autobegin**.  The transaction remains in place for the scope of the
:class:`_engine.Connection` object until the :meth:`_engine.Connection.commit`
or :meth:`_engine.Connection.rollback` methods are called.  Subsequent
to the transaction ending, the :class:`_engine.Connection` waits for the
:meth:`_engine.Connection.execute` method to be called again, at which point
it autobegins again.

This calling style is known as **commit as you go**, and is
illustrated in the example below::

    with engine.connect() as connection:
        connection.execute(some_table.insert(), {"x": 7, "y": "this is some data"})
        connection.execute(
            some_other_table.insert(), {"q": 8, "p": "this is some more data"}
        )

        connection.commit()  # commit the transaction

.. topic::  the Python DBAPI is where autobegin actually happens

    The design of "commit as you go" is intended to be complementary to the
    design of the :term:`DBAPI`, which is the underlying database interface
    that SQLAlchemy interacts with. In the DBAPI, the ``connection`` object does
    not assume changes to the database will be automatically committed, instead
    requiring in the default case that the ``connection.commit()`` method is
    called in order to commit changes to the database. It should be noted that
    the DBAPI itself **does not have a begin() method at all**.  All
    Python DBAPIs implement "autobegin" as the primary means of managing
    transactions, and handle the job of emitting a statement like BEGIN on the
    connection when SQL statements are first emitted.
    SQLAlchemy's API is basically re-stating this behavior in terms of higher
    level Python objects.

In "commit as you go" style, we can call upon :meth:`_engine.Connection.commit`
and :meth:`_engine.Connection.rollback` methods freely within an ongoing
sequence of other statements emitted using :meth:`_engine.Connection.execute`;
each time the transaction is ended, and a new statement is
emitted, a new transaction begins implicitly::

    with engine.connect() as connection:
        connection.execute(text("<some statement>"))
        connection.commit()  # commits "some statement"

        # new transaction starts
        connection.execute(text("<some other statement>"))
        connection.rollback()  # rolls back "some other statement"

        # new transaction starts
        connection.execute(text("<a third statement>"))
        connection.commit()  # commits "a third statement"

.. versionadded:: 2.0 "commit as you go" style is a new feature of
   SQLAlchemy 2.0.  It is also available in SQLAlchemy 1.4's "transitional"
   mode when using a "future" style engine.

Begin Once
~~~~~~~~~~

The :class:`_engine.Connection` object provides a more explicit transaction
management style known as **begin once**. In contrast to "commit as
you go", "begin once" allows the start point of the transaction to be
stated explicitly,
and allows that the transaction itself may be framed out as a context manager
block so that the end of the transaction is instead implicit. To use
"begin once", the :meth:`_engine.Connection.begin` method is used, which returns a
:class:`.Transaction` object which represents the DBAPI transaction.
This object also supports explicit management via its own
:meth:`_engine.Transaction.commit` and :meth:`_engine.Transaction.rollback`
methods, but as a preferred practice also supports the context manager interface,
where it will commit itself when
the block ends normally and emit a rollback if an exception is raised, before
propagating the exception outwards. Below illustrates the form of a "begin
once" block::

    with engine.connect() as connection:
        with connection.begin():
            connection.execute(some_table.insert(), {"x": 7, "y": "this is some data"})
            connection.execute(
                some_other_table.insert(), {"q": 8, "p": "this is some more data"}
            )

        # transaction is committed

Connect and Begin Once from the Engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A convenient shorthand form for the above "begin once" block is to use
the :meth:`_engine.Engine.begin` method at the level of the originating
:class:`_engine.Engine` object, rather than performing the two separate
steps of :meth:`_engine.Engine.connect` and :meth:`_engine.Connection.begin`;
the :meth:`_engine.Engine.begin` method returns a special context manager
that internally maintains both the context manager for the :class:`_engine.Connection`
as well as the context manager for the :class:`_engine.Transaction` normally
returned by the :meth:`_engine.Connection.begin` method::

    with engine.begin() as connection:
        connection.execute(some_table.insert(), {"x": 7, "y": "this is some data"})
        connection.execute(
            some_other_table.insert(), {"q": 8, "p": "this is some more data"}
        )

    # transaction is committed, and Connection is released to the connection
    # pool

.. tip::

    Within the :meth:`_engine.Engine.begin` block, we can call upon the
    :meth:`_engine.Connection.commit` or :meth:`_engine.Connection.rollback`
    methods, which will end the transaction normally demarcated by the block
    ahead of time.  However, if we do so, no further SQL operations may be
    emitted on the :class:`_engine.Connection` until the block ends::

        >>> from sqlalchemy import create_engine
        >>> e = create_engine("sqlite://", echo=True)
        >>> with e.begin() as conn:
        ...     conn.commit()
        ...     conn.begin()
        2021-11-08 09:49:07,517 INFO sqlalchemy.engine.Engine BEGIN (implicit)
        2021-11-08 09:49:07,517 INFO sqlalchemy.engine.Engine COMMIT
        Traceback (most recent call last):
        ...
        sqlalchemy.exc.InvalidRequestError: Can't operate on closed transaction inside
        context manager.  Please complete the context manager before emitting
        further commands.

Mixing Styles
~~~~~~~~~~~~~

The "commit as you go" and "begin once" styles can be freely mixed within
a single :meth:`_engine.Engine.connect` block, provided that the call to
:meth:`_engine.Connection.begin` does not conflict with the "autobegin"
behavior.  To accomplish this, :meth:`_engine.Connection.begin` should only
be called either before any SQL statements have been emitted, or directly
after a previous call to :meth:`_engine.Connection.commit` or :meth:`_engine.Connection.rollback`::

    with engine.connect() as connection:
        with connection.begin():
            # run statements in a "begin once" block
            connection.execute(some_table.insert(), {"x": 7, "y": "this is some data"})

        # transaction is committed

        # run a new statement outside of a block. The connection
        # autobegins
        connection.execute(
            some_other_table.insert(), {"q": 8, "p": "this is some more data"}
        )

        # commit explicitly
        connection.commit()

        # can use a "begin once" block here
        with connection.begin():
            # run more statements
            connection.execute(...)

When developing code that uses "begin once", the library will raise
:class:`_exc.InvalidRequestError` if a transaction was already "autobegun".

.. _dbapi_autocommit:

Setting Transaction Isolation Levels including DBAPI Autocommit
---------------------------------------------------------------

Most DBAPIs support the concept of configurable transaction :term:`isolation` levels.
These are traditionally the four levels "READ UNCOMMITTED", "READ COMMITTED",
"REPEATABLE READ" and "SERIALIZABLE".  These are usually applied to a
DBAPI connection before it begins a new transaction, noting that most
DBAPIs will begin this transaction implicitly when SQL statements are first
emitted.

DBAPIs that support isolation levels also usually support the concept of true
"autocommit", which means that the DBAPI connection itself will be placed into
a non-transactional autocommit mode. This usually means that the typical DBAPI
behavior of emitting "BEGIN" to the database automatically no longer occurs,
but it may also include other directives. SQLAlchemy treats the concept of
"autocommit" like any other isolation level; in that it is an isolation level
that loses not only "read committed" but also loses atomicity.

.. tip::

  It is important to note, as will be discussed further in the section below at
  :ref:`dbapi_autocommit_understanding`, that "autocommit" isolation level like
  any other isolation level does **not** affect the "transactional" behavior of
  the :class:`_engine.Connection` object, which continues to call upon DBAPI
  ``.commit()`` and ``.rollback()`` methods (they just have no net effect under
  autocommit), and for which the ``.begin()`` method assumes the DBAPI will
  start a transaction implicitly (which means that SQLAlchemy's "begin" **does
  not change autocommit mode**).

SQLAlchemy dialects should support these isolation levels as well as autocommit
to as great a degree as possible.

Setting Isolation Level or DBAPI Autocommit for a Connection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For an individual :class:`_engine.Connection` object that's acquired from
:meth:`.Engine.connect`, the isolation level can be set for the duration of
that :class:`_engine.Connection` object using the
:meth:`_engine.Connection.execution_options` method. The parameter is known as
:paramref:`_engine.Connection.execution_options.isolation_level` and the values
are strings which are typically a subset of the following names::

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

    with engine.connect().execution_options(
        isolation_level="REPEATABLE READ"
    ) as connection:
        with connection.begin():
            connection.execute(text("<statement>"))

.. tip::  The return value of
   the :meth:`_engine.Connection.execution_options` method is the same
   :class:`_engine.Connection` object upon which the method was called,
   meaning, it modifies the state of the :class:`_engine.Connection`
   object in place.  This is a new behavior as of SQLAlchemy 2.0.
   This behavior does not apply to the :meth:`_engine.Engine.execution_options`
   method; that method still returns a copy of the :class:`.Engine` and
   as described below may be used to construct multiple :class:`.Engine`
   objects with different execution options, which nonetheless share the same
   dialect and connection pool.

.. note:: The :paramref:`_engine.Connection.execution_options.isolation_level`
   parameter necessarily does not apply to statement level options, such as
   that of :meth:`_sql.Executable.execution_options`, and will be rejected if
   set at this level. This because the option must be set on a DBAPI connection
   on a per-transaction basis.

.. _dbapi_autocommit_engine:

Setting Isolation Level or DBAPI Autocommit for an Engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :paramref:`_engine.Connection.execution_options.isolation_level` option may
also be set engine wide, as is often preferable.  This may be
achieved by passing the :paramref:`_sa.create_engine.isolation_level`
parameter to :func:`.sa.create_engine`::

    from sqlalchemy import create_engine

    eng = create_engine(
        "postgresql://scott:tiger@localhost/test", isolation_level="REPEATABLE READ"
    )

With the above setting, each new DBAPI connection the moment it's created will
be set to use a ``"REPEATABLE READ"`` isolation level setting for all
subsequent operations.

.. tip::

    Prefer to set frequently used isolation levels engine wide as illustrated
    above compared to using per-engine or per-connection execution options for
    maximum performance.

.. _dbapi_autocommit_multiple:

Maintaining Multiple Isolation Levels for a Single Engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The isolation level may also be set per engine, with a potentially greater
level of flexibility but with a small per-connection performance overhead,
using either the :paramref:`_sa.create_engine.execution_options` parameter to
:func:`_sa.create_engine` or the :meth:`_engine.Engine.execution_options`
method, the latter of which will create a copy of the :class:`.Engine` that
shares the dialect and connection pool of the original engine, but has its own
per-connection isolation level setting::

    from sqlalchemy import create_engine

    eng = create_engine(
        "postgresql+psycopg2://scott:tiger@localhost/test",
        execution_options={"isolation_level": "REPEATABLE READ"},
    )

With the above setting, the DBAPI connection will be set to use a
``"REPEATABLE READ"`` isolation level setting for each new transaction
begun; but the connection as pooled will be reset to the original isolation
level that was present when the connection first occurred.   At the level
of :func:`_sa.create_engine`, the end effect is not any different
from using the :paramref:`_sa.create_engine.isolation_level` parameter.

However, an application that frequently chooses to run operations within
different isolation levels may wish to create multiple "sub-engines" of a lead
:class:`_engine.Engine`, each of which will be configured to a different
isolation level. One such use case is an application that has operations that
break into "transactional" and "read-only" operations, a separate
:class:`_engine.Engine` that makes use of ``"AUTOCOMMIT"`` may be separated off
from the main engine::

    from sqlalchemy import create_engine

    eng = create_engine("postgresql+psycopg2://scott:tiger@localhost/test")

    autocommit_engine = eng.execution_options(isolation_level="AUTOCOMMIT")

Above, the :meth:`_engine.Engine.execution_options` method creates a shallow
copy of the original :class:`_engine.Engine`.  Both ``eng`` and
``autocommit_engine`` share the same dialect and connection pool.  However, the
"AUTOCOMMIT" mode will be set upon connections when they are acquired from the
``autocommit_engine``.

The isolation level setting, regardless of which one it is, is unconditionally
reverted when a connection is returned to the connection pool.

.. note::

    The execution options approach, whether used engine wide or per connection,
    incurs a small performance penalty as isolation level instructions
    are sent on connection acquire as well as connection release.   Consider
    the engine-wide isolation setting at :ref:`dbapi_autocommit_engine` so
    that connections are configured at the target isolation level permanently
    as they are pooled.

.. seealso::

      :ref:`SQLite Transaction Isolation <sqlite_isolation_level>`

      :ref:`PostgreSQL Transaction Isolation <postgresql_isolation_level>`

      :ref:`MySQL Transaction Isolation <mysql_isolation_level>`

      :ref:`SQL Server Transaction Isolation <mssql_isolation_level>`

      :ref:`Oracle Database Transaction Isolation <oracle_isolation_level>`

      :ref:`session_transaction_isolation` - for the ORM

      :ref:`faq_execute_retry_autocommit` - a recipe that uses DBAPI autocommit
      to transparently reconnect to the database for read-only operations

.. _dbapi_autocommit_understanding:

Understanding the DBAPI-Level Autocommit Isolation Level
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the parent section, we introduced the concept of the
:paramref:`_engine.Connection.execution_options.isolation_level`
parameter and how it can be used to set database isolation levels, including
DBAPI-level "autocommit" which is treated by SQLAlchemy as another transaction
isolation level.   In this section we will attempt to clarify the implications
of this approach.

If we wanted to check out a :class:`_engine.Connection` object and use it
"autocommit" mode, we would proceed as follows::

    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(text("<statement>"))
        connection.execute(text("<statement>"))

Above illustrates normal usage of "DBAPI autocommit" mode.   There is no
need to make use of methods such as :meth:`_engine.Connection.begin`
or :meth:`_engine.Connection.commit`, as all statements are committed
to the database immediately.  When the block ends, the :class:`_engine.Connection`
object will revert the "autocommit" isolation level, and the DBAPI connection
is released to the connection pool where the DBAPI ``connection.rollback()``
method will normally be invoked, but as the above statements were already
committed, this rollback has no change on the state of the database.

It is important to note that "autocommit" mode
persists even when the :meth:`_engine.Connection.begin` method is called;
the DBAPI will not emit any BEGIN to the database.   When
:meth:`_engine.Connection.commit` is called, the DBAPI may still emit the
"COMMIT" instruction, but this is a no-op at the database level.  This usage is also
not an error scenario, as it is expected that the "autocommit" isolation level
may be applied to code that otherwise was written assuming a transactional context;
the "isolation level" is, after all, a configurational detail of the transaction
itself just like any other isolation level.

In the example below, statements remain
**autocommitting** regardless of SQLAlchemy-level transaction blocks::

    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")

        # this begin() does not affect the DBAPI connection, isolation stays at AUTOCOMMIT
        with connection.begin() as trans:
            connection.execute(text("<statement>"))
            connection.execute(text("<statement>"))

When we run a block like the above with logging turned on, the logging
will attempt to indicate that while a DBAPI level ``.commit()`` is called,
it probably will have no effect due to autocommit mode:

.. sourcecode:: text

    INFO sqlalchemy.engine.Engine BEGIN (implicit)
    ...
    INFO sqlalchemy.engine.Engine COMMIT using DBAPI connection.commit(), has no effect due to autocommit mode

At the same time, even though we are using "DBAPI autocommit", SQLAlchemy's
transactional semantics, that is, the in-Python behavior of :meth:`_engine.Connection.begin`
as well as the behavior of "autobegin", **remain in place, even though these
don't impact the DBAPI connection itself**.  To illustrate, the code
below will raise an error, as :meth:`_engine.Connection.begin` is being
called after autobegin has already occurred::

    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")

        # "transaction" is autobegin (but has no effect due to autocommit)
        connection.execute(text("<statement>"))

        # this will raise; "transaction" is already begun
        with connection.begin() as trans:
            connection.execute(text("<statement>"))

The above example also demonstrates the same theme that the "autocommit"
isolation level is a configurational detail of the underlying database
transaction, and is independent of the begin/commit behavior of the SQLAlchemy
Connection object. The "autocommit" mode will not interact with
:meth:`_engine.Connection.begin` in any way and the :class:`_engine.Connection`
does not consult this status when performing its own state changes with regards
to the transaction (with the exception of suggesting within engine logging that
these blocks are not actually committing). The rationale for this design is to
maintain a completely consistent usage pattern with the
:class:`_engine.Connection` where DBAPI-autocommit mode can be changed
independently without indicating any code changes elsewhere.

.. _dbapi_autocommit_skip_rollback:

Fully preventing ROLLBACK calls under autocommit
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 2.0.43

A common use case is to use AUTOCOMMIT isolation mode to improve performance,
and this is a particularly common practice on MySQL / MariaDB databases.
When seeking this pattern, it should be preferred to set AUTOCOMMIT engine
wide using the :paramref:`.create_engine.isolation_level` so that pooled
connections are permanently set in autocommit mode.   The SQLAlchemy connection
pool as well as the :class:`.Connection` will still seek to invoke the DBAPI
``.rollback()`` method upon connection :term:`release`, as their behavior
remains agnostic of the isolation level that's configured on the connection.
As this rollback still incurs a network round trip under most if not all
DBAPI drivers, this additional network trip may be disabled using the
:paramref:`.create_engine.skip_autocommit_rollback` parameter, which will
apply a rule at the basemost portion of the dialect that invokes DBAPI
``.rollback()`` to first check if the connection is configured in autocommit,
using a method of detection that does not itself incur network overhead::

    autocommit_engine = create_engine(
        "mysql+mysqldb://scott:tiger@mysql80/test",
        skip_autocommit_rollback=True,
        isolation_level="AUTOCOMMIT",
    )

When DBAPI connections are returned to the pool by the :class:`.Connection`,
whether the :class:`.Connection` or the pool attempts to reset the
"transaction", the underlying DBAPI ``.rollback()`` method will be blocked
based on a positive test of "autocommit".

If the dialect in use does not support a no-network means of detecting
autocommit, the dialect will raise ``NotImplementedError`` when a connection
release is attempted.

Changing Between Isolation Levels
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. topic:: TL;DR;

    prefer to use individual :class:`_engine.Connection` objects
    each with just one isolation level, rather than switching isolation on a single
    :class:`_engine.Connection`.  The code will be easier to read and less
    error prone.

Isolation level settings, including autocommit mode, are reset automatically
when the connection is released back to the connection pool. Therefore it is
preferable to avoid trying to switch isolation levels on a single
:class:`_engine.Connection` object as this leads to excess verbosity.

To illustrate how to use "autocommit" in an ad-hoc mode within the scope of a
single :class:`_engine.Connection` checkout, the
:paramref:`_engine.Connection.execution_options.isolation_level` parameter
must be re-applied with the previous isolation level.
The previous section illustrated an attempt to call :meth:`_engine.Connection.begin`
in order to start a transaction while autocommit was taking place; we can
rewrite that example to actually do so by first reverting the isolation level
before we call upon :meth:`_engine.Connection.begin`::

    # if we wanted to flip autocommit on and off on a single connection/
    # which... we usually don't.

    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")

        # run statement(s) in autocommit mode
        connection.execute(text("<statement>"))

        # "commit" the autobegun "transaction"
        connection.commit()

        # switch to default isolation level
        connection.execution_options(isolation_level=connection.default_isolation_level)

        # use a begin block
        with connection.begin() as trans:
            connection.execute(text("<statement>"))

Above, to manually revert the isolation level we made use of
:attr:`_engine.Connection.default_isolation_level` to restore the default
isolation level (assuming that's what we want here). However, it's
probably a better idea to work with the architecture of of the
:class:`_engine.Connection` which already handles resetting of isolation level
automatically upon checkin. The **preferred** way to write the above is to
use two blocks ::

    # use an autocommit block
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        # run statement in autocommit mode
        connection.execute(text("<statement>"))

    # use a regular block
    with engine.begin() as connection:
        connection.execute(text("<statement>"))

To sum up:

1. "DBAPI level autocommit" isolation level is entirely independent of the
   :class:`_engine.Connection` object's notion of "begin" and "commit"
2. use individual :class:`_engine.Connection` checkouts per isolation level.
   Avoid trying to change back and forth between "autocommit" on a single
   connection checkout; let the engine do the work of restoring default
   isolation levels

.. _engine_stream_results:

Using Server Side Cursors (a.k.a. stream results)
-------------------------------------------------

Some backends feature explicit support for the concept of "server side cursors"
versus "client side cursors".  A client side cursor here means that the
database driver fully fetches all rows from a result set into memory before
returning from a statement execution.  Drivers such as those of PostgreSQL and
MySQL/MariaDB generally use client side cursors by default.  A server side
cursor, by contrast, indicates that result rows remain pending within the
database server's state as result rows are consumed by the client.  The drivers
for Oracle Database generally use a "server side" model, for example, and the
SQLite dialect, while not using a real "client / server" architecture, still
uses an unbuffered result fetching approach that will leave result rows outside
of process memory before they are consumed.

.. topic:: What we really mean is "buffered" vs. "unbuffered" results

  Server side cursors also imply a wider set of features with relational
  databases, such as the ability to "scroll" a cursor forwards and backwards.
  SQLAlchemy does not include any explicit support for these behaviors; within
  SQLAlchemy itself, the general term "server side cursors" should be considered
  to mean "unbuffered results" and "client side cursors" means "result rows
  are buffered into memory before the first row is returned".   To work with
  a richer "server side cursor" featureset specific to a certain DBAPI driver,
  see the section :ref:`dbapi_connections_cursor`.

From this basic architecture it follows that a "server side cursor" is more
memory efficient when fetching very large result sets, while at the same time
may introduce more complexity in the client/server communication process
and be less efficient for small result sets (typically less than 10000 rows).

For those dialects that have conditional support for buffered or unbuffered
results, there are usually caveats to the use of the "unbuffered", or server
side cursor mode.   When using the psycopg2 dialect for example, an error is
raised if a server side cursor is used with any kind of DML or DDL statement.
When using MySQL drivers with a server side cursor, the DBAPI connection is in
a more fragile state and does not recover as gracefully from error conditions
nor will it allow a rollback to proceed until the cursor is fully closed.

For this reason, SQLAlchemy's dialects will always default to the less error
prone version of a cursor, which means for PostgreSQL and MySQL dialects
it defaults to a buffered, "client side" cursor where the full set of results
is pulled into memory before any fetch methods are called from the cursor.
This mode of operation is appropriate in the **vast majority** of cases;
unbuffered cursors are not generally useful except in the uncommon case
of an application fetching a very large number of rows in chunks, where
the processing of these rows can be complete before more rows are fetched.

For database drivers that provide client and server side cursor options,
the :paramref:`_engine.Connection.execution_options.stream_results`
and :paramref:`_engine.Connection.execution_options.yield_per` execution
options provide access to "server side cursors" on a per-:class:`_engine.Connection`
or per-statement basis.    Similar options exist when using an ORM
:class:`_orm.Session` as well.


Streaming with a fixed buffer via yield_per
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As individual row-fetch operations with fully unbuffered server side cursors
are typically more expensive than fetching batches of rows at once, The
:paramref:`_engine.Connection.execution_options.yield_per` execution option
configures a :class:`_engine.Connection` or statement to make use of
server-side cursors as are available, while at the same time configuring a
fixed-size buffer of rows that will retrieve rows from the server in batches as
they are consumed. This parameter may be to a positive integer value using the
:meth:`_engine.Connection.execution_options` method on
:class:`_engine.Connection` or on a statement using the
:meth:`.Executable.execution_options` method.

.. versionadded:: 1.4.40 :paramref:`_engine.Connection.execution_options.yield_per` as a
   Core-only option is new as of SQLAlchemy 1.4.40; for prior 1.4 versions,
   use :paramref:`_engine.Connection.execution_options.stream_results`
   directly in combination with :meth:`_engine.Result.yield_per`.

Using this option is equivalent to manually setting the
:paramref:`_engine.Connection.execution_options.stream_results` option,
described in the next section, and then invoking the
:meth:`_engine.Result.yield_per` method on the :class:`_engine.Result`
object with the given integer value.   In both cases, the effect this
combination has includes:

* server side cursors mode is selected for the given backend, if available
  and not already the default behavior for that backend
* as result rows are fetched, they will be buffered in batches, where the
  size of each batch up until the last batch will be equal to the integer
  argument passed to the
  :paramref:`_engine.Connection.execution_options.yield_per` option or the
  :meth:`_engine.Result.yield_per` method; the last batch is then sized against
  the remaining rows fewer than this size
* The default partition size used by the :meth:`_engine.Result.partitions`
  method, if used, will be made equal to this integer size as well.

These three behaviors are illustrated in the example below::

    with engine.connect() as conn:
        with conn.execution_options(yield_per=100).execute(
            text("select * from table")
        ) as result:
            for partition in result.partitions():
                # partition is an iterable that will be at most 100 items
                for row in partition:
                    print(f"{row}")

The above example illustrates the combination of ``yield_per=100`` along
with using the :meth:`_engine.Result.partitions` method to run processing
on rows in batches that match the size fetched from the server.   The
use of :meth:`_engine.Result.partitions` is optional, and if the
:class:`_engine.Result` is iterated directly, a new batch of rows will be
buffered for each 100 rows fetched.    Calling a method such as
:meth:`_engine.Result.all` should **not** be used, as this will fully
fetch all remaining rows at once and defeat the purpose of using ``yield_per``.

.. tip::

    The :class:`.Result` object may be used as a context manager as illustrated
    above.  When iterating with a server-side cursor, this is the best way to
    ensure the :class:`.Result` object is closed, even if exceptions are
    raised within the iteration process.

The :paramref:`_engine.Connection.execution_options.yield_per` option
is portable to the ORM as well, used by a :class:`_orm.Session` to fetch
ORM objects, where it also limits the amount of ORM objects generated at once.
See the section :ref:`orm_queryguide_yield_per` - in the :ref:`queryguide_toplevel`
for further background on using
:paramref:`_engine.Connection.execution_options.yield_per` with the ORM.

.. versionadded:: 1.4.40 Added
   :paramref:`_engine.Connection.execution_options.yield_per`
   as a Core level execution option to conveniently set streaming results,
   buffer size, and partition size all at once in a manner that is transferable
   to that of the ORM's similar use case.

.. _engine_stream_results_sr:

Streaming with a dynamically growing buffer using stream_results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To enable server side cursors without a specific partition size, the
:paramref:`_engine.Connection.execution_options.stream_results` option may be
used, which like :paramref:`_engine.Connection.execution_options.yield_per` may
be called on the :class:`_engine.Connection` object or the statement object.

When a :class:`_engine.Result` object delivered using the
:paramref:`_engine.Connection.execution_options.stream_results` option
is iterated directly, rows are fetched internally
using a default buffering scheme that buffers first a small set of rows,
then a larger and larger buffer on each fetch up to a pre-configured limit
of 1000 rows.   The maximum size of this buffer can be affected using the
:paramref:`_engine.Connection.execution_options.max_row_buffer` execution option::

    with engine.connect() as conn:
        with conn.execution_options(stream_results=True, max_row_buffer=100).execute(
            text("select * from table")
        ) as result:
            for row in result:
                print(f"{row}")

While the :paramref:`_engine.Connection.execution_options.stream_results`
option may be combined with use of the :meth:`_engine.Result.partitions`
method, a specific partition size should be passed to
:meth:`_engine.Result.partitions` so that the entire result is not fetched.
It is usually more straightforward to use the
:paramref:`_engine.Connection.execution_options.yield_per` option when setting
up to use the :meth:`_engine.Result.partitions` method.

.. seealso::

    :ref:`orm_queryguide_yield_per` - in the :ref:`queryguide_toplevel`

    :meth:`_engine.Result.partitions`

    :meth:`_engine.Result.yield_per`


.. _schema_translating:

Translation of Schema Names
---------------------------

To support multi-tenancy applications that distribute common sets of tables
into multiple schemas, the
:paramref:`.Connection.execution_options.schema_translate_map`
execution option may be used to repurpose a set of :class:`_schema.Table` objects
to render under different schema names without any changes.

Given a table::

    user_table = Table(
        "user",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
    )

The "schema" of this :class:`_schema.Table` as defined by the
:paramref:`_schema.Table.schema` attribute is ``None``.  The
:paramref:`.Connection.execution_options.schema_translate_map` can specify
that all :class:`_schema.Table` objects with a schema of ``None`` would instead
render the schema as ``user_schema_one``::

    connection = engine.connect().execution_options(
        schema_translate_map={None: "user_schema_one"}
    )

    result = connection.execute(user_table.select())

The above code will invoke SQL on the database of the form:

.. sourcecode:: sql

    SELECT user_schema_one.user.id, user_schema_one.user.name FROM
    user_schema_one.user

That is, the schema name is substituted with our translated name.  The
map can specify any number of target->destination schemas::

    connection = engine.connect().execution_options(
        schema_translate_map={
            None: "user_schema_one",  # no schema name -> "user_schema_one"
            "special": "special_schema",  # schema="special" becomes "special_schema"
            "public": None,  # Table objects with schema="public" will render with no schema
        }
    )

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

.. tip::

  To use the schema translation feature with the ORM :class:`_orm.Session`,
  set this option at the level of the :class:`_engine.Engine`, then pass that engine
  to the :class:`_orm.Session`.  The :class:`_orm.Session` uses a new
  :class:`_engine.Connection` for each transaction::

      schema_engine = engine.execution_options(schema_translate_map={...})

      session = Session(schema_engine)

      ...

  .. warning::

    When using the ORM :class:`_orm.Session` without extensions, the schema
    translate feature is only supported as
    **a single schema translate map per Session**.   It will **not work** if
    different schema translate maps are given on a per-statement basis, as
    the ORM :class:`_orm.Session` does not take current schema translate
    values into account for individual objects.

    To use a single :class:`_orm.Session` with multiple ``schema_translate_map``
    configurations, the :ref:`horizontal_sharding_toplevel` extension may
    be used.  See the example at :ref:`examples_sharding`.

.. _sql_caching:


SQL Compilation Caching
-----------------------

.. versionadded:: 1.4  SQLAlchemy now has a transparent query caching system
   that substantially lowers the Python computational overhead involved in
   converting SQL statement constructs into SQL strings across both
   Core and ORM.   See the introduction at :ref:`change_4639`.

SQLAlchemy includes a comprehensive caching system for the SQL compiler as well
as its ORM variants.   This caching system is transparent within the
:class:`.Engine` and provides that the SQL compilation process for a given Core
or ORM SQL statement, as well as related computations which assemble
result-fetching mechanics for that statement, will only occur once for that
statement object and all others with the identical
structure, for the duration that the particular structure remains within the
engine's "compiled cache". By "statement objects that have the identical
structure", this generally corresponds to a SQL statement that is
constructed within a function and is built each time that function runs::

    def run_my_statement(connection, parameter):
        stmt = select(table)
        stmt = stmt.where(table.c.col == parameter)
        stmt = stmt.order_by(table.c.id)
        return connection.execute(stmt)

The above statement will generate SQL resembling
``SELECT id, col FROM table WHERE col = :col ORDER BY id``, noting that
while the value of ``parameter`` is a plain Python object such as a string
or an integer, the string SQL form of the statement does not include this
value as it uses bound parameters.  Subsequent invocations of the above
``run_my_statement()`` function will use a cached compilation construct
within the scope of the ``connection.execute()`` call for enhanced performance.

.. note:: it is important to note that the SQL compilation cache is caching
   the **SQL string that is passed to the database only**, and **not the data**
   returned by a query.   It is in no way a data cache and does not
   impact the results returned for a particular SQL statement nor does it
   imply any memory use linked to fetching of result rows.

While SQLAlchemy has had a rudimentary statement cache since the early 1.x
series, and additionally has featured the "Baked Query" extension for the ORM,
both of these systems required a high degree of special API use in order for
the cache to be effective.  The new cache as of 1.4 is instead completely
automatic and requires no change in programming style to be effective.

The cache is automatically used without any configurational changes and no
special steps are needed in order to enable it. The following sections
detail the configuration and advanced usage patterns for the cache.


Configuration
~~~~~~~~~~~~~

The cache itself is a dictionary-like object called an ``LRUCache``, which is
an internal SQLAlchemy dictionary subclass that tracks the usage of particular
keys and features a periodic "pruning" step which removes the least recently
used items when the size of the cache reaches a certain threshold.  The size
of this cache defaults to 500 and may be configured using the
:paramref:`_sa.create_engine.query_cache_size` parameter::

    engine = create_engine(
        "postgresql+psycopg2://scott:tiger@localhost/test", query_cache_size=1200
    )

The size of the cache can grow to be a factor of 150% of the size given, before
it's pruned back down to the target size.  A cache of size 1200 above can therefore
grow to be 1800 elements in size at which point it will be pruned to 1200.

The sizing of the cache is based on a single entry per unique SQL statement rendered,
per engine.   SQL statements generated from both the Core and the ORM are
treated equally.  DDL statements will usually not be cached.  In order to determine
what the cache is doing, engine logging will include details about the
cache's behavior, described in the next section.


.. _sql_caching_logging:

Estimating Cache Performance Using Logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The above cache size of 1200 is actually fairly large.   For small applications,
a size of 100 is likely sufficient.  To estimate the optimal size of the cache,
assuming enough memory is present on the target host, the size of the cache
should be based on the number of unique SQL strings that may be rendered for the
target engine in use.    The most expedient way to see this is to use
SQL echoing, which is most directly enabled by using the
:paramref:`_sa.create_engine.echo` flag, or by using Python logging; see the
section :ref:`dbengine_logging` for background on logging configuration.

As an example, we will examine the logging produced by the following program::

    from sqlalchemy import Column
    from sqlalchemy import create_engine
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy import select
    from sqlalchemy import String
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship
    from sqlalchemy.orm import Session

    Base = declarative_base()


    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)
        data = Column(String)
        bs = relationship("B")


    class B(Base):
        __tablename__ = "b"
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey("a.id"))
        data = Column(String)


    e = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(e)

    s = Session(e)

    s.add_all([A(bs=[B(), B(), B()]), A(bs=[B(), B(), B()]), A(bs=[B(), B(), B()])])
    s.commit()

    for a_rec in s.scalars(select(A)):
        print(a_rec.bs)

When run, each SQL statement that's logged will include a bracketed
cache statistics badge to the left of the parameters passed.   The four
types of message we may see are summarized as follows:

* ``[raw sql]`` - the driver or the end-user emitted raw SQL using
  :meth:`.Connection.exec_driver_sql` - caching does not apply

* ``[no key]`` - the statement object is a DDL statement that is not cached, or
  the statement object contains uncacheable elements such as user-defined
  constructs or arbitrarily large VALUES clauses.

* ``[generated in Xs]`` - the statement was a **cache miss** and had to be
  compiled, then stored in the cache.  it took X seconds to produce the
  compiled construct.  The number X will be in the small fractional seconds.

* ``[cached since Xs ago]`` - the statement was a **cache hit** and did not
  have to be recompiled.  The statement has been stored in the cache since
  X seconds ago.  The number X will be proportional to how long the application
  has been running and how long the statement has been cached, so for example
  would be 86400 for a 24 hour period.

Each badge is described in more detail below.

The first statements we see for the above program will be the SQLite dialect
checking for the existence of the "a" and "b" tables:

.. sourcecode:: text

  INFO sqlalchemy.engine.Engine PRAGMA temp.table_info("a")
  INFO sqlalchemy.engine.Engine [raw sql] ()
  INFO sqlalchemy.engine.Engine PRAGMA main.table_info("b")
  INFO sqlalchemy.engine.Engine [raw sql] ()

For the above two SQLite PRAGMA statements, the badge reads ``[raw sql]``,
which indicates the driver is sending a Python string directly to the
database using :meth:`.Connection.exec_driver_sql`.  Caching does not apply
to such statements because they already exist in string form, and there
is nothing known about what kinds of result rows will be returned since
SQLAlchemy does not parse SQL strings ahead of time.

The next statements we see are the CREATE TABLE statements:

.. sourcecode:: sql

  INFO sqlalchemy.engine.Engine
  CREATE TABLE a (
    id INTEGER NOT NULL,
    data VARCHAR,
    PRIMARY KEY (id)
  )

  INFO sqlalchemy.engine.Engine [no key 0.00007s] ()
  INFO sqlalchemy.engine.Engine
  CREATE TABLE b (
    id INTEGER NOT NULL,
    a_id INTEGER,
    data VARCHAR,
    PRIMARY KEY (id),
    FOREIGN KEY(a_id) REFERENCES a (id)
  )

  INFO sqlalchemy.engine.Engine [no key 0.00006s] ()

For each of these statements, the badge reads ``[no key 0.00006s]``.  This
indicates that these two particular statements, caching did not occur because
the DDL-oriented :class:`_schema.CreateTable` construct did not produce a
cache key.  DDL constructs generally do not participate in caching because
they are not typically subject to being repeated a second time and DDL
is also a database configurational step where performance is not as critical.

The ``[no key]`` badge is important for one other reason, as it can be produced
for SQL statements that are cacheable except for some particular sub-construct
that is not currently cacheable.   Examples of this include custom user-defined
SQL elements that don't define caching parameters, as well as some constructs
that generate arbitrarily long and non-reproducible SQL strings, the main
examples being the :class:`.Values` construct as well as when using "multivalued
inserts" with the :meth:`.Insert.values` method.

So far our cache is still empty.  The next statements will be cached however,
a segment looks like:

.. sourcecode:: sql

  INFO sqlalchemy.engine.Engine INSERT INTO a (data) VALUES (?)
  INFO sqlalchemy.engine.Engine [generated in 0.00011s] (None,)
  INFO sqlalchemy.engine.Engine INSERT INTO a (data) VALUES (?)
  INFO sqlalchemy.engine.Engine [cached since 0.0003533s ago] (None,)
  INFO sqlalchemy.engine.Engine INSERT INTO a (data) VALUES (?)
  INFO sqlalchemy.engine.Engine [cached since 0.0005326s ago] (None,)
  INFO sqlalchemy.engine.Engine INSERT INTO b (a_id, data) VALUES (?, ?)
  INFO sqlalchemy.engine.Engine [generated in 0.00010s] (1, None)
  INFO sqlalchemy.engine.Engine INSERT INTO b (a_id, data) VALUES (?, ?)
  INFO sqlalchemy.engine.Engine [cached since 0.0003232s ago] (1, None)
  INFO sqlalchemy.engine.Engine INSERT INTO b (a_id, data) VALUES (?, ?)
  INFO sqlalchemy.engine.Engine [cached since 0.0004887s ago] (1, None)

Above, we see essentially two unique SQL strings; ``"INSERT INTO a (data) VALUES (?)"``
and ``"INSERT INTO b (a_id, data) VALUES (?, ?)"``.  Since SQLAlchemy uses
bound parameters for all literal values, even though these statements are
repeated many times for different objects, because the parameters are separate,
the actual SQL string stays the same.

.. note:: the above two statements are generated by the ORM unit of work
   process, and in fact will be caching these in a separate cache that is
   local to each mapper.  However the mechanics and terminology are the same.
   The section :ref:`engine_compiled_cache` below will describe how user-facing
   code can also use an alternate caching container on a per-statement basis.

The caching badge we see for the first occurrence of each of these two
statements is ``[generated in 0.00011s]``. This indicates that the statement
was **not in the cache, was compiled into a String in .00011s and was then
cached**.   When we see the ``[generated]`` badge, we know that this means
there was a **cache miss**.  This is to be expected for the first occurrence of
a particular statement.  However, if lots of new ``[generated]`` badges are
observed for a long-running application that is generally using the same series
of SQL statements over and over, this may be a sign that the
:paramref:`_sa.create_engine.query_cache_size` parameter is too small.  When a
statement that was cached is then evicted from the cache due to the LRU
cache pruning lesser used items, it will display the ``[generated]`` badge
when it is next used.

The caching badge that we then see for the subsequent occurrences of each of
these two statements looks like ``[cached since 0.0003533s ago]``.  This
indicates that the statement **was found in the cache, and was originally
placed into the cache .0003533 seconds ago**.   It is important to note that
while the ``[generated]`` and ``[cached since]`` badges refer to a number of
seconds, they mean different things; in the case of ``[generated]``, the number
is a rough timing of how long it took to compile the statement, and will be an
extremely small amount of time.   In the case of ``[cached since]``, this is
the total time that a statement has been present in the cache.  For an
application that's been running for six hours, this number may read ``[cached
since 21600 seconds ago]``, and that's a good thing.    Seeing high numbers for
"cached since" is an indication that these statements have not been subject to
cache misses for a long time.  Statements that frequently have a low number of
"cached since" even if the application has been running a long time may
indicate these statements are too frequently subject to cache misses, and that
the
:paramref:`_sa.create_engine.query_cache_size` may need to be increased.

Our example program then performs some SELECTs where we can see the same
pattern of "generated" then "cached", for the SELECT of the "a" table as well
as for subsequent lazy loads of the "b" table:

.. sourcecode:: text

  INFO sqlalchemy.engine.Engine SELECT a.id AS a_id, a.data AS a_data
  FROM a
  INFO sqlalchemy.engine.Engine [generated in 0.00009s] ()
  INFO sqlalchemy.engine.Engine SELECT b.id AS b_id, b.a_id AS b_a_id, b.data AS b_data
  FROM b
  WHERE ? = b.a_id
  INFO sqlalchemy.engine.Engine [generated in 0.00010s] (1,)
  INFO sqlalchemy.engine.Engine SELECT b.id AS b_id, b.a_id AS b_a_id, b.data AS b_data
  FROM b
  WHERE ? = b.a_id
  INFO sqlalchemy.engine.Engine [cached since 0.0005922s ago] (2,)
  INFO sqlalchemy.engine.Engine SELECT b.id AS b_id, b.a_id AS b_a_id, b.data AS b_data
  FROM b
  WHERE ? = b.a_id

From our above program, a full run shows a total of four distinct SQL strings
being cached.   Which indicates a cache size of **four** would be sufficient.   This is
obviously an extremely small size, and the default size of 500 is fine to be left
at its default.

How much memory does the cache use?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The previous section detailed some techniques to check if the
:paramref:`_sa.create_engine.query_cache_size` needs to be bigger.   How do we know
if the cache is not too large?   The reason we may want to set
:paramref:`_sa.create_engine.query_cache_size` to not be higher than a certain
number would be because we have an application that may make use of a very large
number of different statements, such as an application that is building queries
on the fly from a search UX, and we don't want our host to run out of memory
if for example, a hundred thousand different queries were run in the past 24 hours
and they were all cached.

It is extremely difficult to measure how much memory is occupied by Python
data structures, however using a process to measure growth in memory via ``top`` as a
successive series of 250 new statements are added to the cache suggest a
moderate Core statement takes up about 12K while a small ORM statement takes about
20K, including result-fetching structures which for the ORM will be much greater.


.. _engine_compiled_cache:

Disabling or using an alternate dictionary to cache some (or all) statements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The internal cache used is known as ``LRUCache``, but this is mostly just
a dictionary.  Any dictionary may be used as a cache for any series of
statements by using the :paramref:`.Connection.execution_options.compiled_cache`
option as an execution option.  Execution options may be set on a statement,
on an :class:`_engine.Engine` or :class:`_engine.Connection`, as well as
when using the ORM :meth:`_orm.Session.execute` method for SQLAlchemy-2.0
style invocations.   For example, to run a series of SQL statements and have
them cached in a particular dictionary::

    my_cache = {}
    with engine.connect().execution_options(compiled_cache=my_cache) as conn:
        conn.execute(table.select())

The SQLAlchemy ORM uses the above technique to hold onto per-mapper caches
within the unit of work "flush" process that are separate from the default
cache configured on the :class:`_engine.Engine`, as well as for some
relationship loader queries.

The cache can also be disabled with this argument by sending a value of
``None``::

    # disable caching for this connection
    with engine.connect().execution_options(compiled_cache=None) as conn:
        conn.execute(table.select())

.. _engine_thirdparty_caching:

Caching for Third Party Dialects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The caching feature requires that the dialect's compiler produces SQL
strings that are safe to reuse for many statement invocations, given
a particular cache key that is keyed to that SQL string.  This means
that any literal values in a statement, such as the LIMIT/OFFSET values for
a SELECT, can not be hardcoded in the dialect's compilation scheme, as
the compiled string will not be re-usable.   SQLAlchemy supports rendered
bound parameters using the :meth:`_sql.BindParameter.render_literal_execute`
method which can be applied to the existing ``Select._limit_clause`` and
``Select._offset_clause`` attributes by a custom compiler, which
are illustrated later in this section.

As there are many third party dialects, many of which may be generating literal
values from SQL statements without the benefit of the newer "literal execute"
feature, SQLAlchemy as of version 1.4.5 has added an attribute to dialects
known as :attr:`_engine.Dialect.supports_statement_cache`. This attribute is
checked at runtime for its presence directly on a particular dialect's class,
even if it's already present on a superclass, so that even a third party
dialect that subclasses an existing cacheable SQLAlchemy dialect such as
``sqlalchemy.dialects.postgresql.PGDialect`` must still explicitly include this
attribute for caching to be enabled. The attribute should **only** be enabled
once the dialect has been altered as needed and tested for reusability of
compiled SQL statements with differing parameters.

For all third party dialects that don't support this attribute, the logging for
such a dialect will indicate ``dialect does not support caching``.

When a dialect has been tested against caching, and in particular the SQL
compiler has been updated to not render any literal LIMIT / OFFSET within
a SQL string directly, dialect authors can apply the attribute as follows::

    from sqlalchemy.engine.default import DefaultDialect


    class MyDialect(DefaultDialect):
        supports_statement_cache = True

The flag needs to be applied to all subclasses of the dialect as well::

    class MyDBAPIForMyDialect(MyDialect):
        supports_statement_cache = True

.. versionadded:: 1.4.5

    Added the :attr:`.Dialect.supports_statement_cache` attribute.

The typical case for dialect modification follows.

Example: Rendering LIMIT / OFFSET with post compile parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As an example, suppose a dialect overrides the :meth:`.SQLCompiler.limit_clause`
method, which produces the "LIMIT / OFFSET" clause for a SQL statement,
like this::

    # pre 1.4 style code
    def limit_clause(self, select, **kw):
        text = ""
        if select._limit is not None:
            text += " \n LIMIT %d" % (select._limit,)
        if select._offset is not None:
            text += " \n OFFSET %d" % (select._offset,)
        return text

The above routine renders the :attr:`.Select._limit` and
:attr:`.Select._offset` integer values as literal integers embedded in the SQL
statement. This is a common requirement for databases that do not support using
a bound parameter within the LIMIT/OFFSET clauses of a SELECT statement.
However, rendering the integer value within the initial compilation stage is
directly **incompatible** with caching as the limit and offset integer values
of a :class:`.Select` object are not part of the cache key, so that many
:class:`.Select` statements with different limit/offset values would not render
with the correct value.

The correction for the above code is to move the literal integer into
SQLAlchemy's :ref:`post-compile <change_4808>` facility, which will render the
literal integer outside of the initial compilation stage, but instead at
execution time before the statement is sent to the DBAPI.  This is accessed
within the compilation stage using the :meth:`_sql.BindParameter.render_literal_execute`
method, in conjunction with using the :attr:`.Select._limit_clause` and
:attr:`.Select._offset_clause` attributes, which represent the LIMIT/OFFSET
as a complete SQL expression, as follows::

    # 1.4 cache-compatible code
    def limit_clause(self, select, **kw):
        text = ""

        limit_clause = select._limit_clause
        offset_clause = select._offset_clause

        if select._simple_int_clause(limit_clause):
            text += " \n LIMIT %s" % (
                self.process(limit_clause.render_literal_execute(), **kw)
            )
        elif limit_clause is not None:
            # assuming the DB doesn't support SQL expressions for LIMIT.
            # Otherwise render here normally
            raise exc.CompileError(
                "dialect 'mydialect' can only render simple integers for LIMIT"
            )
        if select._simple_int_clause(offset_clause):
            text += " \n OFFSET %s" % (
                self.process(offset_clause.render_literal_execute(), **kw)
            )
        elif offset_clause is not None:
            # assuming the DB doesn't support SQL expressions for OFFSET.
            # Otherwise render here normally
            raise exc.CompileError(
                "dialect 'mydialect' can only render simple integers for OFFSET"
            )

        return text

The approach above will generate a compiled SELECT statement that looks like:

.. sourcecode:: sql

    SELECT x FROM y
    LIMIT __[POSTCOMPILE_param_1]
    OFFSET __[POSTCOMPILE_param_2]

Where above, the ``__[POSTCOMPILE_param_1]`` and ``__[POSTCOMPILE_param_2]``
indicators will be populated with their corresponding integer values at
statement execution time, after the SQL string has been retrieved from the
cache.

After changes like the above have been made as appropriate, the
:attr:`.Dialect.supports_statement_cache` flag should be set to ``True``.
It is strongly recommended that third party dialects make use of the
`dialect third party test suite <https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst>`_
which will assert that operations like
SELECTs with LIMIT/OFFSET are correctly rendered and cached.

.. seealso::

    :ref:`faq_new_caching` - in the :ref:`faq_toplevel` section


.. _engine_lambda_caching:

Using Lambdas to add significant speed gains to statement production
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. deepalchemy:: This technique is generally non-essential except in very performance
   intensive scenarios, and intended for experienced Python programmers.
   While fairly straightforward, it involves metaprogramming concepts that are
   not appropriate for novice Python developers.  The lambda approach can be
   applied to at a later time to existing code with a minimal amount of effort.

Python functions, typically expressed as lambdas, may be used to generate
SQL expressions which are cacheable based on the Python code location of
the lambda function itself as well as the closure variables within the
lambda.   The rationale is to allow caching of not only the SQL string-compiled
form of a SQL expression construct as is SQLAlchemy's normal behavior when
the lambda system isn't used, but also the in-Python composition
of the SQL expression construct itself, which also has some degree of
Python overhead.

The lambda SQL expression feature is available as a performance enhancing
feature, and is also optionally used in the :func:`_orm.with_loader_criteria`
ORM option in order to provide a generic SQL fragment.

Synopsis
^^^^^^^^

Lambda statements are constructed using the :func:`_sql.lambda_stmt` function,
which returns an instance of :class:`_sql.StatementLambdaElement`, which is
itself an executable statement construct.    Additional modifiers and criteria
are added to the object using the Python addition operator ``+``, or
alternatively the :meth:`_sql.StatementLambdaElement.add_criteria` method which
allows for more options.

It is assumed that the :func:`_sql.lambda_stmt` construct is being invoked
within an enclosing function or method that expects to be used many times
within an application, so that subsequent executions beyond the first one
can take advantage of the compiled SQL being cached.  When the lambda is
constructed inside of an enclosing function in Python it is then subject
to also having closure variables, which are significant to the whole
approach::

    from sqlalchemy import lambda_stmt


    def run_my_statement(connection, parameter):
        stmt = lambda_stmt(lambda: select(table))
        stmt += lambda s: s.where(table.c.col == parameter)
        stmt += lambda s: s.order_by(table.c.id)

        return connection.execute(stmt)


    with engine.connect() as conn:
        result = run_my_statement(some_connection, "some parameter")

Above, the three ``lambda`` callables that are used to define the structure
of a SELECT statement are invoked exactly once, and the resulting SQL
string cached in the compilation cache of the engine.   From that point
forward, the ``run_my_statement()`` function may be invoked any number
of times and the ``lambda`` callables within it will not be called, only
used as cache keys to retrieve the already-compiled SQL.

.. note::  It is important to note that there is already SQL caching in place
   when the lambda system is not used.   The lambda system only adds an
   additional layer of work reduction per SQL statement invoked by caching
   the building up of the SQL construct itself and also using a simpler
   cache key.


Quick Guidelines for Lambdas
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Above all, the emphasis within the lambda SQL system is ensuring that there
is never a mismatch between the cache key generated for a lambda and the
SQL string it will produce.   The :class:`_sql.LambdaElement` and related
objects will run and analyze the given lambda in order to calculate how
it should be cached on each run, trying to detect any potential problems.
Basic guidelines include:

* **Any kind of statement is supported** - while it's expected that
  :func:`_sql.select` constructs are the prime use case for :func:`_sql.lambda_stmt`,
  DML statements such as :func:`_sql.insert` and :func:`_sql.update` are
  equally usable::

    def upd(id_, newname):
        stmt = lambda_stmt(lambda: users.update())
        stmt += lambda s: s.values(name=newname)
        stmt += lambda s: s.where(users.c.id == id_)
        return stmt


    with engine.begin() as conn:
        conn.execute(upd(7, "foo"))

  ..

* **ORM use cases directly supported as well** - the :func:`_sql.lambda_stmt`
  can accommodate ORM functionality completely and used directly with
  :meth:`_orm.Session.execute`::

    def select_user(session, name):
        stmt = lambda_stmt(lambda: select(User))
        stmt += lambda s: s.where(User.name == name)

        row = session.execute(stmt).first()
        return row

  ..

* **Bound parameters are automatically accommodated** - in contrast to SQLAlchemy's
  previous "baked query" system, the lambda SQL system accommodates for
  Python literal values which become SQL bound parameters automatically.
  This means that even though a given lambda runs only once, the values that
  become bound parameters are extracted from the **closure** of the lambda
  on every run:

  .. sourcecode:: pycon+sql

        >>> def my_stmt(x, y):
        ...     stmt = lambda_stmt(lambda: select(func.max(x, y)))
        ...     return stmt
        >>> engine = create_engine("sqlite://", echo=True)
        >>> with engine.connect() as conn:
        ...     print(conn.scalar(my_stmt(5, 10)))
        ...     print(conn.scalar(my_stmt(12, 8)))
        {execsql}SELECT max(?, ?) AS max_1
        [generated in 0.00057s] (5, 10){stop}
        10
        {execsql}SELECT max(?, ?) AS max_1
        [cached since 0.002059s ago] (12, 8){stop}
        12

  Above, :class:`_sql.StatementLambdaElement` extracted the values of ``x``
  and ``y`` from the **closure** of the lambda that is generated each time
  ``my_stmt()`` is invoked; these were substituted into the cached SQL
  construct as the values of the parameters.

* **The lambda should ideally produce an identical SQL structure in all cases** -
  Avoid using conditionals or custom callables inside of lambdas that might make
  it produce different SQL based on inputs; if a function might conditionally
  use two different SQL fragments, use two separate lambdas::

        # **Don't** do this:


        def my_stmt(parameter, thing=False):
            stmt = lambda_stmt(lambda: select(table))
            stmt += lambda s: (
                s.where(table.c.x > parameter) if thing else s.where(table.c.y == parameter)
            )
            return stmt


        # **Do** do this:


        def my_stmt(parameter, thing=False):
            stmt = lambda_stmt(lambda: select(table))
            if thing:
                stmt += lambda s: s.where(table.c.x > parameter)
            else:
                stmt += lambda s: s.where(table.c.y == parameter)
            return stmt

  There are a variety of failures which can occur if the lambda does not
  produce a consistent SQL construct and some are not trivially detectable
  right now.

* **Don't use functions inside the lambda to produce bound values** - the
  bound value tracking approach requires that the actual value to be used in
  the SQL statement be locally present in the closure of the lambda.  This is
  not possible if values are generated from other functions, and the
  :class:`_sql.LambdaElement` should normally raise an error if this is
  attempted::

    >>> def my_stmt(x, y):
    ...     def get_x():
    ...         return x
    ...
    ...     def get_y():
    ...         return y
    ...
    ...     stmt = lambda_stmt(lambda: select(func.max(get_x(), get_y())))
    ...     return stmt
    >>> with engine.connect() as conn:
    ...     print(conn.scalar(my_stmt(5, 10)))
    Traceback (most recent call last):
      # ...
    sqlalchemy.exc.InvalidRequestError: Can't invoke Python callable get_x()
    inside of lambda expression argument at
    <code object <lambda> at 0x7fed15f350e0, file "<stdin>", line 6>;
    lambda SQL constructs should not invoke functions from closure variables
    to produce literal values since the lambda SQL system normally extracts
    bound values without actually invoking the lambda or any functions within it.

  Above, the use of ``get_x()`` and ``get_y()``, if they are necessary, should
  occur **outside** of the lambda and assigned to a local closure variable::

    >>> def my_stmt(x, y):
    ...     def get_x():
    ...         return x
    ...
    ...     def get_y():
    ...         return y
    ...
    ...     x_param, y_param = get_x(), get_y()
    ...     stmt = lambda_stmt(lambda: select(func.max(x_param, y_param)))
    ...     return stmt

  ..

* **Avoid referring to non-SQL constructs inside of lambdas as they are not
  cacheable by default** - this issue refers to how the :class:`_sql.LambdaElement`
  creates a cache key from other closure variables within the statement.  In order
  to provide the best guarantee of an accurate cache key, all objects located
  in the closure of the lambda are considered to be significant, and none
  will be assumed to be appropriate for a cache key by default.
  So the following example will also raise a rather detailed error message::

    >>> class Foo:
    ...     def __init__(self, x, y):
    ...         self.x = x
    ...         self.y = y
    >>> def my_stmt(foo):
    ...     stmt = lambda_stmt(lambda: select(func.max(foo.x, foo.y)))
    ...     return stmt
    >>> with engine.connect() as conn:
    ...     print(conn.scalar(my_stmt(Foo(5, 10))))
    Traceback (most recent call last):
      # ...
    sqlalchemy.exc.InvalidRequestError: Closure variable named 'foo' inside of
    lambda callable <code object <lambda> at 0x7fed15f35450, file
    "<stdin>", line 2> does not refer to a cacheable SQL element, and also
    does not appear to be serving as a SQL literal bound value based on the
    default SQL expression returned by the function.  This variable needs to
    remain outside the scope of a SQL-generating lambda so that a proper cache
    key may be generated from the lambda's state.  Evaluate this variable
    outside of the lambda, set track_on=[<elements>] to explicitly select
    closure elements to track, or set track_closure_variables=False to exclude
    closure variables from being part of the cache key.

  The above error indicates that :class:`_sql.LambdaElement` will not assume
  that the ``Foo`` object passed in will continue to behave the same in all
  cases.    It also won't assume it can use ``Foo`` as part of the cache key
  by default; if it were to use the ``Foo`` object as part of the cache key,
  if there were many different ``Foo`` objects this would fill up the cache
  with duplicate information, and would also hold long-lasting references to
  all of these objects.

  The best way to resolve the above situation is to not refer to ``foo``
  inside of the lambda, and refer to it **outside** instead::

    >>> def my_stmt(foo):
    ...     x_param, y_param = foo.x, foo.y
    ...     stmt = lambda_stmt(lambda: select(func.max(x_param, y_param)))
    ...     return stmt

  In some situations, if the SQL structure of the lambda is guaranteed to
  never change based on input, to pass ``track_closure_variables=False``
  which will disable any tracking of closure variables other than those
  used for bound parameters::

    >>> def my_stmt(foo):
    ...     stmt = lambda_stmt(
    ...         lambda: select(func.max(foo.x, foo.y)), track_closure_variables=False
    ...     )
    ...     return stmt

  There is also the option to add objects to the element to explicitly form
  part of the cache key, using the ``track_on`` parameter; using this parameter
  allows specific values to serve as the cache key and will also prevent other
  closure variables from being considered.  This is useful for cases where part
  of the SQL being constructed originates from a contextual object of some sort
  that may have many different values.  In the example below, the first
  segment of the SELECT statement will disable tracking of the ``foo`` variable,
  whereas the second segment will explicitly track ``self`` as part of the
  cache key::

    >>> def my_stmt(self, foo):
    ...     stmt = lambda_stmt(
    ...         lambda: select(*self.column_expressions), track_closure_variables=False
    ...     )
    ...     stmt = stmt.add_criteria(lambda: self.where_criteria, track_on=[self])
    ...     return stmt

  Using ``track_on`` means the given objects will be stored long term in the
  lambda's internal cache and will have strong references for as long as the
  cache doesn't clear out those objects (an LRU scheme of 1000 entries is used
  by default).

  ..


Cache Key Generation
^^^^^^^^^^^^^^^^^^^^

In order to understand some of the options and behaviors which occur
with lambda SQL constructs, an understanding of the caching system
is helpful.

SQLAlchemy's caching system normally generates a cache key from a given
SQL expression construct by producing a structure that represents all the
state within the construct::

    >>> from sqlalchemy import select, column
    >>> stmt = select(column("q"))
    >>> cache_key = stmt._generate_cache_key()
    >>> print(cache_key)  # somewhat paraphrased
    CacheKey(key=(
      '0',
      <class 'sqlalchemy.sql.selectable.Select'>,
      '_raw_columns',
      (
        (
          '1',
          <class 'sqlalchemy.sql.elements.ColumnClause'>,
          'name',
          'q',
          'type',
          (
            <class 'sqlalchemy.sql.sqltypes.NullType'>,
          ),
        ),
      ),
      # a few more elements are here, and many more for a more
      # complicated SELECT statement
    ),)


The above key is stored in the cache which is essentially a dictionary, and the
value is a construct that among other things stores the string form of the SQL
statement, in this case the phrase "SELECT q".  We can observe that even for an
extremely short query the cache key is pretty verbose as it has to represent
everything that may vary about what's being rendered and potentially executed.

The lambda construction system by contrast creates a different kind of cache
key::

    >>> from sqlalchemy import lambda_stmt
    >>> stmt = lambda_stmt(lambda: select(column("q")))
    >>> cache_key = stmt._generate_cache_key()
    >>> print(cache_key)
    CacheKey(key=(
      <code object <lambda> at 0x7fed1617c710, file "<stdin>", line 1>,
      <class 'sqlalchemy.sql.lambdas.StatementLambdaElement'>,
    ),)

Above, we see a cache key that is vastly shorter than that of the non-lambda
statement, and additionally that production of the ``select(column("q"))``
construct itself was not even necessary; the Python lambda itself contains
an attribute called ``__code__`` which refers to a Python code object that
within the runtime of the application is immutable and permanent.

When the lambda also includes closure variables, in the normal case that these
variables refer to SQL constructs such as column objects, they become
part of the cache key, or if they refer to literal values that will be bound
parameters, they are placed in a separate element of the cache key::

    >>> def my_stmt(parameter):
    ...     col = column("q")
    ...     stmt = lambda_stmt(lambda: select(col))
    ...     stmt += lambda s: s.where(col == parameter)
    ...     return stmt

The above :class:`_sql.StatementLambdaElement` includes two lambdas, both
of which refer to the ``col`` closure variable, so the cache key will
represent both of these segments as well as the ``column()`` object::

    >>> stmt = my_stmt(5)
    >>> key = stmt._generate_cache_key()
    >>> print(key)
    CacheKey(key=(
      <code object <lambda> at 0x7f07323c50e0, file "<stdin>", line 3>,
      (
        '0',
        <class 'sqlalchemy.sql.elements.ColumnClause'>,
        'name',
        'q',
        'type',
        (
          <class 'sqlalchemy.sql.sqltypes.NullType'>,
        ),
      ),
      <code object <lambda> at 0x7f07323c5190, file "<stdin>", line 4>,
      <class 'sqlalchemy.sql.lambdas.LinkedLambdaElement'>,
      (
        '0',
        <class 'sqlalchemy.sql.elements.ColumnClause'>,
        'name',
        'q',
        'type',
        (
          <class 'sqlalchemy.sql.sqltypes.NullType'>,
        ),
      ),
      (
        '0',
        <class 'sqlalchemy.sql.elements.ColumnClause'>,
        'name',
        'q',
        'type',
        (
          <class 'sqlalchemy.sql.sqltypes.NullType'>,
        ),
      ),
    ),)


The second part of the cache key has retrieved the bound parameters that will
be used when the statement is invoked::

    >>> key.bindparams
    [BindParameter('%(139668884281280 parameter)s', 5, type_=Integer())]


For a series of examples of "lambda" caching with performance comparisons,
see the "short_selects" test suite within the :ref:`examples_performance`
performance example.

.. _engine_insertmanyvalues:

"Insert Many Values" Behavior for INSERT statements
---------------------------------------------------

.. versionadded:: 2.0 see :ref:`change_6047` for background on the change
   including sample performance tests

.. tip:: The :term:`insertmanyvalues` feature is a **transparently available**
   performance feature which typically requires no end-user intervention in
   order for it to take place as needed.   This section describes the
   architecture of the feature as well as how to measure its performance and
   tune its behavior in order to optimize the speed of bulk INSERT statements,
   particularly as used by the ORM.

As more databases have added support for INSERT..RETURNING, SQLAlchemy has
undergone a major change in how it approaches the subject of INSERT statements
where there's a need to acquire server-generated values, most importantly
server-generated primary key values which allow the new row to be referenced in
subsequent operations. In particular, this scenario has long been a significant
performance issue in the ORM, which relies on being able to retrieve
server-generated primary key values in order to correctly populate the
:term:`identity map`.

With recent support for RETURNING added to SQLite and MariaDB, SQLAlchemy no
longer needs to rely upon the single-row-only
`cursor.lastrowid <https://peps.python.org/pep-0249/#lastrowid>`_ attribute
provided by the :term:`DBAPI` for most backends; RETURNING may now be used for
all :ref:`SQLAlchemy-included <included_dialects>` backends with the exception
of MySQL. The remaining performance
limitation, that the
`cursor.executemany() <https://peps.python.org/pep-0249/#executemany>`_ DBAPI
method does not allow for rows to be fetched, is resolved for most backends by
foregoing the use of ``executemany()`` and instead restructuring individual
INSERT statements to each accommodate a large number of rows in a single
statement that is invoked using ``cursor.execute()``. This approach originates
from the
`psycopg2 fast execution helpers <https://www.psycopg.org/docs/extras.html#fast-execution-helpers>`_
feature of the ``psycopg2`` DBAPI, which SQLAlchemy incrementally added more
and more support towards in recent release series.

Current Support
~~~~~~~~~~~~~~~

The feature is enabled for all backend included in SQLAlchemy that support
RETURNING, with the exception of Oracle Database for which both the
python-oracledb and cx_Oracle drivers offer their own equivalent feature. The
feature normally takes place when making use of the
:meth:`_dml.Insert.returning` method of an :class:`_dml.Insert` construct in
conjunction with :term:`executemany` execution, which occurs when passing a
list of dictionaries to the :paramref:`_engine.Connection.execute.parameters`
parameter of the :meth:`_engine.Connection.execute` or
:meth:`_orm.Session.execute` methods (as well as equivalent methods under
:ref:`asyncio <asyncio_toplevel>` and shorthand methods like
:meth:`_orm.Session.scalars`). It also takes place within the ORM :term:`unit
of work` process when using methods such as :meth:`_orm.Session.add` and
:meth:`_orm.Session.add_all` to add rows.

For SQLAlchemy's included dialects, support or equivalent support is currently
as follows:

* SQLite - supported for SQLite versions 3.35 and above
* PostgreSQL - all supported Postgresql versions (9 and above)
* SQL Server - all supported SQL Server versions [#]_
* MariaDB - supported for MariaDB versions 10.5 and above
* MySQL - no support, no RETURNING feature is present
* Oracle Database - supports RETURNING with executemany using native python-oracledb / cx_Oracle
  APIs, for all supported Oracle Database versions 9 and above, using multi-row OUT
  parameters. This is not the same implementation as "executemanyvalues", however has
  the same usage patterns and equivalent performance benefits.

.. versionchanged:: 2.0.10

   .. [#] "insertmanyvalues" support for Microsoft SQL Server
      is restored, after being temporarily disabled in version 2.0.9.

Disabling the feature
~~~~~~~~~~~~~~~~~~~~~

To disable the "insertmanyvalues" feature for a given backend for an
:class:`.Engine` overall, pass the
:paramref:`_sa.create_engine.use_insertmanyvalues` parameter as ``False`` to
:func:`_sa.create_engine`::

    engine = create_engine(
        "mariadb+mariadbconnector://scott:tiger@host/db", use_insertmanyvalues=False
    )

The feature can also be disabled from being used implicitly for a particular
:class:`_schema.Table` object by passing the
:paramref:`_schema.Table.implicit_returning` parameter as ``False``::

      t = Table(
          "t",
          metadata,
          Column("id", Integer, primary_key=True),
          Column("x", Integer),
          implicit_returning=False,
      )

The reason one might want to disable RETURNING for a specific table is to
work around backend-specific limitations.


Batched Mode Operation
~~~~~~~~~~~~~~~~~~~~~~

The feature has two modes of operation, which are selected transparently on a
per-dialect, per-:class:`_schema.Table` basis. One is **batched mode**,
which reduces the number of database round trips by rewriting an
INSERT statement of the form:

.. sourcecode:: sql

    INSERT INTO a (data, x, y) VALUES (%(data)s, %(x)s, %(y)s) RETURNING a.id

into a "batched" form such as:

.. sourcecode:: sql

    INSERT INTO a (data, x, y) VALUES
        (%(data_0)s, %(x_0)s, %(y_0)s),
        (%(data_1)s, %(x_1)s, %(y_1)s),
        (%(data_2)s, %(x_2)s, %(y_2)s),
        ...
        (%(data_78)s, %(x_78)s, %(y_78)s)
    RETURNING a.id

where above, the statement is organized against a subset (a "batch") of the
input data, the size of which is determined by the database backend as well as
the number of parameters in each batch to correspond to known limits for
statement size / number of parameters.  The feature then executes the INSERT
statement once for each batch of input data until all records are consumed,
concatenating the RETURNING results for each batch into a single large
rowset that's available from a single :class:`_result.Result` object.

This "batched" form allows INSERT of many rows using much fewer database round
trips, and has been shown to allow dramatic performance improvements for most
backends where it's supported.

.. _engine_insertmanyvalues_returning_order:

Correlating RETURNING rows to parameter sets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: 2.0.10

The "batch" mode query illustrated in the previous section does not guarantee
the order of records returned would correspond with that of the input data.
When used by the SQLAlchemy ORM :term:`unit of work` process, as well as for
applications which correlate returned server-generated values with input data,
the :meth:`_dml.Insert.returning` and :meth:`_dml.UpdateBase.return_defaults`
methods include an option
:paramref:`_dml.Insert.returning.sort_by_parameter_order` which indicates that
"insertmanyvalues" mode should guarantee this correspondence. This is **not
related** to the order in which records are actually INSERTed by the database
backend, which is **not** assumed under any circumstances; only that the
returned records should be organized when received back to correspond to the
order in which the original input data was passed.

When the :paramref:`_dml.Insert.returning.sort_by_parameter_order` parameter is
present, for tables that use server-generated integer primary key values such
as ``IDENTITY``, PostgreSQL ``SERIAL``, MariaDB ``AUTO_INCREMENT``, or SQLite's
``ROWID`` scheme, "batch" mode may instead opt to use a more complex
INSERT..RETURNING form, in conjunction with post-execution sorting of rows
based on the returned values, or if
such a form is not available, the "insertmanyvalues" feature may gracefully
degrade to "non-batched" mode which runs individual INSERT statements for each
parameter set.

For example, on SQL Server when an auto incrementing ``IDENTITY`` column is
used as the primary key, the following SQL form is used [#]_:

.. sourcecode:: sql

    INSERT INTO a (data, x, y)
    OUTPUT inserted.id, inserted.id AS id__1
    SELECT p0, p1, p2 FROM (VALUES
        (?, ?, ?, 0), (?, ?, ?, 1), (?, ?, ?, 2),
        ...
        (?, ?, ?, 77)
    ) AS imp_sen(p0, p1, p2, sen_counter) ORDER BY sen_counter

A similar form is used for PostgreSQL as well, when primary key columns use
SERIAL or IDENTITY. The above form **does not** guarantee the order in which
rows are inserted. However, it does guarantee that the IDENTITY or SERIAL
values will be created in order with each parameter set [#]_. The
"insertmanyvalues" feature then sorts the returned rows for the above INSERT
statement by incrementing integer identity.

For the SQLite database, there is no appropriate INSERT form that can
correlate the production of new ROWID values with the order in which
the parameter sets are passed.  As a result, when using server-generated
primary key values, the SQLite backend will degrade to "non-batched"
mode when ordered RETURNING is requested.
For MariaDB, the default INSERT form used by insertmanyvalues is sufficient,
as this database backend will line up the
order of AUTO_INCREMENT with the order of input data when using InnoDB [#]_.

For a client-side generated primary key, such as when using the Python
``uuid.uuid4()`` function to generate new values for a :class:`.Uuid` column,
the "insertmanyvalues" feature transparently includes this column in the
RETURNING records and correlates its value to that of the given input records,
thus maintaining correspondence between input records and result rows. From
this, it follows that all backends allow for batched, parameter-correlated
RETURNING order when client-side-generated primary key values are used.

The subject of how "insertmanyvalues" "batch" mode determines a column or
columns to use as a point of correspondence between input parameters and
RETURNING rows is known as an :term:`insert sentinel`, which is a specific
column or columns that are used to track such values. The "insert sentinel" is
normally selected automatically, however can also be user-configuration for
extremely special cases; the section
:ref:`engine_insertmanyvalues_sentinel_columns` describes this.

For backends that do not offer an appropriate INSERT form that can deliver
server-generated values deterministically aligned with input values, or
for :class:`_schema.Table` configurations that feature other kinds of
server generated primary key values, "insertmanyvalues" mode will make use
of **non-batched** mode when guaranteed RETURNING ordering is requested.

.. seealso::


    .. [#]

      * Microsoft SQL Server rationale

        "INSERT queries that use SELECT with ORDER BY to populate rows guarantees
        how identity values are computed but not the order in which the rows are inserted."
        https://learn.microsoft.com/en-us/sql/t-sql/statements/insert-transact-sql?view=sql-server-ver16#limitations-and-restrictions

    .. [#]

      * PostgreSQL batched INSERT Discussion

        Original description in 2018 https://www.postgresql.org/message-id/29386.1528813619@sss.pgh.pa.us

        Follow up in 2023 - https://www.postgresql.org/message-id/be108555-da2a-4abc-a46b-acbe8b55bd25%40app.fastmail.com

    .. [#]

      * MariaDB AUTO_INCREMENT behavior (using the same InnoDB engine as MySQL)

        https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html

        https://dba.stackexchange.com/a/72099


.. _engine_insertmanyvalues_non_batch:

Non-Batched Mode Operation
~~~~~~~~~~~~~~~~~~~~~~~~~~

For :class:`_schema.Table` configurations that do not have client side primary
key values, and offer server-generated primary key values (or no primary key)
that the database in question is not able to invoke in a deterministic or
sortable way relative to multiple parameter sets, the "insertmanyvalues"
feature when tasked with satisfying the
:paramref:`_dml.Insert.returning.sort_by_parameter_order` requirement for an
:class:`_dml.Insert` statement may instead opt to use **non-batched mode**.

In this mode, the original SQL form of INSERT is maintained, and the
"insertmanyvalues" feature will instead run the statement as given for each
parameter set individually, organizing the returned rows into a full result
set. Unlike previous SQLAlchemy versions, it does so in a tight loop that
minimizes Python overhead. In some cases, such as on SQLite, "non-batched" mode
performs exactly as well as "batched" mode.

Statement Execution Model
~~~~~~~~~~~~~~~~~~~~~~~~~

For both "batched" and "non-batched" modes, the feature will necessarily
invoke **multiple INSERT statements** using the DBAPI ``cursor.execute()`` method,
within the scope of  **single** call to the Core-level
:meth:`_engine.Connection.execute` method,
with each statement containing up to a fixed limit of parameter sets.
This limit is configurable as described below at :ref:`engine_insertmanyvalues_page_size`.
The separate calls to ``cursor.execute()`` are logged individually and
also individually passed along to event listeners such as
:meth:`.ConnectionEvents.before_cursor_execute` (see :ref:`engine_insertmanyvalues_events`
below).


.. _engine_insertmanyvalues_sentinel_columns:

Configuring Sentinel Columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In typical cases, the "insertmanyvalues" feature in order to provide
INSERT..RETURNING with deterministic row order will automatically determine a
sentinel column from a given table's primary key, gracefully degrading to "row
at a time" mode if one cannot be identified. As a completely **optional**
feature, to get full "insertmanyvalues" bulk performance for tables that have
server generated primary keys whose default generator functions aren't
compatible with the "sentinel" use case, other non-primary key columns may be
marked as "sentinel" columns assuming they meet certain requirements. A typical
example is a non-primary key :class:`_sqltypes.Uuid` column with a client side
default such as the Python ``uuid.uuid4()`` function.  There is also a construct to create
simple integer columns with a a client side integer counter oriented towards
the "insertmanyvalues" use case.

Sentinel columns may be indicated by adding :paramref:`_schema.Column.insert_sentinel`
to qualifying columns.   The most basic "qualifying" column is a not-nullable,
unique column with a client side default, such as a UUID column as follows::

    import uuid

    from sqlalchemy import Column
    from sqlalchemy import FetchedValue
    from sqlalchemy import Integer
    from sqlalchemy import String
    from sqlalchemy import Table
    from sqlalchemy import Uuid

    my_table = Table(
        "some_table",
        metadata,
        # assume some arbitrary server-side function generates
        # primary key values, so cannot be tracked by a bulk insert
        Column("id", String(50), server_default=FetchedValue(), primary_key=True),
        Column("data", String(50)),
        Column(
            "uniqueid",
            Uuid(),
            default=uuid.uuid4,
            nullable=False,
            unique=True,
            insert_sentinel=True,
        ),
    )

When using ORM Declarative models, the same forms are available using
the :class:`_orm.mapped_column` construct::

    import uuid

    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    class Base(DeclarativeBase):
        pass


    class MyClass(Base):
        __tablename__ = "my_table"

        id: Mapped[str] = mapped_column(primary_key=True, server_default=FetchedValue())
        data: Mapped[str] = mapped_column(String(50))
        uniqueid: Mapped[uuid.UUID] = mapped_column(
            default=uuid.uuid4, unique=True, insert_sentinel=True
        )

While the values generated by the default generator **must** be unique, the
actual UNIQUE constraint on the above "sentinel" column, indicated by the
``unique=True`` parameter, itself is optional and may be omitted if not
desired.

There is also a special form of "insert sentinel" that's a dedicated nullable
integer column which makes use of a special default integer counter that's only
used during "insertmanyvalues" operations; as an additional behavior, the
column will omit itself from SQL statements and result sets and behave in a
mostly transparent manner.  It does need to be physically present within
the actual database table, however.  This style of :class:`_schema.Column`
may be constructed using the function :func:`_schema.insert_sentinel`::

    from sqlalchemy import Column
    from sqlalchemy import Integer
    from sqlalchemy import String
    from sqlalchemy import Table
    from sqlalchemy import Uuid
    from sqlalchemy import insert_sentinel

    Table(
        "some_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(50)),
        insert_sentinel("sentinel"),
    )

When using ORM Declarative, a Declarative-friendly version of
:func:`_schema.insert_sentinel` is available called
:func:`_orm.orm_insert_sentinel`, which has the ability to be used on the Base
class or a mixin; if packaged using :func:`_orm.declared_attr`, the column will
apply itself to all table-bound subclasses including within joined inheritance
hierarchies::


    from sqlalchemy.orm import declared_attr
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import orm_insert_sentinel


    class Base(DeclarativeBase):
        @declared_attr
        def _sentinel(cls) -> Mapped[int]:
            return orm_insert_sentinel()


    class MyClass(Base):
        __tablename__ = "my_table"

        id: Mapped[str] = mapped_column(primary_key=True, server_default=FetchedValue())
        data: Mapped[str] = mapped_column(String(50))


    class MySubClass(MyClass):
        __tablename__ = "sub_table"

        id: Mapped[str] = mapped_column(ForeignKey("my_table.id"), primary_key=True)


    class MySingleInhClass(MyClass):
        pass

In the example above, both "my_table" and "sub_table" will have an additional
integer column named "_sentinel" that can be used by the "insertmanyvalues"
feature to help optimize bulk inserts used by the ORM.

.. _engine_insertmanyvalues_monotonic_functions:

Configuring Monotonic Functions such as UUIDV7
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using a monotonic function such as uuidv7 is supported by the "insertmanyvalues"
feature most easily by establishing the function as a client side callable,
e.g. using Python's built-in ``uuid.uuid7()`` call by providing the callable
to the :paramref:`_schema.Connection.default` parameter::

    import uuid

    from sqlalchemy import UUID, Integer

    t = Table(
        "t",
        metadata,
        Column("id", UUID, default=uuid.uuid7, primary_key=True),
        Column("x", Integer),
    )

In the above example, SQLAlchemy will invoke Python's ``uuid.uuid7()`` function
to create new primary key identifiers, which will be batchable by the
"insertmanyvalues" feature.

However, some databases like PostgreSQL provide a server-side function for
uuid7 called ``uuidv7()``; in SQLAlchemy, this would be available from the
:data:`_sql.func` namespace as ``func.uuidv7()``, and may be configured on a
:class:`.Column` using either :paramref:`_schema.Connection.default` to allow
it to be called as needed, or :paramref:`_schema.Connection.server_default` to
establish it as part of the table's DDL.  However, for full batched "insertmanyvalues"
behavior including support for sorted RETURNING (as would allow the ORM to
most effectively batch INSERT statements), an additional directive must be
included indicating that the function produces
monotonically increasing values, which is the ``monotonic=True`` directive.
This is illustrated below as a DDL server default using
:paramref:`_schema.Connection.server_default`::

    from sqlalchemy import func, Integer

    t = Table(
        "t",
        metadata,
        Column("id", UUID, server_default=func.uuidv7(monotonic=True), primary_key=True),
        Column("x", Integer),
    )

Using the above form, a batched INSERT...RETURNING on PostgreSQL with
:paramref:`.UpdateBase.returning.sort_by_parameter_order` set to True will
look like:

.. sourcecode:: sql

     INSERT INTO t (x) SELECT p0::INTEGER FROM
     (VALUES (%(x__0)s, 0), (%(x__1)s, 1), (%(x__2)s, 2),   ...)
     AS imp_sen(p0, sen_counter) ORDER BY sen_counter
     RETURNING t.id, t.id AS id__1

Similarly if the function is configured as an ad-hoc server side function
using :paramref:`_schema.Connection.default`::

    t = Table(
        "t",
        metadata,
        Column("id", UUID, default=func.uuidv7(monotonic=True), primary_key=True),
        Column("x", Integer),
    )

The function will then be rendered in the SQL statement explicitly:

.. sourcecode:: sql

    INSERT INTO t (id, x) SELECT uuidv7(), p1::INTEGER FROM
    (VALUES (%(x__0)s, 0), (%(x__1)s, 1), (%(x__2)s, 2), ...)
    AS imp_sen(p1, sen_counter) ORDER BY sen_counter
    RETURNING t.id, t.id AS id__1

.. versionadded:: 2.1 Added support for explicit monotonic server side functions
   using ``monotonic=True`` with any :class:`.Function`.

.. seealso::

    :ref:`postgresql_monotonic_functions`


.. _engine_insertmanyvalues_page_size:

Controlling the Batch Size
~~~~~~~~~~~~~~~~~~~~~~~~~~

A key characteristic of "insertmanyvalues" is that the size of the INSERT
statement is limited on a fixed max number of "values" clauses as well as a
dialect-specific fixed total number of bound parameters that may be represented
in one INSERT statement at a time. When the number of parameter dictionaries
given exceeds a fixed limit, or when the total number of bound parameters to be
rendered in a single INSERT statement exceeds a fixed limit (the two fixed
limits are separate), multiple INSERT statements will be invoked within the
scope of a single :meth:`_engine.Connection.execute` call, each of which
accommodate for a portion of the parameter dictionaries, known as a
"batch".  The number of parameter dictionaries represented within each
"batch" is then known as the "batch size".  For example, a batch size of
500 means that each INSERT statement emitted will INSERT at most 500 rows.

It's potentially important to be able to adjust the batch size,
as a larger batch size may be more performant for an INSERT where the value
sets themselves are relatively small, and a smaller batch size may be more
appropriate for an INSERT that uses very large value sets, where both the size
of the rendered SQL as well as the total data size being passed in one
statement may benefit from being limited to a certain size based on backend
behavior and memory constraints.  For this reason the batch size
can be configured on a per-:class:`.Engine` as well as a per-statement
basis.   The parameter limit on the other hand is fixed based on the known
characteristics of the database in use.

The batch size defaults to 1000 for most backends, with an additional
per-dialect "max number of parameters" limiting factor that may reduce the
batch size further on a per-statement basis. The max number of parameters
varies by dialect and server version; the largest size is 32700 (chosen as a
healthy distance away from PostgreSQL's limit of 32767 and SQLite's modern
limit of 32766, while leaving room for additional parameters in the statement
as well as for DBAPI quirkiness). Older versions of SQLite (prior to 3.32.0)
will set this value to 999. MariaDB has no established limit however 32700
remains as a limiting factor for SQL message size.

The value of the "batch size" can be affected :class:`_engine.Engine`
wide via the :paramref:`_sa.create_engine.insertmanyvalues_page_size` parameter.
Such as, to affect INSERT statements to include up to 100 parameter sets
in each statement::

    e = create_engine("sqlite://", insertmanyvalues_page_size=100)

The batch size may also be affected on a per statement basis using the
:paramref:`_engine.Connection.execution_options.insertmanyvalues_page_size`
execution option, such as per execution::

    with e.begin() as conn:
        result = conn.execute(
            table.insert().returning(table.c.id),
            parameterlist,
            execution_options={"insertmanyvalues_page_size": 100},
        )

Or configured on the statement itself::

    stmt = (
        table.insert()
        .returning(table.c.id)
        .execution_options(insertmanyvalues_page_size=100)
    )
    with e.begin() as conn:
        result = conn.execute(stmt, parameterlist)

.. _engine_insertmanyvalues_events:

Logging and Events
~~~~~~~~~~~~~~~~~~

The "insertmanyvalues" feature integrates fully with SQLAlchemy's :ref:`statement
logging <dbengine_logging>` as well as cursor events such as :meth:`.ConnectionEvents.before_cursor_execute`.
When the list of parameters is broken into separate batches, **each INSERT
statement is logged and passed to event handlers individually**.   This is a major change
compared to how the psycopg2-only feature worked in previous 1.x series of
SQLAlchemy, where the production of multiple INSERT statements was hidden from
logging and events.  Logging display will truncate the long lists of parameters for readability,
and will also indicate the specific batch of each statement. The example below illustrates
an excerpt of this logging:

.. sourcecode:: text

  INSERT INTO a (data, x, y) VALUES (?, ?, ?), ... 795 characters truncated ...  (?, ?, ?), (?, ?, ?) RETURNING id
  [generated in 0.00177s (insertmanyvalues) 1/10 (unordered)] ('d0', 0, 0, 'd1',  ...
  INSERT INTO a (data, x, y) VALUES (?, ?, ?), ... 795 characters truncated ...  (?, ?, ?), (?, ?, ?) RETURNING id
  [insertmanyvalues 2/10 (unordered)] ('d100', 100, 1000, 'd101', ...

  ...

  INSERT INTO a (data, x, y) VALUES (?, ?, ?), ... 795 characters truncated ...  (?, ?, ?), (?, ?, ?) RETURNING id
  [insertmanyvalues 10/10 (unordered)] ('d900', 900, 9000, 'd901', ...

When :ref:`non-batch mode <engine_insertmanyvalues_non_batch>` takes place, logging
will indicate this along with the insertmanyvalues message:

.. sourcecode:: text

  ...

  INSERT INTO a (data, x, y) VALUES (?, ?, ?) RETURNING id
  [insertmanyvalues 67/78 (ordered; batch not supported)] ('d66', 66, 66)
  INSERT INTO a (data, x, y) VALUES (?, ?, ?) RETURNING id
  [insertmanyvalues 68/78 (ordered; batch not supported)] ('d67', 67, 67)
  INSERT INTO a (data, x, y) VALUES (?, ?, ?) RETURNING id
  [insertmanyvalues 69/78 (ordered; batch not supported)] ('d68', 68, 68)
  INSERT INTO a (data, x, y) VALUES (?, ?, ?) RETURNING id
  [insertmanyvalues 70/78 (ordered; batch not supported)] ('d69', 69, 69)

  ...

.. seealso::

    :ref:`dbengine_logging`

Upsert Support
~~~~~~~~~~~~~~

The PostgreSQL, SQLite, and MariaDB dialects offer backend-specific
"upsert" constructs :func:`_postgresql.insert`, :func:`_sqlite.insert`
and :func:`_mysql.insert`, which are each :class:`_dml.Insert` constructs that
have an additional method such as ``on_conflict_do_update()` or
``on_duplicate_key()``.   These constructs also support "insertmanyvalues"
behaviors when they are used with RETURNING, allowing efficient upserts
with RETURNING to take place.


.. _engine_disposal:

Engine Disposal
---------------

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
  generally do **not** travel across process boundaries.  Use the
  :paramref:`.Engine.dispose.close` parameter set to False in this case.
  See the section :ref:`pooling_multiprocessing` for more background on this
  use case.

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

.. seealso::

    :ref:`pooling_toplevel`

    :ref:`pooling_multiprocessing`

.. _dbapi_connections:

Working with Driver SQL and Raw DBAPI Connections
-------------------------------------------------

The introduction on using :meth:`_engine.Connection.execute` made use of the
:func:`_expression.text` construct in order to illustrate how textual SQL statements
may be invoked.  When working with SQLAlchemy, textual SQL is actually more
of the exception rather than the norm, as the Core expression language
and the ORM both abstract away the textual representation of SQL.  However, the
:func:`_expression.text` construct itself also provides some abstraction of textual
SQL in that it normalizes how bound parameters are passed, as well as that
it supports datatyping behavior for parameters and result set rows.

Invoking SQL strings directly to the driver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For the use case where one wants to invoke textual SQL directly passed to the
underlying driver (known as the :term:`DBAPI`) without any intervention
from the :func:`_expression.text` construct, the :meth:`_engine.Connection.exec_driver_sql`
method may be used::

    with engine.connect() as conn:
        conn.exec_driver_sql("SET param='bar'")

.. versionadded:: 1.4  Added the :meth:`_engine.Connection.exec_driver_sql` method.

.. _dbapi_connections_cursor:

Working with the DBAPI cursor directly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

.. seealso::

    :ref:`faq_dbapi_connection` - includes additional details about how
    the DBAPI connection is accessed as well as the "driver" connection
    when using asyncio drivers.

Some recipes for DBAPI connection use follow.

.. _stored_procedures:

Calling Stored Procedures and User Defined Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQLAlchemy supports calling stored procedures and user defined functions
several ways. Please note that all DBAPIs have different practices, so you must
consult your underlying DBAPI's documentation for specifics in relation to your
particular usage. The following examples are hypothetical and may not work with
your underlying DBAPI.

For stored procedures or functions with special syntactical or parameter concerns,
DBAPI-level `callproc <https://legacy.python.org/dev/peps/pep-0249/#callproc>`_
may potentially be used with your DBAPI. An example of this pattern is::

    connection = engine.raw_connection()
    try:
        cursor_obj = connection.cursor()
        cursor_obj.callproc("my_procedure", ["x", "y", "z"])
        results = list(cursor_obj.fetchall())
        cursor_obj.close()
        connection.commit()
    finally:
        connection.close()

.. note::

  Not all DBAPIs use `callproc` and overall usage details will vary. The above
  example is only an illustration of how it might look to use a particular DBAPI
  function.

Your DBAPI may not have a ``callproc`` requirement *or* may require a stored
procedure or user defined function to be invoked with another pattern, such as
normal SQLAlchemy connection usage. One example of this usage pattern is,
*at the time of this documentation's writing*, executing a stored procedure in
the PostgreSQL database with the psycopg2 DBAPI, which should be invoked
with normal connection usage::

    connection.execute("CALL my_procedure();")

This above example is hypothetical. The underlying database is not guaranteed to
support "CALL" or "SELECT" in these situations, and the keyword may vary
dependent on the function being a stored procedure or a user defined function.
You should consult your underlying DBAPI and database documentation in these
situations to determine the correct syntax and patterns to use.


Multiple Result Sets
~~~~~~~~~~~~~~~~~~~~

Multiple result set support is available from a raw DBAPI cursor using the
`nextset <https://legacy.python.org/dev/peps/pep-0249/#nextset>`_ method::

    connection = engine.raw_connection()
    try:
        cursor_obj = connection.cursor()
        cursor_obj.execute("select * from table1; select * from table2")
        results_one = cursor_obj.fetchall()
        cursor_obj.nextset()
        results_two = cursor_obj.fetchall()
        cursor_obj.close()
    finally:
        connection.close()

Registering New Dialects
------------------------

The :func:`_sa.create_engine` function call locates the given dialect
using setuptools entrypoints.   These entry points can be established
for third party dialects within the setup.py script.  For example,
to create a new dialect "foodialect://", the steps are as follows:

1. Create a package called ``foodialect``.
2. The package should have a module containing the dialect class,
   which is typically a subclass of :class:`sqlalchemy.engine.default.DefaultDialect`.
   In this example let's say it's called ``FooDialect`` and its module is accessed
   via ``foodialect.dialect``.
3. The entry point can be established in ``setup.cfg`` as follows:

   .. sourcecode:: ini

          [options.entry_points]
          sqlalchemy.dialects =
              foodialect = foodialect.dialect:FooDialect

If the dialect is providing support for a particular DBAPI on top of
an existing SQLAlchemy-supported database, the name can be given
including a database-qualification.  For example, if ``FooDialect``
were in fact a MySQL dialect, the entry point could be established like this:

.. sourcecode:: ini

      [options.entry_points]
      sqlalchemy.dialects
          mysql.foodialect = foodialect.dialect:FooDialect

The above entrypoint would then be accessed as ``create_engine("mysql+foodialect://")``.


Registering Dialects In-Process
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQLAlchemy also allows a dialect to be registered within the current process, bypassing
the need for separate installation.   Use the ``register()`` function as follows::

    from sqlalchemy.dialects import registry


    registry.register("mysql.foodialect", "myapp.dialect", "MyMySQLDialect")

The above will respond to ``create_engine("mysql+foodialect://")`` and load the
``MyMySQLDialect`` class from the ``myapp.dialect`` module.


Connection / Engine API
-----------------------

.. autoclass:: Connection
   :members:

.. autoclass:: CreateEnginePlugin
   :members:

.. autoclass:: Engine
   :members:

.. autoclass:: ExceptionContext
   :members:

.. autoclass:: NestedTransaction
    :members:
    :inherited-members:

.. autoclass:: RootTransaction
    :members:
    :inherited-members:

.. autoclass:: Transaction
    :members:

.. autoclass:: TwoPhaseTransaction
    :members:
    :inherited-members:


Result Set API
---------------

.. autoclass:: ChunkedIteratorResult
    :members:

.. autoclass:: CursorResult
    :members:
    :inherited-members:

.. autoclass:: FilterResult
    :members:

.. autoclass:: FrozenResult
    :members:

.. autoclass:: IteratorResult
    :members:

.. autoclass:: MergedResult
    :members:

.. autoclass:: Result
    :members:
    :inherited-members:

.. autoclass:: ScalarResult
    :members:
    :inherited-members:

.. autoclass:: MappingResult
    :members:
    :inherited-members:

.. autoclass:: Row
    :members:
    :private-members: _asdict, _fields, _mapping, _t, _tuple

.. autoclass:: RowMapping
    :members:

.. autoclass:: TupleResult
