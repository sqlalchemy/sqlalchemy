======================================
Transactions and Connection Management
======================================

.. _unitofwork_transaction:

Managing Transactions
=====================

.. versionchanged:: 1.4 Session transaction management has been revised
   to be clearer and easier to use.  In particular, it now features
   "autobegin" operation, which means the point at which a transaction begins
   may be controlled, without using the legacy "autocommit" mode.

The :class:`_orm.Session` tracks the state of a single "virtual" transaction
at a time, using an object called
:class:`_orm.SessionTransaction`.   This object then makes use of the underlying
:class:`_engine.Engine` or engines to which the :class:`_orm.Session`
object is bound in order to start real connection-level transactions using
the :class:`_engine.Connection` object as needed.

This "virtual" transaction is created automatically when needed, or can
alternatively be started using the :meth:`_orm.Session.begin` method.  To
as great a degree as possible, Python context manager use is supported both
at the level of creating :class:`_orm.Session` objects as well as to maintain
the scope of the :class:`_orm.SessionTransaction`.

Below, assume we start with a :class:`_orm.Session`::

    from sqlalchemy.orm import Session

    session = Session(engine)

We can now run operations within a demarcated transaction using a context
manager::

    with session.begin():
        session.add(some_object())
        session.add(some_other_object())
    # commits transaction at the end, or rolls back if there
    # was an exception raised

At the end of the above context, assuming no exceptions were raised, any
pending objects will be flushed to the database and the database transaction
will be committed. If an exception was raised within the above block, then the
transaction would be rolled back.  In both cases, the above
:class:`_orm.Session` subsequent to exiting the block is ready to be used in
subsequent transactions.

The :meth:`_orm.Session.begin` method is optional, and the
:class:`_orm.Session` may also be used in a commit-as-you-go approach, where it
will begin transactions automatically as needed; these only need be committed
or rolled back::

    session = Session(engine)

    session.add(some_object())
    session.add(some_other_object())

    session.commit()  # commits

    # will automatically begin again
    result = session.execute("< some select statement >")
    session.add_all([more_objects, ...])
    session.commit()  # commits

    session.add(still_another_object)
    session.flush()  # flush still_another_object
    session.rollback()  # rolls back still_another_object

The :class:`_orm.Session` itself features a :meth:`_orm.Session.close`
method.  If the :class:`_orm.Session` is begun within a transaction that
has not yet been committed or rolled back, this method will cancel
(i.e. rollback) that transaction, and also expunge all objects contained
within the :class:`_orm.Session` object's state.   If the :class:`_orm.Session`
is being used in such a way that a call to :meth:`_orm.Session.commit`
or :meth:`_orm.Session.rollback` is not guaranteed (e.g. not within a context
manager or similar), the :class:`_orm.Session.close` method may be used
to ensure all resources are released::

    # expunges all objects, releases all transactions unconditionally
    # (with rollback), releases all database connections back to their
    # engines
    session.close()

Finally, the session construction / close process can itself be run
via context manager.  This is the best way to ensure that the scope of
a :class:`_orm.Session` object's use is scoped within a fixed block.
Illustrated via the :class:`_orm.Session` constructor
first::

    with Session(engine) as session:
        session.add(some_object())
        session.add(some_other_object())

        session.commit()  # commits

        session.add(still_another_object)
        session.flush()  # flush still_another_object

        session.commit()  # commits

        result = session.execute("<some SELECT statement>")

    # remaining transactional state from the .execute() call is
    # discarded

Similarly, the :class:`_orm.sessionmaker` can be used in the same way::

    Session = sessionmaker(engine)

    with Session() as session:
        with session.begin():
            session.add(some_object)
        # commits

    # closes the Session

:class:`_orm.sessionmaker` itself includes a :meth:`_orm.sessionmaker.begin`
method to allow both operations to take place at once::

    with Session.begin() as session:
        session.add(some_object)

.. _session_begin_nested:

Using SAVEPOINT
---------------

SAVEPOINT transactions, if supported by the underlying engine, may be
delineated using the :meth:`~.Session.begin_nested`
method::


    Session = sessionmaker()

    with Session.begin() as session:
        session.add(u1)
        session.add(u2)

        nested = session.begin_nested()  # establish a savepoint
        session.add(u3)
        nested.rollback()  # rolls back u3, keeps u1 and u2

    # commits u1 and u2

Each time :meth:`_orm.Session.begin_nested` is called, a new "BEGIN SAVEPOINT"
command is emitted to the database within the scope of the current
database transaction (starting one if not already in progress), and
an object of type :class:`_orm.SessionTransaction` is returned, which
represents a handle to this SAVEPOINT.  When
the ``.commit()`` method on this object is called, "RELEASE SAVEPOINT"
is emitted to the database, and if instead the ``.rollback()``
method is called, "ROLLBACK TO SAVEPOINT" is emitted.  The enclosing
database transaction remains in progress.

:meth:`_orm.Session.begin_nested` is typically used as a context manager
where specific per-instance errors may be caught, in conjunction with
a rollback emitted for that portion of the transaction's state, without
rolling back the whole transaction, as in the example below::

    for record in records:
        try:
            with session.begin_nested():
                session.merge(record)
        except:
            print("Skipped record %s" % record)
    session.commit()

When the context manager yielded by :meth:`_orm.Session.begin_nested`
completes, it "commits" the savepoint,
which includes the usual behavior of flushing all pending state.  When
an error is raised, the savepoint is rolled back and the state of the
:class:`_orm.Session` local to the objects that were changed is expired.

This pattern is ideal for situations such as using PostgreSQL and
catching :class:`.IntegrityError` to detect duplicate rows; PostgreSQL normally
aborts the entire tranasction when such an error is raised, however when using
SAVEPOINT, the outer transaction is maintained.   In the example below
a list of data is persisted into the database, with the occasional
"duplicate primary key" record skipped, without rolling back the entire
operation::

    from sqlalchemy import exc

    with session.begin():
        for record in records:
            try:
                with session.begin_nested():
                    obj = SomeRecord(id=record["identifier"], name=record["name"])
                    session.add(obj)
            except exc.IntegrityError:
                print(f"Skipped record {record} - row already exists")

When :meth:`~.Session.begin_nested` is called, the :class:`_orm.Session` first
flushes all currently pending state to the database; this occurs unconditionally,
regardless of the value of the :paramref:`_orm.Session.autoflush` parameter
which normally may be used to disable automatic flush.  The rationale
for this behavior is so that
when a rollback on this nested transaction occurs, the :class:`_orm.Session`
may expire any in-memory state that was created within the scope of the
SAVEPOINT, while
ensuring that when those expired objects are refreshed, the state of the
object graph prior to the beginning of the SAVEPOINT will be available
to re-load from the database.

In modern versions of SQLAlchemy, when a SAVEPOINT initiated by
:meth:`_orm.Session.begin_nested` is rolled back, in-memory object state that
was modified since the SAVEPOINT was created
is expired, however other object state that was not altered since the SAVEPOINT
began is maintained.  This is so that subsequent operations can continue to make use of the
otherwise unaffected data
without the need for refreshing it from the database.

.. seealso::

    :meth:`_engine.Connection.begin_nested` -  Core SAVEPOINT API

.. _orm_session_vs_engine:

Session-level vs. Engine level transaction control
--------------------------------------------------

The :class:`_engine.Connection` in Core and
:class:`_session.Session` in ORM feature equivalent transactional
semantics, both at the level of the :class:`_orm.sessionmaker` vs.
the :class:`_engine.Engine`, as well as the :class:`_orm.Session` vs.
the :class:`_engine.Connection`.  The following sections detail
these scenarios based on the following scheme:

.. sourcecode:: text

    ORM                                           Core
    -----------------------------------------     -----------------------------------
    sessionmaker                                  Engine
    Session                                       Connection
    sessionmaker.begin()                          Engine.begin()
    some_session.commit()                         some_connection.commit()
    with some_sessionmaker() as session:          with some_engine.connect() as conn:
    with some_sessionmaker.begin() as session:    with some_engine.begin() as conn:
    with some_session.begin_nested() as sp:       with some_connection.begin_nested() as sp:

Commit as you go
~~~~~~~~~~~~~~~~

Both :class:`_orm.Session` and :class:`_engine.Connection` feature
:meth:`_engine.Connection.commit` and :meth:`_engine.Connection.rollback`
methods.   Using SQLAlchemy 2.0-style operation, these methods affect the
**outermost** transaction in all cases.   For the :class:`_orm.Session`, it is
assumed that :paramref:`_orm.Session.autobegin` is left at its default
value of ``True``.



:class:`_engine.Engine`::

    engine = create_engine("postgresql+psycopg2://user:pass@host/dbname")

    with engine.connect() as conn:
        conn.execute(
            some_table.insert(),
            [
                {"data": "some data one"},
                {"data": "some data two"},
                {"data": "some data three"},
            ],
        )
        conn.commit()

:class:`_orm.Session`::

    Session = sessionmaker(engine)

    with Session() as session:
        session.add_all(
            [
                SomeClass(data="some data one"),
                SomeClass(data="some data two"),
                SomeClass(data="some data three"),
            ]
        )
        session.commit()

Begin Once
~~~~~~~~~~

Both :class:`_orm.sessionmaker` and :class:`_engine.Engine` feature a
:meth:`_engine.Engine.begin` method that will both procure a new object
with which to execute SQL statements (the :class:`_orm.Session` and
:class:`_engine.Connection`, respectively) and then return a context manager
that will maintain a begin/commit/rollback context for that object.

Engine::

    engine = create_engine("postgresql+psycopg2://user:pass@host/dbname")

    with engine.begin() as conn:
        conn.execute(
            some_table.insert(),
            [
                {"data": "some data one"},
                {"data": "some data two"},
                {"data": "some data three"},
            ],
        )
    # commits and closes automatically

Session::

    Session = sessionmaker(engine)

    with Session.begin() as session:
        session.add_all(
            [
                SomeClass(data="some data one"),
                SomeClass(data="some data two"),
                SomeClass(data="some data three"),
            ]
        )
    # commits and closes automatically

Nested Transaction
~~~~~~~~~~~~~~~~~~~~

When using a SAVEPOINT via the :meth:`_orm.Session.begin_nested` or
:meth:`_engine.Connection.begin_nested` methods, the transaction object
returned must be used to commit or rollback the SAVEPOINT.  Calling
the :meth:`_orm.Session.commit` or :meth:`_engine.Connection.commit` methods
will always commit the **outermost** transaction; this is a SQLAlchemy 2.0
specific behavior that is reversed from the 1.x series.

Engine::

    engine = create_engine("postgresql+psycopg2://user:pass@host/dbname")

    with engine.begin() as conn:
        savepoint = conn.begin_nested()
        conn.execute(
            some_table.insert(),
            [
                {"data": "some data one"},
                {"data": "some data two"},
                {"data": "some data three"},
            ],
        )
        savepoint.commit()  # or rollback

    # commits automatically

Session::

    Session = sessionmaker(engine)

    with Session.begin() as session:
        savepoint = session.begin_nested()
        session.add_all(
            [
                SomeClass(data="some data one"),
                SomeClass(data="some data two"),
                SomeClass(data="some data three"),
            ]
        )
        savepoint.commit()  # or rollback
    # commits automatically

.. _session_explicit_begin:

Explicit Begin
---------------

The :class:`_orm.Session` features "autobegin" behavior, meaning that as soon
as operations begin to take place, it ensures a :class:`_orm.SessionTransaction`
is present to track ongoing operations.   This transaction is completed
when :meth:`_orm.Session.commit` is called.

It is often desirable, particularly in framework integrations, to control the
point at which the "begin" operation occurs.  To suit this, the
:class:`_orm.Session` uses an "autobegin" strategy, such that the
:meth:`_orm.Session.begin` method may be called directly for a
:class:`_orm.Session` that has not already had a transaction begun::

    Session = sessionmaker(bind=engine)
    session = Session()
    session.begin()
    try:
        item1 = session.get(Item, 1)
        item2 = session.get(Item, 2)
        item1.foo = "bar"
        item2.bar = "foo"
        session.commit()
    except:
        session.rollback()
        raise

The above pattern is more idiomatically invoked using a context manager::

    Session = sessionmaker(bind=engine)
    session = Session()
    with session.begin():
        item1 = session.get(Item, 1)
        item2 = session.get(Item, 2)
        item1.foo = "bar"
        item2.bar = "foo"

The :meth:`_orm.Session.begin` method and the session's "autobegin" process
use the same sequence of steps to begin the transaction.   This includes
that the :meth:`_orm.SessionEvents.after_transaction_create` event is invoked
when it occurs; this hook is used by frameworks in order to integrate their
own transactional processes with that of the ORM :class:`_orm.Session`.



.. _session_twophase:

Enabling Two-Phase Commit
-------------------------

For backends which support two-phase operation (currently MySQL and
PostgreSQL), the session can be instructed to use two-phase commit semantics.
This will coordinate the committing of transactions across databases so that
the transaction is either committed or rolled back in all databases. You can
also :meth:`_orm.Session.prepare` the session for
interacting with transactions not managed by SQLAlchemy. To use two phase
transactions set the flag ``twophase=True`` on the session::

    engine1 = create_engine("postgresql+psycopg2://db1")
    engine2 = create_engine("postgresql+psycopg2://db2")

    Session = sessionmaker(twophase=True)

    # bind User operations to engine 1, Account operations to engine 2
    Session.configure(binds={User: engine1, Account: engine2})

    session = Session()

    # .... work with accounts and users

    # commit.  session will issue a flush to all DBs, and a prepare step to all DBs,
    # before committing both transactions
    session.commit()

.. _session_transaction_isolation:

Setting Transaction Isolation Levels / DBAPI AUTOCOMMIT
-------------------------------------------------------

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
methods like ``.begin()``, ``.commit()`` and ``.rollback()`` pass silently.

SQLAlchemy's dialects support settable isolation modes on a per-:class:`_engine.Engine`
or per-:class:`_engine.Connection` basis, using flags at both the
:func:`_sa.create_engine` level as well as at the :meth:`_engine.Connection.execution_options`
level.

When using the ORM :class:`.Session`, it acts as a *facade* for engines and
connections, but does not expose transaction isolation directly.  So in
order to affect transaction isolation level, we need to act upon the
:class:`_engine.Engine` or :class:`_engine.Connection` as appropriate.

.. seealso::

    :ref:`dbapi_autocommit` - be sure to review how isolation levels work at
    the level of the SQLAlchemy :class:`_engine.Connection` object as well.

Setting Isolation For A Sessionmaker / Engine Wide
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To set up a :class:`.Session` or :class:`.sessionmaker` with a specific
isolation level globally, the first technique is that an
:class:`_engine.Engine` can be constructed against a specific isolation level
in all cases, which is then used as the source of connectivity for a
:class:`_orm.Session` and/or :class:`_orm.sessionmaker`::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    eng = create_engine(
        "postgresql+psycopg2://scott:tiger@localhost/test",
        isolation_level="REPEATABLE READ",
    )

    Session = sessionmaker(eng)

Another option, useful if there are to be two engines with different isolation
levels at once, is to use the :meth:`_engine.Engine.execution_options` method,
which will produce a shallow copy of the original :class:`_engine.Engine` which
shares the same connection pool as the parent engine.  This is often preferable
when operations will be separated into "transactional" and "autocommit"
operations::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    eng = create_engine("postgresql+psycopg2://scott:tiger@localhost/test")

    autocommit_engine = eng.execution_options(isolation_level="AUTOCOMMIT")

    transactional_session = sessionmaker(eng)
    autocommit_session = sessionmaker(autocommit_engine)

Above, both "``eng``" and ``"autocommit_engine"`` share the same dialect and
connection pool.  However the "AUTOCOMMIT" mode will be set upon connections
when they are acquired from the ``autocommit_engine``.  The two
:class:`_orm.sessionmaker` objects "``transactional_session``" and "``autocommit_session"``
then inherit these characteristics when they work with database connections.


The "``autocommit_session``" **continues to have transactional semantics**,
including that
:meth:`_orm.Session.commit` and :meth:`_orm.Session.rollback` still consider
themselves to be "committing" and "rolling back" objects, however the
transaction will be silently absent.  For this reason, **it is typical,
though not strictly required, that a Session with AUTOCOMMIT isolation be
used in a read-only fashion**, that is::


    with autocommit_session() as session:
        some_objects = session.execute("<statement>")
        some_other_objects = session.execute("<statement>")

    # closes connection

Setting Isolation for Individual Sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When we make a new :class:`.Session`, either using the constructor directly
or when we call upon the callable produced by a :class:`.sessionmaker`,
we can pass the ``bind`` argument directly, overriding the pre-existing bind.
We can for example create our :class:`_orm.Session` from a default
:class:`.sessionmaker` and pass an engine set for autocommit::

    plain_engine = create_engine("postgresql+psycopg2://scott:tiger@localhost/test")

    autocommit_engine = plain_engine.execution_options(isolation_level="AUTOCOMMIT")

    # will normally use plain_engine
    Session = sessionmaker(plain_engine)

    # make a specific Session that will use the "autocommit" engine
    with Session(bind=autocommit_engine) as session:
        # work with session
        ...

For the case where the :class:`.Session` or :class:`.sessionmaker` is
configured with multiple "binds", we can either re-specify the ``binds``
argument fully, or if we want to only replace specific binds, we
can use the :meth:`.Session.bind_mapper` or :meth:`.Session.bind_table`
methods::

    with Session() as session:
        session.bind_mapper(User, autocommit_engine)

Setting Isolation for Individual Transactions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A key caveat regarding isolation level is that the setting cannot be
safely modified on a :class:`_engine.Connection` where a transaction has already
started.  Databases cannot change the isolation level of a transaction
in progress, and some DBAPIs and SQLAlchemy dialects
have inconsistent behaviors in this area.

Therefore it is preferable to use a :class:`_orm.Session` that is up front
bound to an engine with the desired isolation level.  However, the isolation
level on a per-connection basis can be affected by using the
:meth:`_orm.Session.connection` method at the start of a transaction::

    from sqlalchemy.orm import Session

    # assume session just constructed
    sess = Session(bind=engine)

    # call connection() with options before any other operations proceed.
    # this will procure a new connection from the bound engine and begin a real
    # database transaction.
    sess.connection(execution_options={"isolation_level": "SERIALIZABLE"})

    # ... work with session in SERIALIZABLE isolation level...

    # commit transaction.  the connection is released
    # and reverted to its previous isolation level.
    sess.commit()

    # subsequent to commit() above, a new transaction may be begun if desired,
    # which will proceed with the previous default isolation level unless
    # it is set again.

Above, we first produce a :class:`.Session` using either the constructor or a
:class:`.sessionmaker`. Then we explicitly set up the start of a database-level
transaction by calling upon :meth:`.Session.connection`, which provides for
execution options that will be passed to the connection before the
database-level transaction is begun.  The transaction proceeds with this
selected isolation level.   When the transaction completes, the isolation
level is reset on the connection to its default before the connection is
returned to the connection pool.

The :meth:`_orm.Session.begin` method may also be used to begin the
:class:`_orm.Session` level transaction; calling upon
:meth:`_orm.Session.connection` subsequent to that call may be used to set up
the per-connection-transaction isolation level::

    sess = Session(bind=engine)

    with sess.begin():
        # call connection() with options before any other operations proceed.
        # this will procure a new connection from the bound engine and begin a
        # real database transaction.
        sess.connection(execution_options={"isolation_level": "SERIALIZABLE"})

        # ... work with session in SERIALIZABLE isolation level...

    # outside the block, the transaction has been committed.  the connection is
    # released and reverted to its previous isolation level.

Tracking Transaction State with Events
--------------------------------------

See the section :ref:`session_transaction_events` for an overview
of the available event hooks for session transaction state changes.

.. _session_external_transaction:

Joining a Session into an External Transaction (such as for test suites)
========================================================================

If a :class:`_engine.Connection` is being used which is already in a transactional
state (i.e. has a :class:`.Transaction` established), a :class:`.Session` can
be made to participate within that transaction by just binding the
:class:`.Session` to that :class:`_engine.Connection`. The usual rationale for this
is a test suite that allows ORM code to work freely with a :class:`.Session`,
including the ability to call :meth:`.Session.commit`, where afterwards the
entire database interaction is rolled back.

.. versionchanged:: 2.0 The "join into an external transaction" recipe is
   newly improved again in 2.0; event handlers to "reset" the nested
   transaction are no longer required.

The recipe works by establishing a :class:`_engine.Connection` within a
transaction and optionally a SAVEPOINT, then passing it to a
:class:`_orm.Session` as the "bind"; the
:paramref:`_orm.Session.join_transaction_mode` parameter is passed with the
setting ``"create_savepoint"``, which indicates that new SAVEPOINTs should be
created in order to implement BEGIN/COMMIT/ROLLBACK for the
:class:`_orm.Session`, which will leave the external transaction in the same
state in which it was passed.

When the test tears down, the external transaction is rolled back so that any
data changes throughout the test are reverted::

    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine
    from unittest import TestCase

    # global application scope.  create Session class, engine
    Session = sessionmaker()

    engine = create_engine("postgresql+psycopg2://...")


    class SomeTest(TestCase):
        def setUp(self):
            # connect to the database
            self.connection = engine.connect()

            # begin a non-ORM transaction
            self.trans = self.connection.begin()

            # bind an individual Session to the connection, selecting
            # "create_savepoint" join_transaction_mode
            self.session = Session(
                bind=self.connection, join_transaction_mode="create_savepoint"
            )

        def test_something(self):
            # use the session in tests.

            self.session.add(Foo())
            self.session.commit()

        def test_something_with_rollbacks(self):
            self.session.add(Bar())
            self.session.flush()
            self.session.rollback()

            self.session.add(Foo())
            self.session.commit()

        def tearDown(self):
            self.session.close()

            # rollback - everything that happened with the
            # Session above (including calls to commit())
            # is rolled back.
            self.trans.rollback()

            # return connection to the Engine
            self.connection.close()

The above recipe is part of SQLAlchemy's own CI to ensure that it remains
working as expected.

