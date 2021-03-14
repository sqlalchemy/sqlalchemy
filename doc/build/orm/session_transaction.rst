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
:class:`_orm.SessionTransaction`.   This object then makes use of the underyling
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
    result = session.execute(< some select statment >)
    session.add_all([more_objects, ...])
    session.commit()  # commits

    session.add(still_another_object)
    session.flush()  # flush still_another_object
    session.rollback()   # rolls back still_another_object

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

        result = session.execute(<some SELECT statement>)

    # remaining transactional state from the .execute() call is
    # discarded

Similarly, the :class:`_orm.sessionmaker` can be used in the same way::

    Session = sesssionmaker(engine)

    with Session() as session:
        with session.begin():
            session.add(some_object)
        # commits

    # closes the Session

:class:`_orm.sessionmaker` itself includes a :meth:`_orm.sessionmaker.begin`
method to allow both operations to take place at once::

    with Session.begin() as session:
        session.add(some_object):



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

        nested = session.begin_nested() # establish a savepoint
        session.add(u3)
        nested.rollback()  # rolls back u3, keeps u1 and u2

    # commits u1 and u2

Each time :meth:`_orm.Session.begin_nested` is called, a new "BEGIN SAVEPOINT"
command is emitted to the database wih a unique identifier.  When
:meth:`_orm.SessionTransaction.commit` is called, "RELEASE SAVEPOINT"
is emitted on the database, and if instead
:meth:`_orm.SessionTransaction.rollback` is called, "ROLLBACK TO SAVEPOINT"
is emitted.

:meth:`_orm.Session.begin_nested` may also be used as a context manager in the
same manner as that of the :meth:`_orm.Session.begin` method::

    for record in records:
        try:
            with session.begin_nested():
                session.merge(record)
        except:
            print("Skipped record %s" % record)
    session.commit()

When :meth:`~.Session.begin_nested` is called, a
:meth:`~.Session.flush` is unconditionally issued
(regardless of the ``autoflush`` setting). This is so that when a
rollback on this nested transaction occurs, the full state of the
session is expired, thus causing all subsequent attribute/instance access to
reference the full state of the :class:`~sqlalchemy.orm.session.Session` right
before :meth:`~.Session.begin_nested` was called.

.. seealso::

    :class:`_engine.NestedTransaction` - the :class:`.NestedTransaction` class is the
    Core-level construct that is used by the :class:`_orm.Session` internally
    to produce SAVEPOINT blocks.

.. _orm_session_vs_engine:

Session-level vs. Engine level transaction control
--------------------------------------------------

As of SQLAlchemy 1.4, the :class:`_orm.sessionmaker` and Core
:class:`_engine.Engine` objects both support :term:`2.0 style` operation,
by making use of the :paramref:`_orm.Session.future` flag as well as the
:paramref:`_engine.create_engine.future` flag so that these two objects
assume 2.0-style semantics.

When using future mode, there should be equivalent semantics between
the two packages, at the level of the :class:`_orm.sessionmaker` vs.
the :class:`_future.Engine`, as well as the :class:`_orm.Session` vs.
the :class:`_future.Connection`.  The following sections detail
these scenarios based on the following scheme::


    ORM (using future Session)                    Core (using future engine)
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

Both :class:`_orm.Session` and :class:`_future.Connection` feature
:meth:`_future.Connection.commit` and :meth:`_future.Connection.rollback`
methods.   Using SQLAlchemy 2.0-style operation, these methods affect the
**outermost** transaction in all cases.


Engine::

    engine = create_engine("postgresql://user:pass@host/dbname", future=True)

    with engine.connect() as conn:
        conn.execute(
            some_table.insert(),
            [
                {"data": "some data one"},
                {"data": "some data two"},
                {"data": "some data three"}
            ]
        )
        conn.commit()

Session::

    Session = sessionmaker(engine, future=True)

    with Session() as session:
        session.add_all([
            SomeClass(data="some data one"),
            SomeClass(data="some data two"),
            SomeClass(data="some data three")
        ])
        session.commit()

Begin Once
~~~~~~~~~~

Both :class:`_orm.sessionmaker` and :class:`_future.Engine` feature a
:meth:`_future.Engine.begin` method that will both procure a new object
with which to execute SQL statements (the :class:`_orm.Session` and
:class:`_future.Connection`, respectively) and then return a context manager
that will maintain a begin/commit/rollback context for that object.

Engine::

    engine = create_engine("postgresql://user:pass@host/dbname", future=True)

    with engine.begin() as conn:
        conn.execute(
            some_table.insert(),
            [
                {"data": "some data one"},
                {"data": "some data two"},
                {"data": "some data three"}
            ]
        )
    # commits and closes automatically

Session::

    Session = sessionmaker(engine, future=True)

    with Session.begin() as session:
        session.add_all([
            SomeClass(data="some data one"),
            SomeClass(data="some data two"),
            SomeClass(data="some data three")
        ])
    # commits and closes automatically


Nested Transaction
~~~~~~~~~~~~~~~~~~~~

When using a SAVEPOINT via the :meth:`_orm.Session.begin_nested` or
:meth:`_engine.Connection.begin_nested` methods, the transaction object
returned must be used to commit or rollback the SAVEPOINT.  Calling
the :meth:`_orm.Session.commit` or :meth:`_future.Connection.commit` methods
will always commit the **outermost** transaction; this is a SQLAlchemy 2.0
specific behavior that is reversed from the 1.x series.

Engine::

    engine = create_engine("postgresql://user:pass@host/dbname", future=True)

    with engine.begin() as conn:
        savepoint = conn.begin_nested()
        conn.execute(
            some_table.insert(),
            [
                {"data": "some data one"},
                {"data": "some data two"},
                {"data": "some data three"}
            ]
        )
        savepoint.commit()  # or rollback

    # commits automatically

Session::

    Session = sessionmaker(engine, future=True)

    with Session.begin() as session:
        savepoint = session.begin_nested()
        session.add_all([
            SomeClass(data="some data one"),
            SomeClass(data="some data two"),
            SomeClass(data="some data three")
        ])
        savepoint.commit()  # or rollback
    # commits automatically




.. _session_autocommit:

.. _session_explicit_begin:

Explicit Begin
---------------

.. versionchanged:: 1.4
    SQLAlchemy 1.4 deprecates "autocommit mode", which is historically enabled
    by using the :paramref:`_orm.Session.autocommit` flag.    Going forward,
    a new approach to allowing usage of the :meth:`_orm.Session.begin` method
    is new "autobegin" behavior so that the method may now be called when
    a :class:`_orm.Session` is first constructed, or after the previous
    transaction has ended and before it begins a new one.

    For background on migrating away from the "subtransaction" pattern for
    frameworks that rely upon nesting of begin()/commit() pairs, see the
    next section :ref:`session_subtransactions`.

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
        item1 = session.query(Item).get(1)
        item2 = session.query(Item).get(2)
        item1.foo = 'bar'
        item2.bar = 'foo'
        session.commit()
    except:
        session.rollback()
        raise

The above pattern is more idiomatically invoked using a context manager::

    Session = sessionmaker(bind=engine)
    session = Session()
    with session.begin():
        item1 = session.query(Item).get(1)
        item2 = session.query(Item).get(2)
        item1.foo = 'bar'
        item2.bar = 'foo'

The :meth:`_orm.Session.begin` method and the session's "autobegin" process
use the same sequence of steps to begin the transaction.   This includes
that the :meth:`_orm.SessionEvents.after_transaction_create` event is invoked
when it occurs; this hook is used by frameworks in order to integrate their
own transactional processes with that of the ORM :class:`_orm.Session`.


.. _session_subtransactions:

Migrating from the "subtransaction" pattern
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. deprecated:: 1.4  The :paramref:`_orm.Session.begin.subtransactions`
   flag is deprecated.  While the :class:`_orm.Session` still uses the
   "subtransactions" pattern internally, it is not suitable for end-user
   use as it leads to confusion, and additionally it may be removed from
   the :class:`_orm.Session` itself in version 2.0 once "autocommit"
   mode is removed.

The "subtransaction" pattern that was often used with autocommit mode is
also deprecated in 1.4.  This pattern allowed the use of the
:meth:`_orm.Session.begin` method when a transaction were already begun,
resulting in a construct called a "subtransaction", which was essentially
a block that would prevent the :meth:`_orm.Session.commit` method from actually
committing.

This pattern has been shown to be confusing in real world applications, and
it is preferable for an application to ensure that the top-most level of database
operations are performed with a single begin/commit pair.

To provide backwards compatibility for applications that make use of this
pattern, the following context manager or a similar implementation based on
a decorator may be used::


    import contextlib

    @contextlib.contextmanager
    def transaction(session):
        if not session.in_transaction():
            with session.begin():
                yield
        else:
            yield


The above context manager may be used in the same way the
"subtransaction" flag works, such as in the following example::


    # method_a starts a transaction and calls method_b
    def method_a(session):
        with transaction(session):
            method_b(session)

    # method_b also starts a transaction, but when
    # called from method_a participates in the ongoing
    # transaction.
    def method_b(session):
        with transaction(session):
            session.add(SomeObject('bat', 'lala'))

    Session = sessionmaker(engine)

    # create a Session and call method_a
    with Session() as session:
        method_a(session)

To compare towards the preferred idiomatic pattern, the begin block should
be at the outermost level.  This removes the need for individual functions
or methods to be concerned with the details of transaction demarcation::

    def method_a(session):
        method_b(session)

    def method_b(session):
        session.add(SomeObject('bat', 'lala'))

    Session = sessionmaker(engine)

    # create a Session and call method_a
    with Session() as session:
        with session.begin():
            method_a(session)

.. seealso::

    :ref:`connections_subtransactions` - similar pattern based on Core only

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

    engine1 = create_engine('postgresql://db1')
    engine2 = create_engine('postgresql://db2')

    Session = sessionmaker(twophase=True)

    # bind User operations to engine 1, Account operations to engine 2
    Session.configure(binds={User:engine1, Account:engine2})

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
        "postgresql://scott:tiger@localhost/test",
        isolation_level='REPEATABLE READ'
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

    eng = create_engine("postgresql://scott:tiger@localhost/test")

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
        some_objects = session.execute(<statement>)
        some_other_objects = session.execute(<statement>)

    # closes connection


Setting Isolation for Individual Sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When we make a new :class:`.Session`, either using the constructor directly
or when we call upon the callable produced by a :class:`.sessionmaker`,
we can pass the ``bind`` argument directly, overriding the pre-existing bind.
We can for example create our :class:`_orm.Session` from the
"``transactional_session``" and pass the "``autocommit_engine``"::

    with transactional_session(bind=autocommit_engine) as session:
        # work with session

For the case where the :class:`.Session` or :class:`.sessionmaker` is
configured with multiple "binds", we can either re-specify the ``binds``
argument fully, or if we want to only replace specific binds, we
can use the :meth:`.Session.bind_mapper` or :meth:`.Session.bind_table`
methods::

    session = maker()
    session.bind_mapper(User, autocommit_engine)

We can also use the individual transaction method that follows.

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

    sess = Session(bind=engine)
    with sess.begin():
        sess.connection(execution_options={'isolation_level': 'SERIALIZABLE'})

    # commits transaction.  the connection is released
    # and reverted to its previous isolation level.

Above, we first produce a :class:`.Session` using either the constructor
or a :class:`.sessionmaker`.   Then we explicitly set up the start of
a transaction by calling upon :meth:`.Session.connection`, which provides
for execution options that will be passed to the connection before the
transaction is begun.


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

.. versionchanged:: 1.4  This section introduces a new version of the
   "join into an external transaction" recipe that will work equally well
   for both :term:`2.0 style` and :term:`1.x style` engines and sessions.
   The recipe here from previous versions such as 1.3 will also continue to
   work for 1.x engines and sessions.


The recipe works by establishing a :class:`_engine.Connection` within a
transaction and optionally a SAVEPOINT, then passing it to a :class:`_orm.Session` as the
"bind".   The :class:`_orm.Session` detects that the given :class:`_engine.Connection`
is already in a transaction and will not run COMMIT on it if the transaction
is in fact an outermost transaction.   Then when the test tears down, the
transaction is rolled back so that any data changes throughout the test
are reverted::

    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine
    from unittest import TestCase

    # global application scope.  create Session class, engine
    Session = sessionmaker()

    engine = create_engine('postgresql://...')

    class SomeTest(TestCase):
        def setUp(self):
            # connect to the database
            self.connection = engine.connect()

            # begin a non-ORM transaction
            self.trans = self.connection.begin()


            # bind an individual Session to the connection
            self.session = Session(bind=self.connection)


            ###    optional     ###

            # if the database supports SAVEPOINT (SQLite needs special
            # config for this to work), starting a savepoint
            # will allow tests to also use rollback within tests

            self.nested = self.connection.begin_nested()

            @event.listens_for(self.session, "after_transaction_end")
            def end_savepoint(session, transaction):
                if not self.nested.is_active:
                    self.nested = self.connection.begin_nested()

            ### ^^^ optional ^^^ ###

        def test_something(self):
            # use the session in tests.

            self.session.add(Foo())
            self.session.commit()

        def test_something_with_rollbacks(self):
            # if the SAVEPOINT steps are taken, then a test can also
            # use session.rollback() and continue working with the database

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

