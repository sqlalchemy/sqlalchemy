======================================
Transactions and Connection Management
======================================

.. _unitofwork_transaction:

Managing Transactions
=====================

A newly constructed :class:`.Session` may be said to be in the "begin" state.
In this state, the :class:`.Session` has not established any connection or
transactional state with any of the :class:`_engine.Engine` objects that may be associated
with it.

The :class:`.Session` then receives requests to operate upon a database connection.
Typically, this means it is called upon to execute SQL statements using a particular
:class:`_engine.Engine`, which may be via :meth:`.Session.query`, :meth:`.Session.execute`,
or within a flush operation of pending data, which occurs when such state exists
and :meth:`.Session.commit` or :meth:`.Session.flush` is called.

As these requests are received, each new :class:`_engine.Engine` encountered is associated
with an ongoing transactional state maintained by the :class:`.Session`.
When the first :class:`_engine.Engine` is operated upon, the :class:`.Session` can be said
to have left the "begin" state and entered "transactional" state.   For each
:class:`_engine.Engine` encountered, a :class:`_engine.Connection` is associated with it,
which is acquired via the :meth:`_engine.Engine.contextual_connect` method.  If a
:class:`_engine.Connection` was directly associated with the :class:`.Session` (see :ref:`session_external_transaction`
for an example of this), it is
added to the transactional state directly.

For each :class:`_engine.Connection`, the :class:`.Session` also maintains a
:class:`.Transaction` object, which is acquired by calling
:meth:`_engine.Connection.begin` on each :class:`_engine.Connection`, or if the
:class:`.Session` object has been established using the flag ``twophase=True``,
a :class:`.TwoPhaseTransaction` object acquired via
:meth:`_engine.Connection.begin_twophase`.  These transactions are all
committed or rolled back corresponding to the invocation of the
:meth:`.Session.commit` and :meth:`.Session.rollback` methods.   A commit
operation will also call the :meth:`.TwoPhaseTransaction.prepare` method on
all transactions if applicable.

When the transactional state is completed after a rollback or commit, the
:class:`.Session`
:term:`releases` all :class:`.Transaction` and :class:`_engine.Connection`
resources, and goes back to the "begin" state, which will again invoke new
:class:`_engine.Connection` and :class:`.Transaction` objects as new
requests to emit SQL statements are received.

The example below illustrates this lifecycle::

    engine = create_engine("...")
    Session = sessionmaker(bind=engine)

    # new session.   no connections are in use.
    session = Session()
    try:
        # first query.  a Connection is acquired
        # from the Engine, and a Transaction
        # started.
        item1 = session.query(Item).get(1)

        # second query.  the same Connection/Transaction
        # are used.
        item2 = session.query(Item).get(2)

        # pending changes are created.
        item1.foo = 'bar'
        item2.bar = 'foo'

        # commit.  The pending changes above
        # are flushed via flush(), the Transaction
        # is committed, the Connection object closed
        # and discarded, the underlying DBAPI connection
        # returned to the connection pool.
        session.commit()
    except:
        # on rollback, the same closure of state
        # as that of commit proceeds.
        session.rollback()
        raise
    finally:
        # close the Session.  This will expunge any remaining
        # objects as well as reset any existing SessionTransaction
        # state.  Neither of these steps are usually essential.
        # However, if the commit() or rollback() itself experienced
        # an unanticipated internal failure (such as due to a mis-behaved
        # user-defined event handler), .close() will ensure that
        # invalid state is removed.
        session.close()



.. _session_begin_nested:

Using SAVEPOINT
---------------

SAVEPOINT transactions, if supported by the underlying engine, may be
delineated using the :meth:`~.Session.begin_nested`
method::

    Session = sessionmaker()
    session = Session()
    session.add(u1)
    session.add(u2)

    session.begin_nested() # establish a savepoint
    session.add(u3)
    session.rollback()  # rolls back u3, keeps u1 and u2

    session.commit() # commits u1 and u2

:meth:`~.Session.begin_nested` may be called any number
of times, which will issue a new SAVEPOINT with a unique identifier for each
call. For each :meth:`~.Session.begin_nested` call, a
corresponding :meth:`~.Session.rollback` or
:meth:`~.Session.commit` must be issued. (But note that if the return value is
used as a context manager, i.e. in a with-statement, then this rollback/commit
is issued by the context manager upon exiting the context, and so should not be
added explicitly.)

When :meth:`~.Session.begin_nested` is called, a
:meth:`~.Session.flush` is unconditionally issued
(regardless of the ``autoflush`` setting). This is so that when a
:meth:`~.Session.rollback` occurs, the full state of the
session is expired, thus causing all subsequent attribute/instance access to
reference the full state of the :class:`~sqlalchemy.orm.session.Session` right
before :meth:`~.Session.begin_nested` was called.

:meth:`~.Session.begin_nested`, in the same manner as the less often
used :meth:`~.Session.begin` method, returns a :class:`.SessionTransaction` object
which works as a context manager.
It can be succinctly used around individual record inserts in order to catch
things like unique constraint exceptions::

    for record in records:
        try:
            with session.begin_nested():
                session.merge(record)
        except:
            print("Skipped record %s" % record)
    session.commit()

.. _session_autocommit:

Autocommit Mode
---------------

.. deprecated::  1.4

    "autocommit" mode is a **legacy mode of use** and should not be considered
    for new projects.  The feature will be deprecated in SQLAlchemy 1.4 and
    removed in version 2.0; both versions provide a more refined
    "autobegin" approach that allows the :meth:`.Session.begin` method
    to be used normally.   If autocommit mode is used, it is strongly
    advised that the application at least ensure that transaction scope is made
    present via the :meth:`.Session.begin` method, rather than using the
    session in pure autocommit mode.

The examples of session lifecycle at :ref:`unitofwork_transaction` refer
to a :class:`.Session` that runs in its default mode of ``autocommit=False``.
In this mode, the :class:`.Session` begins new transactions automatically
as soon as it needs to do work upon a database connection; the transaction
then stays in progress until the :meth:`.Session.commit` or :meth:`.Session.rollback`
methods are called.

The :class:`.Session` also features an older legacy mode of use called
**autocommit mode**, where a transaction is not started implicitly, and unless
the :meth:`.Session.begin` method is invoked, the :class:`.Session` will
perform each database operation on a new connection checked out from the
connection pool, which is then released back to the pool immediately
after the operation completes.  This refers to
methods like :meth:`.Session.execute` as well as when executing a query
returned by :meth:`.Session.query`.  For a flush operation, the :class:`.Session`
starts a new transaction for the duration of the flush, and commits it when
complete.

Modern usage of "autocommit mode" tends to be for framework integrations that
wish to control specifically when the "begin" state occurs.  A session which is
configured with ``autocommit=True`` may be placed into the "begin" state using
the :meth:`.Session.begin` method. After the cycle completes upon
:meth:`.Session.commit` or :meth:`.Session.rollback`, connection and
transaction resources are :term:`released` and the :class:`.Session` goes back
into "autocommit" mode, until :meth:`.Session.begin` is called again::

    Session = sessionmaker(bind=engine, autocommit=True)
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

The :meth:`.Session.begin` method also returns a transactional token which is
compatible with the ``with`` statement::

    Session = sessionmaker(bind=engine, autocommit=True)
    session = Session()
    with session.begin():
        item1 = session.query(Item).get(1)
        item2 = session.query(Item).get(2)
        item1.foo = 'bar'
        item2.bar = 'foo'

.. _session_subtransactions:

Using Subtransactions with Autocommit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. deprecated:: 1.4 The :paramref:`.Session.begin.subtransactions`
   flag will be deprecated in SQLAlchemy 1.4 and removed in SQLAlchemy 2.0.
   For background on migrating away from the "subtransactions" pattern
   see the next section :ref:`session_subtransactions_migrating`.

A subtransaction indicates usage of the :meth:`.Session.begin` method in
conjunction with the :paramref:`.Session.begin.subtransactions` flag set to
``True``.  This produces a
non-transactional, delimiting construct that allows nesting of calls to
:meth:`~.Session.begin` and :meth:`~.Session.commit`. Its purpose is to allow
the construction of code that can function within a transaction both
independently of any external code that starts a transaction, as well as within
a block that has already demarcated a transaction.

``subtransactions=True`` is generally only useful in conjunction with
autocommit, and is equivalent to the pattern described at
:ref:`connections_nested_transactions`, where any number of functions can call
:meth:`_engine.Connection.begin` and :meth:`.Transaction.commit` as though they
are the initiator of the transaction, but in fact may be participating in an
already ongoing transaction::

    # method_a starts a transaction and calls method_b
    def method_a(session):
        session.begin(subtransactions=True)
        try:
            method_b(session)
            session.commit()  # transaction is committed here
        except:
            session.rollback() # rolls back the transaction
            raise

    # method_b also starts a transaction, but when
    # called from method_a participates in the ongoing
    # transaction.
    def method_b(session):
        session.begin(subtransactions=True)
        try:
            session.add(SomeObject('bat', 'lala'))
            session.commit()  # transaction is not committed yet
        except:
            session.rollback() # rolls back the transaction, in this case
                               # the one that was initiated in method_a().
            raise

    # create a Session and call method_a
    session = Session(autocommit=True)
    method_a(session)
    session.close()

Subtransactions are used by the :meth:`.Session.flush` process to ensure that
the flush operation takes place within a transaction, regardless of autocommit.
When autocommit is disabled, it is still useful in that it forces the
:class:`.Session` into a "pending rollback" state, as a failed flush cannot be
resumed in mid-operation, where the end user still maintains the "scope" of the
transaction overall.

.. _session_subtransactions_migrating:

Migrating from the "subtransaction" pattern
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The "subtransaction" pattern will be deprecated in SQLAlchemy 1.4 and removed
in version 2.0 as a public API.  This pattern has been shown to be confusing in
real world applications, and it is preferable for an application to ensure that
the top-most level of database operations are performed with a single
begin/commit pair.

To provide backwards compatibility for applications that make use of this
pattern, the following context manager or a similar implementation based on
a decorator may be used.  It relies on autocommit mode within SQLAlchemy
1.3 but not in SQLAlchemy 1.4::


    import contextlib

    @contextlib.contextmanager
    def transaction(session):
        assert session.autocommit, (
            "this pattern expects the session to be in autocommit mode. "
            "This assertion can be removed for SQLAlchemy 1.4."
        )
        if not session.transaction:
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

    Session = sessionmaker(engine, autocommit=True)

    # create a Session and call method_a
    session = Session()
    try:
        method_a(session)
    finally:
        session.close()

To compare towards the preferred idiomatic pattern, the begin block should
be at the outermost level.  This removes the need for individual functions
or methods to be concerned with the details of transaction demarcation::

    def method_a(session):
        method_b(session)

    def method_b(session):
        session.add(SomeObject('bat', 'lala'))

    Session = sessionmaker(engine)

    # create a Session and call method_a
    session = Session()
    try:
        # Session "begins" the transaction automatically, so the
        # .transaction attribute may be used as a context manager.
        with session.transaction:
            method_a(session)
    finally:
        session.close()

SQLAlchemy 1.4 will feature an improved API for the above transactional
patterns.

.. seealso::

    :ref:`connections_subtransactions` - similar pattern based on Core only

.. _session_twophase:

Enabling Two-Phase Commit
-------------------------

For backends which support two-phase operation (currently MySQL and
PostgreSQL), the session can be instructed to use two-phase commit semantics.
This will coordinate the committing of transactions across databases so that
the transaction is either committed or rolled back in all databases. You can
also :meth:`~.Session.prepare` the session for
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

    session = autocommit_session()
    some_objects = session.query(cls1).filter(...).all()
    some_other_objects = session.query(cls2).filter(...).all()
    session.close()  # closes connection


Setting Isolation for Individual Sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When we make a new :class:`.Session`, either using the constructor directly
or when we call upon the callable produced by a :class:`.sessionmaker`,
we can pass the ``bind`` argument directly, overriding the pre-existing bind.
We can for example create our :class:`_orm.Session` from the
"``transactional_session``" and pass the "``autocommit_engine``"::

    session = transactional_session(bind=autocommit_engine)
    # work with session
    session.close()

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
entire database interaction is rolled back::

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

        def test_something(self):
            # use the session in tests.

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

Above, we issue :meth:`.Session.commit` as well as
:meth:`.Transaction.rollback`. This is an example of where we take advantage
of the :class:`_engine.Connection` object's ability to maintain *subtransactions*, or
nested begin/commit-or-rollback pairs where only the outermost begin/commit
pair actually commits the transaction, or if the outermost block rolls back,
everything is rolled back.

.. topic:: Supporting Tests with Rollbacks

   The above recipe works well for any kind of database enabled test, except
   for a test that needs to actually invoke :meth:`.Session.rollback` within
   the scope of the test itself.   The above recipe can be expanded, such
   that the :class:`.Session` always runs all operations within the scope
   of a SAVEPOINT, which is established at the start of each transaction,
   so that tests can also rollback the "transaction" as well while still
   remaining in the scope of a larger "transaction" that's never committed,
   using two extra events::

      from sqlalchemy import event


      class SomeTest(TestCase):

          def setUp(self):
              # connect to the database
              self.connection = engine.connect()

              # begin a non-ORM transaction
              self.trans = connection.begin()

              # bind an individual Session to the connection
              self.session = Session(bind=self.connection)

              # start the session in a SAVEPOINT...
              self.session.begin_nested()

              # then each time that SAVEPOINT ends, reopen it
              @event.listens_for(self.session, "after_transaction_end")
              def restart_savepoint(session, transaction):
                  if transaction.nested and not transaction._parent.nested:

                      # ensure that state is expired the way
                      # session.commit() at the top level normally does
                      # (optional step)
                      session.expire_all()

                      session.begin_nested()

          # ... the tearDown() method stays the same

