=======================================
Transactions and Connection Management
=======================================

.. _unitofwork_transaction:

Managing Transactions
=====================

A newly constructed :class:`.Session` may be said to be in the "begin" state.
In this state, the :class:`.Session` has not established any connection or
transactional state with any of the :class:`.Engine` objects that may be associated
with it.

The :class:`.Session` then receives requests to operate upon a database connection.
Typically, this means it is called upon to execute SQL statements using a particular
:class:`.Engine`, which may be via :meth:`.Session.query`, :meth:`.Session.execute`,
or within a flush operation of pending data, which occurs when such state exists
and :meth:`.Session.commit` or :meth:`.Session.flush` is called.

As these requests are received, each new :class:`.Engine` encountered is associated
with an ongoing transactional state maintained by the :class:`.Session`.
When the first :class:`.Engine` is operated upon, the :class:`.Session` can be said
to have left the "begin" state and entered "transactional" state.   For each
:class:`.Engine` encountered, a :class:`.Connection` is associated with it,
which is acquired via the :meth:`.Engine.contextual_connect` method.  If a
:class:`.Connection` was directly associated with the :class:`.Session` (see :ref:`session_external_transaction`
for an example of this), it is
added to the transactional state directly.

For each :class:`.Connection`, the :class:`.Session` also maintains a :class:`.Transaction` object,
which is acquired by calling :meth:`.Connection.begin` on each :class:`.Connection`,
or if the :class:`.Session`
object has been established using the flag ``twophase=True``, a :class:`.TwoPhaseTransaction`
object acquired via :meth:`.Connection.begin_twophase`.  These transactions are all committed or
rolled back corresponding to the invocation of the
:meth:`.Session.commit` and :meth:`.Session.rollback` methods.   A commit operation will
also call the :meth:`.TwoPhaseTransaction.prepare` method on all transactions if applicable.

When the transactional state is completed after a rollback or commit, the :class:`.Session`
:term:`releases` all :class:`.Transaction` and :class:`.Connection` resources,
and goes back to the "begin" state, which
will again invoke new :class:`.Connection` and :class:`.Transaction` objects as new
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
used :meth:`~.Session.begin` method, returns a transactional object
which also works as a context manager.
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

The example of :class:`.Session` transaction lifecycle illustrated at
the start of :ref:`unitofwork_transaction` applies to a :class:`.Session` configured in the
default mode of ``autocommit=False``.   Constructing a :class:`.Session`
with ``autocommit=True`` produces a :class:`.Session` placed into "autocommit" mode, where each SQL statement
invoked by a :meth:`.Session.query` or :meth:`.Session.execute` occurs
using a new connection from the connection pool, discarding it after
results have been iterated.   The :meth:`.Session.flush` operation
still occurs within the scope of a single transaction, though this transaction
is closed out after the :meth:`.Session.flush` operation completes.

.. warning::

    "autocommit" mode should **not be considered for general use**.
    If used, it should always be combined with the usage of
    :meth:`.Session.begin` and :meth:`.Session.commit`, to ensure
    a transaction demarcation.

    Executing queries outside of a demarcated transaction is a legacy mode
    of usage, and can in some cases lead to concurrent connection
    checkouts.

    In the absence of a demarcated transaction, the :class:`.Session`
    cannot make appropriate decisions as to when autoflush should
    occur nor when auto-expiration should occur, so these features
    should be disabled with ``autoflush=False, expire_on_commit=False``.

Modern usage of "autocommit" is for framework integrations that need to control
specifically when the "begin" state occurs.  A session which is configured with
``autocommit=True`` may be placed into the "begin" state using the
:meth:`.Session.begin` method.
After the cycle completes upon :meth:`.Session.commit` or :meth:`.Session.rollback`,
connection and transaction resources are :term:`released` and the :class:`.Session`
goes back into "autocommit" mode, until :meth:`.Session.begin` is called again::

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
compatible with the Python 2.6 ``with`` statement::

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

A subtransaction indicates usage of the :meth:`.Session.begin` method in conjunction with
the ``subtransactions=True`` flag.  This produces a non-transactional, delimiting construct that
allows nesting of calls to :meth:`~.Session.begin` and :meth:`~.Session.commit`.
Its purpose is to allow the construction of code that can function within a transaction
both independently of any external code that starts a transaction,
as well as within a block that has already demarcated a transaction.

``subtransactions=True`` is generally only useful in conjunction with
autocommit, and is equivalent to the pattern described at :ref:`connections_nested_transactions`,
where any number of functions can call :meth:`.Connection.begin` and :meth:`.Transaction.commit`
as though they are the initiator of the transaction, but in fact may be participating
in an already ongoing transaction::

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

Subtransactions are used by the :meth:`.Session.flush` process to ensure that the
flush operation takes place within a transaction, regardless of autocommit.   When
autocommit is disabled, it is still useful in that it forces the :class:`.Session`
into a "pending rollback" state, as a failed flush cannot be resumed in mid-operation,
where the end user still maintains the "scope" of the transaction overall.

.. _session_twophase:

Enabling Two-Phase Commit
-------------------------

For backends which support two-phase operaration (currently MySQL and
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

Setting Transaction Isolation Levels
------------------------------------

:term:`Isolation` refers to the behavior of the transaction at the database
level in relation to other transactions occurring concurrently.  There
are four well-known modes of isolation, and typically the Python DBAPI
allows these to be set on a per-connection basis, either through explicit
APIs or via database-specific calls.

SQLAlchemy's dialects support settable isolation modes on a per-:class:`.Engine`
or per-:class:`.Connection` basis, using flags at both the
:func:`.create_engine` level as well as at the :meth:`.Connection.execution_options`
level.

When using the ORM :class:`.Session`, it acts as a *facade* for engines and
connections, but does not expose transaction isolation directly.  So in
order to affect transaction isolation level, we need to act upon the
:class:`.Engine` or :class:`.Connection` as appropriate.

.. seealso::

    :paramref:`.create_engine.isolation_level`

    :ref:`SQLite Transaction Isolation <sqlite_isolation_level>`

    :ref:`PostgreSQL Isolation Level <postgresql_isolation_level>`

    :ref:`MySQL Isolation Level <mysql_isolation_level>`

Setting Isolation Engine-Wide
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To set up a :class:`.Session` or :class:`.sessionmaker` with a specific
isolation level globally, use the :paramref:`.create_engine.isolation_level`
parameter::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    eng = create_engine(
        "postgresql://scott:tiger@localhost/test",
        isolation_level='REPEATABLE_READ')

    maker = sessionmaker(bind=eng)

    session = maker()


Setting Isolation for Individual Sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When we make a new :class:`.Session`, either using the constructor directly
or when we call upon the callable produced by a :class:`.sessionmaker`,
we can pass the ``bind`` argument directly, overriding the pre-existing bind.
We can combine this with the :meth:`.Engine.execution_options` method
in order to produce a copy of the original :class:`.Engine` that will
add this option::

    session = maker(
        bind=engine.execution_options(isolation_level='SERIALIZABLE'))

For the case where the :class:`.Session` or :class:`.sessionmaker` is
configured with multiple "binds", we can either re-specify the ``binds``
argument fully, or if we want to only replace specific binds, we
can use the :meth:`.Session.bind_mapper` or :meth:`.Session.bind_table`
methods::

    session = maker()
    session.bind_mapper(
        User, user_engine.execution_options(isolation_level='SERIALIZABLE'))

We can also use the individual transaction method that follows.

Setting Isolation for Individual Transactions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A key caveat regarding isolation level is that the setting cannot be
safely modified on a :class:`.Connection` where a transaction has already
started.  Databases cannot change the isolation level of a transaction
in progress, and some DBAPIs and SQLAlchemy dialects
have inconsistent behaviors in this area.  Some may implicitly emit a
ROLLBACK and some may implicitly emit a COMMIT, others may ignore the setting
until the next transaction.  Therefore SQLAlchemy emits a warning if this
option is set when a transaction is already in play.  The :class:`.Session`
object does not provide for us a :class:`.Connection` for use in a transaction
where the transaction is not already begun.  So here, we need to pass
execution options to the :class:`.Session` at the start of a transaction
by passing :paramref:`.Session.connection.execution_options`
provided by the :meth:`.Session.connection` method::

    from sqlalchemy.orm import Session

    sess = Session(bind=engine)
    sess.connection(execution_options={'isolation_level': 'SERIALIZABLE'})

    # work with session

    # commit transaction.  the connection is released
    # and reverted to its previous isolation level.
    sess.commit()

Above, we first produce a :class:`.Session` using either the constructor
or a :class:`.sessionmaker`.   Then we explicitly set up the start of
a transaction by calling upon :meth:`.Session.connection`, which provides
for execution options that will be passed to the connection before the
transaction is begun.   If we are working with a :class:`.Session` that
has multiple binds or some other custom scheme for :meth:`.Session.get_bind`,
we can pass additional arguments to :meth:`.Session.connection` in order to
affect how the bind is procured::

    sess = my_sesssionmaker()

    # set up a transaction for the bind associated with
    # the User mapper
    sess.connection(
        mapper=User,
        execution_options={'isolation_level': 'SERIALIZABLE'})

    # work with session

    # commit transaction.  the connection is released
    # and reverted to its previous isolation level.
    sess.commit()

The :paramref:`.Session.connection.execution_options` argument is only
accepted on the **first** call to :meth:`.Session.connection` for a
particular bind within a transaction.  If a transaction is already begun
on the target connection, a warning is emitted::

    >>> session = Session(eng)
    >>> session.execute("select 1")
    <sqlalchemy.engine.result.ResultProxy object at 0x1017a6c50>
    >>> session.connection(execution_options={'isolation_level': 'SERIALIZABLE'})
    sqlalchemy/orm/session.py:310: SAWarning: Connection is already established
    for the given bind; execution_options ignored

.. versionadded:: 0.9.9 Added the
    :paramref:`.Session.connection.execution_options`
    parameter to :meth:`.Session.connection`.

Tracking Transaction State with Events
--------------------------------------

See the section :ref:`session_transaction_events` for an overview
of the available event hooks for session transaction state changes.

.. _session_external_transaction:

Joining a Session into an External Transaction (such as for test suites)
========================================================================

If a :class:`.Connection` is being used which is already in a transactional
state (i.e. has a :class:`.Transaction` established), a :class:`.Session` can
be made to participate within that transaction by just binding the
:class:`.Session` to that :class:`.Connection`. The usual rationale for this
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
of the :class:`.Connection` object's ability to maintain *subtransactions*, or
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

