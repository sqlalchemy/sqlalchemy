from __future__ import annotations

import contextlib
import random
from typing import Optional
from typing import TYPE_CHECKING

from sqlalchemy import event
from sqlalchemy import exc as sa_exc
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy.orm import attributes
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import session as _session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.util import identity_key
from sqlalchemy.sql import elements
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assert_warnings
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.testing import mock
from sqlalchemy.testing.config import Variation
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.util import gc_collect
from test.orm._fixtures import FixtureTest

if TYPE_CHECKING:
    from sqlalchemy import NestedTransaction
    from sqlalchemy import Transaction


class SessionTransactionTest(fixtures.RemovesEvents, FixtureTest):
    run_inserts = None
    __backend__ = True

    def test_no_close_transaction_on_flush(self, connection):
        User, users = self.classes.User, self.tables.users

        c = connection
        self.mapper_registry.map_imperatively(User, users)
        s = Session(bind=c)
        s.begin()
        tran = s.get_transaction()
        s.add(User(name="first"))
        s.flush()
        c.exec_driver_sql("select * from users")
        u = User(name="two")
        s.add(u)
        s.flush()
        u = User(name="third")
        s.add(u)
        s.flush()
        assert s.get_transaction() is tran
        tran.close()

    def test_subtransaction_on_external_no_begin(self, connection_no_trans):
        users, User = self.tables.users, self.classes.User

        connection = connection_no_trans
        self.mapper_registry.map_imperatively(User, users)
        trans = connection.begin()
        sess = Session(bind=connection, autoflush=True)
        u = User(name="ed")
        sess.add(u)
        sess.flush()
        sess.commit()  # commit does nothing
        trans.rollback()  # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.requires.savepoints
    def test_external_nested_transaction(self, connection_no_trans):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        connection = connection_no_trans
        trans = connection.begin()
        sess = Session(bind=connection, autoflush=True)
        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        savepoint = sess.begin_nested()
        u2 = User(name="u2")
        sess.add(u2)
        sess.flush()
        savepoint.rollback()

        trans.commit()
        assert len(sess.query(User).all()) == 1

    join_transaction_mode = testing.variation(
        "join_transaction_mode",
        [
            "none",
            "conditional_savepoint",
            "create_savepoint",
            "control_fully",
            "rollback_only",
        ],
    )

    @join_transaction_mode
    @testing.variation("operation", ["commit", "close", "rollback", "nothing"])
    @testing.variation("external_state", ["none", "transaction", "savepoint"])
    def test_join_transaction_modes(
        self,
        connection_no_trans,
        join_transaction_mode,
        operation,
        external_state: testing.Variation,
    ):
        """test new join_transaction modes added in #9015"""

        connection = connection_no_trans

        t1: Optional[Transaction]
        s1: Optional[NestedTransaction]

        if external_state.none:
            t1 = s1 = None
        elif external_state.transaction:
            t1 = connection.begin()
            s1 = None
        elif external_state.savepoint:
            t1 = connection.begin()
            s1 = connection.begin_nested()
        else:
            external_state.fail()

        if join_transaction_mode.none:
            sess = Session(connection)
        else:
            sess = Session(
                connection, join_transaction_mode=join_transaction_mode.name
            )

        sess.connection()

        if operation.close:
            sess.close()
        elif operation.commit:
            sess.commit()
        elif operation.rollback:
            sess.rollback()
        elif operation.nothing:
            pass
        else:
            operation.fail()

        if external_state.none:
            if operation.nothing:
                assert connection.in_transaction()
            else:
                assert not connection.in_transaction()

        elif external_state.transaction:
            assert t1 is not None

            if (
                join_transaction_mode.none
                or join_transaction_mode.conditional_savepoint
                or join_transaction_mode.rollback_only
            ):
                if operation.rollback:
                    assert t1._deactivated_from_connection
                    assert not t1.is_active
                else:
                    assert not t1._deactivated_from_connection
                    assert t1.is_active
            elif join_transaction_mode.create_savepoint:
                assert not t1._deactivated_from_connection
                assert t1.is_active
            elif join_transaction_mode.control_fully:
                if operation.nothing:
                    assert not t1._deactivated_from_connection
                    assert t1.is_active
                else:
                    assert t1._deactivated_from_connection
                    assert not t1.is_active
            else:
                join_transaction_mode.fail()

            if t1.is_active:
                t1.rollback()
        elif external_state.savepoint:
            assert s1 is not None
            assert t1 is not None

            assert not t1._deactivated_from_connection
            assert t1.is_active

            if join_transaction_mode.rollback_only:
                if operation.rollback:
                    assert s1._deactivated_from_connection
                    assert not s1.is_active
                else:
                    assert not s1._deactivated_from_connection
                    assert s1.is_active
            elif join_transaction_mode.control_fully:
                if operation.nothing:
                    assert not s1._deactivated_from_connection
                    assert s1.is_active
                else:
                    assert s1._deactivated_from_connection
                    assert not s1.is_active
            else:
                if operation.nothing:
                    # session is still open in the sub-savepoint,
                    # so we are not activated on connection
                    assert s1._deactivated_from_connection

                    # but we are still an active savepoint
                    assert s1.is_active

                    # close session, then we're good
                    sess.close()

                assert not s1._deactivated_from_connection
                assert s1.is_active

            if s1.is_active:
                s1.rollback()
            if t1.is_active:
                t1.rollback()
        else:
            external_state.fail()

    @join_transaction_mode
    @testing.variation("operation", ["commit", "close", "rollback"])
    def test_join_transaction_mode_with_event(
        self, join_transaction_mode, operation
    ):
        eng = engines.testing_engine()
        eng_conn = None
        events = []

        @event.listens_for(eng, "commit")
        def on_commit(conn):
            events.append("commit")

        @event.listens_for(eng, "rollback")
        def on_rollback(conn):
            events.append("rollback")

        @event.listens_for(eng.pool, "checkin")
        def on_checkin(conn, record):
            events.append("checkin")

        @event.listens_for(eng, "engine_connect")
        def make_stat(conn):
            nonlocal eng_conn
            eng_conn = conn
            conn.begin()

        if join_transaction_mode.none:
            s = Session(eng)
        else:
            s = Session(eng, join_transaction_mode=join_transaction_mode.name)

        s.connection()

        expected = []
        if operation.commit:
            s.commit()
            expected.append("commit")
        elif operation.rollback:
            s.rollback()
            expected.append("rollback")
        elif operation.close:
            s.close()
            expected.append("rollback")
        else:
            operation.fail()
        is_(eng_conn.in_transaction(), False)

        expected.append("checkin")
        eq_(events, expected)

    def test_subtransaction_on_external_commit(self, connection_no_trans):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        connection = connection_no_trans
        connection.begin()

        sess = Session(bind=connection, autoflush=True)
        u = User(name="ed")
        sess.add(u)
        sess.flush()
        sess.commit()  # commit does nothing
        connection.rollback()  # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    def test_subtransaction_on_external_rollback(self, connection_no_trans):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        connection = connection_no_trans
        connection.begin()

        sess = Session(bind=connection, autoflush=True)
        u = User(name="ed")
        sess.add(u)
        sess.flush()
        sess.rollback()  # rolls back
        connection.commit()  # nothing to commit
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.requires.savepoints
    def test_savepoint_on_external(self, connection_no_trans):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        connection = connection_no_trans
        connection.begin()
        sess = Session(bind=connection, autoflush=True)
        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        n1 = sess.begin_nested()
        u2 = User(name="u2")
        sess.add(u2)
        sess.flush()
        n1.rollback()

        connection.commit()
        assert len(sess.query(User).all()) == 1

    @testing.requires.savepoints
    def test_nested_accounting_new_items_removed(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        session = fixture_session()
        session.begin()
        n1 = session.begin_nested()
        u1 = User(name="u1")
        session.add(u1)
        n1.commit()
        assert u1 in session
        session.rollback()
        assert u1 not in session

    @testing.requires.savepoints
    def test_nested_accounting_deleted_items_restored(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        session = fixture_session()
        session.begin()
        u1 = User(name="u1")
        session.add(u1)
        session.commit()

        session.begin()
        u1 = session.query(User).first()

        n1 = session.begin_nested()
        session.delete(u1)
        n1.commit()
        assert u1 not in session
        session.rollback()
        assert u1 in session

    @testing.requires.savepoints
    def test_dirty_state_transferred_deep_nesting(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        with fixture_session() as s:
            u1 = User(name="u1")
            s.add(u1)
            s.commit()

            nt1 = s.begin_nested()
            nt2 = s.begin_nested()
            u1.name = "u2"
            assert attributes.instance_state(u1) not in nt2._dirty
            assert attributes.instance_state(u1) not in nt1._dirty
            s.flush()
            assert attributes.instance_state(u1) in nt2._dirty
            assert attributes.instance_state(u1) not in nt1._dirty

            nt2.commit()
            assert attributes.instance_state(u1) in nt2._dirty
            assert attributes.instance_state(u1) in nt1._dirty

            nt1.rollback()
            assert attributes.instance_state(u1).expired
            eq_(u1.name, "u1")

    @testing.requires.independent_connections
    def test_transactions_isolated(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        s1 = fixture_session()
        s2 = fixture_session()
        u1 = User(name="u1")
        s1.add(u1)
        s1.flush()

        assert s2.query(User).all() == []

    @testing.requires.two_phase_transactions
    def test_twophase(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        # TODO: mock up a failure condition here
        # to ensure a rollback succeeds
        self.mapper_registry.map_imperatively(User, users)
        self.mapper_registry.map_imperatively(Address, addresses)

        engine2 = engines.testing_engine()
        sess = fixture_session(autoflush=False, twophase=True)
        sess.bind_mapper(User, testing.db)
        sess.bind_mapper(Address, engine2)
        sess.begin()
        u1 = User(name="u1")
        a1 = Address(email_address="u1@e")
        sess.add_all((u1, a1))
        sess.commit()
        sess.close()
        engine2.dispose()
        with testing.db.connect() as conn:
            eq_(conn.scalar(select(func.count("*")).select_from(users)), 1)
            eq_(conn.scalar(select(func.count("*")).select_from(addresses)), 1)

    @testing.requires.independent_connections
    def test_invalidate(self):
        User, users = self.classes.User, self.tables.users
        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        c1 = sess.connection(bind_arguments={"mapper": User})
        dbapi_conn = c1.connection
        assert dbapi_conn.is_valid

        sess.invalidate()

        # Connection object is closed
        assert c1.closed

        # "invalidated" is not part of "closed" state
        assert not c1.invalidated

        # but the DBAPI conn (really ConnectionFairy)
        # is invalidated
        assert not dbapi_conn.is_valid

        eq_(sess.query(User).all(), [])
        c2 = sess.connection(bind_arguments={"mapper": User})
        assert not c2.invalidated
        assert c2.connection.is_valid

    @testing.requires.savepoints
    def test_nested_transaction(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        sess.begin()

        u = User(name="u1")
        sess.add(u)
        sess.flush()

        n1 = sess.begin_nested()  # nested transaction

        u2 = User(name="u2")
        sess.add(u2)
        sess.flush()

        n1.rollback()

        sess.commit()
        assert len(sess.query(User).all()) == 1
        sess.close()

    @testing.requires.savepoints
    def test_nested_autotrans(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        u = User(name="u1")
        sess.add(u)
        sess.flush()

        sess.begin_nested()  # nested transaction

        u2 = User(name="u2")
        sess.add(u2)
        sess.flush()

        sess.rollback()  # rolls back the whole trans

        sess.commit()
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.requires.savepoints
    def test_nested_transaction_connection_add(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()

        sess.begin()
        sess.begin_nested()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        sess.rollback()

        u2 = User(name="u2")
        sess.add(u2)

        sess.commit()

        eq_(set(sess.query(User).all()), {u2})
        sess.rollback()

        sess.begin()
        n1 = sess.begin_nested()

        u3 = User(name="u3")
        sess.add(u3)
        n1.commit()  # commit the nested transaction
        sess.rollback()

        eq_(set(sess.query(User).all()), {u2})

        sess.close()

    @testing.requires.savepoints
    def test_mixed_transaction_close(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()

        sess.begin_nested()

        sess.add(User(name="u1"))
        sess.flush()

        sess.close()

        sess.add(User(name="u2"))
        sess.commit()

        sess.close()

        eq_(len(sess.query(User).all()), 1)

    def test_begin_fails_connection_is_closed(self):
        eng = engines.testing_engine()

        state = []

        @event.listens_for(eng, "begin")
        def do_begin(conn):
            state.append((conn, conn.connection))
            raise Exception("failure")

        s1 = Session(eng)

        assert_raises_message(
            Exception, "failure", s1.execute, text("select 1")
        )

        conn, fairy = state[0]
        assert not fairy.is_valid
        assert conn.closed
        assert not conn.invalidated

        s1.close()

        # close does not occur because references were not saved, however
        # the underlying DBAPI connection was closed
        assert not fairy.is_valid
        assert conn.closed
        assert not conn.invalidated

    def test_begin_savepoint_fails_connection_is_not_closed(self):
        eng = engines.testing_engine()

        state = []

        @event.listens_for(eng, "savepoint")
        def do_begin(conn, name):
            state.append((conn, conn.connection))
            raise Exception("failure")

        s1 = Session(eng)

        s1.begin_nested()
        assert_raises_message(
            Exception, "failure", s1.execute, text("select 1")
        )

        conn, fairy = state[0]
        assert fairy.is_valid
        assert not conn.closed
        assert not conn.invalidated

        s1.close()

        assert conn.closed
        assert not fairy.is_valid

    @testing.requires.independent_connections
    def test_no_rollback_in_committed_state(self):
        """test #7388

        Prior to the fix, using the session.begin() context manager
        would produce the error "This session is in 'committed' state; no
        further SQL can be emitted ", when it attempted to call .rollback()
        if the connection.close() operation failed inside of session.commit().

        While the real exception was chained inside, this still proved to
        be misleading so we now skip the rollback() in this specific case
        and allow the original error to be raised.

        """

        sess = fixture_session()

        def fail(*arg, **kw):
            raise BaseException("some base exception")

        with (
            mock.patch.object(
                testing.db.dialect, "do_rollback", side_effect=fail
            ) as fail_mock,
            mock.patch.object(
                testing.db.dialect,
                "do_commit",
                side_effect=testing.db.dialect.do_commit,
            ) as succeed_mock,
        ):
            # sess.begin() -> commit().  why would do_rollback() be called?
            # because of connection pool finalize_fairy *after* the commit.
            # this will cause the conn.close() in session.commit() to fail,
            # but after the DB commit succeeded.
            with expect_raises_message(BaseException, "some base exception"):
                with sess.begin():
                    conn = sess.connection()
                    fairy_conn = conn.connection

        eq_(succeed_mock.mock_calls, [mock.call(fairy_conn)])
        eq_(fail_mock.mock_calls, [mock.call(fairy_conn)])

    def test_continue_flushing_on_commit(self):
        """test that post-flush actions get flushed also if
        we're in commit()"""
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()

        to_flush = [User(name="ed"), User(name="jack"), User(name="wendy")]

        @event.listens_for(sess, "after_flush_postexec")
        def add_another_user(session, ctx):
            if to_flush:
                session.add(to_flush.pop(0))

        x = [1]

        @event.listens_for(sess, "after_commit")  # noqa
        def add_another_user(session):  # noqa
            x[0] += 1

        sess.add(to_flush.pop())
        sess.commit()
        eq_(x, [2])
        eq_(sess.scalar(select(func.count(users.c.id))), 3)

    def test_continue_flushing_guard(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()

        @event.listens_for(sess, "after_flush_postexec")
        def add_another_user(session, ctx):
            session.add(User(name="x"))

        sess.add(User(name="x"))
        assert_raises_message(
            orm_exc.FlushError,
            "Over 100 subsequent flushes have occurred",
            sess.commit,
        )

    def test_no_sql_during_commit(self):
        sess = fixture_session()

        @event.listens_for(sess, "after_commit")
        def go(session):
            session.execute(text("select 1"))

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This session is in 'committed' state; no further "
            "SQL can be emitted within this transaction.",
            sess.commit,
        )

    def test_no_sql_during_prepare(self):
        sess = fixture_session(twophase=True)

        sess.prepare()

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This session is in 'prepared' state; no further "
            "SQL can be emitted within this transaction.",
            sess.execute,
            text("select 1"),
        )

    def test_no_sql_during_rollback(self):
        sess = fixture_session()

        sess.connection()

        @event.listens_for(sess, "after_rollback")
        def go(session):
            session.execute(text("select 1"))

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This session is in 'inactive' state, due to the SQL transaction "
            "being rolled back; no further SQL can be emitted within this "
            "transaction.",
            sess.rollback,
        )

    @testing.requires.independent_connections
    @testing.emits_warning(".*previous exception")
    def test_failed_rollback_deactivates_transaction(self):
        # test #4050
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        session = Session(bind=testing.db)

        rollback_error = testing.db.dialect.dbapi.InterfaceError(
            "Can't roll back to savepoint"
        )

        def prevent_savepoint_rollback(
            cursor, statement, parameters, context=None
        ):
            if (
                context is not None
                and context.compiled
                and isinstance(
                    context.compiled.statement,
                    elements.RollbackToSavepointClause,
                )
            ):
                raise rollback_error

        self.event_listen(
            testing.db.dialect, "do_execute", prevent_savepoint_rollback
        )

        with session.begin():
            session.add(User(id=1, name="x"))

        session.begin_nested()
        # raises IntegrityError on flush
        session.add(User(id=1, name="x"))
        assert_raises_message(
            sa_exc.InterfaceError,
            "Can't roll back to savepoint",
            session.commit,
        )

        # rollback succeeds, because the Session is deactivated
        eq_(session._transaction._state, _session.DEACTIVE)
        eq_(session.is_active, False)
        session.rollback()

        is_(session._transaction, None)

        session.connection()

        # back to normal
        eq_(session._transaction._state, _session.ACTIVE)
        eq_(session.is_active, True)

        trans = session._transaction

        # leave the outermost trans
        session.rollback()

        # trans is now closed
        eq_(trans._state, _session.CLOSED)

        # outermost transaction is new
        is_not(session._transaction, trans)

        is_(session._transaction, None)
        eq_(session.is_active, True)

    def test_no_prepare_wo_twophase(self):
        sess = fixture_session()

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "'twophase' mode not enabled, or not root "
            "transaction; can't prepare.",
            sess.prepare,
        )

    def test_closed_status_check(self):
        sess = fixture_session()
        trans = sess.begin()
        trans.rollback()
        assert_raises_message(
            sa_exc.ResourceClosedError,
            "This transaction is closed",
            trans.rollback,
        )
        assert_raises_message(
            sa_exc.ResourceClosedError,
            "This transaction is closed",
            trans.commit,
        )

    def _inactive_flushed_session_fixture(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        u1 = User(id=1, name="u1")
        sess.add(u1)
        sess.commit()

        sess.add(User(id=1, name="u2"))

        with expect_warnings("New instance"):
            assert_raises(sa_exc.IntegrityError, sess.flush)
        return sess, u1

    def test_execution_options_begin_transaction(self):
        bind = mock.Mock(
            connect=mock.Mock(
                return_value=mock.Mock(
                    _is_future=False,
                    execution_options=mock.Mock(
                        return_value=mock.Mock(
                            _is_future=False,
                            in_transaction=mock.Mock(return_value=False),
                        )
                    ),
                )
            )
        )

        sess = Session(bind=bind)
        c1 = sess.connection(execution_options={"isolation_level": "FOO"})
        eq_(bind.mock_calls, [mock.call.connect()])
        eq_(
            bind.connect().mock_calls,
            [mock.call.execution_options(isolation_level="FOO")],
        )

        eq_(c1, bind.connect().execution_options())

    def test_execution_options_ignored_mid_transaction(self):
        bind = mock.Mock()
        conn = mock.Mock(
            engine=bind, in_transaction=mock.Mock(return_value=False)
        )
        bind.connect = mock.Mock(return_value=conn)
        sess = Session(bind=bind)
        sess.execute(text("select 1"))
        with expect_warnings(
            "Connection is already established for the "
            "given bind; execution_options ignored"
        ):
            sess.connection(execution_options={"isolation_level": "FOO"})

    def test_warning_on_using_inactive_session_new(self):
        User = self.classes.User

        sess, u1 = self._inactive_flushed_session_fixture()
        u2 = User(name="u2")
        sess.add(u2)

        def go():
            sess.rollback()

        assert_warnings(
            go,
            [
                "Session's state has been changed on a "
                "non-active transaction - this state "
                "will be discarded."
            ],
        )
        assert u2 not in sess
        assert u1 in sess

    def test_warning_on_using_inactive_session_dirty(self):
        sess, u1 = self._inactive_flushed_session_fixture()
        u1.name = "newname"

        def go():
            sess.rollback()

        assert_warnings(
            go,
            [
                "Session's state has been changed on a "
                "non-active transaction - this state "
                "will be discarded."
            ],
        )
        assert u1 in sess
        assert u1 not in sess.dirty

    def test_warning_on_using_inactive_session_delete(self):
        sess, u1 = self._inactive_flushed_session_fixture()
        sess.delete(u1)

        def go():
            sess.rollback()

        assert_warnings(
            go,
            [
                "Session's state has been changed on a "
                "non-active transaction - this state "
                "will be discarded."
            ],
        )
        assert u1 in sess
        assert u1 not in sess.deleted

    def test_warning_on_using_inactive_session_rollback_evt(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session()
        u1 = User(id=1, name="u1")
        sess.add(u1)
        sess.commit()

        u3 = User(name="u3")

        @event.listens_for(sess, "after_rollback")
        def evt(s):
            sess.add(u3)

        sess.add(User(id=1, name="u2"))

        def go():
            assert_raises(orm_exc.FlushError, sess.flush)

        assert u3 not in sess

    def test_preserve_flush_error(self):
        User = self.classes.User

        sess, u1 = self._inactive_flushed_session_fixture()

        for i in range(5):
            assert_raises_message(
                sa_exc.PendingRollbackError,
                "^This Session's transaction has been "
                r"rolled back due to a previous exception "
                "during flush. To "
                "begin a new transaction with this "
                "Session, first issue "
                r"Session.rollback\(\). Original exception "
                "was:",
                sess.commit,
            )
        sess.rollback()
        sess.add(User(id=5, name="some name"))
        sess.commit()

    def test_no_autobegin_after_explicit_commit(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        session = fixture_session()
        session.add(User(name="ed"))
        session.get_transaction().commit()

        is_(session.get_transaction(), None)

        session.connection()
        is_not(session.get_transaction(), None)


class _LocalFixture(FixtureTest):
    run_setup_mappers = "once"
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        User, Address = cls.classes.User, cls.classes.Address
        users, addresses = cls.tables.users, cls.tables.addresses
        cls.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    backref="user",
                    cascade="all, delete-orphan",
                    order_by=addresses.c.id,
                )
            },
        )
        cls.mapper_registry.map_imperatively(Address, addresses)


def subtransaction_recipe_one(self):
    @contextlib.contextmanager
    def transaction(session):
        if session.in_transaction():
            outermost = False
        else:
            outermost = True
            session.begin()

        try:
            yield
        except:
            if session.in_transaction():
                session.rollback()
            raise
        else:
            if outermost and session.in_transaction():
                session.commit()

    return transaction


def subtransaction_recipe_two(self):
    # shorter recipe
    @contextlib.contextmanager
    def transaction(session):
        if not session.in_transaction():
            with session.begin():
                yield
        else:
            yield

    return transaction


def subtransaction_recipe_three(self):
    @contextlib.contextmanager
    def transaction(session):
        if not session.in_transaction():
            session.begin()
            try:
                yield
            except:
                if session.in_transaction():
                    session.rollback()
            else:
                session.commit()
        else:
            try:
                yield
            except:
                if session.in_transaction():
                    session.rollback()
                raise

    return transaction


@testing.combinations(
    (subtransaction_recipe_one, True),
    (subtransaction_recipe_two, False),
    (subtransaction_recipe_three, True),
    argnames="target_recipe,recipe_rollsback_early",
    id_="ns",
)
class SubtransactionRecipeTest(FixtureTest):
    run_inserts = None
    __backend__ = True

    @testing.fixture
    def subtransaction_recipe(self):
        return self.target_recipe()

    @testing.requires.savepoints
    def test_recipe_heavy_nesting(self, subtransaction_recipe):
        users = self.tables.users

        with fixture_session() as session:
            with subtransaction_recipe(session):
                session.connection().execute(
                    users.insert().values(name="user1")
                )
                with subtransaction_recipe(session):
                    savepoint = session.begin_nested()
                    session.connection().execute(
                        users.insert().values(name="user2")
                    )
                    assert (
                        session.connection()
                        .exec_driver_sql("select count(1) from users")
                        .scalar()
                        == 2
                    )
                    savepoint.rollback()

                with subtransaction_recipe(session):
                    assert (
                        session.connection()
                        .exec_driver_sql("select count(1) from users")
                        .scalar()
                        == 1
                    )
                    session.connection().execute(
                        users.insert().values(name="user3")
                    )
                assert (
                    session.connection()
                    .exec_driver_sql("select count(1) from users")
                    .scalar()
                    == 2
                )

    @engines.close_open_connections
    def test_recipe_subtransaction_on_external_subtrans(
        self, subtransaction_recipe
    ):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        with testing.db.connect() as conn:
            trans = conn.begin()
            sess = Session(conn)

            with subtransaction_recipe(sess):
                u = User(name="ed")
                sess.add(u)
                sess.flush()
                # commit does nothing
            trans.rollback()  # rolls back
            assert len(sess.query(User).all()) == 0
            sess.close()

    def test_recipe_commit_one(self, subtransaction_recipe):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        with fixture_session() as sess:
            with subtransaction_recipe(sess):
                u = User(name="u1")
                sess.add(u)
            sess.close()
            assert len(sess.query(User).all()) == 1

    def test_recipe_subtransaction_on_noautocommit(
        self, subtransaction_recipe
    ):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        with fixture_session() as sess:
            sess.begin()
            with subtransaction_recipe(sess):
                u = User(name="u1")
                sess.add(u)
                sess.flush()
            sess.rollback()  # rolls back
            assert len(sess.query(User).all()) == 0
            sess.close()

    @testing.requires.savepoints
    def test_recipe_mixed_transaction_control(self, subtransaction_recipe):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        with fixture_session() as sess:
            sess.begin()
            sess.begin_nested()

            with subtransaction_recipe(sess):
                sess.add(User(name="u1"))

            sess.commit()
            sess.commit()

            eq_(len(sess.query(User).all()), 1)
            sess.close()

            t1 = sess.begin()
            t2 = sess.begin_nested()

            sess.add(User(name="u2"))

            t2.commit()
            assert sess.get_transaction() is t1

    def test_recipe_error_on_using_inactive_session_commands(
        self, subtransaction_recipe
    ):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        with fixture_session() as sess:
            sess.begin()

            try:
                with subtransaction_recipe(sess):
                    sess.add(User(name="u1"))
                    sess.flush()
                    raise Exception("force rollback")
            except:
                pass

            if self.recipe_rollsback_early:
                # that was a real rollback, so no transaction
                assert not sess.in_transaction()
                is_(sess.get_transaction(), None)
            else:
                assert sess.in_transaction()

            sess.close()
            assert not sess.in_transaction()

    def test_recipe_multi_nesting(self, subtransaction_recipe):
        with fixture_session() as sess:
            with subtransaction_recipe(sess):
                assert sess.in_transaction()

                try:
                    with subtransaction_recipe(sess):
                        assert sess.get_transaction()
                        raise Exception("force rollback")
                except:
                    pass

                if self.recipe_rollsback_early:
                    assert not sess.in_transaction()
                else:
                    assert sess.in_transaction()

            assert not sess.in_transaction()

    def test_recipe_deactive_status_check(self, subtransaction_recipe):
        with fixture_session() as sess:
            sess.begin()

            with subtransaction_recipe(sess):
                sess.rollback()

            assert not sess.in_transaction()
            sess.commit()  # no error


class FixtureDataTest(_LocalFixture):
    run_inserts = "each"
    __backend__ = True

    def test_attrs_on_rollback(self):
        User = self.classes.User
        sess = fixture_session()
        u1 = sess.get(User, 7)
        u1.name = "ed"
        sess.rollback()
        eq_(u1.name, "jack")

    def test_commit_persistent(self):
        User = self.classes.User
        sess = fixture_session()
        u1 = sess.get(User, 7)
        u1.name = "ed"
        sess.flush()
        sess.commit()
        eq_(u1.name, "ed")

    def test_concurrent_commit_persistent(self):
        User = self.classes.User
        s1 = fixture_session()
        u1 = s1.get(User, 7)
        u1.name = "ed"
        s1.commit()

        s2 = fixture_session()
        u2 = s2.get(User, 7)
        assert u2.name == "ed"
        u2.name = "will"
        s2.commit()

        assert u1.name == "will"


class CleanSavepointTest(FixtureTest):
    """test the behavior for [ticket:2452] - rollback on begin_nested()
    only expires objects tracked as being modified in that transaction.

    """

    run_inserts = None
    __backend__ = True

    def _run_test(self, update_fn):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        with fixture_session() as s:
            u1 = User(name="u1")
            u2 = User(name="u2")
            s.add_all([u1, u2])
            s.commit()
            u1.name
            u2.name
            trans = s._transaction
            assert trans is not None
            s.begin_nested()
            update_fn(s, u2)
            eq_(u2.name, "u2modified")
            s.rollback()

            assert s._transaction is None
            assert "name" not in u1.__dict__
            assert "name" not in u2.__dict__
            eq_(u2.name, "u2")

    @testing.requires.savepoints
    def test_rollback_ignores_clean_on_savepoint(self):
        def update_fn(s, u2):
            u2.name = "u2modified"

        self._run_test(update_fn)

    @testing.requires.savepoints
    def test_rollback_ignores_clean_on_savepoint_agg_upd_eval(self):
        User = self.classes.User

        def update_fn(s, u2):
            s.query(User).filter_by(name="u2").update(
                dict(name="u2modified"), synchronize_session="evaluate"
            )

        self._run_test(update_fn)

    @testing.requires.savepoints
    def test_rollback_ignores_clean_on_savepoint_agg_upd_fetch(self):
        User = self.classes.User

        def update_fn(s, u2):
            s.query(User).filter_by(name="u2").update(
                dict(name="u2modified"), synchronize_session="fetch"
            )

        self._run_test(update_fn)


class AutoExpireTest(_LocalFixture):
    __backend__ = True

    def test_expunge_pending_on_rollback(self):
        User = self.classes.User
        sess = fixture_session()
        u2 = User(name="newuser")
        sess.add(u2)
        assert u2 in sess
        sess.rollback()
        assert u2 not in sess

    def test_trans_pending_cleared_on_commit(self):
        User = self.classes.User
        sess = fixture_session()
        u2 = User(name="newuser")
        sess.add(u2)
        assert u2 in sess
        sess.commit()
        assert u2 in sess
        u3 = User(name="anotheruser")
        sess.add(u3)
        sess.rollback()
        assert u3 not in sess
        assert u2 in sess

    def test_update_deleted_on_rollback(self):
        User = self.classes.User
        s = fixture_session()
        u1 = User(name="ed")
        s.add(u1)
        s.commit()

        # this actually tests that the delete() operation,
        # when cascaded to the "addresses" collection, does not
        # trigger a flush (via lazyload) before the cascade is complete.
        s.delete(u1)
        assert u1 in s.deleted
        s.rollback()
        assert u1 in s
        assert u1 not in s.deleted

    def test_trans_deleted_cleared_on_rollback(self):
        User = self.classes.User
        s = fixture_session()
        u1 = User(name="ed")
        s.add(u1)
        s.commit()

        s.delete(u1)
        s.commit()
        assert u1 not in s
        s.rollback()
        assert u1 not in s

    def test_update_deleted_on_rollback_cascade(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        u1 = User(name="ed", addresses=[Address(email_address="foo")])
        s.add(u1)
        s.commit()

        s.delete(u1)
        assert u1 in s.deleted
        assert u1.addresses[0] in s.deleted
        s.rollback()
        assert u1 in s
        assert u1 not in s.deleted
        assert u1.addresses[0] not in s.deleted

    def test_update_deleted_on_rollback_orphan(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        u1 = User(name="ed", addresses=[Address(email_address="foo")])
        s.add(u1)
        s.commit()

        a1 = u1.addresses[0]
        u1.addresses.remove(a1)

        s.flush()
        eq_(s.query(Address).filter(Address.email_address == "foo").all(), [])
        s.rollback()
        assert a1 not in s.deleted
        assert u1.addresses == [a1]

    def test_commit_pending(self):
        User = self.classes.User
        sess = fixture_session()
        u1 = User(name="newuser")
        sess.add(u1)
        sess.flush()
        sess.commit()
        eq_(u1.name, "newuser")

    def test_concurrent_commit_pending(self):
        User = self.classes.User
        s1 = fixture_session()
        u1 = User(name="edward")
        s1.add(u1)
        s1.commit()

        s2 = fixture_session()
        u2 = s2.query(User).filter(User.name == "edward").one()
        u2.name = "will"
        s2.commit()

        assert u1.name == "will"


class TwoPhaseTest(_LocalFixture):
    __backend__ = True

    @testing.requires.two_phase_transactions
    def test_rollback_on_prepare(self):
        User = self.classes.User
        s = fixture_session(twophase=True)

        u = User(name="ed")
        s.add(u)
        s.prepare()
        s.rollback()

        assert u not in s


class RollbackRecoverTest(_LocalFixture):
    __backend__ = True

    def test_pk_violation(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()

        a1 = Address(email_address="foo")
        u1 = User(id=1, name="ed", addresses=[a1])
        s.add(u1)
        s.commit()

        a2 = Address(email_address="bar")
        u2 = User(id=1, name="jack", addresses=[a2])

        u1.name = "edward"
        a1.email_address = "foober"
        s.add(u2)

        with expect_warnings("New instance"):
            assert_raises(sa_exc.IntegrityError, s.commit)

        assert_raises(sa_exc.InvalidRequestError, s.commit)
        s.rollback()
        assert u2 not in s
        assert a2 not in s
        assert u1 in s
        assert a1 in s
        assert u1.name == "ed"
        assert a1.email_address == "foo"
        u1.name = "edward"
        a1.email_address = "foober"
        s.commit()
        eq_(
            s.query(User).all(),
            [
                User(
                    id=1,
                    name="edward",
                    addresses=[Address(email_address="foober")],
                )
            ],
        )

    @testing.requires.savepoints
    def test_pk_violation_with_savepoint(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()
        a1 = Address(email_address="foo")
        u1 = User(id=1, name="ed", addresses=[a1])
        s.add(u1)
        s.commit()

        a2 = Address(email_address="bar")
        u2 = User(id=1, name="jack", addresses=[a2])

        u1.name = "edward"
        a1.email_address = "foober"
        nt1 = s.begin_nested()
        s.add(u2)

        with expect_warnings("New instance"):
            assert_raises(sa_exc.IntegrityError, s.commit)
        assert_raises(sa_exc.InvalidRequestError, s.commit)
        nt1.rollback()
        assert u2 not in s
        assert a2 not in s
        assert u1 in s
        assert a1 in s

        s.commit()
        eq_(
            s.query(User).all(),
            [
                User(
                    id=1,
                    name="edward",
                    addresses=[Address(email_address="foober")],
                )
            ],
        )


class SavepointTest(_LocalFixture):
    __backend__ = True

    @testing.requires.savepoints
    def test_savepoint_rollback(self):
        User = self.classes.User
        s = fixture_session()
        u1 = User(name="ed")
        u2 = User(name="jack")
        s.add_all([u1, u2])

        nt1 = s.begin_nested()
        u3 = User(name="wendy")
        u4 = User(name="foo")
        u1.name = "edward"
        u2.name = "jackward"
        s.add_all([u3, u4])
        eq_(
            s.query(User.name).order_by(User.id).all(),
            [("edward",), ("jackward",), ("wendy",), ("foo",)],
        )
        nt1.rollback()
        assert u1.name == "ed"
        assert u2.name == "jack"
        eq_(s.query(User.name).order_by(User.id).all(), [("ed",), ("jack",)])
        s.commit()
        assert u1.name == "ed"
        assert u2.name == "jack"
        eq_(s.query(User.name).order_by(User.id).all(), [("ed",), ("jack",)])

    @testing.requires.savepoints
    def test_savepoint_delete(self):
        User = self.classes.User
        s = fixture_session()
        u1 = User(name="ed")
        s.add(u1)
        s.commit()
        eq_(s.query(User).filter_by(name="ed").count(), 1)
        s.begin_nested()
        s.delete(u1)
        s.commit()
        eq_(s.query(User).filter_by(name="ed").count(), 0)
        s.commit()

    @testing.requires.savepoints
    def test_savepoint_commit(self):
        User = self.classes.User
        s = fixture_session()
        u1 = User(name="ed")
        u2 = User(name="jack")
        s.add_all([u1, u2])

        nt1 = s.begin_nested()
        u3 = User(name="wendy")
        u4 = User(name="foo")
        u1.name = "edward"
        u2.name = "jackward"
        s.add_all([u3, u4])
        eq_(
            s.query(User.name).order_by(User.id).all(),
            [("edward",), ("jackward",), ("wendy",), ("foo",)],
        )
        nt1.commit()

        def go():
            assert u1.name == "edward"
            assert u2.name == "jackward"
            eq_(
                s.query(User.name).order_by(User.id).all(),
                [("edward",), ("jackward",), ("wendy",), ("foo",)],
            )

        self.assert_sql_count(testing.db, go, 1)

        s.commit()
        eq_(
            s.query(User.name).order_by(User.id).all(),
            [("edward",), ("jackward",), ("wendy",), ("foo",)],
        )

    @testing.requires.savepoints
    def test_savepoint_rollback_collections(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()
        u1 = User(name="ed", addresses=[Address(email_address="foo")])
        s.add(u1)
        s.commit()

        u1.name = "edward"
        u1.addresses.append(Address(email_address="bar"))
        nt1 = s.begin_nested()
        u2 = User(name="jack", addresses=[Address(email_address="bat")])
        s.add(u2)
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name="edward",
                    addresses=[
                        Address(email_address="foo"),
                        Address(email_address="bar"),
                    ],
                ),
                User(name="jack", addresses=[Address(email_address="bat")]),
            ],
        )
        nt1.rollback()
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name="edward",
                    addresses=[
                        Address(email_address="foo"),
                        Address(email_address="bar"),
                    ],
                )
            ],
        )
        s.commit()
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name="edward",
                    addresses=[
                        Address(email_address="foo"),
                        Address(email_address="bar"),
                    ],
                )
            ],
        )

    @testing.requires.savepoints
    def test_savepoint_commit_collections(self):
        User, Address = self.classes.User, self.classes.Address
        s = fixture_session()
        u1 = User(name="ed", addresses=[Address(email_address="foo")])
        s.add(u1)
        s.commit()

        u1.name = "edward"
        u1.addresses.append(Address(email_address="bar"))
        s.begin_nested()
        u2 = User(name="jack", addresses=[Address(email_address="bat")])
        s.add(u2)
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name="edward",
                    addresses=[
                        Address(email_address="foo"),
                        Address(email_address="bar"),
                    ],
                ),
                User(name="jack", addresses=[Address(email_address="bat")]),
            ],
        )
        s.commit()
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name="edward",
                    addresses=[
                        Address(email_address="foo"),
                        Address(email_address="bar"),
                    ],
                ),
                User(name="jack", addresses=[Address(email_address="bat")]),
            ],
        )
        s.commit()
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name="edward",
                    addresses=[
                        Address(email_address="foo"),
                        Address(email_address="bar"),
                    ],
                ),
                User(name="jack", addresses=[Address(email_address="bat")]),
            ],
        )

    @testing.requires.savepoints
    def test_expunge_pending_on_rollback(self):
        User = self.classes.User
        sess = fixture_session()

        sess.begin_nested()
        u2 = User(name="newuser")
        sess.add(u2)
        assert u2 in sess
        sess.rollback()
        assert u2 not in sess

    @testing.requires.savepoints
    def test_update_deleted_on_rollback(self):
        User = self.classes.User
        s = fixture_session()
        u1 = User(name="ed")
        s.add(u1)
        s.commit()

        s.begin_nested()
        s.delete(u1)
        assert u1 in s.deleted
        s.rollback()
        assert u1 in s
        assert u1 not in s.deleted

    @testing.requires.savepoints_w_release
    def test_savepoint_lost_still_runs(self):
        User = self.classes.User
        s = fixture_session()
        trans = s.begin_nested()
        s.connection()
        u1 = User(name="ed")
        s.add(u1)

        # kill off the transaction
        nested_trans = trans._connections[self.bind][1]
        nested_trans._do_commit()

        is_(s.get_nested_transaction(), trans)

        with expect_warnings("nested transaction already deassociated"):
            # this previously would raise
            # "savepoint "sa_savepoint_1" does not exist", however as of
            # #5327 the savepoint already knows it's inactive
            s.rollback()

        assert u1 not in s.new

        is_(trans._state, _session.CLOSED)
        is_not(s.get_transaction(), trans)

        s.connection()
        is_(s.get_transaction()._state, _session.ACTIVE)

        is_(s.get_transaction().nested, False)

        is_(s.get_transaction()._parent, None)


class AccountingFlagsTest(_LocalFixture):
    __backend__ = True

    def test_no_expire_on_commit(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session(expire_on_commit=False)
        u1 = User(name="ed")
        sess.add(u1)
        sess.commit()

        sess.execute(
            users.update().where(users.c.name == "ed").values(name="edward")
        )

        assert u1.name == "ed"
        sess.expire_all()
        assert u1.name == "edward"


class ContextManagerPlusFutureTest(FixtureTest):
    run_inserts = None
    __backend__ = True

    @testing.requires.savepoints
    @engines.close_open_connections
    def test_contextmanager_nested_rollback(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()

        def go():
            with sess.begin_nested():
                sess.add(User())  # name can't be null
                sess.flush()

        # and not InvalidRequestError
        assert_raises(sa_exc.DBAPIError, go)

        with sess.begin_nested():
            sess.add(User(name="u1"))

        eq_(sess.query(User).count(), 1)

    def test_contextmanager_commit(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()
        with sess.begin():
            sess.add(User(name="u1"))

        sess.rollback()
        eq_(sess.query(User).count(), 1)

    def test_contextmanager_rollback(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()

        def go():
            with sess.begin():
                sess.add(User())  # name can't be null

        assert_raises(sa_exc.DBAPIError, go)

        eq_(sess.query(User).count(), 0)
        sess.close()

        with sess.begin():
            sess.add(User(name="u1"))
        eq_(sess.query(User).count(), 1)

    def test_explicit_begin(self):
        with fixture_session() as s1:
            with s1.begin() as trans:
                is_(trans, s1.get_transaction())
                s1.connection()

            is_(s1._transaction, None)

    def test_no_double_begin_explicit(self):
        with fixture_session() as s1:
            s1.begin()
            assert_raises_message(
                sa_exc.InvalidRequestError,
                "A transaction is already begun on this Session.",
                s1.begin,
            )

    @testing.requires.savepoints
    def test_rollback_is_global(self):
        users = self.tables.users

        with fixture_session() as s1:
            s1.begin()

            s1.connection().execute(users.insert(), [{"id": 1, "name": "n1"}])

            s1.begin_nested()

            s1.connection().execute(
                users.insert(),
                [{"id": 2, "name": "n2"}, {"id": 3, "name": "n3"}],
            )

            eq_(
                s1.connection().scalar(
                    select(func.count()).select_from(users)
                ),
                3,
            )

            # rolls back the whole transaction
            s1.rollback()
            is_(s1.get_transaction(), None)

            eq_(
                s1.connection().scalar(
                    select(func.count()).select_from(users)
                ),
                0,
            )

            s1.commit()
            is_(s1.get_transaction(), None)

    def test_session_as_ctx_manager_one(self):
        users = self.tables.users

        with fixture_session() as sess:
            is_(sess.get_transaction(), None)

            sess.connection().execute(
                users.insert().values(id=1, name="user1")
            )

            eq_(
                sess.connection().execute(users.select()).all(), [(1, "user1")]
            )

            is_not(sess.get_transaction(), None)

        is_(sess.get_transaction(), None)

        # did not commit
        eq_(sess.connection().execute(users.select()).all(), [])

    def test_session_as_ctx_manager_two(self):
        users = self.tables.users

        try:
            with fixture_session() as sess:
                is_(sess.get_transaction(), None)

                sess.connection().execute(
                    users.insert().values(id=1, name="user1")
                )

                raise Exception("force rollback")
        except:
            pass
        is_(sess.get_transaction(), None)

    def test_begin_context_manager(self):
        users = self.tables.users

        with fixture_session() as sess:
            with sess.begin():
                sess.connection().execute(
                    users.insert().values(id=1, name="user1")
                )

                eq_(
                    sess.connection().execute(users.select()).all(),
                    [(1, "user1")],
                )

        # committed
        eq_(sess.connection().execute(users.select()).all(), [(1, "user1")])

    def test_sessionmaker_begin_context_manager(self):
        users = self.tables.users

        session = sessionmaker(testing.db)

        with session.begin() as sess:
            sess.connection().execute(
                users.insert().values(id=1, name="user1")
            )

            eq_(
                sess.connection().execute(users.select()).all(),
                [(1, "user1")],
            )

        # committed
        eq_(sess.connection().execute(users.select()).all(), [(1, "user1")])
        sess.close()

    def test_begin_context_manager_rollback_trans(self):
        users = self.tables.users

        try:
            with fixture_session() as sess:
                with sess.begin():
                    sess.connection().execute(
                        users.insert().values(id=1, name="user1")
                    )

                    eq_(
                        sess.connection().execute(users.select()).all(),
                        [(1, "user1")],
                    )

                    raise Exception("force rollback")
        except:
            pass

        # rolled back
        eq_(sess.connection().execute(users.select()).all(), [])
        sess.close()

    def test_begin_context_manager_rollback_outer(self):
        users = self.tables.users

        try:
            with fixture_session() as sess:
                with sess.begin():
                    sess.connection().execute(
                        users.insert().values(id=1, name="user1")
                    )

                    eq_(
                        sess.connection().execute(users.select()).all(),
                        [(1, "user1")],
                    )

                raise Exception("force rollback")
        except:
            pass

        # committed
        eq_(sess.connection().execute(users.select()).all(), [(1, "user1")])
        sess.close()

    def test_sessionmaker_begin_context_manager_rollback_trans(self):
        users = self.tables.users

        session = sessionmaker(testing.db)

        try:
            with session.begin() as sess:
                sess.connection().execute(
                    users.insert().values(id=1, name="user1")
                )

                eq_(
                    sess.connection().execute(users.select()).all(),
                    [(1, "user1")],
                )

                raise Exception("force rollback")
        except:
            pass

        # rolled back
        eq_(sess.connection().execute(users.select()).all(), [])
        sess.close()

    def test_sessionmaker_begin_context_manager_rollback_outer(self):
        users = self.tables.users

        session = sessionmaker(testing.db)

        try:
            with session.begin() as sess:
                sess.connection().execute(
                    users.insert().values(id=1, name="user1")
                )

                eq_(
                    sess.connection().execute(users.select()).all(),
                    [(1, "user1")],
                )

            raise Exception("force rollback")
        except:
            pass

        # committed
        eq_(sess.connection().execute(users.select()).all(), [(1, "user1")])
        sess.close()

    def test_interrupt_ctxmanager(self, trans_ctx_manager_fixture):
        fn = trans_ctx_manager_fixture

        session = fixture_session()

        fn(session, trans_on_subject=True, execute_on_subject=True)

    @testing.combinations((True,), (False,), argnames="rollback")
    @testing.combinations((True,), (False,), argnames="expire_on_commit")
    @testing.combinations(
        ("add",),
        ("modify",),
        ("delete",),
        ("begin",),
        argnames="check_operation",
    )
    def test_interrupt_ctxmanager_ops(
        self, rollback, expire_on_commit, check_operation
    ):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        session = fixture_session(expire_on_commit=expire_on_commit)

        with session.begin():
            u1 = User(id=7, name="u1")
            session.add(u1)

        with session.begin():
            u1.name  # unexpire
            u2 = User(id=8, name="u1")
            session.add(u2)

            session.flush()

            if rollback:
                session.rollback()
            else:
                session.commit()

            with expect_raises_message(
                sa_exc.InvalidRequestError,
                "Can't operate on closed transaction "
                "inside context manager.  Please complete the context "
                "manager before emitting further commands.",
            ):
                if check_operation == "add":
                    u3 = User(id=9, name="u2")
                    session.add(u3)
                elif check_operation == "begin":
                    session.begin()
                elif check_operation == "modify":
                    u1.name = "newname"
                elif check_operation == "delete":
                    session.delete(u1)


class TransactionFlagsTest(fixtures.TestBase):
    def test_in_transaction(self):
        with fixture_session() as s1:
            eq_(s1.in_transaction(), False)

            trans = s1.begin()

            eq_(s1.in_transaction(), True)
            is_(s1.get_transaction(), trans)

            n1 = s1.begin_nested()

            eq_(s1.in_transaction(), True)
            is_(s1.get_transaction(), trans)
            is_(s1.get_nested_transaction(), n1)

            n1.rollback()

            is_(s1.get_nested_transaction(), None)
            is_(s1.get_transaction(), trans)

            eq_(s1.in_transaction(), True)

            s1.commit()

            eq_(s1.in_transaction(), False)
            is_(s1.get_transaction(), None)

    def test_in_transaction_subtransactions(self):
        """we'd like to do away with subtransactions for future sessions
        entirely.  at the moment we are still using them internally.
        it might be difficult to keep the internals working in exactly
        the same way if remove this concept, so for now just test that
        the external API works.

        """
        with fixture_session() as s1:
            eq_(s1.in_transaction(), False)

            trans = s1.begin()

            eq_(s1.in_transaction(), True)
            is_(s1.get_transaction(), trans)

            subtrans = s1._autobegin_t()._begin()
            is_(s1.get_transaction(), trans)
            eq_(s1.in_transaction(), True)

            is_(s1._transaction, subtrans)

            s1.rollback()

            eq_(s1.in_transaction(), False)
            is_(s1._transaction, None)

            s1.rollback()

            eq_(s1.in_transaction(), False)
            is_(s1._transaction, None)

    def test_in_transaction_nesting(self):
        with fixture_session() as s1:
            eq_(s1.in_transaction(), False)

            trans = s1.begin()

            eq_(s1.in_transaction(), True)
            is_(s1.get_transaction(), trans)

            sp1 = s1.begin_nested()

            eq_(s1.in_transaction(), True)
            is_(s1.get_transaction(), trans)
            is_(s1.get_nested_transaction(), sp1)

            sp2 = s1.begin_nested()

            eq_(s1.in_transaction(), True)
            eq_(s1.in_nested_transaction(), True)
            is_(s1.get_transaction(), trans)
            is_(s1.get_nested_transaction(), sp2)

            sp2.rollback()

            eq_(s1.in_transaction(), True)
            eq_(s1.in_nested_transaction(), True)
            is_(s1.get_transaction(), trans)
            is_(s1.get_nested_transaction(), sp1)

            sp1.rollback()

            is_(s1.get_nested_transaction(), None)
            eq_(s1.in_transaction(), True)
            eq_(s1.in_nested_transaction(), False)
            is_(s1.get_transaction(), trans)

            s1.rollback()

            eq_(s1.in_transaction(), False)
            is_(s1.get_transaction(), None)


class NaturalPKRollbackTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table("users", metadata, Column("name", String(50), primary_key=True))

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

    def test_rollback_recover(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        session = fixture_session()

        u1, u2, u3 = User(name="u1"), User(name="u2"), User(name="u3")

        session.add_all([u1, u2, u3])

        session.commit()

        session.delete(u2)
        u4 = User(name="u2")
        session.add(u4)
        session.flush()

        u5 = User(name="u3")
        session.add(u5)
        with expect_warnings("New instance"):
            assert_raises(sa_exc.IntegrityError, session.flush)

        assert u5 not in session
        assert u2 not in session.deleted

        session.rollback()

    def test_reloaded_deleted_checked_for_expiry(self):
        """test issue #3677"""
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        u1 = User(name="u1")

        s = fixture_session()
        s.add(u1)
        s.flush()
        del u1
        gc_collect()

        u1 = s.query(User).first()  # noqa

        s.rollback()

        u2 = User(name="u1")
        s.add(u2)
        s.commit()

        assert inspect(u2).persistent

    def test_key_replaced_by_update(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        u1 = User(name="u1")
        u2 = User(name="u2")

        s = fixture_session()
        s.add_all([u1, u2])
        s.commit()

        s.delete(u1)
        s.flush()

        u2.name = "u1"
        s.flush()

        assert u1 not in s
        s.rollback()

        assert u1 in s
        assert u2 in s

        assert s.identity_map[identity_key(User, ("u1",))] is u1
        assert s.identity_map[identity_key(User, ("u2",))] is u2

    @testing.requires.savepoints
    def test_key_replaced_by_update_nested(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        u1 = User(name="u1")

        s = fixture_session()
        s.add(u1)
        s.commit()

        with s.begin_nested():
            u2 = User(name="u2")
            s.add(u2)
            s.flush()

            u2.name = "u3"

        s.rollback()

        assert u1 in s
        assert u2 not in s

        u1.name = "u5"

        s.commit()

    def test_multiple_key_replaced_by_update(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        u1 = User(name="u1")
        u2 = User(name="u2")
        u3 = User(name="u3")

        s = fixture_session()
        s.add_all([u1, u2, u3])
        s.commit()

        s.delete(u1)
        s.delete(u2)
        s.flush()

        u3.name = "u1"
        s.flush()

        u3.name = "u2"
        s.flush()

        s.rollback()

        assert u1 in s
        assert u2 in s
        assert u3 in s

        assert s.identity_map[identity_key(User, ("u1",))] is u1
        assert s.identity_map[identity_key(User, ("u2",))] is u2
        assert s.identity_map[identity_key(User, ("u3",))] is u3

    def test_key_replaced_by_oob_insert(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        u1 = User(name="u1")

        s = fixture_session()
        s.add(u1)
        s.commit()

        s.delete(u1)
        s.flush()

        s.execute(users.insert().values(name="u1"))
        u2 = s.get(User, "u1")

        assert u1 not in s
        s.rollback()

        assert u1 in s
        assert u2 not in s

        assert s.identity_map[identity_key(User, ("u1",))] is u1


class JoinIntoAnExternalTransactionFixture:
    """Test the "join into an external transaction" examples"""

    def setup_test(self):
        self.engine = engines.testing_engine(
            options={"use_reaper": False, "sqlite_savepoint": True}
        )
        self.connection = self.engine.connect()

        self.metadata = MetaData()
        self.table = Table(
            "t1",
            self.metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        with self.connection.begin():
            self.table.create(self.connection, checkfirst=True)

        self.setup_session()

    def teardown_test(self):
        self.teardown_session()

        with self.connection.begin():
            self._assert_count(0)

        with self.connection.begin():
            self.table.drop(self.connection)

        self.connection.close()

    def test_something(self, connection):
        A = self.A

        a1 = A()
        self.session.add(a1)
        self.session.commit()

        self._assert_count(1)

    def _assert_count(self, count):
        result = self.connection.scalar(
            select(func.count()).select_from(self.table)
        )
        eq_(result, count)


class CtxManagerJoinIntoAnExternalTransactionFixture(
    JoinIntoAnExternalTransactionFixture
):
    @testing.requires.compat_savepoints
    def test_something_with_context_managers(self):
        A = self.A

        a1 = A()

        with self.session.begin():
            self.session.add(a1)
            self.session.flush()

            self._assert_count(1)
            self.session.rollback()

        self._assert_count(0)

        a1 = A()
        with self.session.begin():
            self.session.add(a1)

        self._assert_count(1)

        a2 = A()

        with self.session.begin():
            self.session.add(a2)
            self.session.flush()
            self._assert_count(2)

            self.session.rollback()
        self._assert_count(1)

    @testing.requires.compat_savepoints
    def test_super_abusive_nesting(self):
        session = self.session

        for i in range(random.randint(5, 30)):
            choice = random.randint(1, 3)
            if choice == 1:
                if session.in_transaction():
                    session.begin_nested()
                else:
                    session.begin()
            elif choice == 2:
                session.rollback()
            elif choice == 3:
                session.commit()

            session.connection()

        # remaining nested / etc. are cleanly cleared out
        session.close()


class NewStyleJoinIntoAnExternalTransactionTest(
    CtxManagerJoinIntoAnExternalTransactionFixture, fixtures.MappedTest
):
    """test the 1.4 join to an external transaction fixture.

    In 1.4, this works for both legacy and future engines/sessions

    """

    def setup_session(self):
        # begin a non-ORM transaction
        self.trans = self.connection.begin()

        class A:
            pass

        clear_mappers()
        self.mapper_registry.map_imperatively(A, self.table)
        self.A = A

        # bind an individual Session to the connection
        self.session = Session(bind=self.connection)

        if testing.requires.compat_savepoints.enabled:
            self.nested = self.connection.begin_nested()

            @event.listens_for(self.session, "after_transaction_end")
            def end_savepoint(session, transaction):
                if not self.nested.is_active:
                    self.nested = self.connection.begin_nested()

    def teardown_session(self):
        self.session.close()

        # rollback - everything that happened with the
        # Session above (including calls to commit())
        # is rolled back.
        if self.trans.is_active:
            self.trans.rollback()


@testing.combinations(
    *Variation.generate_cases(
        "join_mode",
        [
            "create_savepoint",
            "conditional_w_savepoint",
            "create_savepoint_w_savepoint",
        ],
    ),
    argnames="join_mode",
    id_="s",
)
class ReallyNewJoinIntoAnExternalTransactionTest(
    CtxManagerJoinIntoAnExternalTransactionFixture, fixtures.MappedTest
):
    """2.0 only recipe for "join into an external transaction" that works
    without event handlers

    """

    def setup_session(self):
        self.trans = self.connection.begin()

        if (
            self.join_mode.conditional_w_savepoint
            or self.join_mode.create_savepoint_w_savepoint
        ):
            self.nested = self.connection.begin_nested()

        class A:
            pass

        clear_mappers()
        self.mapper_registry.map_imperatively(A, self.table)
        self.A = A

        self.session = Session(
            self.connection,
            join_transaction_mode=(
                "create_savepoint"
                if (
                    self.join_mode.create_savepoint
                    or self.join_mode.create_savepoint_w_savepoint
                )
                else "conditional_savepoint"
            ),
        )

    def teardown_session(self):
        self.session.close()

        if (
            self.join_mode.conditional_w_savepoint
            or self.join_mode.create_savepoint_w_savepoint
        ):
            assert not self.nested._deactivated_from_connection
            assert self.nested.is_active
            self.nested.rollback()

        assert not self.trans._deactivated_from_connection
        assert self.trans.is_active
        self.trans.rollback()


class LegacyJoinIntoAnExternalTransactionTest(
    JoinIntoAnExternalTransactionFixture,
    fixtures.MappedTest,
):
    """test the 1.3 join to an external transaction fixture"""

    def setup_session(self):
        # begin a non-ORM transaction
        self.trans = self.connection.begin()

        class A:
            pass

        # TODO: py2 is not hitting this correctly for some reason,
        # some mro issue
        self.mapper_registry.map_imperatively(A, self.table)
        self.A = A

        # bind an individual Session to the connection
        self.session = Session(bind=self.connection)

        if testing.requires.compat_savepoints.enabled:
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

    def teardown_session(self):
        self.session.close()

        # rollback - everything that happened with the
        # Session above (including calls to commit())
        # is rolled back.
        self.trans.rollback()
