from __future__ import annotations

import contextlib
from typing import List
from typing import Optional

from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_object_session
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import close_all_sessions
from sqlalchemy.ext.asyncio import exc as async_exc
from sqlalchemy.ext.asyncio.base import ReversibleProxy
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import async_test
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import expect_deprecated
from sqlalchemy.testing.assertions import in_
from sqlalchemy.testing.assertions import is_false
from sqlalchemy.testing.assertions import not_in
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.provision import normalize_sequence
from sqlalchemy.testing.schema import Column
from .test_engine_py3k import AsyncFixture as _AsyncFixture
from ...orm import _fixtures


class AsyncFixture(_AsyncFixture, _fixtures.FixtureTest):
    __requires__ = ("async_dialect",)

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    @testing.fixture
    def async_engine(self):
        return engines.testing_engine(asyncio=True, transfer_staticpool=True)

    # TODO: this seems to cause deadlocks in
    # OverrideSyncSession for some reason
    # @testing.fixture
    # def async_engine(self, async_testing_engine):
    # return async_testing_engine(transfer_staticpool=True)

    @testing.fixture
    def async_session(self, async_engine):
        return AsyncSession(async_engine)


class AsyncSessionTest(AsyncFixture):
    def test_requires_async_engine(self, async_engine):
        testing.assert_raises_message(
            exc.ArgumentError,
            "AsyncEngine expected, got Engine",
            AsyncSession,
            bind=async_engine.sync_engine,
        )

    def test_info(self, async_session):
        async_session.info["foo"] = "bar"

        eq_(async_session.sync_session.info, {"foo": "bar"})

    def test_init(self, async_engine):
        ss = AsyncSession(bind=async_engine)
        is_(ss.bind, async_engine)

        binds = {Table: async_engine}
        ss = AsyncSession(binds=binds)
        is_(ss.binds, binds)

    @async_test
    @testing.combinations((True,), (False,), argnames="use_scalar")
    @testing.requires.sequences
    async def test_sequence_execute(
        self, async_session: AsyncSession, metadata, use_scalar
    ):
        seq = normalize_sequence(
            config, Sequence("some_sequence", metadata=metadata)
        )

        sync_connection = (await async_session.connection()).sync_connection

        await (await async_session.connection()).run_sync(metadata.create_all)

        if use_scalar:
            eq_(
                await async_session.scalar(seq),
                sync_connection.dialect.default_sequence_base,
            )
        else:
            with expect_deprecated(
                r"Using the .execute\(\) method to invoke a "
                r"DefaultGenerator object is deprecated; please use "
                r"the .scalar\(\) method."
            ):
                eq_(
                    await async_session.execute(seq),
                    sync_connection.dialect.default_sequence_base,
                )

    @async_test
    async def test_close_all(self, async_engine):
        User = self.classes.User

        s1 = AsyncSession(async_engine)
        u1 = User()
        s1.add(u1)

        s2 = AsyncSession(async_engine)
        u2 = User()
        s2.add(u2)

        in_(u1, s1)
        in_(u2, s2)

        await close_all_sessions()

        not_in(u1, s1)
        not_in(u2, s2)

    @async_test
    async def test_session_close_all_deprecated(self, async_engine):
        User = self.classes.User

        s1 = AsyncSession(async_engine)
        u1 = User()
        s1.add(u1)

        s2 = AsyncSession(async_engine)
        u2 = User()
        s2.add(u2)

        in_(u1, s1)
        in_(u2, s2)

        with expect_deprecated(
            r"The AsyncSession.close_all\(\) method is deprecated and will "
            "be removed in a future release. "
        ):
            await AsyncSession.close_all()

        not_in(u1, s1)
        not_in(u2, s2)


class AsyncSessionQueryTest(AsyncFixture):
    @async_test
    @testing.combinations(
        {}, dict(execution_options={"logging_token": "test"}), argnames="kw"
    )
    async def test_execute(self, async_session, kw):
        User = self.classes.User

        stmt = (
            select(User)
            .options(selectinload(User.addresses))
            .order_by(User.id)
        )

        result = await async_session.execute(stmt, **kw)
        eq_(result.scalars().all(), self.static.user_address_result)

    @async_test
    async def test_scalar(self, async_session):
        User = self.classes.User

        stmt = select(User.id).order_by(User.id).limit(1)

        result = await async_session.scalar(stmt)
        eq_(result, 7)

    @testing.requires.python310
    @async_test
    async def test_session_aclose(self, async_session):
        User = self.classes.User
        u = User(name="u")
        async with contextlib.aclosing(async_session) as session:
            session.add(u)
            await session.commit()
        assert async_session.sync_session.identity_map.values() == []

    @testing.combinations(
        ("scalars",), ("stream_scalars",), argnames="filter_"
    )
    @async_test
    async def test_scalars(self, async_session, filter_):
        User = self.classes.User

        stmt = (
            select(User)
            .options(selectinload(User.addresses))
            .order_by(User.id)
        )

        if filter_ == "scalars":
            result = (await async_session.scalars(stmt)).all()
        elif filter_ == "stream_scalars":
            result = await (await async_session.stream_scalars(stmt)).all()
        eq_(result, self.static.user_address_result)

    @async_test
    async def test_get(self, async_session):
        User = self.classes.User

        u1 = await async_session.get(User, 7)

        eq_(u1.name, "jack")

        u2 = await async_session.get(User, 7)

        is_(u1, u2)

        u3 = await async_session.get(User, 12)
        is_(u3, None)

    @async_test
    async def test_get_one(self, async_session):
        User = self.classes.User

        u1 = await async_session.get_one(User, 7)
        u2 = await async_session.get_one(User, 10)
        u3 = await async_session.get_one(User, 7)

        is_(u1, u3)
        eq_(u1.name, "jack")
        eq_(u2.name, "chuck")

        with testing.expect_raises_message(
            exc.NoResultFound,
            "No row was found when one was required",
        ):
            await async_session.get_one(User, 12)

    @async_test
    async def test_force_a_lazyload(self, async_session):
        """test for #9298"""

        User = self.classes.User

        stmt = select(User).order_by(User.id)

        result = (await async_session.scalars(stmt)).all()

        for user_obj in result:
            await async_session.refresh(user_obj, ["addresses"])

        eq_(result, self.static.user_address_result)

    @async_test
    async def test_get_loader_options(self, async_session):
        User = self.classes.User

        u = await async_session.get(
            User, 7, options=[selectinload(User.addresses)]
        )

        eq_(u.name, "jack")
        eq_(len(u.__dict__["addresses"]), 1)

    @async_test
    @testing.requires.independent_cursors
    @testing.combinations(
        {}, dict(execution_options={"logging_token": "test"}), argnames="kw"
    )
    async def test_stream_partitions(self, async_session, kw):
        User = self.classes.User

        stmt = (
            select(User)
            .options(selectinload(User.addresses))
            .order_by(User.id)
        )

        result = await async_session.stream(stmt, **kw)

        assert_result = []
        async for partition in result.scalars().partitions(3):
            assert_result.append(partition)

        eq_(
            assert_result,
            [
                self.static.user_address_result[0:3],
                self.static.user_address_result[3:],
            ],
        )

    @testing.combinations("statement", "execute", argnames="location")
    @async_test
    @testing.requires.server_side_cursors
    async def test_no_ss_cursor_w_execute(self, async_session, location):
        User = self.classes.User

        stmt = select(User)
        if location == "statement":
            stmt = stmt.execution_options(stream_results=True)

        with expect_raises_message(
            async_exc.AsyncMethodRequired,
            r"Can't use the AsyncSession.execute\(\) method with a "
            r"server-side cursor. Use the AsyncSession.stream\(\) "
            r"method for an async streaming result set.",
        ):
            if location == "execute":
                await async_session.execute(
                    stmt, execution_options={"stream_results": True}
                )
            else:
                await async_session.execute(stmt)


class AsyncSessionTransactionTest(AsyncFixture):
    run_inserts = None

    @async_test
    async def test_interrupt_ctxmanager_connection(
        self, async_trans_ctx_manager_fixture, async_session
    ):
        fn = async_trans_ctx_manager_fixture

        await fn(async_session, trans_on_subject=True, execute_on_subject=True)

    @async_test
    async def test_orm_sessionmaker_block_one(self, async_engine):
        User = self.classes.User
        maker = sessionmaker(async_engine, class_=AsyncSession)

        session = maker()

        async with session.begin():
            u1 = User(name="u1")
            assert session.in_transaction()
            session.add(u1)

        assert not session.in_transaction()

        async with maker() as session:
            result = await session.execute(
                select(User).where(User.name == "u1")
            )

            u1 = result.scalar_one()

            eq_(u1.name, "u1")

    @async_test
    async def test_orm_sessionmaker_block_two(self, async_engine):
        User = self.classes.User
        maker = sessionmaker(async_engine, class_=AsyncSession)

        async with maker.begin() as session:
            u1 = User(name="u1")
            assert session.in_transaction()
            session.add(u1)

        assert not session.in_transaction()

        async with maker() as session:
            result = await session.execute(
                select(User).where(User.name == "u1")
            )

            u1 = result.scalar_one()

            eq_(u1.name, "u1")

    @async_test
    async def test_async_sessionmaker_block_one(self, async_engine):
        User = self.classes.User
        maker = async_sessionmaker(async_engine)

        session = maker()

        async with session.begin():
            u1 = User(name="u1")
            assert session.in_transaction()
            session.add(u1)

        assert not session.in_transaction()

        async with maker() as session:
            result = await session.execute(
                select(User).where(User.name == "u1")
            )

            u1 = result.scalar_one()

            eq_(u1.name, "u1")

    @async_test
    async def test_async_sessionmaker_block_two(self, async_engine):
        User = self.classes.User
        maker = async_sessionmaker(async_engine)

        async with maker.begin() as session:
            u1 = User(name="u1")
            assert session.in_transaction()
            session.add(u1)

        assert not session.in_transaction()

        async with maker() as session:
            result = await session.execute(
                select(User).where(User.name == "u1")
            )

            u1 = result.scalar_one()

            eq_(u1.name, "u1")

    @async_test
    async def test_trans(self, async_session, async_engine):
        async with async_engine.connect() as outer_conn:
            User = self.classes.User

            async with async_session.begin():
                eq_(await outer_conn.scalar(select(func.count(User.id))), 0)

                u1 = User(name="u1")

                async_session.add(u1)

                result = await async_session.execute(select(User))
                eq_(result.scalar(), u1)

            await outer_conn.rollback()
            eq_(await outer_conn.scalar(select(func.count(User.id))), 1)

    @async_test
    async def test_commit_as_you_go(self, async_session, async_engine):
        async with async_engine.connect() as outer_conn:
            User = self.classes.User

            eq_(await outer_conn.scalar(select(func.count(User.id))), 0)

            u1 = User(name="u1")

            async_session.add(u1)

            result = await async_session.execute(select(User))
            eq_(result.scalar(), u1)

            await async_session.commit()

            await outer_conn.rollback()
            eq_(await outer_conn.scalar(select(func.count(User.id))), 1)

    @async_test
    async def test_trans_noctx(self, async_session, async_engine):
        async with async_engine.connect() as outer_conn:
            User = self.classes.User

            trans = await async_session.begin()
            try:
                eq_(await outer_conn.scalar(select(func.count(User.id))), 0)

                u1 = User(name="u1")

                async_session.add(u1)

                result = await async_session.execute(select(User))
                eq_(result.scalar(), u1)
            finally:
                await trans.commit()

            await outer_conn.rollback()
            eq_(await outer_conn.scalar(select(func.count(User.id))), 1)

    @async_test
    async def test_delete(self, async_session):
        User = self.classes.User

        async with async_session.begin():
            u1 = User(name="u1")

            async_session.add(u1)

            await async_session.flush()

            conn = await async_session.connection()

            eq_(await conn.scalar(select(func.count(User.id))), 1)

            await async_session.delete(u1)

            await async_session.flush()

            eq_(await conn.scalar(select(func.count(User.id))), 0)

    @async_test
    async def test_flush(self, async_session):
        User = self.classes.User

        async with async_session.begin():
            u1 = User(name="u1")

            async_session.add(u1)

            conn = await async_session.connection()

            eq_(await conn.scalar(select(func.count(User.id))), 0)

            await async_session.flush()

            eq_(await conn.scalar(select(func.count(User.id))), 1)

    @async_test
    async def test_refresh(self, async_session):
        User = self.classes.User

        async with async_session.begin():
            u1 = User(name="u1")

            async_session.add(u1)
            await async_session.flush()

            conn = await async_session.connection()

            await conn.execute(
                update(User)
                .values(name="u2")
                .execution_options(synchronize_session=None)
            )

            eq_(u1.name, "u1")

            await async_session.refresh(u1)

            eq_(u1.name, "u2")

            eq_(await conn.scalar(select(func.count(User.id))), 1)

    @async_test
    async def test_merge(self, async_session):
        User = self.classes.User

        async with async_session.begin():
            u1 = User(id=1, name="u1")

            async_session.add(u1)

        async with async_session.begin():
            new_u = User(id=1, name="new u1")

            new_u_merged = await async_session.merge(new_u)

            is_(new_u_merged, u1)
            eq_(u1.name, "new u1")

    @async_test
    async def test_merge_loader_options(self, async_session):
        User = self.classes.User
        Address = self.classes.Address

        async with async_session.begin():
            u1 = User(id=1, name="u1", addresses=[Address(email_address="e1")])

            async_session.add(u1)

        await async_session.close()

        async with async_session.begin():
            new_u1 = User(id=1, name="new u1")

            new_u_merged = await async_session.merge(
                new_u1, options=[selectinload(User.addresses)]
            )

            eq_(new_u_merged.name, "new u1")
            eq_(len(new_u_merged.__dict__["addresses"]), 1)

    @async_test
    async def test_join_to_external_transaction(self, async_engine):
        User = self.classes.User

        async with async_engine.connect() as conn:
            t1 = await conn.begin()

            async_session = AsyncSession(conn)

            aconn = await async_session.connection()

            eq_(aconn.get_transaction(), t1)

            eq_(aconn, conn)
            is_(aconn.sync_connection, conn.sync_connection)

            u1 = User(id=1, name="u1")

            async_session.add(u1)

            await async_session.commit()

            assert conn.in_transaction()
            await conn.rollback()

        async with AsyncSession(async_engine) as async_session:
            result = await async_session.execute(select(User))
            eq_(result.all(), [])

    @testing.requires.savepoints
    @async_test
    async def test_join_to_external_transaction_with_savepoints(
        self, async_engine
    ):
        """This is the full 'join to an external transaction' recipe
        implemented for async using savepoints.

        It's not particularly simple to understand as we have to switch between
        async / sync APIs but it works and it's a start.

        """

        User = self.classes.User

        async with async_engine.connect() as conn:
            await conn.begin()

            await conn.begin_nested()

            async_session = AsyncSession(conn)

            @event.listens_for(
                async_session.sync_session, "after_transaction_end"
            )
            def end_savepoint(session, transaction):
                """here's an event.  inside the event we write blocking
                style code.    wow will this be fun to try to explain :)

                """

                if conn.closed:
                    return

                if not conn.in_nested_transaction():
                    conn.sync_connection.begin_nested()

            aconn = await async_session.connection()
            is_(aconn.sync_connection, conn.sync_connection)

            u1 = User(id=1, name="u1")

            async_session.add(u1)

            await async_session.commit()

            result = (await async_session.execute(select(User))).all()
            eq_(len(result), 1)

            u2 = User(id=2, name="u2")
            async_session.add(u2)

            await async_session.flush()

            result = (await async_session.execute(select(User))).all()
            eq_(len(result), 2)

            # a rollback inside the session ultimately ends the savepoint
            await async_session.rollback()

            # but the previous thing we "committed" is still in the DB
            result = (await async_session.execute(select(User))).all()
            eq_(len(result), 1)

            assert conn.in_transaction()
            await conn.rollback()

        async with AsyncSession(async_engine) as async_session:
            result = await async_session.execute(select(User))
            eq_(result.all(), [])

    @async_test
    @testing.requires.independent_connections
    async def test_invalidate(self, async_session):
        await async_session.execute(select(1))
        conn = async_session.sync_session.connection()
        fairy = conn.connection
        connection_rec = fairy._connection_record

        is_false(conn.closed)
        is_false(connection_rec._is_hard_or_soft_invalidated())
        await async_session.invalidate()
        is_true(conn.closed)
        is_true(connection_rec._is_hard_or_soft_invalidated())

        eq_(async_session.in_transaction(), False)


class AsyncCascadesTest(AsyncFixture):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        User, Address = cls.classes("User", "Address")
        users, addresses = cls.tables("users", "addresses")

        cls.mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, cascade="all, delete-orphan"
                )
            },
        )
        cls.mapper(
            Address,
            addresses,
        )

    @async_test
    async def test_delete_w_cascade(self, async_session):
        User = self.classes.User
        Address = self.classes.Address

        async with async_session.begin():
            u1 = User(id=1, name="u1", addresses=[Address(email_address="e1")])

            async_session.add(u1)

        async with async_session.begin():
            u1 = (await async_session.execute(select(User))).scalar_one()

            await async_session.delete(u1)

        eq_(
            (
                await async_session.execute(
                    select(func.count()).select_from(Address)
                )
            ).scalar(),
            0,
        )


class AsyncORMBehaviorsTest(AsyncFixture):
    @testing.fixture
    def one_to_one_fixture(self, registry, async_engine):
        async def go(legacy_inactive_history_style):
            @registry.mapped
            class A:
                __tablename__ = "a"

                id = Column(
                    Integer, primary_key=True, test_needs_autoincrement=True
                )
                b = relationship(
                    "B",
                    uselist=False,
                    _legacy_inactive_history_style=(
                        legacy_inactive_history_style
                    ),
                )

            @registry.mapped
            class B:
                __tablename__ = "b"
                id = Column(
                    Integer, primary_key=True, test_needs_autoincrement=True
                )
                a_id = Column(ForeignKey("a.id"))

            async with async_engine.begin() as conn:
                await conn.run_sync(registry.metadata.create_all)

            return A, B

        return go

    @testing.combinations(
        ("legacy_style", True),
        ("new_style", False),
        argnames="_legacy_inactive_history_style",
        id_="ia",
    )
    @async_test
    async def test_new_style_active_history(
        self, async_session, one_to_one_fixture, _legacy_inactive_history_style
    ):
        A, B = await one_to_one_fixture(_legacy_inactive_history_style)

        a1 = A()
        b1 = B()

        a1.b = b1
        async_session.add(a1)

        await async_session.commit()

        b2 = B()

        if _legacy_inactive_history_style:
            # aiomysql dialect having problems here, emitting weird
            # pytest warnings and we might need to just skip for aiomysql
            # here, which is also raising StatementError w/ MissingGreenlet
            # inside of it
            with testing.expect_raises(
                (exc.StatementError, exc.MissingGreenlet)
            ):
                a1.b = b2

        else:
            a1.b = b2

            await async_session.flush()

            await async_session.refresh(b1)

            eq_(
                (
                    await async_session.execute(
                        select(func.count())
                        .where(B.id == b1.id)
                        .where(B.a_id == None)
                    )
                ).scalar(),
                1,
            )


class AsyncEventTest(AsyncFixture):
    """The engine events all run in their normal synchronous context.

    we do not provide an asyncio event interface at this time.

    """

    __backend__ = True

    @async_test
    async def test_no_async_listeners(self, async_session):
        with testing.expect_raises_message(
            NotImplementedError,
            "asynchronous events are not implemented at this time.  "
            "Apply synchronous listeners to the AsyncSession.sync_session.",
        ):
            event.listen(async_session, "before_flush", mock.Mock())

    @async_test
    async def test_sync_before_commit(self, async_session):
        canary = mock.Mock()

        event.listen(async_session.sync_session, "before_commit", canary)

        async with async_session.begin():
            pass

        eq_(
            canary.mock_calls,
            [mock.call(async_session.sync_session)],
        )


class AsyncProxyTest(AsyncFixture):
    @async_test
    async def test_get_connection_engine_bound(self, async_session):
        c1 = await async_session.connection()

        c2 = await async_session.connection()

        is_(c1, c2)
        is_(c1.engine, c2.engine)

    @async_test
    async def test_get_connection_kws(self, async_session):
        c1 = await async_session.connection(
            execution_options={"isolation_level": "AUTOCOMMIT"}
        )

        eq_(
            c1.sync_connection._execution_options,
            {"isolation_level": "AUTOCOMMIT"},
        )

    @async_test
    async def test_get_connection_connection_bound(self, async_engine):
        async with async_engine.begin() as conn:
            async_session = AsyncSession(conn)

            c1 = await async_session.connection()

            is_(c1, conn)
            is_(c1.engine, conn.engine)

    @async_test
    async def test_get_transaction(self, async_session):
        is_(async_session.get_transaction(), None)
        is_(async_session.get_nested_transaction(), None)

        t1 = await async_session.begin()

        is_(async_session.get_transaction(), t1)
        is_(async_session.get_nested_transaction(), None)

        n1 = await async_session.begin_nested()

        is_(async_session.get_transaction(), t1)
        is_(async_session.get_nested_transaction(), n1)

        await n1.commit()

        is_(async_session.get_transaction(), t1)
        is_(async_session.get_nested_transaction(), None)

        await t1.commit()

        is_(async_session.get_transaction(), None)
        is_(async_session.get_nested_transaction(), None)

    @async_test
    async def test_get_transaction_gced(self, async_session):
        """test #12471

        this tests that the AsyncSessionTransaction is regenerated if
        we don't have any reference to it beforehand.

        """
        is_(async_session.get_transaction(), None)
        is_(async_session.get_nested_transaction(), None)

        await async_session.begin()

        trans = async_session.get_transaction()
        is_not(trans, None)
        is_(trans.session, async_session)
        is_false(trans.nested)
        is_(
            trans.sync_transaction,
            async_session.sync_session.get_transaction(),
        )

        await async_session.begin_nested()
        nested = async_session.get_nested_transaction()
        is_not(nested, None)
        is_true(nested.nested)
        is_(nested.session, async_session)
        is_(
            nested.sync_transaction,
            async_session.sync_session.get_nested_transaction(),
        )

    @async_test
    async def test_async_object_session(self, async_engine):
        User = self.classes.User

        s1 = AsyncSession(async_engine)

        s2 = AsyncSession(async_engine)

        u1 = await s1.get(User, 7)

        u2 = User(name="n1")

        s2.add(u2)

        u3 = User(name="n2")

        is_(async_object_session(u1), s1)
        is_(async_object_session(u2), s2)

        is_(async_object_session(u3), None)

        await s2.reset()
        is_(async_object_session(u2), None)
        s2.add(u2)

        is_(async_object_session(u2), s2)
        await s2.close()
        is_(async_object_session(u2), None)

    @async_test
    async def test_async_object_session_custom(self, async_engine):
        User = self.classes.User

        class MyCustomAsync(AsyncSession):
            pass

        s1 = MyCustomAsync(async_engine)

        u1 = await s1.get(User, 7)

        assert isinstance(async_object_session(u1), MyCustomAsync)

    @testing.requires.predictable_gc
    @async_test
    async def test_async_object_session_del(self, async_engine):
        User = self.classes.User

        s1 = AsyncSession(async_engine)

        u1 = await s1.get(User, 7)

        is_(async_object_session(u1), s1)

        await s1.rollback()
        del s1
        is_(async_object_session(u1), None)

    @async_test
    async def test_inspect_session(self, async_engine):
        User = self.classes.User

        s1 = AsyncSession(async_engine)

        s2 = AsyncSession(async_engine)

        u1 = await s1.get(User, 7)

        u2 = User(name="n1")

        s2.add(u2)

        u3 = User(name="n2")

        is_(inspect(u1).async_session, s1)
        is_(inspect(u2).async_session, s2)

        is_(inspect(u3).async_session, None)

    def test_inspect_session_no_asyncio_used(self):
        User = self.classes.User

        s1 = Session(testing.db)
        u1 = s1.get(User, 7)

        is_(inspect(u1).async_session, None)

    def test_inspect_session_no_asyncio_imported(self):
        with mock.patch("sqlalchemy.orm.state._async_provider", None):
            User = self.classes.User

            s1 = Session(testing.db)
            u1 = s1.get(User, 7)

            is_(inspect(u1).async_session, None)

    @testing.requires.predictable_gc
    def test_gc(self, async_engine):
        ReversibleProxy._proxy_objects.clear()

        eq_(len(ReversibleProxy._proxy_objects), 0)

        async_session = AsyncSession(async_engine)

        eq_(len(ReversibleProxy._proxy_objects), 1)

        del async_session

        eq_(len(ReversibleProxy._proxy_objects), 0)


class _MySession(Session):
    pass


class _MyAS(AsyncSession):
    sync_session_class = _MySession


class OverrideSyncSession(AsyncFixture):
    def test_default(self, async_engine):
        ass = AsyncSession(async_engine)

        is_true(isinstance(ass.sync_session, Session))
        is_(ass.sync_session.__class__, Session)
        is_(ass.sync_session_class, Session)

    def test_init_class(self, async_engine):
        ass = AsyncSession(async_engine, sync_session_class=_MySession)

        is_true(isinstance(ass.sync_session, _MySession))
        is_(ass.sync_session_class, _MySession)

    def test_init_orm_sessionmaker(self, async_engine):
        sm = sessionmaker(
            async_engine, class_=AsyncSession, sync_session_class=_MySession
        )
        ass = sm()

        is_true(isinstance(ass.sync_session, _MySession))
        is_(ass.sync_session_class, _MySession)

    def test_init_asyncio_sessionmaker(self, async_engine):
        sm = async_sessionmaker(async_engine, sync_session_class=_MySession)
        ass = sm()

        is_true(isinstance(ass.sync_session, _MySession))
        is_(ass.sync_session_class, _MySession)

    def test_subclass(self, async_engine):
        ass = _MyAS(async_engine)

        is_true(isinstance(ass.sync_session, _MySession))
        is_(ass.sync_session_class, _MySession)

    def test_subclass_override(self, async_engine):
        ass = _MyAS(async_engine, sync_session_class=Session)

        is_true(not isinstance(ass.sync_session, _MySession))
        is_(ass.sync_session_class, Session)


class AsyncAttrsTest(
    testing.AssertsExecutionResults, _AsyncFixture, fixtures.TestBase
):
    __requires__ = ("async_dialect",)

    @config.fixture
    def decl_base(self, metadata):
        _md = metadata

        class Base(ComparableEntity, AsyncAttrs, DeclarativeBase):
            metadata = _md
            type_annotation_map = {
                str: String().with_variant(
                    String(50), "mysql", "mariadb", "oracle"
                )
            }

        yield Base
        Base.registry.dispose()

    @testing.fixture
    def async_engine(self, async_testing_engine):
        yield async_testing_engine(transfer_staticpool=True)

    @testing.fixture
    def ab_fixture(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            data: Mapped[Optional[str]]
            bs: Mapped[List[B]] = relationship(order_by=lambda: B.id)

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
            data: Mapped[Optional[str]]

        decl_base.metadata.create_all(testing.db)

        return A, B

    @async_test
    async def test_lazyloaders(self, async_engine, ab_fixture):
        A, B = ab_fixture

        async with AsyncSession(async_engine) as session:
            b1, b2, b3 = B(data="b1"), B(data="b2"), B(data="b3")
            a1 = A(data="a1", bs=[b1, b2, b3])
            session.add(a1)

            await session.commit()

            assert inspect(a1).expired

            with self.assert_statement_count(async_engine.sync_engine, 1):
                eq_(await a1.awaitable_attrs.data, "a1")

            with self.assert_statement_count(async_engine.sync_engine, 1):
                eq_(await a1.awaitable_attrs.bs, [b1, b2, b3])

            # now it's loaded, lazy loading not used anymore
            eq_(a1.bs, [b1, b2, b3])

    @async_test
    async def test_it_didnt_load_but_is_ok(self, async_engine, ab_fixture):
        A, B = ab_fixture

        async with AsyncSession(async_engine) as session:
            b1, b2, b3 = B(data="b1"), B(data="b2"), B(data="b3")
            a1 = A(data="a1", bs=[b1, b2, b3])
            session.add(a1)

            await session.commit()

        async with AsyncSession(async_engine) as session:
            a1 = (
                await session.scalars(select(A).options(selectinload(A.bs)))
            ).one()

            with self.assert_statement_count(async_engine.sync_engine, 0):
                eq_(await a1.awaitable_attrs.bs, [b1, b2, b3])

    @async_test
    async def test_the_famous_lazyloader_gotcha(
        self, async_engine, ab_fixture
    ):
        A, B = ab_fixture

        async with AsyncSession(async_engine) as session:
            a1 = A(data="a1")
            session.add(a1)

            await session.flush()

            with self.assert_statement_count(async_engine.sync_engine, 1):
                eq_(await a1.awaitable_attrs.bs, [])
