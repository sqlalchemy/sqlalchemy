from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import async_test
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from ...orm import _fixtures


class AsyncFixture(_fixtures.FixtureTest):
    __requires__ = ("async_dialect",)

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    @testing.fixture
    def async_engine(self):
        return engines.testing_engine(asyncio=True)

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


class AsyncSessionQueryTest(AsyncFixture):
    @async_test
    async def test_execute(self, async_session):
        User = self.classes.User

        stmt = (
            select(User)
            .options(selectinload(User.addresses))
            .order_by(User.id)
        )

        result = await async_session.execute(stmt)
        eq_(result.scalars().all(), self.static.user_address_result)

    @async_test
    async def test_scalar(self, async_session):
        User = self.classes.User

        stmt = select(User.id).order_by(User.id).limit(1)

        result = await async_session.scalar(stmt)
        eq_(result, 7)

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
    @testing.requires.independent_cursors
    async def test_stream_partitions(self, async_session):
        User = self.classes.User

        stmt = (
            select(User)
            .options(selectinload(User.addresses))
            .order_by(User.id)
        )

        result = await async_session.stream(stmt)

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


class AsyncSessionTransactionTest(AsyncFixture):
    run_inserts = None

    @async_test
    async def test_sessionmaker_block_one(self, async_engine):

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
    async def test_sessionmaker_block_two(self, async_engine):

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

            async_session.delete(u1)

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


class AsyncEventTest(AsyncFixture):
    """The engine events all run in their normal synchronous context.

    we do not provide an asyncio event interface at this time.

    """

    __backend__ = True

    @async_test
    async def test_no_async_listeners(self, async_session):
        with testing.expect_raises(
            NotImplementedError,
            "NotImplementedError: asynchronous events are not implemented "
            "at this time.  Apply synchronous listeners to the "
            "AsyncEngine.sync_engine or "
            "AsyncConnection.sync_connection attributes.",
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
