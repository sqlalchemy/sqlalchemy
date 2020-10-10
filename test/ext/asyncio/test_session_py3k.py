from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import selectinload
from sqlalchemy.testing import async_test
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
        return create_async_engine(testing.db.url)

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
    async def test_trans(self, async_session, async_engine):
        async with async_engine.connect() as outer_conn:

            User = self.classes.User

            async with async_session.begin():

                eq_(await outer_conn.scalar(select(func.count(User.id))), 0)

                u1 = User(name="u1")

                async_session.add(u1)

                result = await async_session.execute(select(User))
                eq_(result.scalar(), u1)

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
