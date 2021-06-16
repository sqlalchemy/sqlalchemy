import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.ext.asyncio import AsyncSession as _AsyncSession
from sqlalchemy.testing import async_test
from sqlalchemy.testing import eq_
from sqlalchemy.testing import is_
from .test_session_py3k import AsyncFixture


class AsyncScopedSessionTest(AsyncFixture):
    @testing.requires.python37
    @async_test
    async def test_basic(self, async_engine):
        from asyncio import current_task

        AsyncSession = async_scoped_session(
            sa.orm.sessionmaker(async_engine, class_=_AsyncSession),
            scopefunc=current_task,
        )

        some_async_session = AsyncSession()
        some_other_async_session = AsyncSession()

        is_(some_async_session, some_other_async_session)
        is_(some_async_session.bind, async_engine)

        User = self.classes.User

        async with AsyncSession.begin():
            user_name = "scoped_async_session_u1"
            u1 = User(name=user_name)

            AsyncSession.add(u1)

            await AsyncSession.flush()

            conn = await AsyncSession.connection()
            stmt = select(func.count(User.id)).where(User.name == user_name)
            eq_(await conn.scalar(stmt), 1)

            await AsyncSession.delete(u1)
            await AsyncSession.flush()
            eq_(await conn.scalar(stmt), 0)
