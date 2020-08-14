from sqlalchemy import exc
from sqlalchemy.testing import async_test
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.util import await_fallback
from sqlalchemy.util import await_only
from sqlalchemy.util import greenlet_spawn


async def run1():
    return 1


async def run2():
    return 2


def go(*fns):
    return sum(await_only(fn()) for fn in fns)


class TestAsyncioCompat(fixtures.TestBase):
    @async_test
    async def test_ok(self):

        eq_(await greenlet_spawn(go, run1, run2), 3)

    @async_test
    async def test_async_error(self):
        async def err():
            raise ValueError("an error")

        with expect_raises_message(ValueError, "an error"):
            await greenlet_spawn(go, run1, err)

    @async_test
    async def test_sync_error(self):
        def go():
            await_only(run1())
            raise ValueError("sync error")

        with expect_raises_message(ValueError, "sync error"):
            await greenlet_spawn(go)

    def test_await_fallback_no_greenlet(self):
        to_await = run1()
        await_fallback(to_await)

    def test_await_only_no_greenlet(self):
        to_await = run1()
        with expect_raises_message(
            exc.InvalidRequestError,
            r"greenlet_spawn has not been called; can't call await_\(\) here.",
        ):
            await_only(to_await)

        # ensure no warning
        await_fallback(to_await)

    @async_test
    async def test_await_fallback_error(self):
        to_await = run1()

        await to_await

        async def inner_await():
            nonlocal to_await
            to_await = run1()
            await_fallback(to_await)

        def go():
            await_fallback(inner_await())

        with expect_raises_message(
            exc.InvalidRequestError,
            "greenlet_spawn has not been called and asyncio event loop",
        ):
            await greenlet_spawn(go)

        await to_await

    @async_test
    async def test_await_only_error(self):
        to_await = run1()

        await to_await

        async def inner_await():
            nonlocal to_await
            to_await = run1()
            await_only(to_await)

        def go():
            await_only(inner_await())

        with expect_raises_message(
            exc.InvalidRequestError,
            r"greenlet_spawn has not been called; can't call await_\(\) here.",
        ):
            await greenlet_spawn(go)

        await to_await
