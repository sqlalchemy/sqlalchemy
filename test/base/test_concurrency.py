import asyncio
import contextvars
from multiprocessing import get_context
import random
import threading

from sqlalchemy import exc
from sqlalchemy import testing
from sqlalchemy.testing import async_test
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_true
from sqlalchemy.testing.config import combinations
from sqlalchemy.util import await_
from sqlalchemy.util import greenlet_spawn
from sqlalchemy.util import queue
from ._concurrency_fixtures import greenlet_not_imported
from ._concurrency_fixtures import greenlet_setup_in_ext
from ._concurrency_fixtures import greenlet_setup_on_call

try:
    from greenlet import greenlet
except ImportError:
    greenlet = None


async def run1():
    return 1


async def run2():
    return 2


def go(*fns):
    return sum(await_(fn()) for fn in fns)


class TestAsyncioCompat(fixtures.TestBase):
    __requires__ = ("greenlet",)

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
    async def test_propagate_cancelled(self):
        """test #6652"""
        cleanup = []

        async def async_meth_raise():
            raise asyncio.CancelledError()

        def sync_meth():
            try:
                await_(async_meth_raise())
            except Exception:
                cleanup.append(True)
                raise

        async def run_w_cancel():
            await greenlet_spawn(sync_meth)

        with expect_raises(asyncio.CancelledError, check_context=False):
            await run_w_cancel()

        assert cleanup

    @async_test
    async def test_sync_error(self):
        def go():
            await_(run1())
            raise ValueError("sync error")

        with expect_raises_message(ValueError, "sync error"):
            await greenlet_spawn(go)

    @async_test
    async def test_await_only_no_greenlet(self):
        to_await = run1()
        with expect_raises_message(
            exc.MissingGreenlet,
            "greenlet_spawn has not been called; "
            r"can't call await_\(\) here.",
        ):
            await_(to_await)

        # existing awaitable is done
        with expect_raises(RuntimeError):
            await greenlet_spawn(await_, to_await)

        # no warning for a new one...
        to_await = run1()
        await greenlet_spawn(await_, to_await)

    @async_test
    async def test_await_only_error(self):
        to_await = run1()

        await to_await

        async def inner_await():
            nonlocal to_await
            to_await = run1()
            await_(to_await)

        def go():
            await_(inner_await())

        with expect_raises_message(
            exc.InvalidRequestError,
            "greenlet_spawn has not been called; "
            r"can't call await_\(\) here.",
        ):
            await greenlet_spawn(go)

        with expect_raises(RuntimeError):
            await to_await

    @async_test
    async def test_contextvars(self):
        var = contextvars.ContextVar("var")
        concurrency = 500

        # NOTE: sleep here is not necessary. It's used to simulate IO
        # ensuring that task are not run sequentially
        async def async_inner(val):
            await asyncio.sleep(random.uniform(0.005, 0.015))
            eq_(val, var.get())
            return var.get()

        async def async_set(val):
            await asyncio.sleep(random.uniform(0.005, 0.015))
            var.set(val)

        def inner(val):
            retval = await_(async_inner(val))
            eq_(val, var.get())
            eq_(retval, val)

            # set the value in a sync function
            newval = val + concurrency
            var.set(newval)
            syncset = await_(async_inner(newval))
            eq_(newval, var.get())
            eq_(syncset, newval)

            # set the value in an async function
            retval = val + 2 * concurrency
            await_(async_set(retval))
            eq_(var.get(), retval)
            eq_(await_(async_inner(retval)), retval)

            return retval

        async def task(val):
            await asyncio.sleep(random.uniform(0.005, 0.015))
            var.set(val)
            await asyncio.sleep(random.uniform(0.005, 0.015))
            return await greenlet_spawn(inner, val)

        values = {
            await coro
            for coro in asyncio.as_completed(
                [task(i) for i in range(concurrency)]
            )
        }
        eq_(values, set(range(concurrency * 2, concurrency * 3)))

    @async_test
    async def test_require_await(self):
        def run():
            return 1 + 1

        assert (await greenlet_spawn(run)) == 2

        with expect_raises_message(
            exc.AwaitRequired,
            "The current operation required an async execution but none was",
        ):
            await greenlet_spawn(run, _require_await=True)


class TestAsyncAdaptedQueue(fixtures.TestBase):
    __requires__ = ("greenlet",)

    def test_lazy_init(self):
        run = [False]

        def thread_go(q):
            def go():
                q.get(timeout=0.1)

            with expect_raises(queue.Empty):
                asyncio.run(greenlet_spawn(go))
            run[0] = True

        t = threading.Thread(
            target=thread_go, args=[queue.AsyncAdaptedQueue()]
        )
        t.start()
        t.join()

        is_true(run[0])

    @async_test
    async def test_error_other_loop(self):
        run = [False]

        def thread_go(q):
            def go():
                eq_(q.get(block=False), 1)
                q.get(timeout=0.1)

            with expect_raises_message(
                RuntimeError, ".* to a different .*loop"
            ):
                asyncio.run(greenlet_spawn(go))

            run[0] = True

        q = queue.AsyncAdaptedQueue()

        def prime():
            with expect_raises(queue.Empty):
                q.get(timeout=0.1)

        await greenlet_spawn(prime)
        q.put_nowait(1)
        t = threading.Thread(target=thread_go, args=[q])
        t.start()
        t.join()

        is_true(run[0])


class GreenletImportTests(fixtures.TestBase):
    def _run_in_process(self, fn):
        ctx = get_context("spawn")
        process = ctx.Process(target=fn)
        try:
            process.start()
            process.join(10)
            eq_(process.exitcode, 0)
        finally:
            process.kill()

    @combinations(
        greenlet_not_imported,
        (greenlet_setup_in_ext, testing.requires.greenlet),
        (greenlet_setup_on_call, testing.requires.greenlet),
    )
    def test_concurrency_fn(self, fn):
        self._run_in_process(fn)


class GracefulNoGreenletTest(fixtures.TestBase):
    __requires__ = ("no_greenlet",)

    def test_await_only_graceful(self):
        async def async_fn():
            pass

        with expect_raises_message(
            ImportError,
            "The SQLAlchemy asyncio module requires that the Python "
            "'greenlet' library is installed",
        ):
            await_(async_fn())
