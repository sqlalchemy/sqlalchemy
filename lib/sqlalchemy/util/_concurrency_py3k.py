import asyncio
import sys
from typing import Any
from typing import Callable
from typing import Coroutine

import greenlet

from .. import exc

try:
    from contextvars import copy_context as _copy_context

    # If greenlet.gr_context is present in current version of greenlet,
    # it will be set with a copy of the current context on creation.
    # Refs: https://github.com/python-greenlet/greenlet/pull/198
    getattr(greenlet.greenlet, "gr_context")
except (ImportError, AttributeError):
    _copy_context = None


# implementation based on snaury gist at
# https://gist.github.com/snaury/202bf4f22c41ca34e56297bae5f33fef
# Issue for context: https://github.com/python-greenlet/greenlet/issues/173


class _AsyncIoGreenlet(greenlet.greenlet):
    def __init__(self, fn, driver):
        greenlet.greenlet.__init__(self, fn, driver)
        self.driver = driver
        if _copy_context is not None:
            self.gr_context = _copy_context()


def await_only(awaitable: Coroutine) -> Any:
    """Awaits an async function in a sync method.

    The sync method must be inside a :func:`greenlet_spawn` context.
    :func:`await_` calls cannot be nested.

    :param awaitable: The coroutine to call.

    """
    # this is called in the context greenlet while running fn
    current = greenlet.getcurrent()
    if not isinstance(current, _AsyncIoGreenlet):
        raise exc.MissingGreenlet(
            "greenlet_spawn has not been called; can't call await_() here. "
            "Was IO attempted in an unexpected place?"
        )

    # returns the control to the driver greenlet passing it
    # a coroutine to run. Once the awaitable is done, the driver greenlet
    # switches back to this greenlet with the result of awaitable that is
    # then returned to the caller (or raised as error)
    return current.driver.switch(awaitable)


def await_fallback(awaitable: Coroutine) -> Any:
    """Awaits an async function in a sync method.

    The sync method must be inside a :func:`greenlet_spawn` context.
    :func:`await_` calls cannot be nested.

    :param awaitable: The coroutine to call.

    """
    # this is called in the context greenlet while running fn
    current = greenlet.getcurrent()
    if not isinstance(current, _AsyncIoGreenlet):
        loop = asyncio.get_event_loop()
        if loop.is_running():
            raise exc.MissingGreenlet(
                "greenlet_spawn has not been called and asyncio event "
                "loop is already running; can't call await_() here. "
                "Was IO attempted in an unexpected place?"
            )
        return loop.run_until_complete(awaitable)

    return current.driver.switch(awaitable)


async def greenlet_spawn(
    fn: Callable, *args, _require_await=False, **kwargs
) -> Any:
    """Runs a sync function ``fn`` in a new greenlet.

    The sync function can then use :func:`await_` to wait for async
    functions.

    :param fn: The sync callable to call.
    :param \\*args: Positional arguments to pass to the ``fn`` callable.
    :param \\*\\*kwargs: Keyword arguments to pass to the ``fn`` callable.
    """

    context = _AsyncIoGreenlet(fn, greenlet.getcurrent())
    # runs the function synchronously in gl greenlet. If the execution
    # is interrupted by await_, context is not dead and result is a
    # coroutine to wait. If the context is dead the function has
    # returned, and its result can be returned.
    switch_occurred = False
    try:
        result = context.switch(*args, **kwargs)
        while not context.dead:
            switch_occurred = True
            try:
                # wait for a coroutine from await_ and then return its
                # result back to it.
                value = await result
            except Exception:
                # this allows an exception to be raised within
                # the moderated greenlet so that it can continue
                # its expected flow.
                result = context.throw(*sys.exc_info())
            else:
                result = context.switch(value)
    finally:
        # clean up to avoid cycle resolution by gc
        del context.driver
    if _require_await and not switch_occurred:
        raise exc.AwaitRequired(
            "The current operation required an async execution but none was "
            "detected. This will usually happen when using a non compatible "
            "DBAPI driver. Please ensure that an async DBAPI is used."
        )
    return result


class AsyncAdaptedLock:
    def __init__(self):
        self.mutex = asyncio.Lock()

    def __enter__(self):
        await_fallback(self.mutex.acquire())
        return self

    def __exit__(self, *arg, **kw):
        self.mutex.release()


def _util_async_run_coroutine_function(fn, *args, **kwargs):
    """for test suite/ util only"""

    loop = asyncio.get_event_loop()
    if loop.is_running():
        raise Exception(
            "for async run coroutine we expect that no greenlet or event "
            "loop is running when we start out"
        )
    return loop.run_until_complete(fn(*args, **kwargs))


def _util_async_run(fn, *args, **kwargs):
    """for test suite/ util only"""

    loop = asyncio.get_event_loop()
    if not loop.is_running():
        return loop.run_until_complete(greenlet_spawn(fn, *args, **kwargs))
    else:
        # allow for a wrapped test function to call another
        assert isinstance(greenlet.getcurrent(), _AsyncIoGreenlet)
        return fn(*args, **kwargs)
