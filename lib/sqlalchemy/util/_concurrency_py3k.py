import asyncio
import sys
from typing import Any
from typing import Callable
from typing import Coroutine

from .. import exc

try:
    import greenlet

    # implementation based on snaury gist at
    # https://gist.github.com/snaury/202bf4f22c41ca34e56297bae5f33fef
    # Issue for context: https://github.com/python-greenlet/greenlet/issues/173

    class _AsyncIoGreenlet(greenlet.greenlet):
        def __init__(self, fn, driver):
            greenlet.greenlet.__init__(self, fn, driver)
            self.driver = driver

    def await_only(awaitable: Coroutine) -> Any:
        """Awaits an async function in a sync method.

        The sync method must be insice a :func:`greenlet_spawn` context.
        :func:`await_` calls cannot be nested.

        :param awaitable: The coroutine to call.

        """
        # this is called in the context greenlet while running fn
        current = greenlet.getcurrent()
        if not isinstance(current, _AsyncIoGreenlet):
            raise exc.InvalidRequestError(
                "greenlet_spawn has not been called; can't call await_() here."
            )

        # returns the control to the driver greenlet passing it
        # a coroutine to run. Once the awaitable is done, the driver greenlet
        # switches back to this greenlet with the result of awaitable that is
        # then returned to the caller (or raised as error)
        return current.driver.switch(awaitable)

    def await_fallback(awaitable: Coroutine) -> Any:
        """Awaits an async function in a sync method.

        The sync method must be insice a :func:`greenlet_spawn` context.
        :func:`await_` calls cannot be nested.

        :param awaitable: The coroutine to call.

        """
        # this is called in the context greenlet while running fn
        current = greenlet.getcurrent()
        if not isinstance(current, _AsyncIoGreenlet):
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise exc.InvalidRequestError(
                    "greenlet_spawn has not been called and asyncio event "
                    "loop is already running; can't call await_() here."
                )
            return loop.run_until_complete(awaitable)

        return current.driver.switch(awaitable)

    async def greenlet_spawn(fn: Callable, *args, **kwargs) -> Any:
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
        try:
            result = context.switch(*args, **kwargs)
            while not context.dead:
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
        return result


except ImportError:  # pragma: no cover
    greenlet = None

    def await_fallback(awaitable):
        return asyncio.get_event_loop().run_until_complete(awaitable)

    def await_only(awaitable):
        raise ValueError("Greenlet is required to use this function")

    async def greenlet_spawn(fn, *args, **kw):
        raise ValueError("Greenlet is required to use this function")


class AsyncAdaptedLock:
    def __init__(self):
        self.mutex = asyncio.Lock()

    def __enter__(self):
        await_fallback(self.mutex.acquire())
        return self

    def __exit__(self, *arg, **kw):
        self.mutex.release()
