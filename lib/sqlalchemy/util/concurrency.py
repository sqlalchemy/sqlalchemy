# util/concurrency.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: allow-untyped-defs, allow-untyped-calls

"""asyncio-related concurrency functions."""

from __future__ import annotations

import asyncio
import sys
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Coroutine
from typing import Literal
from typing import NoReturn
from typing import TYPE_CHECKING
from typing import TypeGuard
from typing import TypeVar
from typing import Union

from .compat import py311
from .langhelpers import memoized_property
from .typing import Self
from .. import exc

_T = TypeVar("_T")


def is_exit_exception(e: BaseException) -> bool:
    # note asyncio.CancelledError is already BaseException
    # so was an exit exception in any case
    return not isinstance(e, Exception) or isinstance(
        e, (asyncio.TimeoutError, asyncio.CancelledError)
    )


_ERROR_MESSAGE = (
    "The SQLAlchemy asyncio module requires that the Python 'greenlet' "
    "library is installed.  In order to ensure this dependency is "
    "available, use the 'sqlalchemy[asyncio]' install target:  "
    "'pip install sqlalchemy[asyncio]'"
)


def _not_implemented(*arg: Any, **kw: Any) -> NoReturn:
    raise ImportError(_ERROR_MESSAGE)


class _concurrency_shim_cls:
    """Late import shim for greenlet"""

    __slots__ = (
        "_has_greenlet",
        "greenlet",
        "_AsyncIoGreenlet",
        "getcurrent",
    )

    def _initialize(self, *, raise_: bool = True) -> None:
        """Import greenlet and initialize the class"""
        if "greenlet" in globals():
            return

        if not TYPE_CHECKING:
            global getcurrent, greenlet, _AsyncIoGreenlet
            global _has_gr_context

        try:
            from greenlet import getcurrent
            from greenlet import greenlet
        except ImportError as e:
            if not TYPE_CHECKING:
                # set greenlet in the global scope to prevent re-init
                greenlet = None
            self._has_greenlet = False
            self._initialize_no_greenlet()
            if raise_:
                raise ImportError(_ERROR_MESSAGE) from e
        else:
            self._has_greenlet = True
            # If greenlet.gr_context is present in current version of greenlet,
            # it will be set with the current context on creation.
            # Refs: https://github.com/python-greenlet/greenlet/pull/198
            _has_gr_context = hasattr(getcurrent(), "gr_context")

            # implementation based on snaury gist at
            # https://gist.github.com/snaury/202bf4f22c41ca34e56297bae5f33fef
            # Issue for context: https://github.com/python-greenlet/greenlet/issues/173 # noqa: E501

            class _AsyncIoGreenlet(greenlet):
                dead: bool

                __sqlalchemy_greenlet_provider__ = True

                def __init__(self, fn: Callable[..., Any], driver: greenlet):
                    greenlet.__init__(self, fn, driver)
                    if _has_gr_context:
                        self.gr_context = driver.gr_context

            self.greenlet = greenlet
            self.getcurrent = getcurrent
            self._AsyncIoGreenlet = _AsyncIoGreenlet

    def _initialize_no_greenlet(self):
        self.getcurrent = _not_implemented
        self.greenlet = _not_implemented  # type: ignore[assignment]
        self._AsyncIoGreenlet = _not_implemented  # type: ignore[assignment]

    def __getattr__(self, key: str) -> Any:
        if key in self.__slots__:
            self._initialize()
            return getattr(self, key)
        else:
            raise AttributeError(key)


_concurrency_shim = _concurrency_shim_cls()

if TYPE_CHECKING:
    _T_co = TypeVar("_T_co", covariant=True)

    def iscoroutine(
        awaitable: Awaitable[_T_co],
    ) -> TypeGuard[Coroutine[Any, Any, _T_co]]: ...

else:
    iscoroutine = asyncio.iscoroutine


def _safe_cancel_awaitable(awaitable: Awaitable[Any]) -> None:
    # https://docs.python.org/3/reference/datamodel.html#coroutine.close

    if iscoroutine(awaitable):
        awaitable.close()


def in_greenlet() -> bool:
    current = _concurrency_shim.getcurrent()
    return getattr(current, "__sqlalchemy_greenlet_provider__", False)


def await_(awaitable: Awaitable[_T]) -> _T:
    """Awaits an async function in a sync method.

    The sync method must be inside a :func:`greenlet_spawn` context.
    :func:`await_` calls cannot be nested.

    :param awaitable: The coroutine to call.

    """
    # this is called in the context greenlet while running fn
    current = _concurrency_shim.getcurrent()
    if not getattr(current, "__sqlalchemy_greenlet_provider__", False):
        _safe_cancel_awaitable(awaitable)

        raise exc.MissingGreenlet(
            "greenlet_spawn has not been called; can't call await_() "
            "here. Was IO attempted in an unexpected place?"
        )

    # returns the control to the driver greenlet passing it
    # a coroutine to run. Once the awaitable is done, the driver greenlet
    # switches back to this greenlet with the result of awaitable that is
    # then returned to the caller (or raised as error)
    assert current.parent
    return current.parent.switch(awaitable)  # type: ignore[no-any-return]


await_only = await_  # old name. deprecated on 2.2


async def greenlet_spawn(
    fn: Callable[..., _T],
    *args: Any,
    _require_await: bool = False,
    **kwargs: Any,
) -> _T:
    """Runs a sync function ``fn`` in a new greenlet.

    The sync function can then use :func:`await_` to wait for async
    functions.

    :param fn: The sync callable to call.
    :param \\*args: Positional arguments to pass to the ``fn`` callable.
    :param \\*\\*kwargs: Keyword arguments to pass to the ``fn`` callable.
    """

    result: Any
    context = _concurrency_shim._AsyncIoGreenlet(
        fn, _concurrency_shim.getcurrent()
    )
    # runs the function synchronously in gl greenlet. If the execution
    # is interrupted by await_, context is not dead and result is a
    # coroutine to wait. If the context is dead the function has
    # returned, and its result can be returned.
    switch_occurred = False

    result = context.switch(*args, **kwargs)
    while not context.dead:
        switch_occurred = True
        try:
            # wait for a coroutine from await_ and then return its
            # result back to it.
            value = await result
        except BaseException:
            # this allows an exception to be raised within
            # the moderated greenlet so that it can continue
            # its expected flow.
            result = context.throw(*sys.exc_info())
        else:
            result = context.switch(value)

    if _require_await and not switch_occurred:
        raise exc.AwaitRequired(
            "The current operation required an async execution but none was "
            "detected. This will usually happen when using a non compatible "
            "DBAPI driver. Please ensure that an async DBAPI is used."
        )
    return result  # type: ignore[no-any-return]


class AsyncAdaptedLock:
    @memoized_property
    def mutex(self) -> asyncio.Lock:
        # there should not be a race here for coroutines creating the
        # new lock as we are not using await, so therefore no concurrency
        return asyncio.Lock()

    def __enter__(self) -> bool:
        # await is used to acquire the lock only after the first calling
        # coroutine has created the mutex.
        return await_(self.mutex.acquire())

    def __exit__(self, *arg: Any, **kw: Any) -> None:
        self.mutex.release()


if not TYPE_CHECKING and py311:
    _Runner = asyncio.Runner
else:

    class _Runner:
        """Runner implementation for test only"""

        _loop: Union[None, asyncio.AbstractEventLoop, Literal[False]]

        def __init__(self) -> None:
            self._loop = None

        def __enter__(self) -> Self:
            self._lazy_init()
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.close()

        def close(self) -> None:
            if self._loop:
                try:
                    self._loop.run_until_complete(
                        self._loop.shutdown_asyncgens()
                    )
                finally:
                    self._loop.close()
                    self._loop = False

        def get_loop(self) -> asyncio.AbstractEventLoop:
            """Return embedded event loop."""
            self._lazy_init()
            assert self._loop
            return self._loop

        def run(self, coro: Coroutine[Any, Any, _T]) -> _T:
            self._lazy_init()
            assert self._loop
            return self._loop.run_until_complete(coro)

        def _lazy_init(self) -> None:
            if self._loop is False:
                raise RuntimeError("Runner is closed")
            if self._loop is None:
                self._loop = asyncio.new_event_loop()


class _AsyncUtil:
    """Asyncio util for test suite/ util only"""

    def __init__(self) -> None:
        self.runner = _Runner()  # runner it lazy so it can be created here

    def run(
        self,
        fn: Callable[..., Coroutine[Any, Any, _T]],
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        """Run coroutine on the loop"""
        return self.runner.run(fn(*args, **kwargs))

    def run_in_greenlet(
        self, fn: Callable[..., _T], *args: Any, **kwargs: Any
    ) -> _T:
        """Run sync function in greenlet. Support nested calls"""
        _concurrency_shim._initialize(raise_=False)

        if _concurrency_shim._has_greenlet:
            if self.runner.get_loop().is_running():
                # allow for a wrapped test function to call another
                assert in_greenlet()
                return fn(*args, **kwargs)
            else:
                return self.runner.run(greenlet_spawn(fn, *args, **kwargs))
        else:
            return fn(*args, **kwargs)

    def close(self) -> None:
        self.runner.close()
