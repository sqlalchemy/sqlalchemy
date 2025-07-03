# util/concurrency.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from . import compat

have_greenlet = False
greenlet_error = None

if compat.py3k:
    try:
        import greenlet  # noqa: F401
    except ImportError as e:
        greenlet_error = str(e)
    else:
        have_greenlet = True
        from ._concurrency_py3k import await_only
        from ._concurrency_py3k import await_fallback
        from ._concurrency_py3k import greenlet_spawn
        from ._concurrency_py3k import is_exit_exception
        from ._concurrency_py3k import AsyncAdaptedLock
        from ._concurrency_py3k import _Runner
        from ._concurrency_py3k import asyncio  # noqa: F401

    # does not need greenlet, just Python 3
    from ._compat_py3k import asynccontextmanager  # noqa: F401


class _AsyncUtil:
    """Asyncio util for test suite/ util only"""

    def __init__(self):
        if have_greenlet:
            self.runner = _Runner()

    def run(self, fn, *args, **kwargs):
        """Run coroutine on the loop"""
        return self.runner.run(fn(*args, **kwargs))

    def run_in_greenlet(self, fn, *args, **kwargs):
        """Run sync function in greenlet. Support nested calls"""
        if have_greenlet:
            if self.runner.get_loop().is_running():
                return fn(*args, **kwargs)
            else:
                return self.runner.run(greenlet_spawn(fn, *args, **kwargs))
        else:
            return fn(*args, **kwargs)

    def close(self):
        if have_greenlet:
            self.runner.close()


if not have_greenlet:

    asyncio = None  # noqa: F811

    def _not_implemented():
        # this conditional is to prevent pylance from considering
        # greenlet_spawn() etc as "no return" and dimming out code below it
        if have_greenlet:
            return None

        if not compat.py3k:
            raise ValueError("Cannot use this function in py2.")
        else:
            raise ValueError(
                "the greenlet library is required to use this function."
                " %s" % greenlet_error
                if greenlet_error
                else ""
            )

    def is_exit_exception(e):  # noqa: F811
        return not isinstance(e, Exception)

    def await_only(thing):  # noqa: F811
        _not_implemented()

    def await_fallback(thing):  # noqa: F811
        return thing

    def greenlet_spawn(fn, *args, **kw):  # noqa: F811
        _not_implemented()

    def AsyncAdaptedLock(*args, **kw):  # noqa: F811
        _not_implemented()

    def _util_async_run(fn, *arg, **kw):  # noqa: F811
        return fn(*arg, **kw)

    def _util_async_run_coroutine_function(fn, *arg, **kw):  # noqa: F811
        _not_implemented()
