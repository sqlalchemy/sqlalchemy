from . import compat

have_greenlet = False

if compat.py3k:
    try:
        import greenlet  # noqa F401
    except ImportError:
        pass
    else:
        have_greenlet = True
        from ._concurrency_py3k import await_only
        from ._concurrency_py3k import await_fallback
        from ._concurrency_py3k import greenlet_spawn
        from ._concurrency_py3k import AsyncAdaptedLock
        from ._concurrency_py3k import _util_async_run  # noqa F401
        from ._concurrency_py3k import (
            _util_async_run_coroutine_function,
        )  # noqa F401, E501
        from ._concurrency_py3k import asyncio  # noqa F401

if not have_greenlet:

    asyncio = None  # noqa F811

    def _not_implemented():
        if not compat.py3k:
            raise ValueError("Cannot use this function in py2.")
        else:
            raise ValueError(
                "the greenlet library is required to use this function."
            )

    def await_only(thing):  # noqa F811
        _not_implemented()

    def await_fallback(thing):  # noqa F81
        return thing

    def greenlet_spawn(fn, *args, **kw):  # noqa F81
        _not_implemented()

    def AsyncAdaptedLock(*args, **kw):  # noqa F81
        _not_implemented()

    def _util_async_run(fn, *arg, **kw):  # noqa F81
        return fn(*arg, **kw)

    def _util_async_run_coroutine_function(fn, *arg, **kw):  # noqa F81
        _not_implemented()
