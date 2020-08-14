from . import compat


if compat.py3k:
    import asyncio
    from ._concurrency_py3k import await_only
    from ._concurrency_py3k import await_fallback
    from ._concurrency_py3k import greenlet
    from ._concurrency_py3k import greenlet_spawn
else:
    asyncio = None
    greenlet = None

    def await_only(thing):
        return thing

    def await_fallback(thing):
        return thing

    def greenlet_spawn(fn, *args, **kw):
        raise ValueError("Cannot use this function in py2.")
