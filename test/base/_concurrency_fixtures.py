"""Module that defines function that are run in a separate process.
NOTE: the module must not import sqlalchemy at the top level.
"""

import asyncio  # noqa: F401
import sys


def greenlet_not_imported():
    assert "greenlet" not in sys.modules
    assert "sqlalchemy" not in sys.modules

    import sqlalchemy
    import sqlalchemy.util.concurrency  # noqa: F401
    from sqlalchemy.util import greenlet_spawn  # noqa: F401
    from sqlalchemy.util.concurrency import await_  # noqa: F401

    assert "greenlet" not in sys.modules


def greenlet_setup_in_ext():
    assert "greenlet" not in sys.modules
    assert "sqlalchemy" not in sys.modules

    import sqlalchemy.ext.asyncio  # noqa: F401
    from sqlalchemy.util import greenlet_spawn

    assert "greenlet" in sys.modules
    value = -1

    def go(arg):
        nonlocal value
        value = arg

    async def call():
        await greenlet_spawn(go, 42)

    asyncio.run(call())

    assert value == 42


def greenlet_setup_on_call():
    from sqlalchemy.util import greenlet_spawn

    assert "greenlet" not in sys.modules
    value = -1

    def go(arg):
        nonlocal value
        value = arg

    async def call():
        await greenlet_spawn(go, 42)

    asyncio.run(call())

    assert "greenlet" in sys.modules
    assert value == 42
