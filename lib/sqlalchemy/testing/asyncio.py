from .assertions import assert_raises as _assert_raises
from .assertions import assert_raises_message as _assert_raises_message
from ..util import await_fallback as await_
from ..util import greenlet_spawn


async def assert_raises_async(except_cls, msg, coroutine):
    await greenlet_spawn(_assert_raises, except_cls, await_, coroutine)


async def assert_raises_message_async(except_cls, msg, coroutine):
    await greenlet_spawn(
        _assert_raises_message, except_cls, msg, await_, coroutine
    )
