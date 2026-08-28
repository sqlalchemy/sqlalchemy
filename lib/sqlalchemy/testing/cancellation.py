# testing/cancellation.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

"""Deterministic cancellation testing for the asyncio dialects.

An asyncio task may be cancelled at any ``await``, and reproducing a
specific one of those points by racing ``task.cancel()`` against real
network traffic is unreliable.  Here the choice is made exact.
:func:`.greenlet_spawn` drives every await performed by an asyncio-driven
SQLAlchemy operation, ``connect()`` included, so instrumenting the greenlet
it switches into gives a stable enumeration of a scenario's IO points, and
cancelling at point ``N`` is reproducible run to run.

What is asserted afterwards is that nothing was leaked: every DBAPI
connection a pool creates must eventually be passed successfully through
``Pool._close_connection()``.  A connection that isn't has become
unreachable, and for an asyncio driver the garbage collector cannot close
it either, since closing needs the event loop.

Pool *availability* is deliberately not asserted.  A cancellation can leave
a record checked out until the ``_ConnectionFairy`` is collected and
``_finalize_fairy()`` hands it back, so what the pool has available
immediately afterwards is a function of garbage collection timing rather
than of anything the dialect did.

"""

from __future__ import annotations

import asyncio
import contextlib
import gc
from unittest import mock

from .. import exc
from ..util import concurrency


class _InstrumentedAwaitable:
    """An awaitable that invokes ``hook`` when it is awaited."""

    __slots__ = ("awaitable", "hook")

    def __init__(self, awaitable, hook):
        self.awaitable = awaitable
        self.hook = hook

    def __await__(self):
        self.hook(self.awaitable)
        return self.awaitable.__await__()


class AwaitPoints:
    """Counts await points, and cancels the running task at one of them."""

    def __init__(self, cancel_at=None):
        self.count = 0
        self.cancel_at = cancel_at
        self.cancelled_at = None

    def __call__(self, awaitable):
        self.count += 1
        if self.count != self.cancel_at:
            return

        self.cancelled_at = getattr(awaitable, "__qualname__", repr(awaitable))

        task = asyncio.current_task()
        assert task is not None, (
            "cancellation can only be delivered to an operation running "
            "within an asyncio task"
        )

        # cancel the task rather than raise CancelledError from here: the
        # awaitable then still runs, and asyncio interrupts it at its own
        # first suspension point, as it does in production
        task.cancel()


@contextlib.contextmanager
def await_points(cancel_at=None):
    """Count the await points performed within the block, cancelling the
    running task at ``cancel_at`` if given."""

    points = AwaitPoints(cancel_at)
    greenlet_cls = concurrency._concurrency_shim._AsyncIoGreenlet
    real_switch = greenlet_cls.switch
    real_throw = greenlet_cls.throw

    def instrument(greenlet, result):
        # a greenlet that has ended hands back the return value of the
        # function it ran, which nothing awaits
        if greenlet.dead:
            return result
        return _InstrumentedAwaitable(result, points)

    def switch(self, *arg, **kw):
        return instrument(self, real_switch(self, *arg, **kw))

    def throw(self, *arg):
        return instrument(self, real_throw(self, *arg))

    with (
        mock.patch.object(greenlet_cls, "switch", switch),
        mock.patch.object(greenlet_cls, "throw", throw),
    ):
        yield points


class ConnectionAccounting:
    """Records which DBAPI connections a pool created and which it managed
    to close."""

    def __init__(self):
        self.created = set()
        self.closed = set()

    @property
    def leaked(self):
        """Connections that were created but never successfully closed."""

        return self.created - self.closed


@contextlib.contextmanager
def connection_accounting(engine):
    """Account for the DBAPI connections created by ``engine``'s pool.

    Creation is observed at ``Pool._invoke_creator`` rather than through the
    ``connect`` pool event, because the ``connect`` event is itself where
    ``dialect.initialize()`` runs; a listener added there does not fire for
    precisely the connections that a failure in that event strands.

    """
    accounting = ConnectionAccounting()
    pool = engine.pool
    real_creator = pool._invoke_creator
    real_close = pool._close_connection

    def _invoke_creator(rec):
        dbapi_connection = real_creator(rec)
        accounting.created.add(dbapi_connection)
        return dbapi_connection

    def _close_connection(dbapi_connection, *, terminate=False):
        real_close(dbapi_connection, terminate=terminate)
        accounting.closed.add(dbapi_connection)

    with (
        mock.patch.object(pool, "_invoke_creator", _invoke_creator),
        mock.patch.object(pool, "_close_connection", _close_connection),
    ):
        yield accounting


async def run_scenario(engine_factory, scenario, cancel_at=None, warm=False):
    """Run ``scenario`` against a fresh engine, optionally cancelling it.

    ``engine_factory`` is a zero-argument callable returning a new
    :class:`_asyncio.AsyncEngine`; ``scenario`` is a coroutine function
    taking that engine.  If ``warm`` is True the scenario is run once
    un-cancelled first, so that the await points counted are those of
    checkout, execution and return-to-pool rather than those of
    ``connect()``.

    Returns the :class:`.AwaitPoints` for the run, and a description of
    what the cancellation cost the pool, or ``None`` if it cost nothing.

    """
    engine = engine_factory()
    with connection_accounting(engine) as accounting:
        if warm:
            await scenario(engine)

        with await_points(cancel_at) as points:
            try:
                await asyncio.create_task(scenario(engine))
            except BaseException:
                # the cancellation itself, or whatever the dialect raised
                # in response to it.  What the scenario raised is not the
                # thing under test; what became of the connection is.
                #
                # if nothing here asked for a cancellation, this is not
                # ours; let it out.  asyncio.Runner delivers the first
                # Ctrl-C as a cancellation of the main task rather than as
                # KeyboardInterrupt, so swallowing everything here would
                # otherwise absorb it and carry on sweeping
                if points.cancelled_at is None:
                    raise

        gc.collect()
        try:
            await engine.dispose()
        except exc.SQLAlchemyError:
            # a pool holding damaged connections may not be able to close
            # them either; that is reported as a leak below
            pass

        if accounting.leaked:
            return points, (
                "%d of %d connection(s) were left open with nothing able "
                "to close them"
                % (len(accounting.leaked), len(accounting.created))
            )

    return points, None


async def sweep(engine_factory, scenario, warm=False):
    """Cancel ``scenario`` at each of its await points in turn.

    Returns a list of strings describing what went wrong, empty if every
    cancellation point was handled cleanly.  The strings are meant to be
    read in an assertion failure::

        eq_(await sweep(engine_factory, scenario), [])

    """
    points, _ = await run_scenario(engine_factory, scenario, warm=warm)
    total = points.count

    reported = []
    for index in range(1, total + 1):
        points, failure = await run_scenario(
            engine_factory, scenario, cancel_at=index, warm=warm
        )
        if failure is not None:
            reported.append(
                "cancel at await point %d of %d (%s): %s"
                % (index, total, points.cancelled_at, failure)
            )
    return reported
