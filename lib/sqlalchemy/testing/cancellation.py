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

The cancellation is delivered to the task the application is running,
which is not necessarily the task performing the await; SQLAlchemy runs
some operations, such as the ``close()`` performed by an async context
manager on exit, in a task of its own.

Two things are asserted afterwards:

* Every DBAPI connection the pool creates is eventually closed through
  ``Pool._close_connection()``.  A connection that is never closed is
  leaked; for an asyncio driver the garbage collector cannot close it
  either, since closing requires the event loop.

* Once the application's task has finished and garbage has been
  collected, no pool slot is still checked out.  A slot still checked out
  at that point is lost: either nothing remains that could return it, or
  it is being returned by a task SQLAlchemy left running in the
  background, which the application has no way to wait for.  In the
  latter case, an application that disposes of its engine at that point
  has the connection returned into the disposed pool afterwards, where
  nothing closes it.

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
    """Counts await points, and cancels the running task at each of the
    ones given.

    ``cancel_at`` is a tuple of await point numbers; each one is a separate
    ``task.cancel()`` call.  asyncio delivers a given cancellation exactly
    once, so a second number models a second, independent canceller
    reaching a task that is still cleaning up after the first.

    """

    def __init__(self, cancel_at=()):
        self._count = 0
        self._cancel_at = cancel_at
        self._cancelled_at = []
        self._task = None

    @property
    def count(self):
        """The number of await points counted so far."""

        return self._count

    @property
    def cancelled_at(self):
        """Descriptions of the awaitables at which a cancellation was
        delivered."""

        return tuple(self._cancelled_at)

    def create_task(self, coro):
        """Run ``coro`` in a new task, which is the task that cancellations
        are delivered to."""

        self._task = asyncio.create_task(coro)
        return self._task

    def __call__(self, awaitable):
        self._count += 1
        if self._count not in self._cancel_at:
            return

        self._cancelled_at.append(
            getattr(awaitable, "__qualname__", repr(awaitable))
        )

        # cancel the task the application is running, which is not
        # necessarily the one performing this await; SQLAlchemy runs some
        # operations, such as the close() performed by an async context
        # manager on exit, in a task of their own
        task = self._task if self._task is not None else asyncio.current_task()
        assert task is not None, (
            "cancellation can only be delivered to an operation running "
            "within an asyncio task"
        )

        # cancel the task rather than raise CancelledError from here: the
        # awaitable then still runs, and asyncio interrupts it at its own
        # first suspension point, as it does in production
        task.cancel()


@contextlib.contextmanager
def await_points(cancel_at=()):
    """Count the await points performed within the block, cancelling the
    running task at each of ``cancel_at``."""

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
    to close, and how many of the pool's slots were lost.

    Creation is observed at ``Pool._invoke_creator`` rather than through the
    ``connect`` pool event, because the ``connect`` event is itself where
    ``dialect.initialize()`` runs; a listener added there does not fire for
    precisely the connections that a failure in that event strands.

    """

    def __init__(self, pool):
        self._pool = pool
        self._real_invoke_creator = pool._invoke_creator
        self._real_close_connection = pool._close_connection
        self._lost_slots = 0
        self._created = set()
        self._closed = set()

    @contextlib.contextmanager
    def start(self):
        """Account for the pool's connections within the block."""

        with (
            mock.patch.object(
                self._pool, "_invoke_creator", self._invoke_creator
            ),
            mock.patch.object(
                self._pool, "_close_connection", self._close_connection
            ),
        ):
            yield self

    def _invoke_creator(self, rec):
        dbapi_connection = self._real_invoke_creator(rec)
        self._created.add(dbapi_connection)
        return dbapi_connection

    def _close_connection(self, dbapi_connection, *, terminate=False):
        try:
            self._real_close_connection(dbapi_connection, terminate=terminate)
        except asyncio.CancelledError:
            # AsyncAdapt_terminate force-closes the connection before
            # letting a cancellation of its graceful close propagate
            if terminate and self._pool._dialect.has_terminate:
                self._closed.add(dbapi_connection)
            raise
        self._closed.add(dbapi_connection)

    @property
    def leaked(self):
        """Connections that were created but never successfully closed."""

        return self._created - self._closed

    def record_lost_slots(self):
        """Record the pool slots still checked out.

        To be called once the application's task is done, and before the
        engine is disposed.  Every :class:`._ConnectionFairy` the task made
        is gone once garbage is collected, and :func:`._finalize_fairy` has
        returned its record; a record still checked out after that is lost.

        """
        gc.collect()
        self._lost_slots = self._pool.checkedout()

    def get_failure_messages(self):
        """Return a list of strings describing each leaked connection and
        lost pool slot, empty if there were none."""

        failures = []
        if self.leaked:
            failures.append(
                f"{len(self.leaked)} of {len(self._created)} connection(s) "
                "were left open with nothing able to close them"
            )
        if self._lost_slots > 0:
            failures.append(
                f"{self._lost_slots} pool slot(s) were left checked out "
                "with nothing able to return them"
            )
        return failures


def connection_accounting(engine):
    """Account for the DBAPI connections created by ``engine``'s pool.

    Returns a context manager yielding a :class:`.ConnectionAccounting`.

    """
    return ConnectionAccounting(engine.pool).start()


async def run_scenario(
    engine_factory, scenario, cancel_at=(), warm=False, recycle=False
):
    """Run ``scenario`` against a fresh engine, optionally cancelling it.

    ``engine_factory`` is a zero-argument callable returning a new
    :class:`_asyncio.AsyncEngine`; ``scenario`` is a coroutine function
    taking that engine.  If ``warm`` is True the scenario is run once
    un-cancelled first, so that the await points counted are those of
    checkout, execution and return-to-pool rather than those of
    ``connect()``.  If ``recycle`` is also True, the warmed pool is then
    invalidated, so that the checkout counted closes the old connection
    before opening its replacement.

    Returns the :class:`.AwaitPoints` for the run, and a description of
    what the cancellation cost the pool, or ``None`` if it cost nothing.

    """
    assert warm or not recycle, "recycle requires 'warm'"

    engine = engine_factory()
    with connection_accounting(engine) as accounting:
        if warm:
            await scenario(engine)
            if recycle:
                engine.pool._invalidate(None)

        with await_points(cancel_at) as points:
            try:
                await points.create_task(scenario(engine))
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
                if not points.cancelled_at:
                    raise

        # the scenario is over as far as the application can tell
        accounting.record_lost_slots()

        # the application may now dispose of the engine
        try:
            await engine.dispose()
        except exc.SQLAlchemyError:
            # a pool holding damaged connections may not be able to close
            # them either; that is reported as a leak below
            pass

        # let anything that was left running in the background finish, so
        # that a connection it returns to the disposed pool is counted as
        # the leak that it is
        others = asyncio.all_tasks() - {asyncio.current_task()}
        if others:
            await asyncio.wait(others, timeout=5)
        gc.collect()

    return points, "; ".join(accounting.get_failure_messages()) or None


async def sweep(
    engine_factory, scenario, warm=False, recycle=False, cancels=1
):
    """Cancel ``scenario`` at each of its await points in turn.

    With ``cancels=2``, each run that was cancelled at point ``N`` is then
    repeated with a second cancellation at each await point after ``N``,
    which is to say at each point of the cleanup the first cancellation
    set off.

    Returns a list of strings describing what went wrong, empty if every
    cancellation point was handled cleanly.  The strings are meant to be
    read in an assertion failure::

        eq_(await sweep(engine_factory, scenario), [])

    """

    reported = []

    async def _sweep(cancel_at):
        points, _ = await run_scenario(
            engine_factory,
            scenario,
            cancel_at=cancel_at,
            warm=warm,
            recycle=recycle,
        )
        total = points.count
        start = cancel_at[-1] + 1 if cancel_at else 1
        for index in range(start, total + 1):
            points, failure = await run_scenario(
                engine_factory,
                scenario,
                cancel_at=cancel_at + (index,),
                warm=warm,
                recycle=recycle,
            )
            if failure is not None:
                indexes = ", ".join(str(i) for i in cancel_at + (index,))
                awaitables = ", ".join(points.cancelled_at)
                reported.append(
                    f"cancel at await point(s) {indexes} of {total} "
                    f"({awaitables}): {failure}"
                )
            if len(cancel_at) + 1 < cancels:
                await _sweep(cancel_at + (index,))

    await _sweep(())
    return reported
