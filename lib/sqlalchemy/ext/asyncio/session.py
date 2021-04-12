# ext/asyncio/session.py
# Copyright (C) 2020-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from typing import Any
from typing import Callable
from typing import Mapping
from typing import Optional
from typing import TypeVar

from . import engine
from . import result as _result
from .base import StartableContext
from .engine import AsyncEngine
from ... import util
from ...engine import Result
from ...orm import Session
from ...sql import Executable
from ...util.concurrency import greenlet_spawn


T = TypeVar("T")


@util.create_proxy_methods(
    Session,
    ":class:`_orm.Session`",
    ":class:`_asyncio.AsyncSession`",
    classmethods=["object_session", "identity_key"],
    methods=[
        "__contains__",
        "__iter__",
        "add",
        "add_all",
        "expire",
        "expire_all",
        "expunge",
        "expunge_all",
        "get_bind",
        "is_modified",
        "in_transaction",
    ],
    attributes=[
        "dirty",
        "deleted",
        "new",
        "identity_map",
        "is_active",
        "autoflush",
        "no_autoflush",
        "info",
    ],
)
class AsyncSession:
    """Asyncio version of :class:`_orm.Session`.


    .. versionadded:: 1.4

    """

    __slots__ = (
        "binds",
        "bind",
        "sync_session",
        "_proxied",
        "_slots_dispatch",
    )

    dispatch = None

    def __init__(
        self,
        bind: AsyncEngine = None,
        binds: Mapping[object, AsyncEngine] = None,
        **kw
    ):
        kw["future"] = True
        if bind:
            self.bind = bind
            bind = engine._get_sync_engine_or_connection(bind)

        if binds:
            self.binds = binds
            binds = {
                key: engine._get_sync_engine_or_connection(b)
                for key, b in binds.items()
            }

        self.sync_session = self._proxied = Session(
            bind=bind, binds=binds, **kw
        )

    async def refresh(
        self, instance, attribute_names=None, with_for_update=None
    ):
        """Expire and refresh the attributes on the given instance.

        A query will be issued to the database and all attributes will be
        refreshed with their current database value.

        This is the async version of the :meth:`_orm.Session.refresh` method.
        See that method for a complete description of all options.

        """

        return await greenlet_spawn(
            self.sync_session.refresh,
            instance,
            attribute_names=attribute_names,
            with_for_update=with_for_update,
        )

    async def run_sync(self, fn: Callable[..., T], *arg, **kw) -> T:
        """Invoke the given sync callable passing sync self as the first
        argument.

        This method maintains the asyncio event loop all the way through
        to the database connection by running the given callable in a
        specially instrumented greenlet.

        E.g.::

            with AsyncSession(async_engine) as session:
                await session.run_sync(some_business_method)

        .. note::

            The provided callable is invoked inline within the asyncio event
            loop, and will block on traditional IO calls.  IO within this
            callable should only call into SQLAlchemy's asyncio database
            APIs which will be properly adapted to the greenlet context.

        .. seealso::

            :ref:`session_run_sync`
        """

        return await greenlet_spawn(fn, self.sync_session, *arg, **kw)

    async def execute(
        self,
        statement: Executable,
        params: Optional[Mapping] = None,
        execution_options: Mapping = util.EMPTY_DICT,
        bind_arguments: Optional[Mapping] = None,
        **kw
    ) -> Result:
        """Execute a statement and return a buffered
        :class:`_engine.Result` object."""

        execution_options = execution_options.union({"prebuffer_rows": True})

        return await greenlet_spawn(
            self.sync_session.execute,
            statement,
            params=params,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
            **kw
        )

    async def scalar(
        self,
        statement: Executable,
        params: Optional[Mapping] = None,
        execution_options: Mapping = util.EMPTY_DICT,
        bind_arguments: Optional[Mapping] = None,
        **kw
    ) -> Any:
        """Execute a statement and return a scalar result."""

        result = await self.execute(
            statement,
            params=params,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
            **kw
        )
        return result.scalar()

    async def get(
        self,
        entity,
        ident,
        options=None,
        populate_existing=False,
        with_for_update=None,
        identity_token=None,
    ):
        """Return an instance based on the given primary key identifier,
        or ``None`` if not found.


        """
        return await greenlet_spawn(
            self.sync_session.get,
            entity,
            ident,
            options=options,
            populate_existing=populate_existing,
            with_for_update=with_for_update,
            identity_token=identity_token,
        )

    async def stream(
        self,
        statement,
        params=None,
        execution_options=util.EMPTY_DICT,
        bind_arguments=None,
        **kw
    ):
        """Execute a statement and return a streaming
        :class:`_asyncio.AsyncResult` object."""

        execution_options = execution_options.union({"stream_results": True})

        result = await greenlet_spawn(
            self.sync_session.execute,
            statement,
            params=params,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
            **kw
        )
        return _result.AsyncResult(result)

    async def delete(self, instance):
        """Mark an instance as deleted.

        The database delete operation occurs upon ``flush()``.

        As this operation may need to cascade along unloaded relationships,
        it is awaitable to allow for those queries to take place.


        """
        return await greenlet_spawn(self.sync_session.delete, instance)

    async def merge(self, instance, load=True):
        """Copy the state of a given instance into a corresponding instance
        within this :class:`_asyncio.AsyncSession`.

        """
        return await greenlet_spawn(
            self.sync_session.merge, instance, load=load
        )

    async def flush(self, objects=None):
        """Flush all the object changes to the database.

        .. seealso::

            :meth:`_orm.Session.flush`

        """
        await greenlet_spawn(self.sync_session.flush, objects=objects)

    async def connection(self):
        r"""Return a :class:`_asyncio.AsyncConnection` object corresponding to this
        :class:`.Session` object's transactional state.

        """

        # POSSIBLY TODO: here, we see that the sync engine / connection
        # that are generated from AsyncEngine / AsyncConnection don't
        # provide any backlink from those sync objects back out to the
        # async ones.   it's not *too* big a deal since AsyncEngine/Connection
        # are just proxies and all the state is actually in the sync
        # version of things.  However!  it has to stay that way :)
        sync_connection = await greenlet_spawn(self.sync_session.connection)
        return engine.AsyncConnection(
            engine.AsyncEngine(sync_connection.engine), sync_connection
        )

    def begin(self, **kw):
        """Return an :class:`_asyncio.AsyncSessionTransaction` object.

        The underlying :class:`_orm.Session` will perform the
        "begin" action when the :class:`_asyncio.AsyncSessionTransaction`
        object is entered::

            async with async_session.begin():
                # .. ORM transaction is begun

        Note that database IO will not normally occur when the session-level
        transaction is begun, as database transactions begin on an
        on-demand basis.  However, the begin block is async to accommodate
        for a :meth:`_orm.SessionEvents.after_transaction_create`
        event hook that may perform IO.

        For a general description of ORM begin, see
        :meth:`_orm.Session.begin`.

        """

        return AsyncSessionTransaction(self)

    def begin_nested(self, **kw):
        """Return an :class:`_asyncio.AsyncSessionTransaction` object
        which will begin a "nested" transaction, e.g. SAVEPOINT.

        Behavior is the same as that of :meth:`_asyncio.AsyncSession.begin`.

        For a general description of ORM begin nested, see
        :meth:`_orm.Session.begin_nested`.

        """

        return AsyncSessionTransaction(self, nested=True)

    async def rollback(self):
        """Rollback the current transaction in progress."""
        return await greenlet_spawn(self.sync_session.rollback)

    async def commit(self):
        """Commit the current transaction in progress."""
        return await greenlet_spawn(self.sync_session.commit)

    async def close(self):
        """Close this :class:`_asyncio.AsyncSession`."""
        return await greenlet_spawn(self.sync_session.close)

    @classmethod
    async def close_all(self):
        """Close all :class:`_asyncio.AsyncSession` sessions."""
        return await greenlet_spawn(self.sync_session.close_all)

    async def __aenter__(self):
        return self

    async def __aexit__(self, type_, value, traceback):
        await self.close()

    def _maker_context_manager(self):
        # no @contextlib.asynccontextmanager until python3.7, gr
        return _AsyncSessionContextManager(self)


class _AsyncSessionContextManager:
    def __init__(self, async_session):
        self.async_session = async_session

    async def __aenter__(self):
        self.trans = self.async_session.begin()
        await self.trans.__aenter__()
        return self.async_session

    async def __aexit__(self, type_, value, traceback):
        await self.trans.__aexit__(type_, value, traceback)
        await self.async_session.__aexit__(type_, value, traceback)


class AsyncSessionTransaction(StartableContext):
    """A wrapper for the ORM :class:`_orm.SessionTransaction` object.

    This object is provided so that a transaction-holding object
    for the :meth:`_asyncio.AsyncSession.begin` may be returned.

    The object supports both explicit calls to
    :meth:`_asyncio.AsyncSessionTransaction.commit` and
    :meth:`_asyncio.AsyncSessionTransaction.rollback`, as well as use as an
    async context manager.


    .. versionadded:: 1.4

    """

    __slots__ = ("session", "sync_transaction", "nested")

    def __init__(self, session, nested=False):
        self.session = session
        self.nested = nested
        self.sync_transaction = None

    @property
    def is_active(self):
        return (
            self._sync_transaction() is not None
            and self._sync_transaction().is_active
        )

    def _sync_transaction(self):
        if not self.sync_transaction:
            self._raise_for_not_started()
        return self.sync_transaction

    async def rollback(self):
        """Roll back this :class:`_asyncio.AsyncTransaction`."""
        await greenlet_spawn(self._sync_transaction().rollback)

    async def commit(self):
        """Commit this :class:`_asyncio.AsyncTransaction`."""

        await greenlet_spawn(self._sync_transaction().commit)

    async def start(self):
        self.sync_transaction = await greenlet_spawn(
            self.session.sync_session.begin_nested
            if self.nested
            else self.session.sync_session.begin
        )
        return self

    async def __aexit__(self, type_, value, traceback):
        return await greenlet_spawn(
            self._sync_transaction().__exit__, type_, value, traceback
        )
