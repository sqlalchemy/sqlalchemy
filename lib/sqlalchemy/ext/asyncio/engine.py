# ext/asyncio/engine.py
# Copyright (C) 2020-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from typing import Any
from typing import Callable
from typing import Mapping
from typing import Optional

from . import exc as async_exc
from .base import ProxyComparable
from .base import StartableContext
from .result import AsyncResult
from ... import exc
from ... import util
from ...engine import create_engine as _create_engine
from ...engine import Result
from ...engine import Transaction
from ...future import Connection
from ...future import Engine
from ...sql import Executable
from ...util.concurrency import greenlet_spawn


def create_async_engine(*arg, **kw):
    """Create a new async engine instance.

    Arguments passed to :func:`_asyncio.create_async_engine` are mostly
    identical to those passed to the :func:`_sa.create_engine` function.
    The specified dialect must be an asyncio-compatible dialect
    such as :ref:`dialect-postgresql-asyncpg`.

    .. versionadded:: 1.4

    """

    if kw.get("server_side_cursors", False):
        raise async_exc.AsyncMethodRequired(
            "Can't set server_side_cursors for async engine globally; "
            "use the connection.stream() method for an async "
            "streaming result set"
        )
    kw["future"] = True
    sync_engine = _create_engine(*arg, **kw)
    return AsyncEngine(sync_engine)


class AsyncConnectable:
    __slots__ = "_slots_dispatch", "__weakref__"


@util.create_proxy_methods(
    Connection,
    ":class:`_future.Connection`",
    ":class:`_asyncio.AsyncConnection`",
    classmethods=[],
    methods=[],
    attributes=[
        "closed",
        "invalidated",
        "dialect",
        "default_isolation_level",
    ],
)
class AsyncConnection(ProxyComparable, StartableContext, AsyncConnectable):
    """An asyncio proxy for a :class:`_engine.Connection`.

    :class:`_asyncio.AsyncConnection` is acquired using the
    :meth:`_asyncio.AsyncEngine.connect`
    method of :class:`_asyncio.AsyncEngine`::

        from sqlalchemy.ext.asyncio import create_async_engine
        engine = create_async_engine("postgresql+asyncpg://user:pass@host/dbname")

        async with engine.connect() as conn:
            result = await conn.execute(select(table))

    .. versionadded:: 1.4

    """  # noqa

    # AsyncConnection is a thin proxy; no state should be added here
    # that is not retrievable from the "sync" engine / connection, e.g.
    # current transaction, info, etc.   It should be possible to
    # create a new AsyncConnection that matches this one given only the
    # "sync" elements.
    __slots__ = (
        "sync_engine",
        "sync_connection",
    )

    def __init__(
        self,
        async_engine: "AsyncEngine",
        sync_connection: Optional[Connection] = None,
    ):
        self.engine = async_engine
        self.sync_engine = async_engine.sync_engine
        self.sync_connection = sync_connection

    async def start(self, is_ctxmanager=False):
        """Start this :class:`_asyncio.AsyncConnection` object's context
        outside of using a Python ``with:`` block.

        """
        if self.sync_connection:
            raise exc.InvalidRequestError("connection is already started")
        self.sync_connection = await (greenlet_spawn(self.sync_engine.connect))
        return self

    @property
    def connection(self):
        """Not implemented for async; call
        :meth:`_asyncio.AsyncConnection.get_raw_connection`.

        """
        raise exc.InvalidRequestError(
            "AsyncConnection.connection accessor is not implemented as the "
            "attribute may need to reconnect on an invalidated connection.  "
            "Use the get_raw_connection() method."
        )

    async def get_raw_connection(self):
        """Return the pooled DBAPI-level connection in use by this
        :class:`_asyncio.AsyncConnection`.

        This is typically the SQLAlchemy connection-pool proxied connection
        which then has an attribute .connection that refers to the actual
        DBAPI-level connection.
        """
        conn = self._sync_connection()

        return await greenlet_spawn(getattr, conn, "connection")

    @property
    def _proxied(self):
        return self.sync_connection

    @property
    def info(self):
        """Return the :attr:`_engine.Connection.info` dictionary of the
        underlying :class:`_engine.Connection`.

        This dictionary is freely writable for user-defined state to be
        associated with the database connection.

        This attribute is only available if the :class:`.AsyncConnection` is
        currently connected.   If the :attr:`.AsyncConnection.closed` attribute
        is ``True``, then accessing this attribute will raise
        :class:`.ResourceClosedError`.

        .. versionadded:: 1.4.0b2

        """
        return self.sync_connection.info

    def _sync_connection(self):
        if not self.sync_connection:
            self._raise_for_not_started()
        return self.sync_connection

    def begin(self) -> "AsyncTransaction":
        """Begin a transaction prior to autobegin occurring."""
        self._sync_connection()
        return AsyncTransaction(self)

    def begin_nested(self) -> "AsyncTransaction":
        """Begin a nested transaction and return a transaction handle."""
        self._sync_connection()
        return AsyncTransaction(self, nested=True)

    async def invalidate(self, exception=None):
        """Invalidate the underlying DBAPI connection associated with
        this :class:`_engine.Connection`.

        See the method :meth:`_engine.Connection.invalidate` for full
        detail on this method.

        """

        conn = self._sync_connection()
        return await greenlet_spawn(conn.invalidate, exception=exception)

    async def get_isolation_level(self):
        conn = self._sync_connection()
        return await greenlet_spawn(conn.get_isolation_level)

    async def set_isolation_level(self):
        conn = self._sync_connection()
        return await greenlet_spawn(conn.get_isolation_level)

    def in_transaction(self):
        """Return True if a transaction is in progress.

        .. versionadded:: 1.4.0b2

        """

        conn = self._sync_connection()

        return conn.in_transaction()

    def in_nested_transaction(self):
        """Return True if a transaction is in progress.

        .. versionadded:: 1.4.0b2

        """
        conn = self._sync_connection()

        return conn.in_nested_transaction()

    def get_transaction(self):
        """Return an :class:`.AsyncTransaction` representing the current
        transaction, if any.

        This makes use of the underlying synchronous connection's
        :meth:`_engine.Connection.get_transaction` method to get the current
        :class:`_engine.Transaction`, which is then proxied in a new
        :class:`.AsyncTransaction` object.

        .. versionadded:: 1.4.0b2

        """
        conn = self._sync_connection()

        trans = conn.get_transaction()
        if trans is not None:
            return AsyncTransaction._from_existing_transaction(self, trans)
        else:
            return None

    def get_nested_transaction(self):
        """Return an :class:`.AsyncTransaction` representing the current
        nested (savepoint) transaction, if any.

        This makes use of the underlying synchronous connection's
        :meth:`_engine.Connection.get_nested_transaction` method to get the
        current :class:`_engine.Transaction`, which is then proxied in a new
        :class:`.AsyncTransaction` object.

        .. versionadded:: 1.4.0b2

        """
        conn = self._sync_connection()

        trans = conn.get_nested_transaction()
        if trans is not None:
            return AsyncTransaction._from_existing_transaction(
                self, trans, True
            )
        else:
            return None

    async def execution_options(self, **opt):
        r"""Set non-SQL options for the connection which take effect
        during execution.

        This returns this :class:`_asyncio.AsyncConnection` object with
        the new options added.

        See :meth:`_future.Connection.execution_options` for full details
        on this method.

        """

        conn = self._sync_connection()
        c2 = await greenlet_spawn(conn.execution_options, **opt)
        assert c2 is conn
        return self

    async def commit(self):
        """Commit the transaction that is currently in progress.

        This method commits the current transaction if one has been started.
        If no transaction was started, the method has no effect, assuming
        the connection is in a non-invalidated state.

        A transaction is begun on a :class:`_future.Connection` automatically
        whenever a statement is first executed, or when the
        :meth:`_future.Connection.begin` method is called.

        """
        conn = self._sync_connection()
        await greenlet_spawn(conn.commit)

    async def rollback(self):
        """Roll back the transaction that is currently in progress.

        This method rolls back the current transaction if one has been started.
        If no transaction was started, the method has no effect.  If a
        transaction was started and the connection is in an invalidated state,
        the transaction is cleared using this method.

        A transaction is begun on a :class:`_future.Connection` automatically
        whenever a statement is first executed, or when the
        :meth:`_future.Connection.begin` method is called.


        """
        conn = self._sync_connection()
        await greenlet_spawn(conn.rollback)

    async def close(self):
        """Close this :class:`_asyncio.AsyncConnection`.

        This has the effect of also rolling back the transaction if one
        is in place.

        """
        conn = self._sync_connection()
        await greenlet_spawn(conn.close)

    async def exec_driver_sql(
        self,
        statement: Executable,
        parameters: Optional[Mapping] = None,
        execution_options: Mapping = util.EMPTY_DICT,
    ) -> Result:
        r"""Executes a driver-level SQL string and return buffered
        :class:`_engine.Result`.

        """

        conn = self._sync_connection()

        result = await greenlet_spawn(
            conn.exec_driver_sql,
            statement,
            parameters,
            execution_options,
            _require_await=True,
        )
        if result.context._is_server_side:
            raise async_exc.AsyncMethodRequired(
                "Can't use the connection.exec_driver_sql() method with a "
                "server-side cursor."
                "Use the connection.stream() method for an async "
                "streaming result set."
            )

        return result

    async def stream(
        self,
        statement: Executable,
        parameters: Optional[Mapping] = None,
        execution_options: Mapping = util.EMPTY_DICT,
    ) -> AsyncResult:
        """Execute a statement and return a streaming
        :class:`_asyncio.AsyncResult` object."""

        conn = self._sync_connection()

        result = await greenlet_spawn(
            conn._execute_20,
            statement,
            parameters,
            util.EMPTY_DICT.merge_with(
                execution_options, {"stream_results": True}
            ),
            _require_await=True,
        )
        if not result.context._is_server_side:
            # TODO: real exception here
            assert False, "server side result expected"
        return AsyncResult(result)

    async def execute(
        self,
        statement: Executable,
        parameters: Optional[Mapping] = None,
        execution_options: Mapping = util.EMPTY_DICT,
    ) -> Result:
        r"""Executes a SQL statement construct and return a buffered
        :class:`_engine.Result`.

        :param object: The statement to be executed.  This is always
         an object that is in both the :class:`_expression.ClauseElement` and
         :class:`_expression.Executable` hierarchies, including:

         * :class:`_expression.Select`
         * :class:`_expression.Insert`, :class:`_expression.Update`,
           :class:`_expression.Delete`
         * :class:`_expression.TextClause` and
           :class:`_expression.TextualSelect`
         * :class:`_schema.DDL` and objects which inherit from
           :class:`_schema.DDLElement`

        :param parameters: parameters which will be bound into the statement.
         This may be either a dictionary of parameter names to values,
         or a mutable sequence (e.g. a list) of dictionaries.  When a
         list of dictionaries is passed, the underlying statement execution
         will make use of the DBAPI ``cursor.executemany()`` method.
         When a single dictionary is passed, the DBAPI ``cursor.execute()``
         method will be used.

        :param execution_options: optional dictionary of execution options,
         which will be associated with the statement execution.  This
         dictionary can provide a subset of the options that are accepted
         by :meth:`_future.Connection.execution_options`.

        :return: a :class:`_engine.Result` object.

        """
        conn = self._sync_connection()

        result = await greenlet_spawn(
            conn._execute_20,
            statement,
            parameters,
            execution_options,
            _require_await=True,
        )
        if result.context._is_server_side:
            raise async_exc.AsyncMethodRequired(
                "Can't use the connection.execute() method with a "
                "server-side cursor."
                "Use the connection.stream() method for an async "
                "streaming result set."
            )
        return result

    async def scalar(
        self,
        statement: Executable,
        parameters: Optional[Mapping] = None,
        execution_options: Mapping = util.EMPTY_DICT,
    ) -> Any:
        r"""Executes a SQL statement construct and returns a scalar object.

        This method is shorthand for invoking the
        :meth:`_engine.Result.scalar` method after invoking the
        :meth:`_future.Connection.execute` method.  Parameters are equivalent.

        :return: a scalar Python value representing the first column of the
         first row returned.

        """
        result = await self.execute(statement, parameters, execution_options)
        return result.scalar()

    async def run_sync(self, fn: Callable, *arg, **kw) -> Any:
        """Invoke the given sync callable passing self as the first argument.

        This method maintains the asyncio event loop all the way through
        to the database connection by running the given callable in a
        specially instrumented greenlet.

        E.g.::

            with async_engine.begin() as conn:
                await conn.run_sync(metadata.create_all)

        .. note::

            The provided callable is invoked inline within the asyncio event
            loop, and will block on traditional IO calls.  IO within this
            callable should only call into SQLAlchemy's asyncio database
            APIs which will be properly adapted to the greenlet context.

        .. seealso::

            :ref:`session_run_sync`
        """

        conn = self._sync_connection()

        return await greenlet_spawn(fn, conn, *arg, **kw)

    def __await__(self):
        return self.start().__await__()

    async def __aexit__(self, type_, value, traceback):
        await self.close()


@util.create_proxy_methods(
    Engine,
    ":class:`_future.Engine`",
    ":class:`_asyncio.AsyncEngine`",
    classmethods=[],
    methods=[
        "clear_compiled_cache",
        "update_execution_options",
        "get_execution_options",
    ],
    attributes=["url", "pool", "dialect", "engine", "name", "driver", "echo"],
)
class AsyncEngine(ProxyComparable, AsyncConnectable):
    """An asyncio proxy for a :class:`_engine.Engine`.

    :class:`_asyncio.AsyncEngine` is acquired using the
    :func:`_asyncio.create_async_engine` function::

        from sqlalchemy.ext.asyncio import create_async_engine
        engine = create_async_engine("postgresql+asyncpg://user:pass@host/dbname")

    .. versionadded:: 1.4

    """  # noqa

    # AsyncEngine is a thin proxy; no state should be added here
    # that is not retrievable from the "sync" engine / connection, e.g.
    # current transaction, info, etc.   It should be possible to
    # create a new AsyncEngine that matches this one given only the
    # "sync" elements.
    __slots__ = ("sync_engine", "_proxied")

    _connection_cls = AsyncConnection

    _option_cls: type

    class _trans_ctx(StartableContext):
        def __init__(self, conn):
            self.conn = conn

        async def start(self, is_ctxmanager=False):
            await self.conn.start(is_ctxmanager=is_ctxmanager)
            self.transaction = self.conn.begin()
            await self.transaction.__aenter__()

            return self.conn

        async def __aexit__(self, type_, value, traceback):
            await self.transaction.__aexit__(type_, value, traceback)
            await self.conn.close()

    def __init__(self, sync_engine: Engine):
        if not sync_engine.dialect.is_async:
            raise exc.InvalidRequestError(
                "The asyncio extension requires an async driver to be used. "
                f"The loaded {sync_engine.dialect.driver!r} is not async."
            )
        self.sync_engine = self._proxied = sync_engine

    def begin(self):
        """Return a context manager which when entered will deliver an
        :class:`_asyncio.AsyncConnection` with an
        :class:`_asyncio.AsyncTransaction` established.

        E.g.::

            async with async_engine.begin() as conn:
                await conn.execute(
                    text("insert into table (x, y, z) values (1, 2, 3)")
                )
                await conn.execute(text("my_special_procedure(5)"))


        """
        conn = self.connect()
        return self._trans_ctx(conn)

    def connect(self) -> AsyncConnection:
        """Return an :class:`_asyncio.AsyncConnection` object.

        The :class:`_asyncio.AsyncConnection` will procure a database
        connection from the underlying connection pool when it is entered
        as an async context manager::

            async with async_engine.connect() as conn:
                result = await conn.execute(select(user_table))

        The :class:`_asyncio.AsyncConnection` may also be started outside of a
        context manager by invoking its :meth:`_asyncio.AsyncConnection.start`
        method.

        """

        return self._connection_cls(self)

    async def raw_connection(self) -> Any:
        """Return a "raw" DBAPI connection from the connection pool.

        .. seealso::

            :ref:`dbapi_connections`

        """
        return await greenlet_spawn(self.sync_engine.raw_connection)

    def execution_options(self, **opt):
        """Return a new :class:`_asyncio.AsyncEngine` that will provide
        :class:`_asyncio.AsyncConnection` objects with the given execution
        options.

        Proxied from :meth:`_future.Engine.execution_options`.  See that
        method for details.

        """

        return AsyncEngine(self.sync_engine.execution_options(**opt))

    async def dispose(self):
        """Dispose of the connection pool used by this
        :class:`_asyncio.AsyncEngine`.

        This will close all connection pool connections that are
        **currently checked in**.  See the documentation for the underlying
        :meth:`_future.Engine.dispose` method for further notes.

        .. seealso::

            :meth:`_future.Engine.dispose`

        """

        return await greenlet_spawn(self.sync_engine.dispose)


class AsyncTransaction(ProxyComparable, StartableContext):
    """An asyncio proxy for a :class:`_engine.Transaction`."""

    __slots__ = ("connection", "sync_transaction", "nested")

    def __init__(self, connection: AsyncConnection, nested: bool = False):
        self.connection = connection
        self.sync_transaction: Optional[Transaction] = None
        self.nested = nested

    @classmethod
    def _from_existing_transaction(
        cls,
        connection: AsyncConnection,
        sync_transaction: Transaction,
        nested: bool = False,
    ):
        obj = cls.__new__(cls)
        obj.connection = connection
        obj.sync_transaction = sync_transaction
        obj.nested = nested
        return obj

    def _sync_transaction(self):
        if not self.sync_transaction:
            self._raise_for_not_started()
        return self.sync_transaction

    @property
    def _proxied(self):
        return self.sync_transaction

    @property
    def is_valid(self) -> bool:
        return self._sync_transaction().is_valid

    @property
    def is_active(self) -> bool:
        return self._sync_transaction().is_active

    async def close(self):
        """Close this :class:`.Transaction`.

        If this transaction is the base transaction in a begin/commit
        nesting, the transaction will rollback().  Otherwise, the
        method returns.

        This is used to cancel a Transaction without affecting the scope of
        an enclosing transaction.

        """
        await greenlet_spawn(self._sync_transaction().close)

    async def rollback(self):
        """Roll back this :class:`.Transaction`."""
        await greenlet_spawn(self._sync_transaction().rollback)

    async def commit(self):
        """Commit this :class:`.Transaction`."""

        await greenlet_spawn(self._sync_transaction().commit)

    async def start(self, is_ctxmanager=False):
        """Start this :class:`_asyncio.AsyncTransaction` object's context
        outside of using a Python ``with:`` block.

        """

        self.sync_transaction = await greenlet_spawn(
            self.connection._sync_connection().begin_nested
            if self.nested
            else self.connection._sync_connection().begin
        )
        if is_ctxmanager:
            self.sync_transaction.__enter__()
        return self

    async def __aexit__(self, type_, value, traceback):
        await greenlet_spawn(
            self._sync_transaction().__exit__, type_, value, traceback
        )


def _get_sync_engine_or_connection(async_engine):
    if isinstance(async_engine, AsyncConnection):
        return async_engine.sync_connection

    try:
        return async_engine.sync_engine
    except AttributeError as e:
        raise exc.ArgumentError(
            "AsyncEngine expected, got %r" % async_engine
        ) from e
