# connectors/asyncio.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""generic asyncio-adapted versions of DBAPI connection and cursor"""

from __future__ import annotations

import asyncio
import collections
import sys
import types
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Deque
from typing import Iterator
from typing import NoReturn
from typing import Optional
from typing import Protocol
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import TYPE_CHECKING

from ..engine import AdaptedConnection
from ..exc import EmulatedDBAPIException
from ..util import EMPTY_DICT
from ..util.concurrency import await_
from ..util.concurrency import in_greenlet

if TYPE_CHECKING:
    from ..engine.interfaces import _DBAPICursorDescription
    from ..engine.interfaces import _DBAPIMultiExecuteParams
    from ..engine.interfaces import _DBAPISingleExecuteParams
    from ..engine.interfaces import DBAPIModule
    from ..util.typing import Self


class AsyncIODBAPIConnection(Protocol):
    """protocol representing an async adapted version of a
    :pep:`249` database connection.


    """

    # note that async DBAPIs dont agree if close() should be awaitable,
    # so it is omitted here and picked up by the __getattr__ hook below

    async def commit(self) -> None: ...

    def cursor(self, *args: Any, **kwargs: Any) -> AsyncIODBAPICursor: ...

    async def rollback(self) -> None: ...

    def __getattr__(self, key: str) -> Any: ...

    def __setattr__(self, key: str, value: Any) -> None: ...


class AsyncIODBAPICursor(Protocol):
    """protocol representing an async adapted version
    of a :pep:`249` database cursor.


    """

    def __aenter__(self) -> Any: ...

    @property
    def description(
        self,
    ) -> _DBAPICursorDescription:
        """The description attribute of the Cursor."""
        ...

    @property
    def rowcount(self) -> int: ...

    arraysize: int

    lastrowid: int

    async def close(self) -> None: ...

    async def execute(
        self,
        operation: Any,
        parameters: Optional[_DBAPISingleExecuteParams] = None,
    ) -> Any: ...

    async def executemany(
        self,
        operation: Any,
        parameters: _DBAPIMultiExecuteParams,
    ) -> Any: ...

    async def fetchone(self) -> Optional[Any]: ...

    async def fetchmany(self, size: Optional[int] = ...) -> Sequence[Any]: ...

    async def fetchall(self) -> Sequence[Any]: ...

    async def setinputsizes(self, sizes: Sequence[Any]) -> None: ...

    def setoutputsize(self, size: Any, column: Any) -> None: ...

    async def callproc(
        self, procname: str, parameters: Sequence[Any] = ...
    ) -> Any: ...

    async def nextset(self) -> Optional[bool]: ...

    def __aiter__(self) -> AsyncIterator[Any]: ...


class AsyncAdapt_dbapi_module:
    if TYPE_CHECKING:
        Error = DBAPIModule.Error
        OperationalError = DBAPIModule.OperationalError
        InterfaceError = DBAPIModule.InterfaceError
        IntegrityError = DBAPIModule.IntegrityError

        def __getattr__(self, key: str) -> Any: ...

    def __init__(
        self,
        driver: types.ModuleType,
        *,
        dbapi_module: types.ModuleType | None = None,
    ):
        self.driver = driver
        self.dbapi_module = dbapi_module

    @property
    def exceptions_module(self) -> types.ModuleType:
        """Return the module which we think will have the exception hierarchy.

        For an asyncio driver that wraps a plain DBAPI like aiomysql,
        aioodbc, aiosqlite, etc. these exceptions will be from the
        dbapi_module.  For a "pure" driver like asyncpg these will come
        from the driver module.

        .. versionadded:: 2.1

        """
        if self.dbapi_module is not None:
            return self.dbapi_module
        else:
            return self.driver


class AsyncAdapt_dbapi_cursor:
    server_side = False
    __slots__ = (
        "_adapt_connection",
        "_connection",
        "_cursor",
        "_rows",
        "_soft_closed_memoized",
    )

    _awaitable_cursor_close: bool = True

    _cursor: AsyncIODBAPICursor
    _adapt_connection: AsyncAdapt_dbapi_connection
    _connection: AsyncIODBAPIConnection
    _rows: Deque[Any]

    def __init__(self, adapt_connection: AsyncAdapt_dbapi_connection):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection

        cursor = self._make_new_cursor(self._connection)
        self._cursor = self._aenter_cursor(cursor)
        self._soft_closed_memoized = EMPTY_DICT
        if not self.server_side:
            self._rows = collections.deque()

    def _aenter_cursor(self, cursor: AsyncIODBAPICursor) -> AsyncIODBAPICursor:
        try:
            return await_(cursor.__aenter__())  # type: ignore[no-any-return]
        except Exception as error:
            self._adapt_connection._handle_exception(error)

    def _make_new_cursor(
        self, connection: AsyncIODBAPIConnection
    ) -> AsyncIODBAPICursor:
        return connection.cursor()

    @property
    def description(self) -> Optional[_DBAPICursorDescription]:
        if "description" in self._soft_closed_memoized:
            return self._soft_closed_memoized["description"]  # type: ignore[no-any-return]  # noqa: E501
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._cursor.arraysize = value

    @property
    def lastrowid(self) -> int:
        return self._cursor.lastrowid

    async def _async_soft_close(self) -> None:
        """close the cursor but keep the results pending, and memoize the
        description.

        .. versionadded:: 2.0.44

        """

        if not self._awaitable_cursor_close or self.server_side:
            return

        self._soft_closed_memoized = self._soft_closed_memoized.union(
            {
                "description": self._cursor.description,
            }
        )
        await self._cursor.close()

    def close(self) -> None:
        self._rows.clear()

        # updated as of 2.0.44
        # try to "close" the cursor based on what we know about the driver
        # and if we are able to.  otherwise, hope that the asyncio
        # extension called _async_soft_close() if the cursor is going into
        # a sync context
        if self._cursor is None or bool(self._soft_closed_memoized):
            return

        if not self._awaitable_cursor_close:
            self._cursor.close()  # type: ignore[unused-coroutine]
        elif in_greenlet():
            await_(self._cursor.close())

    def execute(
        self,
        operation: Any,
        parameters: Optional[_DBAPISingleExecuteParams] = None,
    ) -> Any:
        try:
            return await_(self._execute_async(operation, parameters))
        except Exception as error:
            self._adapt_connection._handle_exception(error)

    def executemany(
        self,
        operation: Any,
        seq_of_parameters: _DBAPIMultiExecuteParams,
    ) -> Any:
        try:
            return await_(
                self._executemany_async(operation, seq_of_parameters)
            )
        except Exception as error:
            self._adapt_connection._handle_exception(error)

    async def _execute_async(
        self, operation: Any, parameters: Optional[_DBAPISingleExecuteParams]
    ) -> Any:
        async with self._adapt_connection._execute_mutex:
            if parameters is None:
                result = await self._cursor.execute(operation)
            else:
                result = await self._cursor.execute(operation, parameters)

            if self._cursor.description and not self.server_side:
                self._rows = collections.deque(await self._cursor.fetchall())
            return result

    async def _executemany_async(
        self,
        operation: Any,
        seq_of_parameters: _DBAPIMultiExecuteParams,
    ) -> Any:
        async with self._adapt_connection._execute_mutex:
            return await self._cursor.executemany(operation, seq_of_parameters)

    def nextset(self) -> None:
        await_(self._cursor.nextset())
        if self._cursor.description and not self.server_side:
            self._rows = collections.deque(await_(self._cursor.fetchall()))

    def setinputsizes(self, *inputsizes: Any) -> None:
        # NOTE: this is overrridden in aioodbc due to
        # see https://github.com/aio-libs/aioodbc/issues/451
        # right now

        return await_(self._cursor.setinputsizes(*inputsizes))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        self.close()

    def __iter__(self) -> Iterator[Any]:
        while self._rows:
            yield self._rows.popleft()

    def fetchone(self) -> Optional[Any]:
        if self._rows:
            return self._rows.popleft()
        else:
            return None

    def fetchmany(self, size: Optional[int] = None) -> Sequence[Any]:
        if size is None:
            size = self.arraysize
        rr = self._rows
        return [rr.popleft() for _ in range(min(size, len(rr)))]

    def fetchall(self) -> Sequence[Any]:
        retval = list(self._rows)
        self._rows.clear()
        return retval


class AsyncAdapt_dbapi_ss_cursor(AsyncAdapt_dbapi_cursor):
    __slots__ = ()
    server_side = True

    def close(self) -> None:
        if self._cursor is not None:
            await_(self._cursor.close())
            self._cursor = None  # type: ignore

    def fetchone(self) -> Optional[Any]:
        return await_(self._cursor.fetchone())

    def fetchmany(self, size: Optional[int] = None) -> Any:
        return await_(self._cursor.fetchmany(size=size))

    def fetchall(self) -> Sequence[Any]:
        return await_(self._cursor.fetchall())

    def __iter__(self) -> Iterator[Any]:
        iterator = self._cursor.__aiter__()
        while True:
            try:
                yield await_(iterator.__anext__())
            except StopAsyncIteration:
                break


class AsyncAdapt_dbapi_connection(AdaptedConnection):
    _cursor_cls = AsyncAdapt_dbapi_cursor
    _ss_cursor_cls = AsyncAdapt_dbapi_ss_cursor

    __slots__ = ("dbapi", "_execute_mutex")

    _connection: AsyncIODBAPIConnection

    @classmethod
    async def create(
        cls,
        dbapi: Any,
        connection_awaitable: Awaitable[AsyncIODBAPIConnection],
        **kw: Any,
    ) -> Self:
        try:
            connection = await connection_awaitable
        except Exception as error:
            cls._handle_exception_no_connection(dbapi, error)
        else:
            return cls(dbapi, connection, **kw)

    def __init__(self, dbapi: Any, connection: AsyncIODBAPIConnection):
        self.dbapi = dbapi
        self._connection = connection
        self._execute_mutex = asyncio.Lock()

    def cursor(self, server_side: bool = False) -> AsyncAdapt_dbapi_cursor:
        if server_side:
            return self._ss_cursor_cls(self)
        else:
            return self._cursor_cls(self)

    def execute(
        self,
        operation: Any,
        parameters: Optional[_DBAPISingleExecuteParams] = None,
    ) -> Any:
        """lots of DBAPIs seem to provide this, so include it"""
        cursor = self.cursor()
        cursor.execute(operation, parameters)
        return cursor

    @classmethod
    def _handle_exception_no_connection(
        cls, dbapi: Any, error: Exception
    ) -> NoReturn:
        exc_info = sys.exc_info()

        raise error.with_traceback(exc_info[2])

    def _handle_exception(self, error: Exception) -> NoReturn:
        self._handle_exception_no_connection(self.dbapi, error)

    def rollback(self) -> None:
        try:
            await_(self._connection.rollback())
        except Exception as error:
            self._handle_exception(error)

    def commit(self) -> None:
        try:
            await_(self._connection.commit())
        except Exception as error:
            self._handle_exception(error)

    def close(self) -> None:
        await_(self._connection.close())


class AsyncAdapt_terminate:
    """Mixin for a AsyncAdapt_dbapi_connection to add terminate support."""

    __slots__ = ()

    def terminate(self) -> None:
        if in_greenlet():
            # in a greenlet; this is the connection was invalidated case.
            try:
                # try to gracefully close; see #10717
                await_(asyncio.shield(self._terminate_graceful_close()))
            except self._terminate_handled_exceptions() as e:
                # in the case where we are recycling an old connection
                # that may have already been disconnected, close() will
                # fail.  In this case, terminate
                # the connection without any further waiting.
                # see issue #8419
                self._terminate_force_close()
                if isinstance(e, asyncio.CancelledError):
                    # re-raise CancelledError if we were cancelled
                    raise
        else:
            # not in a greenlet; this is the gc cleanup case
            self._terminate_force_close()

    def _terminate_handled_exceptions(self) -> Tuple[Type[BaseException], ...]:
        """Returns the exceptions that should be handled when
        calling _graceful_close.
        """
        return (asyncio.TimeoutError, asyncio.CancelledError, OSError)

    async def _terminate_graceful_close(self) -> None:
        """Try to close connection gracefully"""
        raise NotImplementedError

    def _terminate_force_close(self) -> None:
        """Terminate the connection"""
        raise NotImplementedError


class AsyncAdapt_Error(EmulatedDBAPIException):
    """Provide for the base of DBAPI ``Error`` base class for dialects
    that need to emulate the DBAPI exception hierarchy.

    .. versionadded:: 2.1

    """
