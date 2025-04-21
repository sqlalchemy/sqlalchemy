# dialects/mysql/asyncmy.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors <see AUTHORS
# file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: disable-error-code="import-not-found"

r"""
.. dialect:: mysql+asyncmy
    :name: asyncmy
    :dbapi: asyncmy
    :connectstring: mysql+asyncmy://user:password@host:port/dbname[?key=value&key=value...]
    :url: https://github.com/long2ice/asyncmy

Using a special asyncio mediation layer, the asyncmy dialect is usable
as the backend for the :ref:`SQLAlchemy asyncio <asyncio_toplevel>`
extension package.

This dialect should normally be used only with the
:func:`_asyncio.create_async_engine` engine creation function::

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        "mysql+asyncmy://user:pass@hostname/dbname?charset=utf8mb4"
    )

"""  # noqa
from __future__ import annotations

from types import ModuleType
from typing import Any
from typing import Iterable
from typing import NoReturn
from typing import Optional
from typing import SupportsBytes
from typing import SupportsIndex
from typing import TYPE_CHECKING
from typing import Union

from .pymysql import MySQLDialect_pymysql
from ... import util
from ...connectors.asyncio import AsyncAdapt_dbapi_connection
from ...connectors.asyncio import AsyncAdapt_dbapi_cursor
from ...connectors.asyncio import AsyncAdapt_dbapi_ss_cursor
from ...util.concurrency import await_

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer
    from asyncmy.connection import Connection as AsyncmyConnection
    from asyncmy.pool import pool as AsyncmyPool

    from ...connectors.asyncio import AsyncIODBAPIConnection
    from ...connectors.asyncio import AsyncIODBAPICursor
    from ...engine.interfaces import ConnectArgsType
    from ...engine.url import URL


class AsyncAdapt_asyncmy_cursor(AsyncAdapt_dbapi_cursor):
    __slots__ = ()


class AsyncAdapt_asyncmy_ss_cursor(
    AsyncAdapt_dbapi_ss_cursor, AsyncAdapt_asyncmy_cursor
):
    __slots__ = ()

    def _make_new_cursor(
        self, connection: AsyncIODBAPIConnection
    ) -> AsyncIODBAPICursor:
        return connection.cursor(
            self._adapt_connection.dbapi.asyncmy.cursors.SSCursor
        )


class AsyncAdapt_asyncmy_connection(AsyncAdapt_dbapi_connection):
    __slots__ = ()

    _cursor_cls = AsyncAdapt_asyncmy_cursor
    _ss_cursor_cls = AsyncAdapt_asyncmy_ss_cursor
    _connection: AsyncmyConnection

    def _handle_exception(self, error: Exception) -> NoReturn:
        if isinstance(error, AttributeError):
            raise self.dbapi.InternalError(
                "network operation failed due to asyncmy attribute error"
            )

        raise error

    def ping(self, reconnect: bool) -> None:
        assert not reconnect
        return await_(self._do_ping())

    async def _do_ping(self) -> None:
        try:
            async with self._execute_mutex:
                await self._connection.ping(False)
        except Exception as error:
            self._handle_exception(error)

    def character_set_name(self) -> Optional[str]:
        return self._connection.character_set_name()  # type: ignore[no-any-return]  # noqa: E501

    def autocommit(self, value: Any) -> None:
        await_(self._connection.autocommit(value))

    def terminate(self) -> None:
        # it's not awaitable.
        self._connection.close()

    def close(self) -> None:
        await_(self._connection.ensure_closed())


def _Binary(
    x: Union[
        Iterable[SupportsIndex],
        SupportsIndex,
        SupportsBytes,
        ReadableBuffer,
    ],
) -> bytes:
    """Return x as a binary type."""
    return bytes(x)


class AsyncAdapt_asyncmy_dbapi:
    def __init__(self, asyncmy: ModuleType):
        self.asyncmy = asyncmy
        self.paramstyle = "format"
        self._init_dbapi_attributes()

    def _init_dbapi_attributes(self) -> None:
        for name in (
            "Warning",
            "Error",
            "InterfaceError",
            "DataError",
            "DatabaseError",
            "OperationalError",
            "InterfaceError",
            "IntegrityError",
            "ProgrammingError",
            "InternalError",
            "NotSupportedError",
        ):
            setattr(self, name, getattr(self.asyncmy.errors, name))

    STRING = util.symbol("STRING")
    NUMBER = util.symbol("NUMBER")
    BINARY = util.symbol("BINARY")
    DATETIME = util.symbol("DATETIME")
    TIMESTAMP = util.symbol("TIMESTAMP")
    Binary = staticmethod(_Binary)

    def connect(self, *arg: Any, **kw: Any) -> AsyncAdapt_asyncmy_connection:
        creator_fn = kw.pop("async_creator_fn", self.asyncmy.connect)

        return AsyncAdapt_asyncmy_connection(
            self,
            await_(creator_fn(*arg, **kw)),
        )


class MySQLDialect_asyncmy(MySQLDialect_pymysql):
    driver = "asyncmy"
    supports_statement_cache = True

    supports_server_side_cursors = True  # type: ignore[assignment]
    _sscursor = AsyncAdapt_asyncmy_ss_cursor

    is_async = True
    has_terminate = True

    @classmethod
    def import_dbapi(cls) -> AsyncAdapt_asyncmy_dbapi:  # type: ignore[override]  # noqa: E501
        return AsyncAdapt_asyncmy_dbapi(__import__("asyncmy"))

    def do_terminate(self, dbapi_connection: AsyncmyPool) -> None:
        dbapi_connection.terminate()

    def create_connect_args(self, url: URL) -> ConnectArgsType:  # type: ignore[override]  # noqa: E501
        return super().create_connect_args(
            url, _translate_args=dict(username="user", database="db")
        )

    def is_disconnect(
        self, e: Exception, connection: Any, cursor: Any
    ) -> bool:
        if super().is_disconnect(e, connection, cursor):
            return True
        else:
            str_e = str(e).lower()
            return (
                "not connected" in str_e or "network operation failed" in str_e
            )

    def _found_rows_client_flag(self) -> int:
        from asyncmy.constants import CLIENT

        return CLIENT.FOUND_ROWS  # type: ignore[no-any-return]

    def get_driver_connection(  # type: ignore[override]
        self, connection: AsyncAdapt_asyncmy_connection
    ) -> AsyncmyConnection:
        return connection._connection


dialect = MySQLDialect_asyncmy
