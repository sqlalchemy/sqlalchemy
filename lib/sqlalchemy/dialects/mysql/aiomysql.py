# dialects/mysql/aiomysql.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors <see AUTHORS
# file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

r"""
.. dialect:: mysql+aiomysql
    :name: aiomysql
    :dbapi: aiomysql
    :connectstring: mysql+aiomysql://user:password@host:port/dbname[?key=value&key=value...]
    :url: https://github.com/aio-libs/aiomysql

The aiomysql dialect is SQLAlchemy's second Python asyncio dialect.

Using a special asyncio mediation layer, the aiomysql dialect is usable
as the backend for the :ref:`SQLAlchemy asyncio <asyncio_toplevel>`
extension package.

This dialect should normally be used only with the
:func:`_asyncio.create_async_engine` engine creation function::

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        "mysql+aiomysql://user:pass@hostname/dbname?charset=utf8mb4"
    )

"""  # noqa
from __future__ import annotations

from types import ModuleType
from typing import Any
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from .pymysql import MySQLDialect_pymysql
from ...connectors.asyncio import AsyncAdapt_dbapi_connection
from ...connectors.asyncio import AsyncAdapt_dbapi_cursor
from ...connectors.asyncio import AsyncAdapt_dbapi_module
from ...connectors.asyncio import AsyncAdapt_dbapi_ss_cursor
from ...util.concurrency import await_

if TYPE_CHECKING:

    from ...connectors.asyncio import AsyncIODBAPIConnection
    from ...connectors.asyncio import AsyncIODBAPICursor
    from ...engine.interfaces import ConnectArgsType
    from ...engine.interfaces import DBAPIConnection
    from ...engine.interfaces import DBAPICursor
    from ...engine.interfaces import DBAPIModule
    from ...engine.interfaces import PoolProxiedConnection
    from ...engine.url import URL


class AsyncAdapt_aiomysql_cursor(AsyncAdapt_dbapi_cursor):
    __slots__ = ()

    def _make_new_cursor(
        self, connection: AsyncIODBAPIConnection
    ) -> AsyncIODBAPICursor:
        return connection.cursor(self._adapt_connection.dbapi.Cursor)


class AsyncAdapt_aiomysql_ss_cursor(
    AsyncAdapt_dbapi_ss_cursor, AsyncAdapt_aiomysql_cursor
):
    __slots__ = ()

    def _make_new_cursor(
        self, connection: AsyncIODBAPIConnection
    ) -> AsyncIODBAPICursor:
        return connection.cursor(
            self._adapt_connection.dbapi.aiomysql.cursors.SSCursor
        )


class AsyncAdapt_aiomysql_connection(AsyncAdapt_dbapi_connection):
    __slots__ = ()

    _cursor_cls = AsyncAdapt_aiomysql_cursor
    _ss_cursor_cls = AsyncAdapt_aiomysql_ss_cursor

    def ping(self, reconnect: bool) -> None:
        assert not reconnect
        await_(self._connection.ping(reconnect))

    def character_set_name(self) -> Optional[str]:
        return self._connection.character_set_name()  # type: ignore[no-any-return]  # noqa: E501

    def autocommit(self, value: Any) -> None:
        await_(self._connection.autocommit(value))

    def terminate(self) -> None:
        # it's not awaitable.
        self._connection.close()

    def close(self) -> None:
        await_(self._connection.ensure_closed())


class AsyncAdapt_aiomysql_dbapi(AsyncAdapt_dbapi_module):
    def __init__(self, aiomysql: ModuleType, pymysql: ModuleType):
        self.aiomysql = aiomysql
        self.pymysql = pymysql
        self.paramstyle = "format"
        self._init_dbapi_attributes()
        self.Cursor, self.SSCursor = self._init_cursors_subclasses()

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
            setattr(self, name, getattr(self.aiomysql, name))

        for name in (
            "NUMBER",
            "STRING",
            "DATETIME",
            "BINARY",
            "TIMESTAMP",
            "Binary",
        ):
            setattr(self, name, getattr(self.pymysql, name))

    def connect(self, *arg: Any, **kw: Any) -> AsyncAdapt_aiomysql_connection:
        creator_fn = kw.pop("async_creator_fn", self.aiomysql.connect)

        return AsyncAdapt_aiomysql_connection(
            self,
            await_(creator_fn(*arg, **kw)),
        )

    def _init_cursors_subclasses(
        self,
    ) -> tuple[AsyncIODBAPICursor, AsyncIODBAPICursor]:
        # suppress unconditional warning emitted by aiomysql
        class Cursor(self.aiomysql.Cursor):  # type: ignore[misc, name-defined]
            async def _show_warnings(
                self, conn: AsyncIODBAPIConnection
            ) -> None:
                pass

        class SSCursor(self.aiomysql.SSCursor):  # type: ignore[misc, name-defined]   # noqa: E501
            async def _show_warnings(
                self, conn: AsyncIODBAPIConnection
            ) -> None:
                pass

        return Cursor, SSCursor  # type: ignore[return-value]


class MySQLDialect_aiomysql(MySQLDialect_pymysql):
    driver = "aiomysql"
    supports_statement_cache = True

    supports_server_side_cursors = True
    _sscursor = AsyncAdapt_aiomysql_ss_cursor

    is_async = True
    has_terminate = True

    @classmethod
    def import_dbapi(cls) -> AsyncAdapt_aiomysql_dbapi:
        return AsyncAdapt_aiomysql_dbapi(
            __import__("aiomysql"), __import__("pymysql")
        )

    def do_terminate(self, dbapi_connection: DBAPIConnection) -> None:
        dbapi_connection.terminate()

    def create_connect_args(
        self, url: URL, _translate_args: Optional[dict[str, Any]] = None
    ) -> ConnectArgsType:
        return super().create_connect_args(
            url, _translate_args=dict(username="user", database="db")
        )

    def is_disconnect(
        self,
        e: DBAPIModule.Error,
        connection: Optional[Union[PoolProxiedConnection, DBAPIConnection]],
        cursor: Optional[DBAPICursor],
    ) -> bool:
        if super().is_disconnect(e, connection, cursor):
            return True
        else:
            str_e = str(e).lower()
            return "not connected" in str_e

    def _found_rows_client_flag(self) -> int:
        from pymysql.constants import CLIENT  # type: ignore

        return CLIENT.FOUND_ROWS  # type: ignore[no-any-return]

    def get_driver_connection(
        self, connection: DBAPIConnection
    ) -> AsyncIODBAPIConnection:
        return connection._connection  # type: ignore[no-any-return]


dialect = MySQLDialect_aiomysql
