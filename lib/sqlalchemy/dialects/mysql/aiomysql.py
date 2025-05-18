# dialects/mysql/aiomysql.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors <see AUTHORS
# file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

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
from .pymysql import MySQLDialect_pymysql
from ... import pool
from ... import util
from ...connectors.asyncio import AsyncAdapt_dbapi_connection
from ...connectors.asyncio import AsyncAdapt_dbapi_cursor
from ...connectors.asyncio import AsyncAdapt_dbapi_ss_cursor
from ...util.concurrency import await_fallback
from ...util.concurrency import await_only


class AsyncAdapt_aiomysql_cursor(AsyncAdapt_dbapi_cursor):
    __slots__ = ()

    def _make_new_cursor(self, connection):
        return connection.cursor(self._adapt_connection.dbapi.Cursor)


class AsyncAdapt_aiomysql_ss_cursor(
    AsyncAdapt_dbapi_ss_cursor, AsyncAdapt_aiomysql_cursor
):
    __slots__ = ()

    def _make_new_cursor(self, connection):
        return connection.cursor(
            self._adapt_connection.dbapi.aiomysql.cursors.SSCursor
        )


class AsyncAdapt_aiomysql_connection(AsyncAdapt_dbapi_connection):
    __slots__ = ()

    _cursor_cls = AsyncAdapt_aiomysql_cursor
    _ss_cursor_cls = AsyncAdapt_aiomysql_ss_cursor

    def ping(self, reconnect):
        assert not reconnect
        return self.await_(self._connection.ping(reconnect))

    def character_set_name(self):
        return self._connection.character_set_name()

    def autocommit(self, value):
        self.await_(self._connection.autocommit(value))

    def terminate(self):
        # it's not awaitable.
        self._connection.close()

    def close(self) -> None:
        self.await_(self._connection.ensure_closed())


class AsyncAdaptFallback_aiomysql_connection(AsyncAdapt_aiomysql_connection):
    __slots__ = ()

    await_ = staticmethod(await_fallback)


class AsyncAdapt_aiomysql_dbapi:
    def __init__(self, aiomysql, pymysql):
        self.aiomysql = aiomysql
        self.pymysql = pymysql
        self.paramstyle = "format"
        self._init_dbapi_attributes()
        self.Cursor, self.SSCursor = self._init_cursors_subclasses()

    def _init_dbapi_attributes(self):
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

    def connect(self, *arg, **kw):
        async_fallback = kw.pop("async_fallback", False)
        creator_fn = kw.pop("async_creator_fn", self.aiomysql.connect)

        if util.asbool(async_fallback):
            return AsyncAdaptFallback_aiomysql_connection(
                self,
                await_fallback(creator_fn(*arg, **kw)),
            )
        else:
            return AsyncAdapt_aiomysql_connection(
                self,
                await_only(creator_fn(*arg, **kw)),
            )

    def _init_cursors_subclasses(self):
        # suppress unconditional warning emitted by aiomysql
        class Cursor(self.aiomysql.Cursor):
            async def _show_warnings(self, conn):
                pass

        class SSCursor(self.aiomysql.SSCursor):
            async def _show_warnings(self, conn):
                pass

        return Cursor, SSCursor


class MySQLDialect_aiomysql(MySQLDialect_pymysql):
    driver = "aiomysql"
    supports_statement_cache = True

    supports_server_side_cursors = True
    _sscursor = AsyncAdapt_aiomysql_ss_cursor

    is_async = True
    has_terminate = True

    @classmethod
    def import_dbapi(cls):
        return AsyncAdapt_aiomysql_dbapi(
            __import__("aiomysql"), __import__("pymysql")
        )

    @classmethod
    def get_pool_class(cls, url):
        async_fallback = url.query.get("async_fallback", False)

        if util.asbool(async_fallback):
            return pool.FallbackAsyncAdaptedQueuePool
        else:
            return pool.AsyncAdaptedQueuePool

    def do_terminate(self, dbapi_connection) -> None:
        dbapi_connection.terminate()

    def create_connect_args(self, url):
        return super().create_connect_args(
            url, _translate_args=dict(username="user", database="db")
        )

    def is_disconnect(self, e, connection, cursor):
        if super().is_disconnect(e, connection, cursor):
            return True
        else:
            str_e = str(e).lower()
            return "not connected" in str_e

    def _found_rows_client_flag(self):
        from pymysql.constants import CLIENT

        return CLIENT.FOUND_ROWS

    def get_driver_connection(self, connection):
        return connection._connection


dialect = MySQLDialect_aiomysql
