# dialects/sqlite/aiosqlite.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php


r"""

.. dialect:: sqlite+aiosqlite
    :name: aiosqlite
    :dbapi: aiosqlite
    :connectstring: sqlite+aiosqlite:///file_path
    :url: https://pypi.org/project/aiosqlite/

The aiosqlite dialect provides support for the SQLAlchemy asyncio interface
running on top of pysqlite.

aiosqlite is a wrapper around pysqlite that uses a background thread for
each connection.   It does not actually use non-blocking IO, as SQLite
databases are not socket-based.  However it does provide a working asyncio
interface that's useful for testing and prototyping purposes.

Using a special asyncio mediation layer, the aiosqlite dialect is usable
as the backend for the :ref:`SQLAlchemy asyncio <asyncio_toplevel>`
extension package.

This dialect should normally be used only with the
:func:`_asyncio.create_async_engine` engine creation function::

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine("sqlite+aiosqlite:///filename")

The URL passes through all arguments to the ``pysqlite`` driver, so all
connection arguments are the same as they are for that of :ref:`pysqlite`.

.. _aiosqlite_udfs:

User-Defined Functions
----------------------

aiosqlite extends pysqlite to support async, so we can create our own user-defined functions (UDFs)
in Python and use them directly in SQLite queries as described here: :ref:`pysqlite_udfs`.

.. _aiosqlite_serializable:

Serializable isolation / Savepoints / Transactional DDL (asyncio version)
-------------------------------------------------------------------------

A newly revised version of this important section is now available
at the top level of the SQLAlchemy SQLite documentation, in the section
:ref:`sqlite_transactions`.


.. _aiosqlite_pooling:

Pooling Behavior
----------------

The SQLAlchemy ``aiosqlite`` DBAPI establishes the connection pool differently
based on the kind of SQLite database that's requested:

* When a ``:memory:`` SQLite database is specified, the dialect by default
  will use :class:`.StaticPool`. This pool maintains a single
  connection, so that all access to the engine
  use the same ``:memory:`` database.
* When a file-based database is specified, the dialect will use
  :class:`.AsyncAdaptedQueuePool` as the source of connections.

  .. versionchanged:: 2.0.38

    SQLite file database engines now use :class:`.AsyncAdaptedQueuePool` by default.
    Previously, :class:`.NullPool` were used.  The :class:`.NullPool` class
    may be used by specifying it via the
    :paramref:`_sa.create_engine.poolclass` parameter.

"""  # noqa
from __future__ import annotations

import asyncio
from functools import partial
from types import ModuleType
from typing import Any
from typing import cast
from typing import NoReturn
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from .base import SQLiteExecutionContext
from .pysqlite import SQLiteDialect_pysqlite
from ... import pool
from ...connectors.asyncio import AsyncAdapt_dbapi_connection
from ...connectors.asyncio import AsyncAdapt_dbapi_cursor
from ...connectors.asyncio import AsyncAdapt_dbapi_ss_cursor
from ...engine.interfaces import DBAPIModule
from ...util.concurrency import await_

if TYPE_CHECKING:
    from ...connectors.asyncio import AsyncIODBAPIConnection
    from ...engine.interfaces import DBAPIConnection
    from ...engine.interfaces import DBAPICursor
    from ...engine.url import URL
    from ...pool.base import PoolProxiedConnection


class AsyncAdapt_aiosqlite_cursor(AsyncAdapt_dbapi_cursor):
    __slots__ = ()


class AsyncAdapt_aiosqlite_ss_cursor(AsyncAdapt_dbapi_ss_cursor):
    __slots__ = ()


class AsyncAdapt_aiosqlite_connection(AsyncAdapt_dbapi_connection):
    __slots__ = ()

    _cursor_cls = AsyncAdapt_aiosqlite_cursor
    _ss_cursor_cls = AsyncAdapt_aiosqlite_ss_cursor

    @property
    def isolation_level(self) -> Optional[str]:
        return cast(str, self._connection.isolation_level)

    @isolation_level.setter
    def isolation_level(self, value: Optional[str]) -> None:
        # aiosqlite's isolation_level setter works outside the Thread
        # that it's supposed to, necessitating setting check_same_thread=False.
        # for improved stability, we instead invent our own awaitable version
        # using aiosqlite's async queue directly.

        def set_iso(
            connection: AsyncAdapt_aiosqlite_connection, value: Optional[str]
        ) -> None:
            connection.isolation_level = value

        function = partial(set_iso, self._connection._conn, value)
        future = asyncio.get_event_loop().create_future()

        self._connection._tx.put_nowait((future, function))

        try:
            return cast(None, await_(future))
        except Exception as error:
            self._handle_exception(error)

    def create_function(self, *args: Any, **kw: Any) -> None:
        try:
            cast(None, await_(self._connection.create_function(*args, **kw)))
        except Exception as error:
            self._handle_exception(error)

    def rollback(self) -> None:
        if self._connection._connection:
            super().rollback()

    def commit(self) -> None:
        if self._connection._connection:
            super().commit()

    def close(self) -> None:
        try:
            await_(self._connection.close())
        except ValueError:
            # this is undocumented for aiosqlite, that ValueError
            # was raised if .close() was called more than once, which is
            # both not customary for DBAPI and is also not a DBAPI.Error
            # exception. This is now fixed in aiosqlite via my PR
            # https://github.com/omnilib/aiosqlite/pull/238, so we can be
            # assured this will not become some other kind of exception,
            # since it doesn't raise anymore.

            pass
        except Exception as error:
            self._handle_exception(error)

    def _handle_exception(self, error: Exception) -> NoReturn:
        if isinstance(error, ValueError) and error.args[0].lower() in (
            "no active connection",
            "connection closed",
        ):
            raise self.dbapi.sqlite.OperationalError(error.args[0]) from error
        else:
            super()._handle_exception(error)


class AsyncAdapt_aiosqlite_dbapi:
    def __init__(self, aiosqlite: ModuleType, sqlite: ModuleType):
        self.aiosqlite = aiosqlite
        self.sqlite = sqlite
        self.paramstyle = "qmark"
        self._init_dbapi_attributes()

    def _init_dbapi_attributes(self) -> None:
        for name in (
            "DatabaseError",
            "Error",
            "IntegrityError",
            "NotSupportedError",
            "OperationalError",
            "ProgrammingError",
            "sqlite_version",
            "sqlite_version_info",
        ):
            setattr(self, name, getattr(self.aiosqlite, name))

        for name in ("PARSE_COLNAMES", "PARSE_DECLTYPES"):
            setattr(self, name, getattr(self.sqlite, name))

        for name in ("Binary",):
            setattr(self, name, getattr(self.sqlite, name))

    def connect(self, *arg: Any, **kw: Any) -> AsyncAdapt_aiosqlite_connection:
        creator_fn = kw.pop("async_creator_fn", None)
        if creator_fn:
            connection = creator_fn(*arg, **kw)
        else:
            connection = self.aiosqlite.connect(*arg, **kw)
            # it's a Thread.   you'll thank us later
            connection.daemon = True

        return AsyncAdapt_aiosqlite_connection(
            self,
            await_(connection),
        )

    def __getattr__(self, key: str) -> Any: ...


class SQLiteExecutionContext_aiosqlite(SQLiteExecutionContext):
    def create_server_side_cursor(self) -> DBAPICursor:
        return self._dbapi_connection.cursor(server_side=True)


class SQLiteDialect_aiosqlite(SQLiteDialect_pysqlite):
    driver = "aiosqlite"
    supports_statement_cache = True

    is_async = True

    supports_server_side_cursors = True

    execution_ctx_cls = SQLiteExecutionContext_aiosqlite

    @classmethod
    def import_dbapi(cls) -> AsyncAdapt_aiosqlite_dbapi:
        return AsyncAdapt_aiosqlite_dbapi(
            __import__("aiosqlite"), __import__("sqlite3")
        )

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        if cls._is_url_file_db(url):
            return pool.AsyncAdaptedQueuePool
        else:
            return pool.StaticPool

    def is_disconnect(
        self,
        e: DBAPIModule.Error,
        connection: Optional[Union[PoolProxiedConnection, DBAPIConnection]],
        cursor: Optional[DBAPICursor],
    ) -> bool:
        self.dbapi = cast(DBAPIModule, self.dbapi)
        if isinstance(e, self.dbapi.OperationalError):
            err_lower = str(e).lower()
            if (
                "no active connection" in err_lower
                or "connection closed" in err_lower
            ):
                return True

        return super().is_disconnect(e, connection, cursor)

    def get_driver_connection(
        self, connection: DBAPIConnection
    ) -> AsyncIODBAPIConnection:
        return connection._connection  # type: ignore[no-any-return]


dialect = SQLiteDialect_aiosqlite
