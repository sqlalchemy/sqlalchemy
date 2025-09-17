# dialects/mssql/mssqlpython.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

"""
.. dialect:: mssql+mssqlpython
    :name: mssqlpython
    :dbapi: mssql-python
    :connectstring: mssql+mssqlpython://<username>:<password>@<host>:<port>/<dbname>
    :url: https://github.com/microsoft/mssql-python

mssql-python is a driver for Microsoft SQL Server produced by Microsoft.

.. versionadded:: 2.1.0b2


The driver is generally similar to pyodbc in most aspects as it is based
on the same ODBC framework.

Connection Strings
------------------

Examples of connecting with the mssql-python driver::

    from sqlalchemy import create_engine

    # Basic connection
    engine = create_engine(
        "mssql+mssqlpython://user:password@hostname/database"
    )

    # With Windows Authentication
    engine = create_engine(
        "mssql+mssqlpython://hostname/database?authentication=ActiveDirectoryIntegrated"
    )

"""  # noqa

from __future__ import annotations

import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from .base import MSDialect
from .pyodbc import _ms_numeric_pyodbc
from ... import util
from ...sql import sqltypes

if TYPE_CHECKING:
    from ... import pool
    from ...engine import interfaces
    from ...engine.interfaces import ConnectArgsType
    from ...engine.interfaces import DBAPIModule
    from ...engine.interfaces import IsolationLevel
    from ...engine.interfaces import URL


class _MSNumeric_mssqlpython(_ms_numeric_pyodbc, sqltypes.Numeric):
    pass


class _MSFloat_mssqlpython(_ms_numeric_pyodbc, sqltypes.Float):
    pass


class MSDialect_mssqlpython(MSDialect):
    driver = "mssqlpython"
    supports_statement_cache = True

    supports_sane_rowcount_returning = True
    supports_sane_multi_rowcount = True
    supports_native_uuid = True
    scope_identity_must_be_embedded = True

    supports_native_decimal = True

    # used by pyodbc _ms_numeric_pyodbc class
    _need_decimal_fix = True

    colspecs = util.update_copy(
        MSDialect.colspecs,
        {
            sqltypes.Numeric: _MSNumeric_mssqlpython,
            sqltypes.Float: _MSFloat_mssqlpython,
        },
    )

    def __init__(self, enable_pooling=False, **kw):
        super().__init__(**kw)
        if not enable_pooling and self.dbapi is not None:
            self.loaded_dbapi.pooling(enabled=False)

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        return __import__("mssql_python")

    def create_connect_args(self, url: URL) -> ConnectArgsType:
        opts = url.translate_connect_args(username="user")
        opts.update(url.query)

        keys = opts

        query = url.query

        connect_args: Dict[str, Any] = {}
        connectors: List[str]

        def check_quote(token: str) -> str:
            if ";" in str(token) or str(token).startswith("{"):
                token = "{%s}" % token.replace("}", "}}")
            return token

        keys = {k: check_quote(v) for k, v in keys.items()}

        port = ""
        if "port" in keys and "port" not in query:
            port = ",%d" % int(keys.pop("port"))

        connectors = []

        connectors.extend(
            [
                "Server=%s%s" % (keys.pop("host", ""), port),
                "Database=%s" % keys.pop("database", ""),
            ]
        )

        user = keys.pop("user", None)
        if user:
            connectors.append("UID=%s" % user)
            pwd = keys.pop("password", "")
            if pwd:
                connectors.append("PWD=%s" % pwd)
        else:
            authentication = keys.pop("authentication", None)
            if authentication:
                connectors.append("Authentication=%s" % authentication)

        connectors.extend(["%s=%s" % (k, v) for k, v in keys.items()])

        return ((";".join(connectors),), connect_args)

    def is_disconnect(
        self,
        e: Exception,
        connection: Optional[
            Union[pool.PoolProxiedConnection, interfaces.DBAPIConnection]
        ],
        cursor: Optional[interfaces.DBAPICursor],
    ) -> bool:
        if isinstance(e, self.loaded_dbapi.ProgrammingError):
            return (
                "The cursor's connection has been closed." in str(e)
                or "Attempt to use a closed connection." in str(e)
                or "Driver Error: Operation cannot be performed" in str(e)
            )
        elif isinstance(e, self.loaded_dbapi.InterfaceError):
            return bool(re.search(r"Cannot .* on closed connection", str(e)))
        else:
            return False

    def _dbapi_version(self) -> interfaces.VersionInfoType:
        if not self.dbapi:
            return ()
        return self._parse_dbapi_version(self.dbapi.version)

    def _parse_dbapi_version(self, vers: str) -> interfaces.VersionInfoType:
        m = re.match(r"(?:py.*-)?([\d\.]+)(?:-(\w+))?", vers)
        if not m:
            return ()
        vers_tuple: interfaces.VersionInfoType = tuple(
            [int(x) for x in m.group(1).split(".")]
        )
        if m.group(2):
            vers_tuple += (m.group(2),)
        return vers_tuple

    def _get_server_version_info(self, connection):
        vers = connection.exec_driver_sql("select @@version").scalar()
        m = re.match(r"Microsoft .*? - (\d+)\.(\d+)\.(\d+)\.(\d+)", vers)
        if m:
            return tuple(int(x) for x in m.group(1, 2, 3, 4))
        else:
            return None

    def get_isolation_level_values(
        self, dbapi_connection: interfaces.DBAPIConnection
    ) -> List[IsolationLevel]:
        return [
            *super().get_isolation_level_values(dbapi_connection),
            "AUTOCOMMIT",
        ]

    def set_isolation_level(
        self,
        dbapi_connection: interfaces.DBAPIConnection,
        level: IsolationLevel,
    ) -> None:
        if level == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
        else:
            dbapi_connection.autocommit = False
            super().set_isolation_level(dbapi_connection, level)

    def detect_autocommit_setting(
        self, dbapi_conn: interfaces.DBAPIConnection
    ) -> bool:
        return bool(dbapi_conn.autocommit)


dialect = MSDialect_mssqlpython
