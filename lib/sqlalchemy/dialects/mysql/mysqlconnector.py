# dialects/mysql/mysqlconnector.py
# Copyright (C) 2005-2024 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php


r"""
.. dialect:: mysql+mysqlconnector
    :name: MySQL Connector/Python
    :dbapi: myconnpy
    :connectstring: mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>
    :url: https://pypi.org/project/mysql-connector-python/

.. note::

    The MySQL Connector/Python DBAPI has had many issues since its release,
    some of which may remain unresolved, and the mysqlconnector dialect is
    **not tested as part of SQLAlchemy's continuous integration**.
    The recommended MySQL dialects are mysqlclient and PyMySQL.

"""  # noqa

import re
from types import ModuleType
from typing import Any
from typing import Sequence
from typing import TYPE_CHECKING

from .base import MySQLCompiler
from .base import MySQLDialect
from .base import MySQLIdentifierPreparer
from .mariadb import MariaDBDialect
from .types import BIT
from ... import util
from ...util.typing import Unpack

if TYPE_CHECKING:
    from mysql import connector
    from mysql.connector.abstracts import MySQLConnectionAbstract

    from ...engine.base import Connection
    from ...engine.cursor import CursorResult
    from ...engine.interfaces import ConnectArgsType
    from ...engine.row import Row
    from ...engine.url import URL
    from ...sql.elements import BinaryExpression
    from ...util.typing import TupleAny

    dbapi_connection = (
        connector.pooling.PooledMySQLConnection | MySQLConnectionAbstract
    )


class MySQLCompiler_mysqlconnector(MySQLCompiler):
    def visit_mod_binary(
        self, binary: "BinaryExpression[Any]", operator: Any, **kw: Any
    ) -> str:
        return (
            self.process(binary.left, **kw)
            + " % "
            + self.process(binary.right, **kw)
        )


class MySQLIdentifierPreparer_mysqlconnector(MySQLIdentifierPreparer):
    @property
    def _double_percents(self) -> bool:
        return False

    @_double_percents.setter
    def _double_percents(self, value: Any) -> None:
        pass

    def _escape_identifier(self, value: str) -> str:
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value


class _myconnpyBIT(BIT):
    def result_processor(self, dialect: Any, coltype: Any) -> None:
        """MySQL-connector already converts mysql bits, so."""

        return None


class MySQLDialect_mysqlconnector(MySQLDialect):
    driver = "mysqlconnector"
    supports_statement_cache = True

    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_native_decimal = True

    default_paramstyle = "format"
    statement_compiler = MySQLCompiler_mysqlconnector

    preparer: type[MySQLIdentifierPreparer] = (
        MySQLIdentifierPreparer_mysqlconnector
    )

    colspecs = util.update_copy(MySQLDialect.colspecs, {BIT: _myconnpyBIT})
    dbapi: "connector"  # type: ignore[valid-type]

    @classmethod
    def import_dbapi(cls) -> ModuleType:
        from mysql import connector

        return connector

    def do_ping(
        self, dbapi_connection: "dbapi_connection"  # type:ignore[override]
    ) -> bool:
        dbapi_connection.ping(False)
        return True

    def create_connect_args(self, url: "URL") -> "ConnectArgsType":
        opts = url.translate_connect_args(username="user")

        opts.update(url.query)

        util.coerce_kw_type(opts, "allow_local_infile", bool)
        util.coerce_kw_type(opts, "autocommit", bool)
        util.coerce_kw_type(opts, "buffered", bool)
        util.coerce_kw_type(opts, "client_flag", int)
        util.coerce_kw_type(opts, "compress", bool)
        util.coerce_kw_type(opts, "connection_timeout", int)
        util.coerce_kw_type(opts, "connect_timeout", int)
        util.coerce_kw_type(opts, "consume_results", bool)
        util.coerce_kw_type(opts, "force_ipv6", bool)
        util.coerce_kw_type(opts, "get_warnings", bool)
        util.coerce_kw_type(opts, "pool_reset_session", bool)
        util.coerce_kw_type(opts, "pool_size", int)
        util.coerce_kw_type(opts, "raise_on_warnings", bool)
        util.coerce_kw_type(opts, "raw", bool)
        util.coerce_kw_type(opts, "ssl_verify_cert", bool)
        util.coerce_kw_type(opts, "use_pure", bool)
        util.coerce_kw_type(opts, "use_unicode", bool)

        # unfortunately, MySQL/connector python refuses to release a
        # cursor without reading fully, so non-buffered isn't an option
        opts.setdefault("buffered", True)

        # FOUND_ROWS must be set in ClientFlag to enable
        # supports_sane_rowcount.
        if self.dbapi is not None:
            try:
                from mysql.connector.constants import ClientFlag

                client_flags = opts.get(
                    "client_flags", ClientFlag.get_default()
                )
                client_flags |= ClientFlag.FOUND_ROWS
                opts["client_flags"] = client_flags
            except Exception:
                pass
        return [], opts

    @util.memoized_property
    def _mysqlconnector_version_info(self) -> None | tuple[int, ...]:
        if self.dbapi and hasattr(self.dbapi, "__version__"):
            m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", self.dbapi.__version__)  # type: ignore[attr-defined] # noqa: E501
            if m:
                return tuple(int(x) for x in m.group(1, 2, 3) if x is not None)
        return None

    def _detect_charset(self, connection: "Connection") -> str:
        return connection.connection.charset  # type: ignore

    def _extract_error_code(self, exception: BaseException) -> int:
        return exception.errno  # type: ignore

    def is_disconnect(
        self, e: Exception, connection: Any, cursor: Any
    ) -> bool:
        errnos = (2006, 2013, 2014, 2045, 2055, 2048)
        exceptions = (self.dbapi.OperationalError, self.dbapi.InterfaceError)  # type: ignore[attr-defined] # noqa: E501
        if isinstance(e, exceptions):
            return (
                e.errno in errnos
                or "MySQL Connection not available." in str(e)
                or "Connection to MySQL is not available" in str(e)
            )
        else:
            return False

    def _compat_fetchall(
        self, rp: "CursorResult[Unpack[TupleAny]]", charset: str | None = None
    ) -> "Sequence[Row[tuple[Any, ...]]]":
        return rp.fetchall()

    def _compat_fetchone(
        self, rp: "CursorResult[Unpack[TupleAny]]", charset: str | None = None
    ) -> "Row[Unpack[tuple[Any, ...]]] | None":
        return rp.fetchone()

    _isolation_lookup = {
        "SERIALIZABLE",
        "READ UNCOMMITTED",
        "READ COMMITTED",
        "REPEATABLE READ",
        "AUTOCOMMIT",
    }


class MariaDBDialect_mysqlconnector(
    MariaDBDialect, MySQLDialect_mysqlconnector
):
    supports_statement_cache = True
    _allows_uuid_binds = False


dialect = MySQLDialect_mysqlconnector
mariadb_dialect = MariaDBDialect_mysqlconnector
