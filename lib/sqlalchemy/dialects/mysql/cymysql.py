# dialects/mysql/cymysql.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: disable-error-code="import-untyped,import-not-found"

r"""

.. dialect:: mysql+cymysql
    :name: CyMySQL
    :dbapi: cymysql
    :connectstring: mysql+cymysql://<username>:<password>@<host>/<dbname>[?<options>]
    :url: https://github.com/nakagami/CyMySQL

.. note::

    The CyMySQL dialect is **not tested as part of SQLAlchemy's continuous
    integration** and may have unresolved issues.  The recommended MySQL
    dialects are mysqlclient and PyMySQL.

"""  # noqa
from __future__ import annotations

from types import ModuleType
from typing import Any
from typing import Iterable
from typing import Optional
from typing import TYPE_CHECKING

from .base import MySQLDialect
from .mysqldb import MySQLDialect_mysqldb
from .types import BIT
from ... import util

if TYPE_CHECKING:
    import cymysql

    from ...engine.base import Connection
    from ...engine.interfaces import Dialect
    from ...sql.type_api import _ResultProcessorType


class _cymysqlBIT(BIT):
    def result_processor(
        self, dialect: Dialect, coltype: object
    ) -> Optional[_ResultProcessorType[Any]]:
        """Convert MySQL's 64 bit, variable length binary string to a long."""

        def process(value: Optional[Iterable[int]]) -> Optional[int]:
            if value is not None:
                v = 0
                for i in iter(value):
                    v = v << 8 | i
                return v
            return value

        return process


class MySQLDialect_cymysql(MySQLDialect_mysqldb):
    driver = "cymysql"
    supports_statement_cache = True

    description_encoding = None
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_unicode_statements = True
    dbapi: cymysql

    colspecs = util.update_copy(MySQLDialect.colspecs, {BIT: _cymysqlBIT})

    @classmethod
    def import_dbapi(cls) -> ModuleType:
        return __import__("cymysql")

    def _detect_charset(self, connection: Connection) -> str:
        return connection.connection.charset  # type: ignore

    def _extract_error_code(self, exception: BaseException) -> int:
        return exception.errno  # type: ignore

    def is_disconnect(
        self, e: Exception, connection: Any, cursor: Any
    ) -> bool:
        if isinstance(e, self.dbapi.OperationalError):
            return self._extract_error_code(e) in (
                2006,
                2013,
                2014,
                2045,
                2055,
            )
        elif isinstance(e, self.dbapi.InterfaceError):
            # if underlying connection is closed,
            # this is the error you get
            return True
        else:
            return False


dialect = MySQLDialect_cymysql
