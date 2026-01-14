# dialects/mysql/mariadb.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from __future__ import annotations

from typing import Any

from .base import MySQLDialect
from ...sql import sqltypes


class INET4(sqltypes.TypeEngine[str]):
    """INET4 column type for MariaDB

    .. versionadded:: 2.0.37
    """

    __visit_name__ = "INET4"


class INET6(sqltypes.TypeEngine[str]):
    """INET6 column type for MariaDB

    .. versionadded:: 2.0.37
    """

    __visit_name__ = "INET6"


class MariaDBDialect(MySQLDialect):
    is_mariadb = True
    supports_statement_cache = True
    supports_native_uuid = True

    _allows_uuid_binds = True

    name = "mariadb"

    def __init__(self, **kw: Any) -> None:
        kw["is_mariadb"] = True
        super().__init__(**kw)


def loader(driver: str) -> type[MariaDBDialect]:
    dialect_mod = __import__(
        "sqlalchemy.dialects.mysql.%s" % driver
    ).dialects.mysql

    driver_mod = getattr(dialect_mod, driver)
    if hasattr(driver_mod, "mariadb_dialect"):
        driver_cls = driver_mod.mariadb_dialect
        return driver_cls  # type: ignore[no-any-return]
    else:
        driver_cls = driver_mod.dialect

        return type(
            "MariaDBDialect_%s" % driver,
            (
                MariaDBDialect,
                driver_cls,
            ),
            {"supports_statement_cache": True},
        )
