# dialects/mysql/mariadb.py
# Copyright (C) 2005-2023 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors
from .base import MariaDBIdentifierPreparer, MySQLTypeCompiler
from .base import MySQLDialect


class MariaDBTypeCompiler(MySQLTypeCompiler):
    def visit_uuid(self, type_, **kw):
        if self.dialect.server_version_info >= (10, 7) and type_.native_uuid:
            return self.visit_UUID(type_, **kw)
        else:
            return super().visit_uuid(type_, **kw)


class MariaDBDialect(MySQLDialect):
    is_mariadb = True
    supports_statement_cache = True
    name = "mariadb"
    preparer = MariaDBIdentifierPreparer
    type_compiler_cls = MariaDBTypeCompiler


def loader(driver):
    driver_mod = __import__(
        "sqlalchemy.dialects.mysql.%s" % driver
    ).dialects.mysql
    driver_cls = getattr(driver_mod, driver).dialect

    return type(
        "MariaDBDialect_%s" % driver,
        (
            MariaDBDialect,
            driver_cls,
        ),
        {"supports_statement_cache": True},
    )
