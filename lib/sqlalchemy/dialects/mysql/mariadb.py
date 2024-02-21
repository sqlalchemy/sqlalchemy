# dialects/mysql/mariadb.py
# Copyright (C) 2005-2024 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors
from .base import MariaDBIdentifierPreparer
from .base import MySQLDialect
from ... import util
from ...sql.sqltypes import UUID
from ...sql.sqltypes import Uuid


class _MariaDBUUID(UUID):
    def __init__(self, as_uuid: bool = True, native_uuid: bool = True):
        self.as_uuid = as_uuid

        # the _MariaDBUUID internal type is only invoked for a Uuid() with
        # native_uuid=True.   for non-native uuid type, the plain Uuid
        # returns itself due to the workings of the Emulated superclass.
        assert native_uuid

        # for internal type, force string conversion for result_processor() as
        # current drivers are returning a string, not a Python UUID object
        self.native_uuid = False

    @property
    def native(self):
        # override to return True, this is a native type, just turning
        # off native_uuid for internal data handling
        return True

    def bind_processor(self, dialect):
        if not dialect.supports_native_uuid or not dialect._allows_uuid_binds:
            return super().bind_processor(dialect)
        else:
            return None

    def _sentinel_value_resolver(self, dialect):
        """Return a callable that will receive the uuid object or string
        as it is normally passed to the DB in the parameter set, after
        bind_processor() is called.  Convert this value to match
        what it would be as coming back from MariaDB RETURNING.  this seems
        to be *after* SQLAlchemy's datatype has converted, so these
        will be UUID objects if as_uuid=True and dashed strings if
        as_uuid=False

        """

        if not dialect._allows_uuid_binds:

            def process(value):
                return (
                    f"{value[0:8]}-{value[8:12]}-"
                    f"{value[12:16]}-{value[16:20]}-{value[20:]}"
                )

            return process
        elif self.as_uuid:
            return str
        else:
            return None


class MariaDBDialect(MySQLDialect):
    is_mariadb = True
    supports_statement_cache = True
    supports_native_uuid = True

    _allows_uuid_binds = True

    name = "mariadb"
    preparer = MariaDBIdentifierPreparer

    colspecs = util.update_copy(MySQLDialect.colspecs, {Uuid: _MariaDBUUID})

    def initialize(self, connection):
        super().initialize(connection)

        self.supports_native_uuid = (
            self.server_version_info is not None
            and self.server_version_info >= (10, 7)
        )


def loader(driver):
    dialect_mod = __import__(
        "sqlalchemy.dialects.mysql.%s" % driver
    ).dialects.mysql

    driver_mod = getattr(dialect_mod, driver)
    if hasattr(driver_mod, "mariadb_dialect"):
        driver_cls = driver_mod.mariadb_dialect
        return driver_cls
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
