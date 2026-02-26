# dialects/mysql/_mariadb_shim.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from __future__ import annotations

from typing import Any
from typing import cast
from typing import Optional
from typing import Type
from typing import TYPE_CHECKING

from .reserved_words import RESERVED_WORDS_MARIADB
from ... import exc
from ... import schema as sa_schema
from ... import util
from ...engine import cursor as _cursor
from ...engine import default
from ...engine.default import DefaultDialect
from ...engine.interfaces import TypeCompiler
from ...sql import elements
from ...sql import sqltypes
from ...sql.compiler import DDLCompiler
from ...sql.compiler import IdentifierPreparer
from ...sql.compiler import SQLCompiler
from ...sql.schema import SchemaConst
from ...sql.sqltypes import _UUID_RETURN
from ...sql.sqltypes import UUID
from ...sql.sqltypes import Uuid

if TYPE_CHECKING:
    from .base import MySQLIdentifierPreparer
    from .mariadb import INET4
    from .mariadb import INET6
    from ...engine import URL
    from ...engine.base import Connection
    from ...sql import ddl
    from ...sql.schema import IdentityOptions
    from ...sql.schema import Sequence as Sequence_SchemaItem
    from ...sql.type_api import _BindProcessorType


class _MariaDBUUID(UUID[_UUID_RETURN]):
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
    def native(self) -> bool:  # type: ignore[override]
        # override to return True, this is a native type, just turning
        # off native_uuid for internal data handling
        return True

    def bind_processor(self, dialect: MariaDBShim) -> Optional[_BindProcessorType[_UUID_RETURN]]:  # type: ignore[override] # noqa: E501
        if not dialect.supports_native_uuid or not dialect._allows_uuid_binds:
            return super().bind_processor(dialect)  # type: ignore[return-value] # noqa: E501
        else:
            return None


class MariaDBTypeCompilerShim(TypeCompiler):
    def visit_INET4(self, type_: INET4, **kwargs: Any) -> str:
        return "INET4"

    def visit_INET6(self, type_: INET6, **kwargs: Any) -> str:
        return "INET6"


class MariadbExecutionContextShim(default.DefaultExecutionContext):
    def post_exec(self) -> None:
        if (
            self.isdelete
            and cast(SQLCompiler, self.compiled).effective_returning
            and not self.cursor.description
        ):
            # All MySQL/mariadb drivers appear to not include
            # cursor.description for DELETE..RETURNING with no rows if the
            # WHERE criteria is a straight "false" condition such as our EMPTY
            # IN condition. manufacture an empty result in this case (issue
            # #10505)
            #
            # taken from cx_Oracle implementation
            self.cursor_fetch_strategy = (
                _cursor.FullyBufferedCursorFetchStrategy(
                    self.cursor,
                    [
                        (entry.keyname, None)  # type: ignore[misc]
                        for entry in cast(
                            SQLCompiler, self.compiled
                        )._result_columns
                    ],
                    [],
                )
            )

    def fire_sequence(
        self, seq: Sequence_SchemaItem, type_: sqltypes.Integer
    ) -> int:
        return self._execute_scalar(  # type: ignore[no-any-return]
            (
                "select nextval(%s)"
                % self.identifier_preparer.format_sequence(seq)
            ),
            type_,
        )


class MariaDBIdentifierPreparerShim(IdentifierPreparer):
    def _set_mariadb(self) -> None:
        self.reserved_words = RESERVED_WORDS_MARIADB


class MariaDBSQLCompilerShim(SQLCompiler):
    def visit_sequence(self, sequence: sa_schema.Sequence, **kw: Any) -> str:
        return "nextval(%s)" % self.preparer.format_sequence(sequence)

    def _mariadb_regexp_flags(
        self, flags: str, pattern: elements.ColumnElement[Any], **kw: Any
    ) -> str:
        return "CONCAT('(?', %s, ')', %s)" % (
            self.render_literal_value(flags, sqltypes.STRINGTYPE),
            self.process(pattern, **kw),
        )

    def _mariadb_regexp_match(
        self,
        op_string: str,
        binary: elements.BinaryExpression[Any],
        operator: Any,
        **kw: Any,
    ) -> str:
        flags = binary.modifiers["flags"]
        return "%s%s%s" % (
            self.process(binary.left, **kw),
            op_string,
            self._mariadb_regexp_flags(flags, binary.right),
        )

    def _mariadb_regexp_replace_op_binary(
        self, binary: elements.BinaryExpression[Any], operator: Any, **kw: Any
    ) -> str:
        flags = binary.modifiers["flags"]
        return "REGEXP_REPLACE(%s, %s, %s)" % (
            self.process(binary.left, **kw),
            self._mariadb_regexp_flags(flags, binary.right.clauses[0]),
            self.process(binary.right.clauses[1], **kw),
        )

    def _mariadb_visit_drop_check_constraint(
        self, drop: ddl.DropConstraint, **kw: Any
    ) -> str:
        constraint = drop.element
        qual = "CONSTRAINT "
        const = self.preparer.format_constraint(constraint)
        return "ALTER TABLE %s DROP %s%s" % (
            self.preparer.format_table(constraint.table),
            qual,
            const,
        )


class MariaDBDDLCompilerShim(DDLCompiler):
    dialect: MariaDBShim

    def _mariadb_get_column_specification(
        self, column: sa_schema.Column[Any], **kw: Any
    ) -> str:

        if (
            column.computed is not None
            and column._user_defined_nullable is SchemaConst.NULL_UNSPECIFIED
        ):
            kw["_force_column_to_nullable"] = True

        return self._mysql_get_column_specification(column, **kw)

    def _mysql_get_column_specification(
        self,
        column: sa_schema.Column[Any],
        *,
        _force_column_to_nullable: bool = False,
        **kw: Any,
    ) -> str:
        raise NotImplementedError()

    def get_identity_options(self, identity_options: IdentityOptions) -> str:
        text = super().get_identity_options(identity_options)
        text = text.replace("NO CYCLE", "NOCYCLE")
        return text

    def _mariadb_visit_drop_check_constraint(
        self, drop: ddl.DropConstraint, **kw: Any
    ) -> str:
        constraint = drop.element
        qual = "CONSTRAINT "
        const = self.preparer.format_constraint(constraint)
        return "ALTER TABLE %s DROP %s%s" % (
            self.preparer.format_table(constraint.table),
            qual,
            const,
        )


class MariaDBShim(DefaultDialect):
    server_version_info: tuple[int, ...]
    is_mariadb: bool
    _allows_uuid_binds = False

    identifier_preparer: MySQLIdentifierPreparer
    preparer: Type[MySQLIdentifierPreparer]

    def _set_mariadb(
        self, is_mariadb: Optional[bool], server_version_info: tuple[int, ...]
    ) -> None:
        if is_mariadb is None:
            return

        if not is_mariadb and self.is_mariadb:
            raise exc.InvalidRequestError(
                "MySQL version %s is not a MariaDB variant."
                % (".".join(map(str, server_version_info)),)
            )
        if is_mariadb:
            assert isinstance(self.colspecs, dict)
            self.colspecs = util.update_copy(
                self.colspecs, {Uuid: _MariaDBUUID}
            )

            self.identifier_preparer = self.preparer(self)
            self.identifier_preparer._set_mariadb()

            # this will be updated on first connect in initialize()
            # if using older mariadb version
            self.delete_returning = True
            self.insert_returning = True

        self.is_mariadb = is_mariadb

    @property
    def _mariadb_normalized_version_info(self) -> tuple[int, ...]:
        return self.server_version_info

    @property
    def _is_mariadb(self) -> bool:
        return self.is_mariadb

    @classmethod
    def _is_mariadb_from_url(cls, url: URL) -> bool:
        dbapi = cls.import_dbapi()
        dialect = cls(dbapi=dbapi)

        cargs, cparams = dialect.create_connect_args(url)
        conn = dialect.connect(*cargs, **cparams)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION() LIKE '%MariaDB%'")
            val = cursor.fetchone()[0]  # type: ignore[index]
        except Exception:
            raise
        else:
            return bool(val)
        finally:
            conn.close()

    def _initialize_mariadb(self, connection: Connection) -> None:
        assert self.is_mariadb

        self.supports_sequences = self.server_version_info >= (10, 3)

        self.delete_returning = self.server_version_info >= (10, 0, 5)

        self.insert_returning = self.server_version_info >= (10, 5)

        self._warn_for_known_db_issues()

        self.supports_native_uuid = (
            self.server_version_info is not None
            and self.server_version_info >= (10, 7)
        )
        self._allows_uuid_binds = True

        # ref https://mariadb.com/kb/en/mariadb-1021-release-notes/
        self._support_default_function = self.server_version_info >= (10, 2, 1)

        # ref https://mariadb.com/kb/en/mariadb-1045-release-notes/
        self._support_float_cast = self.server_version_info >= (10, 4, 5)

    def _warn_for_known_db_issues(self) -> None:
        if self.is_mariadb:
            mdb_version = self.server_version_info
            assert mdb_version is not None
            if mdb_version > (10, 2) and mdb_version < (10, 2, 9):
                util.warn(
                    "MariaDB %r before 10.2.9 has known issues regarding "
                    "CHECK constraints, which impact handling of NULL values "
                    "with SQLAlchemy's boolean datatype (MDEV-13596). An "
                    "additional issue prevents proper migrations of columns "
                    "with CHECK constraints (MDEV-11114).  Please upgrade to "
                    "MariaDB 10.2.9 or greater, or use the MariaDB 10.1 "
                    "series, to avoid these issues." % (mdb_version,)
                )
