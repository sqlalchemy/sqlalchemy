# Copyright (C) 2013-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

import datetime as dt
from typing import Any

from ... import schema
from ... import util
from ...sql import sqltypes
from ...sql.ddl import InvokeDDLBase


_DECIMAL_TYPES = (1231, 1700)
_FLOAT_TYPES = (700, 701, 1021, 1022)
_INT_TYPES = (20, 21, 23, 26, 1005, 1007, 1016)


class PGUuid(sqltypes.UUID):
    render_bind_cast = True
    render_literal_cast = True


class BYTEA(sqltypes.LargeBinary[bytes]):
    __visit_name__ = "BYTEA"


class INET(sqltypes.TypeEngine[str]):
    __visit_name__ = "INET"


PGInet = INET


class CIDR(sqltypes.TypeEngine[str]):
    __visit_name__ = "CIDR"


PGCidr = CIDR


class MACADDR(sqltypes.TypeEngine[str]):
    __visit_name__ = "MACADDR"


PGMacAddr = MACADDR


class MONEY(sqltypes.TypeEngine[str]):

    r"""Provide the PostgreSQL MONEY type.

    Depending on driver, result rows using this type may return a
    string value which includes currency symbols.

    For this reason, it may be preferable to provide conversion to a
    numerically-based currency datatype using :class:`_types.TypeDecorator`::

        import re
        import decimal
        from sqlalchemy import TypeDecorator

        class NumericMoney(TypeDecorator):
            impl = MONEY

            def process_result_value(self, value: Any, dialect: Any) -> None:
                if value is not None:
                    # adjust this for the currency and numeric
                    m = re.match(r"\$([\d.]+)", value)
                    if m:
                        value = decimal.Decimal(m.group(1))
                return value

    Alternatively, the conversion may be applied as a CAST using
    the :meth:`_types.TypeDecorator.column_expression` method as follows::

        import decimal
        from sqlalchemy import cast
        from sqlalchemy import TypeDecorator

        class NumericMoney(TypeDecorator):
            impl = MONEY

            def column_expression(self, column: Any):
                return cast(column, Numeric())

    .. versionadded:: 1.2

    """

    __visit_name__ = "MONEY"


class OID(sqltypes.TypeEngine[int]):

    """Provide the PostgreSQL OID type.

    .. versionadded:: 0.9.5

    """

    __visit_name__ = "OID"


class REGCLASS(sqltypes.TypeEngine[str]):

    """Provide the PostgreSQL REGCLASS type.

    .. versionadded:: 1.2.7

    """

    __visit_name__ = "REGCLASS"


class TIMESTAMP(sqltypes.TIMESTAMP):
    def __init__(self, timezone=False, precision=None):
        super(TIMESTAMP, self).__init__(timezone=timezone)
        self.precision = precision


class TIME(sqltypes.TIME):
    def __init__(self, timezone=False, precision=None):
        super(TIME, self).__init__(timezone=timezone)
        self.precision = precision


class INTERVAL(sqltypes.NativeForEmulated, sqltypes._AbstractInterval):

    """PostgreSQL INTERVAL type."""

    __visit_name__ = "INTERVAL"
    native = True

    def __init__(self, precision=None, fields=None):
        """Construct an INTERVAL.

        :param precision: optional integer precision value
        :param fields: string fields specifier.  allows storage of fields
         to be limited, such as ``"YEAR"``, ``"MONTH"``, ``"DAY TO HOUR"``,
         etc.

         .. versionadded:: 1.2

        """
        self.precision = precision
        self.fields = fields

    @classmethod
    def adapt_emulated_to_native(cls, interval, **kw):
        return INTERVAL(precision=interval.second_precision)

    @property
    def _type_affinity(self):
        return sqltypes.Interval

    def as_generic(self, allow_nulltype=False):
        return sqltypes.Interval(native=True, second_precision=self.precision)

    @property
    def python_type(self):
        return dt.timedelta


PGInterval = INTERVAL


class BIT(sqltypes.TypeEngine[int]):
    __visit_name__ = "BIT"

    def __init__(self, length=None, varying=False):
        if not varying:
            # BIT without VARYING defaults to length 1
            self.length = length or 1
        else:
            # but BIT VARYING can be unlimited-length, so no default
            self.length = length
        self.varying = varying


PGBit = BIT


class TSVECTOR(sqltypes.TypeEngine[Any]):

    """The :class:`_postgresql.TSVECTOR` type implements the PostgreSQL
    text search type TSVECTOR.

    It can be used to do full text queries on natural language
    documents.

    .. versionadded:: 0.9.0

    .. seealso::

        :ref:`postgresql_match`

    """

    __visit_name__ = "TSVECTOR"


class ENUM(sqltypes.NativeForEmulated, sqltypes.Enum):

    """PostgreSQL ENUM type.

    This is a subclass of :class:`_types.Enum` which includes
    support for PG's ``CREATE TYPE`` and ``DROP TYPE``.

    When the builtin type :class:`_types.Enum` is used and the
    :paramref:`.Enum.native_enum` flag is left at its default of
    True, the PostgreSQL backend will use a :class:`_postgresql.ENUM`
    type as the implementation, so the special create/drop rules
    will be used.

    The create/drop behavior of ENUM is necessarily intricate, due to the
    awkward relationship the ENUM type has in relationship to the
    parent table, in that it may be "owned" by just a single table, or
    may be shared among many tables.

    When using :class:`_types.Enum` or :class:`_postgresql.ENUM`
    in an "inline" fashion, the ``CREATE TYPE`` and ``DROP TYPE`` is emitted
    corresponding to when the :meth:`_schema.Table.create` and
    :meth:`_schema.Table.drop`
    methods are called::

        table = Table('sometable', metadata,
            Column('some_enum', ENUM('a', 'b', 'c', name='myenum'))
        )

        table.create(engine)  # will emit CREATE ENUM and CREATE TABLE
        table.drop(engine)  # will emit DROP TABLE and DROP ENUM

    To use a common enumerated type between multiple tables, the best
    practice is to declare the :class:`_types.Enum` or
    :class:`_postgresql.ENUM` independently, and associate it with the
    :class:`_schema.MetaData` object itself::

        my_enum = ENUM('a', 'b', 'c', name='myenum', metadata=metadata)

        t1 = Table('sometable_one', metadata,
            Column('some_enum', myenum)
        )

        t2 = Table('sometable_two', metadata,
            Column('some_enum', myenum)
        )

    When this pattern is used, care must still be taken at the level
    of individual table creates.  Emitting CREATE TABLE without also
    specifying ``checkfirst=True`` will still cause issues::

        t1.create(engine) # will fail: no such type 'myenum'

    If we specify ``checkfirst=True``, the individual table-level create
    operation will check for the ``ENUM`` and create if not exists::

        # will check if enum exists, and emit CREATE TYPE if not
        t1.create(engine, checkfirst=True)

    When using a metadata-level ENUM type, the type will always be created
    and dropped if either the metadata-wide create/drop is called::

        metadata.create_all(engine)  # will emit CREATE TYPE
        metadata.drop_all(engine)  # will emit DROP TYPE

    The type can also be created and dropped directly::

        my_enum.create(engine)
        my_enum.drop(engine)

    .. versionchanged:: 1.0.0 The PostgreSQL :class:`_postgresql.ENUM` type
       now behaves more strictly with regards to CREATE/DROP.  A metadata-level
       ENUM type will only be created and dropped at the metadata level,
       not the table level, with the exception of
       ``table.create(checkfirst=True)``.
       The ``table.drop()`` call will now emit a DROP TYPE for a table-level
       enumerated type.

    """

    native_enum = True

    def __init__(self, *enums, **kw):
        """Construct an :class:`_postgresql.ENUM`.

        Arguments are the same as that of
        :class:`_types.Enum`, but also including
        the following parameters.

        :param create_type: Defaults to True.
         Indicates that ``CREATE TYPE`` should be
         emitted, after optionally checking for the
         presence of the type, when the parent
         table is being created; and additionally
         that ``DROP TYPE`` is called when the table
         is dropped.    When ``False``, no check
         will be performed and no ``CREATE TYPE``
         or ``DROP TYPE`` is emitted, unless
         :meth:`~.postgresql.ENUM.create`
         or :meth:`~.postgresql.ENUM.drop`
         are called directly.
         Setting to ``False`` is helpful
         when invoking a creation scheme to a SQL file
         without access to the actual database -
         the :meth:`~.postgresql.ENUM.create` and
         :meth:`~.postgresql.ENUM.drop` methods can
         be used to emit SQL to a target bind.

        """
        native_enum = kw.pop("native_enum", None)
        if native_enum is False:
            util.warn(
                "the native_enum flag does not apply to the "
                "sqlalchemy.dialects.postgresql.ENUM datatype; this type "
                "always refers to ENUM.   Use sqlalchemy.types.Enum for "
                "non-native enum."
            )
        self.create_type = kw.pop("create_type", True)
        super(ENUM, self).__init__(*enums, **kw)

    @classmethod
    def adapt_emulated_to_native(cls, impl, **kw):
        """Produce a PostgreSQL native :class:`_postgresql.ENUM` from plain
        :class:`.Enum`.

        """
        kw.setdefault("validate_strings", impl.validate_strings)
        kw.setdefault("name", impl.name)
        kw.setdefault("schema", impl.schema)
        kw.setdefault("inherit_schema", impl.inherit_schema)
        kw.setdefault("metadata", impl.metadata)
        kw.setdefault("_create_events", False)
        kw.setdefault("values_callable", impl.values_callable)
        kw.setdefault("omit_aliases", impl._omit_aliases)
        return cls(**kw)

    def create(self, bind=None, checkfirst=True):
        """Emit ``CREATE TYPE`` for this
        :class:`_postgresql.ENUM`.

        If the underlying dialect does not support
        PostgreSQL CREATE TYPE, no action is taken.

        :param bind: a connectable :class:`_engine.Engine`,
         :class:`_engine.Connection`, or similar object to emit
         SQL.
        :param checkfirst: if ``True``, a query against
         the PG catalog will be first performed to see
         if the type does not exist already before
         creating.

        """
        if not bind.dialect.supports_native_enum:
            return

        bind._run_ddl_visitor(self.EnumGenerator, self, checkfirst=checkfirst)

    def drop(self, bind=None, checkfirst=True):
        """Emit ``DROP TYPE`` for this
        :class:`_postgresql.ENUM`.

        If the underlying dialect does not support
        PostgreSQL DROP TYPE, no action is taken.

        :param bind: a connectable :class:`_engine.Engine`,
         :class:`_engine.Connection`, or similar object to emit
         SQL.
        :param checkfirst: if ``True``, a query against
         the PG catalog will be first performed to see
         if the type actually exists before dropping.

        """
        if not bind.dialect.supports_native_enum:
            return

        bind._run_ddl_visitor(self.EnumDropper, self, checkfirst=checkfirst)

    class EnumGenerator(InvokeDDLBase):
        def __init__(self, dialect, connection, checkfirst=False, **kwargs):
            super(ENUM.EnumGenerator, self).__init__(connection, **kwargs)
            self.checkfirst = checkfirst

        def _can_create_enum(self, enum):
            if not self.checkfirst:
                return True

            effective_schema = self.connection.schema_for_object(enum)

            return not self.connection.dialect.has_type(
                self.connection, enum.name, schema=effective_schema
            )

        def visit_enum(self, enum):
            if not self._can_create_enum(enum):
                return

            self.connection.execute(CreateEnumType(enum))

    class EnumDropper(InvokeDDLBase):
        def __init__(self, dialect, connection, checkfirst=False, **kwargs):
            super(ENUM.EnumDropper, self).__init__(connection, **kwargs)
            self.checkfirst = checkfirst

        def _can_drop_enum(self, enum):
            if not self.checkfirst:
                return True

            effective_schema = self.connection.schema_for_object(enum)

            return self.connection.dialect.has_type(
                self.connection, enum.name, schema=effective_schema
            )

        def visit_enum(self, enum):
            if not self._can_drop_enum(enum):
                return

            self.connection.execute(DropEnumType(enum))

    def get_dbapi_type(self, dbapi):
        """dont return dbapi.STRING for ENUM in PostgreSQL, since that's
        a different type"""

        return None

    def _check_for_name_in_memos(self, checkfirst, kw):
        """Look in the 'ddl runner' for 'memos', then
        note our name in that collection.

        This to ensure a particular named enum is operated
        upon only once within any kind of create/drop
        sequence without relying upon "checkfirst".

        """
        if not self.create_type:
            return True
        if "_ddl_runner" in kw:
            ddl_runner = kw["_ddl_runner"]
            if "_pg_enums" in ddl_runner.memo:
                pg_enums = ddl_runner.memo["_pg_enums"]
            else:
                pg_enums = ddl_runner.memo["_pg_enums"] = set()
            present = (self.schema, self.name) in pg_enums
            pg_enums.add((self.schema, self.name))
            return present
        else:
            return False

    def _on_table_create(self, target, bind, checkfirst=False, **kw):
        if (
            checkfirst
            or (
                not self.metadata
                and not kw.get("_is_metadata_operation", False)
            )
        ) and not self._check_for_name_in_memos(checkfirst, kw):
            self.create(bind=bind, checkfirst=checkfirst)

    def _on_table_drop(self, target, bind, checkfirst=False, **kw):
        if (
            not self.metadata
            and not kw.get("_is_metadata_operation", False)
            and not self._check_for_name_in_memos(checkfirst, kw)
        ):
            self.drop(bind=bind, checkfirst=checkfirst)

    def _on_metadata_create(self, target, bind, checkfirst=False, **kw):
        if not self._check_for_name_in_memos(checkfirst, kw):
            self.create(bind=bind, checkfirst=checkfirst)

    def _on_metadata_drop(self, target, bind, checkfirst=False, **kw):
        if not self._check_for_name_in_memos(checkfirst, kw):
            self.drop(bind=bind, checkfirst=checkfirst)


class CreateEnumType(schema._CreateDropBase):
    __visit_name__ = "create_enum_type"


class DropEnumType(schema._CreateDropBase):
    __visit_name__ = "drop_enum_type"
