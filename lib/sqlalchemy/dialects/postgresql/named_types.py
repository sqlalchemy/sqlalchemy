# dialects/postgresql/named_types.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors
from __future__ import annotations

from typing import Any
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from ...engine import Dialect
from ... import schema
from ... import util
from ...sql import coercions
from ...sql import ddl
from ...sql import elements
from ...sql import roles
from ...sql import sqltypes
from ...sql import type_api
from ...sql.base import _NoArg
from ...sql.schema import SchemaType

if TYPE_CHECKING:
    from ...sql._typing import _TypeEngineArgument


NamedType = SchemaType


class ENUM(type_api.NativeForEmulated, sqltypes.Enum):
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

        table = Table(
            "sometable",
            metadata,
            Column("some_enum", ENUM("a", "b", "c", name="myenum")),
        )

        table.create(engine)  # will emit CREATE ENUM and CREATE TABLE
        table.drop(engine)  # will emit DROP TABLE and DROP ENUM

    To use a common enumerated type between multiple tables, the best
    practice is to declare the :class:`_types.Enum` or
    :class:`_postgresql.ENUM` independently, and associate it with the
    :class:`_schema.MetaData` object itself::

        my_enum = ENUM("a", "b", "c", name="myenum", metadata=metadata)

        t1 = Table("sometable_one", metadata, Column("some_enum", myenum))

        t2 = Table("sometable_two", metadata, Column("some_enum", myenum))

    When this pattern is used, care must still be taken at the level
    of individual table creates.  Emitting CREATE TABLE without also
    specifying ``checkfirst=True`` will still cause issues::

        t1.create(engine)  # will fail: no such type 'myenum'

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

    """

    native_enum = True

    def __init__(
        self,
        *enums,
        name: Union[str, _NoArg, None] = _NoArg.NO_ARG,
        create_type: bool = True,
        **kw,
    ):
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
        self.create_type = create_type
        if name is not _NoArg.NO_ARG:
            kw["name"] = name
        super().__init__(*enums, create_type=create_type, **kw)

    def coerce_compared_value(self, op, value):
        super_coerced_type = super().coerce_compared_value(op, value)
        if (
            super_coerced_type._type_affinity
            is type_api.STRINGTYPE._type_affinity
        ):
            return self
        else:
            return super_coerced_type

    @classmethod
    def __test_init__(cls):
        return cls(name="name")

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
        kw.setdefault("values_callable", impl.values_callable)
        kw.setdefault("omit_aliases", impl._omit_aliases)
        kw.setdefault("_adapted_from", impl)

        return cls(**kw)

    def get_dbapi_type(self, dbapi):
        """dont return dbapi.STRING for ENUM in PostgreSQL, since that's
        a different type"""

        return None


class DOMAIN(NamedType, type_api.TypeEngine[str]):
    r"""Represent the DOMAIN PostgreSQL type.

    A domain is essentially a data type with optional constraints
    that restrict the allowed set of values. E.g.::

        PositiveInt = DOMAIN("pos_int", Integer, check="VALUE > 0", not_null=True)

        UsPostalCode = DOMAIN(
            "us_postal_code",
            Text,
            check="VALUE ~ '^\d{5}$' OR VALUE ~ '^\d{5}-\d{4}$'",
        )

    See the `PostgreSQL documentation`__ for additional details

    __ https://www.postgresql.org/docs/current/sql-createdomain.html

    .. versionadded:: 2.0

    """  # noqa: E501

    __visit_name__ = "DOMAIN"

    def __init__(
        self,
        name: str,
        data_type: _TypeEngineArgument[Any],
        *,
        collation: Optional[str] = None,
        default: Union[elements.TextClause, str, None] = None,
        constraint_name: Optional[str] = None,
        not_null: Optional[bool] = None,
        check: Union[elements.TextClause, str, None] = None,
        create_type: bool = True,
        **kw: Any,
    ):
        """
        Construct a DOMAIN.

        :param name: the name of the domain
        :param data_type: The underlying data type of the domain.
          This can include array specifiers.
        :param collation: An optional collation for the domain.
          If no collation is specified, the underlying data type's default
          collation is used. The underlying type must be collatable if
          ``collation`` is specified.
        :param default: The DEFAULT clause specifies a default value for
          columns of the domain data type. The default should be a string
          or a :func:`_expression.text` value.
          If no default value is specified, then the default value is
          the null value.
        :param constraint_name: An optional name for a constraint.
          If not specified, the backend generates a name.
        :param not_null: Values of this domain are prevented from being null.
          By default domain are allowed to be null. If not specified
          no nullability clause will be emitted.
        :param check: CHECK clause specify integrity constraint or test
          which values of the domain must satisfy. A constraint must be
          an expression producing a Boolean result that can use the key
          word VALUE to refer to the value being tested.
          Differently from PostgreSQL, only a single check clause is
          currently allowed in SQLAlchemy.
        :param schema: optional schema name
        :param metadata: optional :class:`_schema.MetaData` object which
         this :class:`_postgresql.DOMAIN` will be directly associated
        :param create_type: Defaults to True.
         Indicates that ``CREATE TYPE`` should be emitted, after optionally
         checking for the presence of the type, when the parent table is
         being created; and additionally that ``DROP TYPE`` is called
         when the table is dropped.

        """
        self.data_type = type_api.to_instance(data_type)
        self.default = default
        self.collation = collation
        self.constraint_name = constraint_name
        self.not_null = bool(not_null)
        if check is not None:
            check = coercions.expect(roles.DDLExpressionRole, check)
        self.check = check
        super().__init__(name=name, create_type=create_type, **kw)

    @classmethod
    def __test_init__(cls):
        return cls("name", sqltypes.Integer)

    def adapt(self, impl, **kw):
        if self.constraint_name is not None:
            kw["constraint_name"] = self.constraint_name
        if self.default is not None:
            kw["default"] = self.default
        if self.collation is not None:
            kw["collation"] = self.collation
        if self.not_null:
            kw["not_null"] = self.not_null
        if self.check is not None:
            kw["check"] = str(self.check)
        if self.create_type:
            kw["create_type"] = self.create_type

        return super().adapt(impl, **kw)

    def supports_create(self, dialect: Dialect):
        return dialect.name == "postgresql"

CreateEnumType = ddl.CreateEnum
DropEnumType = ddl.DropEnum


class CreateDomainType(schema._CreateDropBase):
    """Represent a CREATE DOMAIN statement."""

    __visit_name__ = "create_domain_type"


class DropDomainType(schema._CreateDropBase):
    """Represent a DROP DOMAIN statement."""

    __visit_name__ = "drop_domain_type"
