# dialects/postgresql/types.py
# Copyright (C) 2013-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
from __future__ import annotations

import datetime as dt
from typing import Any
from typing import Literal
from typing import Optional
from typing import overload
from typing import Type
from typing import TYPE_CHECKING
from uuid import UUID as _python_UUID

from .bitstring import BitString
from ...sql import sqltypes
from ...sql import type_api
from ...sql.type_api import TypeEngine
from ...types import OperatorClass

if TYPE_CHECKING:
    from ...engine.interfaces import Dialect
    from ...sql.operators import ColumnOperators
    from ...sql.operators import OperatorType
    from ...sql.type_api import _BindProcessorType
    from ...sql.type_api import _LiteralProcessorType
    from ...sql.type_api import _ResultProcessorType

_DECIMAL_TYPES = (1231, 1700)
_FLOAT_TYPES = (700, 701, 1021, 1022)
_INT_TYPES = (20, 21, 23, 26, 1005, 1007, 1016)


class PGUuid(sqltypes.UUID[sqltypes._UUID_RETURN]):
    render_bind_cast = True
    render_literal_cast = True

    if TYPE_CHECKING:

        @overload
        def __init__(
            self: PGUuid[_python_UUID], as_uuid: Literal[True] = ...
        ) -> None: ...

        @overload
        def __init__(
            self: PGUuid[str], as_uuid: Literal[False] = ...
        ) -> None: ...

        def __init__(self, as_uuid: bool = True) -> None: ...


class BYTEA(sqltypes.LargeBinary):
    __visit_name__ = "BYTEA"


class _NetworkAddressTypeMixin:
    operator_classes = OperatorClass.BASE | OperatorClass.COMPARISON

    def coerce_compared_value(
        self, op: Optional[OperatorType], value: Any
    ) -> TypeEngine[Any]:
        if TYPE_CHECKING:
            assert isinstance(self, TypeEngine)
        return self


class INET(_NetworkAddressTypeMixin, sqltypes.TypeEngine[str]):
    __visit_name__ = "INET"


PGInet = INET


class CIDR(_NetworkAddressTypeMixin, sqltypes.TypeEngine[str]):
    __visit_name__ = "CIDR"


PGCidr = CIDR


class MACADDR(_NetworkAddressTypeMixin, sqltypes.TypeEngine[str]):
    __visit_name__ = "MACADDR"


PGMacAddr = MACADDR


class MACADDR8(_NetworkAddressTypeMixin, sqltypes.TypeEngine[str]):
    __visit_name__ = "MACADDR8"


PGMacAddr8 = MACADDR8


class MONEY(sqltypes.TypeEngine[str]):
    r"""Provide the PostgreSQL MONEY type.

    Depending on driver, result rows using this type may return a
    string value which includes currency symbols.

    For this reason, it may be preferable to provide conversion to a
    numerically-based currency datatype using :class:`_types.TypeDecorator`::

        import re
        import decimal
        from sqlalchemy import Dialect
        from sqlalchemy import TypeDecorator


        class NumericMoney(TypeDecorator):
            impl = MONEY

            def process_result_value(self, value: Any, dialect: Dialect) -> None:
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

    """  # noqa: E501

    __visit_name__ = "MONEY"


class OID(sqltypes.TypeEngine[int]):
    """Provide the PostgreSQL OID type."""

    __visit_name__ = "OID"

    operator_classes = OperatorClass.BASE | OperatorClass.COMPARISON


class REGCONFIG(sqltypes.TypeEngine[str]):
    """Provide the PostgreSQL REGCONFIG type.

    .. versionadded:: 2.0.0rc1

    """

    __visit_name__ = "REGCONFIG"

    operator_classes = OperatorClass.BASE | OperatorClass.COMPARISON


class TSQUERY(sqltypes.TypeEngine[str]):
    """Provide the PostgreSQL TSQUERY type.

    .. versionadded:: 2.0.0rc1

    """

    __visit_name__ = "TSQUERY"

    operator_classes = OperatorClass.BASE | OperatorClass.COMPARISON


class REGCLASS(sqltypes.TypeEngine[str]):
    """Provide the PostgreSQL REGCLASS type."""

    __visit_name__ = "REGCLASS"

    operator_classes = OperatorClass.BASE | OperatorClass.COMPARISON


class TIMESTAMP(sqltypes.TIMESTAMP):
    """Provide the PostgreSQL TIMESTAMP type."""

    __visit_name__ = "TIMESTAMP"

    def __init__(
        self, timezone: bool = False, precision: Optional[int] = None
    ) -> None:
        """Construct a TIMESTAMP.

        :param timezone: boolean value if timezone present, default False
        :param precision: optional integer precision value

         .. versionadded:: 1.4

        """
        super().__init__(timezone=timezone)
        self.precision = precision


class TIME(sqltypes.TIME):
    """PostgreSQL TIME type."""

    __visit_name__ = "TIME"

    def __init__(
        self, timezone: bool = False, precision: Optional[int] = None
    ) -> None:
        """Construct a TIME.

        :param timezone: boolean value if timezone present, default False
        :param precision: optional integer precision value

         .. versionadded:: 1.4

        """
        super().__init__(timezone=timezone)
        self.precision = precision


class INTERVAL(type_api.NativeForEmulated, sqltypes._AbstractInterval):
    """PostgreSQL INTERVAL type."""

    __visit_name__ = "INTERVAL"
    native = True

    def __init__(
        self, precision: Optional[int] = None, fields: Optional[str] = None
    ) -> None:
        """Construct an INTERVAL.

        :param precision: optional integer precision value
        :param fields: string fields specifier.  allows storage of fields
         to be limited, such as ``"YEAR"``, ``"MONTH"``, ``"DAY TO HOUR"``,
         etc.

        """
        self.precision = precision
        self.fields = fields

    @classmethod
    def adapt_emulated_to_native(
        cls, interval: sqltypes.Interval, **kw: Any  # type: ignore[override]
    ) -> INTERVAL:
        return INTERVAL(precision=interval.second_precision)

    @property
    def _type_affinity(self) -> Type[sqltypes.Interval]:
        return sqltypes.Interval

    def as_generic(self, allow_nulltype: bool = False) -> sqltypes.Interval:
        return sqltypes.Interval(native=True, second_precision=self.precision)

    @property
    def python_type(self) -> Type[dt.timedelta]:
        return dt.timedelta

    def literal_processor(
        self, dialect: Dialect
    ) -> Optional[_LiteralProcessorType[dt.timedelta]]:
        def process(value: dt.timedelta) -> str:
            return f"make_interval(secs=>{value.total_seconds()})"

        return process


PGInterval = INTERVAL


class BIT(sqltypes.TypeEngine[BitString]):
    """Represent the PostgreSQL BIT type.

    The :class:`_postgresql.BIT` type yields values in the form of the
    :class:`_postgresql.BitString` Python value type.

    .. versionchanged:: 2.1  The :class:`_postgresql.BIT` type now works
       with :class:`_postgresql.BitString` values rather than plain strings.

    """

    render_bind_cast = True
    __visit_name__ = "BIT"

    operator_classes = (
        OperatorClass.BASE | OperatorClass.COMPARISON | OperatorClass.BITWISE
    )

    def __init__(
        self, length: Optional[int] = None, varying: bool = False
    ) -> None:
        if varying:
            # BIT VARYING can be unlimited-length, so no default
            self.length = length
        else:
            # BIT without VARYING defaults to length 1
            self.length = length or 1
        self.varying = varying

    def bind_processor(
        self, dialect: Dialect
    ) -> _BindProcessorType[BitString]:
        def bound_value(value: Any) -> Any:
            if isinstance(value, BitString):
                return str(value)
            return value

        return bound_value

    def result_processor(
        self, dialect: Dialect, coltype: object
    ) -> _ResultProcessorType[BitString]:
        def from_result_value(value: Any) -> Any:
            if value is not None:
                value = BitString(value)
            return value

        return from_result_value

    def coerce_compared_value(
        self, op: OperatorType | None, value: Any
    ) -> TypeEngine[Any]:
        if isinstance(value, str):
            return self
        return super().coerce_compared_value(op, value)

    @property
    def python_type(self) -> type[Any]:
        return BitString

    class comparator_factory(TypeEngine.Comparator[BitString]):
        def __lshift__(self, other: Any) -> ColumnOperators:
            return self.bitwise_lshift(other)

        def __rshift__(self, other: Any) -> ColumnOperators:
            return self.bitwise_rshift(other)

        def __and__(self, other: Any) -> ColumnOperators:
            return self.bitwise_and(other)

        def __or__(self, other: Any) -> ColumnOperators:
            return self.bitwise_or(other)

        # NOTE: __xor__ is not defined on sql.operators.ColumnOperators.
        # Use `bitwise_xor` directly instead.
        # def __xor__(self, other: Any) -> ColumnOperators:
        #     return self.bitwise_xor(other)

        def __invert__(self) -> ColumnOperators:
            return self.bitwise_not()


PGBit = BIT


class TSVECTOR(sqltypes.TypeEngine[str]):
    """The :class:`_postgresql.TSVECTOR` type implements the PostgreSQL
    text search type TSVECTOR.

    It can be used to do full text queries on natural language
    documents.

    .. seealso::

        :ref:`postgresql_match`

    """

    __visit_name__ = "TSVECTOR"

    operator_classes = OperatorClass.STRING


class CITEXT(sqltypes.TEXT):
    """Provide the PostgreSQL CITEXT type.

    .. versionadded:: 2.0.7

    """

    __visit_name__ = "CITEXT"

    def coerce_compared_value(
        self, op: Optional[OperatorType], value: Any
    ) -> TypeEngine[Any]:
        return self
