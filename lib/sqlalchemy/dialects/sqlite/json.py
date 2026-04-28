# dialects/sqlite/json.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
from __future__ import annotations

from typing import Any
from typing import TYPE_CHECKING

from ... import types as sqltypes
from ...sql import func
from ...sql.sqltypes import _T_JSON

if TYPE_CHECKING:
    from ...engine.interfaces import Dialect
    from ...sql.elements import ColumnElement
    from ...sql.type_api import _BindProcessorType
    from ...sql.type_api import _LiteralProcessorType


class JSON(sqltypes.JSON[_T_JSON]):
    """SQLite JSON type.

    SQLite supports JSON as of version 3.9 through its JSON1_ extension. Note
    that JSON1_ is a
    `loadable extension <https://www.sqlite.org/loadext.html>`_ and as such
    may not be available, or may require run-time loading.

    :class:`_sqlite.JSON` is used automatically whenever the base
    :class:`_types.JSON` datatype is used against a SQLite backend.

    .. seealso::

        :class:`_types.JSON` - main documentation for the generic
        cross-platform JSON datatype.

    The :class:`_sqlite.JSON` type supports persistence of JSON values
    as well as the core index operations provided by :class:`_types.JSON`
    datatype, by adapting the operations to render the ``JSON_EXTRACT``
    function wrapped in the ``JSON_QUOTE`` function at the database level.
    Extracted values are quoted in order to ensure that the results are
    always JSON string values.


    .. _JSON1: https://www.sqlite.org/json1.html

    """


class JSONB(JSON[_T_JSON]):
    """SQLite JSONB type.

    Stores JSON data in SQLite's binary JSONB format, available as of
    SQLite version 3.45.0.  The binary format is more compact and faster
    to parse than the text-based :class:`_sqlite.JSON` type.

    Values are transparently stored using the ``jsonb()`` SQL function and
    retrieved as text JSON via the ``json()`` SQL function, so the Python
    side behaves identically to :class:`_sqlite.JSON`.

    .. seealso::

        :class:`_sqlite.JSON`

        https://sqlite.org/jsonb.html

    """

    __visit_name__ = "JSONB"

    def bind_expression(
        self, bindvalue: ColumnElement[Any]
    ) -> ColumnElement[Any]:
        return func.jsonb(bindvalue, type_=self)

    def column_expression(self, col: ColumnElement[Any]) -> ColumnElement[Any]:
        return func.json(col, type_=self)


# Note: these objects currently match exactly those of MySQL, however since
# these are not generalizable to all JSON implementations, remain separately
# implemented for each dialect.
class _FormatTypeMixin:
    def _format_value(self, value: Any) -> str:
        raise NotImplementedError()

    def bind_processor(self, dialect: Dialect) -> _BindProcessorType[Any]:
        super_proc = self.string_bind_processor(dialect)  # type: ignore[attr-defined]  # noqa: E501

        def process(value: Any) -> Any:
            value = self._format_value(value)
            if super_proc:
                value = super_proc(value)
            return value

        return process

    def literal_processor(
        self, dialect: Dialect
    ) -> _LiteralProcessorType[Any]:
        super_proc = self.string_literal_processor(dialect)  # type: ignore[attr-defined]  # noqa: E501

        def process(value: Any) -> str:
            value = self._format_value(value)
            if super_proc:
                value = super_proc(value)
            return value  # type: ignore[no-any-return]

        return process


class JSONIndexType(_FormatTypeMixin, sqltypes.JSON.JSONIndexType):
    def _format_value(self, value: Any) -> str:
        if isinstance(value, int):
            formatted_value = "$[%s]" % value
        else:
            formatted_value = '$."%s"' % value
        return formatted_value


class JSONPathType(_FormatTypeMixin, sqltypes.JSON.JSONPathType):
    def _format_value(self, value: Any) -> str:
        return "$%s" % (
            "".join(
                [
                    "[%s]" % elem if isinstance(elem, int) else '."%s"' % elem
                    for elem in value
                ]
            )
        )
