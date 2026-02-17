# dialects/oracle/json.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

from __future__ import annotations
from typing import Any
from decimal import Decimal
from typing import TYPE_CHECKING
import json

from ... import types as sqltypes

if TYPE_CHECKING:
    from ...engine.interfaces import Dialect
    from ...sql.type_api import _BindProcessorType
    from ...sql.type_api import _LiteralProcessorType


class JSON(sqltypes.JSON):
    """
    Note: The oracledb Python driver automatically deserializes JSON column data,
    returning native Python objects (dict, list, bool, int, float, str) directly.
    """

    def result_processor(self, dialect, coltype):  # type: ignore[override]
        string_process = self._str_impl.result_processor(dialect, coltype)
        json_deserializer = getattr(dialect, "_json_deserializer", None) or json.loads

        def process(value):
            if value is None:
                return None

            if string_process:
                value = string_process(value)

            if isinstance(value, Decimal):
                return float(value)

            # If it's a string, it might be JSON that needs deserializing
            # This can happen with CAST operations or when reading from VARCHAR2 columns
            if isinstance(value, str):
                try:
                    return json_deserializer(value)
                except (json.JSONDecodeError, TypeError):
                    return value

            return value

        return process


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
