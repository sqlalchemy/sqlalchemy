# dialects/oracle/json.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

from typing import Any

from ... import types as sqltypes


class JSON(sqltypes.JSON):
    """Oracle JSON type."""

    def result_processor(self, dialect, coltype):
        string_process = self._str_impl.result_processor(dialect, coltype)
        json_deserializer = (
            dialect._json_deserializer or __import__('json').loads
        )

        def process(value):
            if value is None:
                return None
            # Oracle drivers may return JSON as already-deserialized
            # Python objects
            if isinstance(value, (dict, list)):
                return value
            # Otherwise, deserialize the JSON string
            if string_process:
                value = string_process(value)
            return json_deserializer(value)

        return process


class _FormatTypeMixin:
    def _format_value(self, value):
        raise NotImplementedError()

    def bind_processor(self, dialect):
        super_proc = self.string_bind_processor(dialect)

        def process(value):
            value = self._format_value(value)
            if super_proc:
                value = super_proc(value)
            return value

        return process

    def literal_processor(self, dialect):
        super_proc = self.string_literal_processor(dialect)

        def process(value):
            value = self._format_value(value)
            if super_proc:
                value = super_proc(value)
            return value

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
