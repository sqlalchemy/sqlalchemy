# mysql/json.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from __future__ import absolute_import

import json

from ...sql import elements
from ... import types as sqltypes
from ... import util


class JSON(sqltypes.JSON):
    """MySQL JSON type.

    MySQL supports JSON as of version 5.7.  Note that MariaDB does **not**
    support JSON at the time of this writing.

    The :class:`.mysql.JSON` type supports persistence of JSON values
    as well as the core index operations provided by :class:`.types.JSON`
    datatype, by adapting the operations to render the ``JSON_EXTRACT``
    function at the database level.

    .. versionadded:: 1.1

    """

    @util.memoized_property
    def _str_impl(self):
        return sqltypes.String(convert_unicode=True)

    def bind_processor(self, dialect):
        string_process = self._str_impl.bind_processor(dialect)

        json_serializer = dialect._json_serializer or json.dumps

        def process(value):
            if value is self.NULL:
                value = None
            elif isinstance(value, elements.Null) or (
                value is None and self.none_as_null
            ):
                return None

            serialized = json_serializer(value)
            if string_process:
                serialized = string_process(serialized)
            return serialized

        return process

    def result_processor(self, dialect, coltype):
        string_process = self._str_impl.result_processor(dialect, coltype)
        json_deserializer = dialect._json_deserializer or json.loads

        def process(value):
            if value is None:
                return None
            if string_process:
                value = string_process(value)
            return json_deserializer(value)
        return process


class JSONIndexType(sqltypes.JSON.JSONIndexType):
    def bind_processor(self, dialect):
        def process(value):
            if isinstance(value, int):
                return "$[%s]" % value
            else:
                return '$."%s"' % value

        return process


class JSONPathType(sqltypes.JSON.JSONPathType):
    def bind_processor(self, dialect):
        def process(value):
            return "$%s" % (
                "".join([
                    "[%s]" % elem if isinstance(elem, int)
                    else '."%s"' % elem for elem in value
                ])
            )

        return process
