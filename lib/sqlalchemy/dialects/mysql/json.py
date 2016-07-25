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

    pass

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
