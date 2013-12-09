# postgresql/json.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import json

from .base import ARRAY, ischema_names
from ... import types as sqltypes
from ...sql import functions as sqlfunc
from ...sql.operators import custom_op
from ... import util

__all__ = ('JSON', 'json')


class JSON(sqltypes.TypeEngine):
    """Represent the Postgresql HSTORE type.

    The :class:`.JSON` type stores arbitrary JSON format data, e.g.::

        data_table = Table('data_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', JSON)
        )

        with engine.connect() as conn:
            conn.execute(
                data_table.insert(),
                data = {"key1": "value1", "key2": "value2"}
            )

    :class:`.JSON` provides two operations:

    * Index operations::

        data_table.c.data['some key'] == 'some value'

    * Path Index operations::

        data_table.c.data.get_path('{key_1, key_2, ..., key_n}']

    Please be aware that when used with the SQL Alchemy ORM, you will need to
    replace the JSON object present on an attribute with a new object in order
    for any changes to be properly persisted.

    .. versionadded:: 0.9
    """

    __visit_name__ = 'JSON'

    def __init__(self, json_serializer=None, json_deserializer=None):
        if json_serializer:
            self.json_serializer = json_serializer
        else:
            self.json_serializer = json.dumps
        if json_deserializer:
            self.json_deserializer = json_deserializer
        else:
            self.json_deserializer = json.loads

    class comparator_factory(sqltypes.Concatenable.Comparator):
        """Define comparison operations for :class:`.JSON`."""

        def __getitem__(self, other):
            """Text expression.  Get the value at a given key."""
            # I'm choosing to return text here so the result can be cast,
            # compared with strings, etc.
            #
            # The only downside to this is that you cannot dereference more
            # than one level deep in json structures, though comparator
            # support for multi-level dereference is lacking anyhow.
            return self.expr.op('->>', precedence=5)(other)

        def get_path(self, other):
            """Text expression.  Get the value at a given path.  Paths are of
            the form {key_1, key_2, ..., key_n}."""
            return self.expr.op('#>>', precedence=5)(other)

        def _adapt_expression(self, op, other_comparator):
            if isinstance(op, custom_op):
                if op.opstring == '->':
                    return op, sqltypes.Text
            return sqltypes.Concatenable.Comparator.\
                _adapt_expression(self, op, other_comparator)

    def bind_processor(self, dialect):
        if util.py2k:
            encoding = dialect.encoding
            def process(value):
                return self.json_serializer(value).encode(encoding)
        else:
            def process(value):
                return self.json_serializer(value)
        return process

    def result_processor(self, dialect, coltype):
        if util.py2k:
            encoding = dialect.encoding
            def process(value):
                return self.json_deserializer(value.decode(encoding))
        else:
            def process(value):
                return self.json_deserializer(value)
        return process


ischema_names['json'] = JSON
