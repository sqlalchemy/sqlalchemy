# postgresql/json.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
from __future__ import absolute_import

import json

from .base import ischema_names
from ... import types as sqltypes
from ...sql.operators import custom_op
from ... import util

__all__ = ('JSON', )


class JSON(sqltypes.TypeEngine):
    """Represent the Postgresql JSON type.

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

    :class:`.JSON` provides several operations:

    * Index operations::

        data_table.c.data['some key']

    * Index operations returning text (required for text comparison or casting)::

        data_table.c.data.astext['some key'] == 'some value'

    * Path index operations::

        data_table.c.data[('key_1', 'key_2', ..., 'key_n')]

    * Path index operations returning text (required for text comparison or casting)::

        data_table.c.data.astext[('key_1', 'key_2', ..., 'key_n')] == 'some value'

    The :class:`.JSON` type, when used with the SQLAlchemy ORM, does not detect
    in-place mutations to the structure.  In order to detect these, the
    :mod:`sqlalchemy.ext.mutable` extension must be used.  This extension will
    allow "in-place" changes to the datastructure to produce events which
    will be detected by the unit of work.  See the example at :class:`.HSTORE`
    for a simple example involving a dictionary.

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

        class _astext(object):
            def __init__(self, parent):
                self.parent = parent

            def __getitem__(self, other):
                return self.parent.expr._get_item(other, True)

        def _get_item(self, other, astext):
            if hasattr(other, '__iter__') and \
                not isinstance(other, util.string_types):
                op = "#>"
                other = "{%s}" % (", ".join(util.text_type(elem) for elem in other))
            else:
                op = "->"

            if astext:
                op += ">"

            # ops: ->, ->>, #>, #>>
            return self.expr.op(op, precedence=5)(other)

        def __getitem__(self, other):
            """Get the value at a given key."""

            return self._get_item(other, False)

        @property
        def astext(self):
            return self._astext(self)

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
