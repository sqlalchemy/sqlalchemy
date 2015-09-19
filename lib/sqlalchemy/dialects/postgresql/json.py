# postgresql/json.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
from __future__ import absolute_import

import collections
import json

from .base import ischema_names
from ... import types as sqltypes
from ...sql import operators
from ...sql import elements
from ... import util

__all__ = ('JSON', 'JSONB')


# json : returns json
INDEX = operators.custom_op(
    "->", precedence=5, natural_self_precedent=True
)

# path operator: returns json
PATHIDX = operators.custom_op(
    "#>", precedence=5, natural_self_precedent=True
)

# json + astext: returns text
ASTEXT = operators.custom_op(
    "->>", precedence=5, natural_self_precedent=True
)

# path operator  + astext: returns text
ASTEXT_PATHIDX = operators.custom_op(
    "#>>", precedence=5, natural_self_precedent=True
)

HAS_KEY = operators.custom_op(
    "?", precedence=5, natural_self_precedent=True
)

HAS_ALL = operators.custom_op(
    "?&", precedence=5, natural_self_precedent=True
)

HAS_ANY = operators.custom_op(
    "?|", precedence=5, natural_self_precedent=True
)

CONTAINS = operators.custom_op(
    "@>", precedence=5, natural_self_precedent=True
)

CONTAINED_BY = operators.custom_op(
    "<@", precedence=5, natural_self_precedent=True
)


class JSON(sqltypes.Indexable, sqltypes.TypeEngine):
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

    * Index operations (the ``->`` operator)::

        data_table.c.data['some key']

    * Index operations returning text (the ``->>`` operator)::

        data_table.c.data['some key'].astext == 'some value'

    * Index operations with CAST
      (equivalent to ``CAST(col ->> ['some key'] AS <type>)``)::

        data_table.c.data['some key'].astext.cast(Integer) == 5

    * Path index operations (the ``#>`` operator)::

        data_table.c.data[('key_1', 'key_2', ..., 'key_n')]

    * Path index operations returning text (the ``#>>`` operator)::

        data_table.c.data[('key_1', 'key_2', ..., 'key_n')].astext == \
'some value'

    .. versionchanged:: 1.1  The :meth:`.ColumnElement.cast` operator on
       JSON objects now requires that the :attr:`.JSON.Comparator.astext`
       modifier be called explicitly, if the cast works only from a textual
       string.

    Index operations return an expression object whose type defaults to
    :class:`.JSON` by default, so that further JSON-oriented instructions
    may be called upon the result type.

    The :class:`.JSON` type, when used with the SQLAlchemy ORM, does not
    detect in-place mutations to the structure.  In order to detect these, the
    :mod:`sqlalchemy.ext.mutable` extension must be used.  This extension will
    allow "in-place" changes to the datastructure to produce events which
    will be detected by the unit of work.  See the example at :class:`.HSTORE`
    for a simple example involving a dictionary.

    When working with NULL values, the :class:`.JSON` type recommends the
    use of two specific constants in order to differentiate between a column
    that evaluates to SQL NULL, e.g. no value, vs. the JSON-encoded string
    of ``"null"``.   To insert or select against a value that is SQL NULL,
    use the constant :func:`.null`::

        conn.execute(table.insert(), json_value=null())

    To insert or select against a value that is JSON ``"null"``, use the
    constant :attr:`.JSON.NULL`::

        conn.execute(table.insert(), json_value=JSON.NULL)

    The :class:`.JSON` type supports a flag
    :paramref:`.JSON.none_as_null` which when set to True will result
    in the Python constant ``None`` evaluating to the value of SQL
    NULL, and when set to False results in the Python constant
    ``None`` evaluating to the value of JSON ``"null"``.    The Python
    value ``None`` may be used in conjunction with either
    :attr:`.JSON.NULL` and :func:`.null` in order to indicate NULL
    values, but care must be taken as to the value of the
    :paramref:`.JSON.none_as_null` in these cases.

    Custom serializers and deserializers are specified at the dialect level,
    that is using :func:`.create_engine`.  The reason for this is that when
    using psycopg2, the DBAPI only allows serializers at the per-cursor
    or per-connection level.   E.g.::

        engine = create_engine("postgresql://scott:tiger@localhost/test",
                                json_serializer=my_serialize_fn,
                                json_deserializer=my_deserialize_fn
                        )

    When using the psycopg2 dialect, the json_deserializer is registered
    against the database using ``psycopg2.extras.register_default_json``.

    .. versionadded:: 0.9

    .. seealso::

        :class:`.JSONB`

    """

    __visit_name__ = 'JSON'

    hashable = False
    astext_type = sqltypes.Text()

    NULL = util.symbol('JSON_NULL')
    """Describe the json value of NULL.

    This value is used to force the JSON value of ``"null"`` to be
    used as the value.   A value of Python ``None`` will be recognized
    either as SQL NULL or JSON ``"null"``, based on the setting
    of the :paramref:`.JSON.none_as_null` flag; the :attr:`.JSON.NULL`
    constant can be used to always resolve to JSON ``"null"`` regardless
    of this setting.  This is in contrast to the :func:`.sql.null` construct,
    which always resolves to SQL NULL.  E.g.::

        from sqlalchemy import null
        from sqlalchemy.dialects.postgresql import JSON

        obj1 = MyObject(json_value=null())  # will *always* insert SQL NULL
        obj2 = MyObject(json_value=JSON.NULL)  # will *always* insert JSON string "null"

        session.add_all([obj1, obj2])
        session.commit()

    .. versionadded:: 1.1

    """

    def __init__(self, none_as_null=False, astext_type=None):
        """Construct a :class:`.JSON` type.

        :param none_as_null: if True, persist the value ``None`` as a
         SQL NULL value, not the JSON encoding of ``null``.   Note that
         when this flag is False, the :func:`.null` construct can still
         be used to persist a NULL value::

             from sqlalchemy import null
             conn.execute(table.insert(), data=null())

         .. versionchanged:: 0.9.8 - Added ``none_as_null``, and :func:`.null`
            is now supported in order to persist a NULL value.

         .. seealso::

              :attr:`.JSON.NULL`

        :param astext_type: the type to use for the
         :attr:`.JSON.Comparator.astext`
         accessor on indexed attributes.  Defaults to :class:`.types.Text`.

         .. versionadded:: 1.1.0

         """
        self.none_as_null = none_as_null
        if astext_type is not None:
            self.astext_type = astext_type

    class Comparator(
            sqltypes.Indexable.Comparator, sqltypes.Concatenable.Comparator):
        """Define comparison operations for :class:`.JSON`."""

        @property
        def astext(self):
            """On an indexed expression, use the "astext" (e.g. "->>")
            conversion when rendered in SQL.

            E.g.::

                select([data_table.c.data['some key'].astext])

            .. seealso::

                :meth:`.ColumnElement.cast`

            """
            against = self.expr.operator
            if against is PATHIDX:
                against = ASTEXT_PATHIDX
            else:
                against = ASTEXT

            return self.expr.left.operate(
                against, self.expr.right, result_type=self.type.astext_type)

        def _setup_getitem(self, index):
            if not isinstance(index, util.string_types):
                assert isinstance(index, collections.Sequence)
                tokens = [util.text_type(elem) for elem in index]
                index = "{%s}" % (", ".join(tokens))
                operator = PATHIDX
            else:
                operator = INDEX

            return operator, index, self.type

    comparator_factory = Comparator

    @property
    def should_evaluate_none(self):
        return not self.none_as_null

    def bind_processor(self, dialect):
        json_serializer = dialect._json_serializer or json.dumps
        if util.py2k:
            encoding = dialect.encoding
        else:
            encoding = None

        def process(value):
            if value is self.NULL:
                value = None
            elif isinstance(value, elements.Null) or (
                value is None and self.none_as_null
            ):
                return None
            if encoding:
                return json_serializer(value).encode(encoding)
            else:
                return json_serializer(value)

        return process

    def result_processor(self, dialect, coltype):
        json_deserializer = dialect._json_deserializer or json.loads
        if util.py2k:
            encoding = dialect.encoding
        else:
            encoding = None

        def process(value):
            if value is None:
                return None
            if encoding:
                value = value.decode(encoding)
            return json_deserializer(value)
        return process


ischema_names['json'] = JSON


class JSONB(JSON):
    """Represent the Postgresql JSONB type.

    The :class:`.JSONB` type stores arbitrary JSONB format data, e.g.::

        data_table = Table('data_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', JSONB)
        )

        with engine.connect() as conn:
            conn.execute(
                data_table.insert(),
                data = {"key1": "value1", "key2": "value2"}
            )

    The :class:`.JSONB` type includes all operations provided by
    :class:`.JSON`, including the same behaviors for indexing operations.
    It also adds additional operators specific to JSONB, including
    :meth:`.JSONB.Comparator.has_key`, :meth:`.JSONB.Comparator.has_all`,
    :meth:`.JSONB.Comparator.has_any`, :meth:`.JSONB.Comparator.contains`,
    and :meth:`.JSONB.Comparator.contained_by`.

    Like the :class:`.JSON` type, the :class:`.JSONB` type does not detect
    in-place changes when used with the ORM, unless the
    :mod:`sqlalchemy.ext.mutable` extension is used.

    Custom serializers and deserializers
    are shared with the :class:`.JSON` class, using the ``json_serializer``
    and ``json_deserializer`` keyword arguments.  These must be specified
    at the dialect level using :func:`.create_engine`.  When using
    psycopg2, the serializers are associated with the jsonb type using
    ``psycopg2.extras.register_default_jsonb`` on a per-connection basis,
    in the same way that ``psycopg2.extras.register_default_json`` is used
    to register these handlers with the json type.

    .. versionadded:: 0.9.7

    .. seealso::

        :class:`.JSON`

    """

    __visit_name__ = 'JSONB'

    class Comparator(JSON.Comparator):
        """Define comparison operations for :class:`.JSON`."""

        def has_key(self, other):
            """Boolean expression.  Test for presence of a key.  Note that the
            key may be a SQLA expression.
            """
            return self.operate(HAS_KEY, other, result_type=sqltypes.Boolean)

        def has_all(self, other):
            """Boolean expression.  Test for presence of all keys in jsonb
            """
            return self.operate(HAS_ALL, other, result_type=sqltypes.Boolean)

        def has_any(self, other):
            """Boolean expression.  Test for presence of any key in jsonb
            """
            return self.operate(HAS_ANY, other, result_type=sqltypes.Boolean)

        def contains(self, other, **kwargs):
            """Boolean expression.  Test if keys (or array) are a superset
            of/contained the keys of the argument jsonb expression.
            """
            return self.operate(CONTAINS, other, result_type=sqltypes.Boolean)

        def contained_by(self, other):
            """Boolean expression.  Test if keys are a proper subset of the
            keys of the argument jsonb expression.
            """
            return self.operate(
                CONTAINED_BY, other, result_type=sqltypes.Boolean)

    comparator_factory = Comparator

ischema_names['jsonb'] = JSONB
