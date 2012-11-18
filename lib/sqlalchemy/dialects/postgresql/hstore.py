# postgresql/hstore.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import re

from .base import ARRAY
from ... import types as sqltypes
from ...sql import functions as sqlfunc
from ...sql.operators import custom_op
from ...exc import SQLAlchemyError

__all__ = ('HStoreSyntaxError', 'HSTORE', 'hstore')

# My best guess at the parsing rules of hstore literals, since no formal
# grammar is given.  This is mostly reverse engineered from PG's input parser
# behavior.
HSTORE_PAIR_RE = re.compile(r"""
(
  "(?P<key> (\\ . | [^"])* )"       # Quoted key
)
[ ]* => [ ]*    # Pair operator, optional adjoining whitespace
(
    (?P<value_null> NULL )          # NULL value
  | "(?P<value> (\\ . | [^"])* )"   # Quoted value
)
""", re.VERBOSE)

HSTORE_DELIMITER_RE = re.compile(r"""
[ ]* , [ ]*
""", re.VERBOSE)


class HStoreSyntaxError(SQLAlchemyError):
    """Indicates an error unmarshalling an hstore value."""

    def __init__(self, hstore_str, pos):
        self.hstore_str = hstore_str
        self.pos = pos

        ctx = 20
        hslen = len(hstore_str)

        parsed_tail = hstore_str[max(pos - ctx - 1, 0):min(pos, hslen)]
        residual = hstore_str[min(pos, hslen):min(pos + ctx + 1, hslen)]

        if len(parsed_tail) > ctx:
            parsed_tail = '[...]' + parsed_tail[1:]
        if len(residual) > ctx:
            residual = residual[:-1] + '[...]'

        super(HStoreSyntaxError, self).__init__(
            "After %r, could not parse residual at position %d: %r" %
            (parsed_tail, pos, residual)
        )


def _parse_hstore(hstore_str):
    """Parse an hstore from it's literal string representation.

    Attempts to approximate PG's hstore input parsing rules as closely as
    possible. Although currently this is not strictly necessary, since the
    current implementation of hstore's output syntax is stricter than what it
    accepts as input, the documentation makes no guarantees that will always
    be the case.

    Throws HStoreSyntaxError if parsing fails.

    """
    result = {}
    pos = 0
    pair_match = HSTORE_PAIR_RE.match(hstore_str)

    while pair_match is not None:
        key = pair_match.group('key')
        if pair_match.group('value_null'):
            value = None
        else:
            value = pair_match.group('value').replace(r'\"', '"')
        result[key] = value

        pos += pair_match.end()

        delim_match = HSTORE_DELIMITER_RE.match(hstore_str[pos:])
        if delim_match is not None:
            pos += delim_match.end()

        pair_match = HSTORE_PAIR_RE.match(hstore_str[pos:])

    if pos != len(hstore_str):
        raise HStoreSyntaxError(hstore_str, pos)

    return result


def _serialize_hstore(val):
    """Serialize a dictionary into an hstore literal.  Keys and values must
    both be strings (except None for values).

    """
    def esc(s, position):
        if position == 'value' and s is None:
            return 'NULL'
        elif isinstance(s, basestring):
            return '"%s"' % s.replace('"', r'\"')
        else:
            raise ValueError("%r in %s position is not a string." %
                             (s, position))

    return ', '.join('%s=>%s' % (esc(k, 'key'), esc(v, 'value'))
                     for k, v in val.iteritems())


class HSTORE(sqltypes.Concatenable, sqltypes.TypeEngine):
    """Represent the Postgresql HSTORE type.

    The :class:`.HSTORE` type stores dictionaries containing strings, e.g.::

        data_table = Table('data_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', HSTORE)
        )

        with engine.connect() as conn:
            conn.execute(
                data_table.insert(),
                data = {"key1": "value1", "key2": "value2"}
            )

    :class:`.HSTORE` provides for a wide range of operations, including:

    * :meth:`.HSTORE.comparatopr_factory.has_key`

    * :meth:`.HSTORE.comparatopr_factory.has_all`

    * :meth:`.HSTORE.comparatopr_factory.defined`

    For usage with the SQLAlchemy ORM, it may be desirable to combine
    the usage of :class:`.HSTORE` with the :mod:`sqlalchemy.ext.mutable`
    extension.  This extension will allow in-place changes to dictionary
    values to be detected by the unit of work::

        from sqlalchemy.ext.mutable import Mutable

        class MyClass(Base):
            __tablename__ = 'data_table'

            id = Column(Integer, primary_key=True)
            data = Column(Mutable.as_mutable(HSTORE))

        my_object = session.query(MyClass).one()

        # in-place mutation, requires Mutable extension
        # in order for the ORM to detect
        my_object.data['some_key'] = 'some value'

        session.commit()

    """

    __visit_name__ = 'HSTORE'

    class comparator_factory(sqltypes.TypeEngine.Comparator):
        def has_key(self, other):
            """Boolean expression.  Test for presence of a key.  Note that the
            key may be a SQLA expression.
            """
            return self.expr.op('?')(other)

        def has_all(self, other):
            """Boolean expression.  Test for presence of all keys in the PG
            array.
            """
            return self.expr.op('?&')(other)

        def has_any(self, other):
            """Boolean expression.  Test for presence of any key in the PG
            array.
            """
            return self.expr.op('?|')(other)

        def defined(self, key):
            """Boolean expression.  Test for presence of a non-NULL value for
            the key.  Note that the key may be a SQLA expression.
            """
            return _HStoreDefinedFunction(self.expr, key)

        def contains(self, other, **kwargs):
            """Boolean expression.  Test if keys are a superset of the keys of
            the argument hstore expression.
            """
            return self.expr.op('@>')(other)

        def contained_by(self, other):
            """Boolean expression.  Test if keys are a proper subset of the
            keys of the argument hstore expression.
            """
            return self.expr.op('<@')(other)

        def __getitem__(self, other):
            """Text expression.  Get the value at a given key.  Note that the
            key may be a SQLA expression.
            """
            return self.expr.op('->', precedence=5)(other)

        def __add__(self, other):
            """HStore expression.  Merge the left and right hstore expressions,
            with duplicate keys taking the value from the right expression.
            """
            return self.expr.concat(other)

        def delete(self, key):
            """HStore expression.  Returns the contents of this hstore with the
            given key deleted.  Note that the key may be a SQLA expression.
            """
            if isinstance(key, dict):
                key = _serialize_hstore(key)
            return _HStoreDeleteFunction(self.expr, key)

        def slice(self, array):
            """HStore expression.  Returns a subset of an hstore defined by
            array of keys.
            """
            return _HStoreSliceFunction(self.expr, array)

        def keys(self):
            """Text array expression.  Returns array of keys."""
            return _HStoreKeysFunction(self.expr)

        def vals(self):
            """Text array expression.  Returns array of values."""
            return _HStoreValsFunction(self.expr)

        def array(self):
            """Text array expression.  Returns array of alternating keys and
            values.
            """
            return _HStoreArrayFunction(self.expr)

        def matrix(self):
            """Text array expression.  Returns array of [key, value] pairs."""
            return _HStoreMatrixFunction(self.expr)

        def _adapt_expression(self, op, other_comparator):
            if isinstance(op, custom_op):
                if op.opstring in ['?', '?&', '?|', '@>', '<@']:
                    return op, sqltypes.Boolean
                elif op.opstring == '->':
                    return op, sqltypes.Text
            return op, other_comparator.type

    #@util.memoized_property
    #@property
    #def _expression_adaptations(self):
    #    return {
    #        operators.getitem: {
    #           sqltypes.String: sqltypes.String
    #        },
    #    }

    def bind_processor(self, dialect):
        def process(value):
            if isinstance(value, dict):
                return _serialize_hstore(value)
            else:
                return value
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return _parse_hstore(value)
            else:
                return value
        return process


class hstore(sqlfunc.GenericFunction):
    """Construct an hstore on the server side using the hstore function.

    The single argument or a pair of arguments are evaluated as SQLAlchemy
    expressions, so both may contain columns, function calls, or any other
    valid SQL expressions which evaluate to text or array.

    """
    type = HSTORE
    name = 'hstore'


class _HStoreDefinedFunction(sqlfunc.GenericFunction):
    type = sqltypes.Boolean
    name = 'defined'


class _HStoreDeleteFunction(sqlfunc.GenericFunction):
    type = HSTORE
    name = 'delete'


class _HStoreSliceFunction(sqlfunc.GenericFunction):
    type = HSTORE
    name = 'slice'


class _HStoreKeysFunction(sqlfunc.GenericFunction):
    type = ARRAY(sqltypes.Text)
    name = 'akeys'


class _HStoreValsFunction(sqlfunc.GenericFunction):
    type = ARRAY(sqltypes.Text)
    name = 'avals'


class _HStoreArrayFunction(sqlfunc.GenericFunction):
    type = ARRAY(sqltypes.Text)
    name = 'hstore_to_array'


class _HStoreMatrixFunction(sqlfunc.GenericFunction):
    type = ARRAY(sqltypes.Text)
    name = 'hstore_to_matrix'
