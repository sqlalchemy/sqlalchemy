# postgresql/array.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .base import ischema_names
from ...sql import expression, operators
from ... import types as sqltypes

try:
    from uuid import UUID as _python_UUID
except ImportError:
    _python_UUID = None


class Any(expression.ColumnElement):

    """Represent the clause ``left operator ANY (right)``.  ``right`` must be
    an array expression.

    .. seealso::

        :class:`.postgresql.ARRAY`

        :meth:`.postgresql.ARRAY.Comparator.any` - ARRAY-bound method

    """
    __visit_name__ = 'any'

    def __init__(self, left, right, operator=operators.eq):
        self.type = sqltypes.Boolean()
        self.left = expression._literal_as_binds(left)
        self.right = right
        self.operator = operator


class All(expression.ColumnElement):

    """Represent the clause ``left operator ALL (right)``.  ``right`` must be
    an array expression.

    .. seealso::

        :class:`.postgresql.ARRAY`

        :meth:`.postgresql.ARRAY.Comparator.all` - ARRAY-bound method

    """
    __visit_name__ = 'all'

    def __init__(self, left, right, operator=operators.eq):
        self.type = sqltypes.Boolean()
        self.left = expression._literal_as_binds(left)
        self.right = right
        self.operator = operator


class array(expression.Tuple):

    """A Postgresql ARRAY literal.

    This is used to produce ARRAY literals in SQL expressions, e.g.::

        from sqlalchemy.dialects.postgresql import array
        from sqlalchemy.dialects import postgresql
        from sqlalchemy import select, func

        stmt = select([
                        array([1,2]) + array([3,4,5])
                    ])

        print stmt.compile(dialect=postgresql.dialect())

    Produces the SQL::

        SELECT ARRAY[%(param_1)s, %(param_2)s] ||
            ARRAY[%(param_3)s, %(param_4)s, %(param_5)s]) AS anon_1

    An instance of :class:`.array` will always have the datatype
    :class:`.ARRAY`.  The "inner" type of the array is inferred from
    the values present, unless the ``type_`` keyword argument is passed::

        array(['foo', 'bar'], type_=CHAR)

    .. versionadded:: 0.8 Added the :class:`~.postgresql.array` literal type.

    See also:

    :class:`.postgresql.ARRAY`

    """
    __visit_name__ = 'array'

    def __init__(self, clauses, **kw):
        super(array, self).__init__(*clauses, **kw)
        self.type = ARRAY(self.type)

    def _bind_param(self, operator, obj):
        return array([
            expression.BindParameter(None, o, _compared_to_operator=operator,
                                     _compared_to_type=self.type, unique=True)
            for o in obj
        ])

    def self_group(self, against=None):
        return self


CONTAINS = operators.custom_op("@>", precedence=5)

CONTAINED_BY = operators.custom_op("<@", precedence=5)

OVERLAP = operators.custom_op("&&", precedence=5)


class ARRAY(sqltypes.Indexable, sqltypes.Concatenable, sqltypes.TypeEngine):

    """Postgresql ARRAY type.

    Represents values as Python lists.

    An :class:`.ARRAY` type is constructed given the "type"
    of element::

        mytable = Table("mytable", metadata,
                Column("data", ARRAY(Integer))
            )

    The above type represents an N-dimensional array,
    meaning Postgresql will interpret values with any number
    of dimensions automatically.   To produce an INSERT
    construct that passes in a 1-dimensional array of integers::

        connection.execute(
                mytable.insert(),
                data=[1,2,3]
        )

    The :class:`.ARRAY` type can be constructed given a fixed number
    of dimensions::

        mytable = Table("mytable", metadata,
                Column("data", ARRAY(Integer, dimensions=2))
            )

    This has the effect of the :class:`.ARRAY` type
    specifying that number of bracketed blocks when a :class:`.Table`
    is used in a CREATE TABLE statement, or when the type is used
    within a :func:`.expression.cast` construct; it also causes
    the bind parameter and result set processing of the type
    to optimize itself to expect exactly that number of dimensions.
    Note that Postgresql itself still allows N dimensions with such a type.

    SQL expressions of type :class:`.ARRAY` have support for "index" and
    "slice" behavior.  The Python ``[]`` operator works normally here, given
    integer indexes or slices.  Note that Postgresql arrays default
    to 1-based indexing.  The operator produces binary expression
    constructs which will produce the appropriate SQL, both for
    SELECT statements::

        select([mytable.c.data[5], mytable.c.data[2:7]])

    as well as UPDATE statements when the :meth:`.Update.values` method
    is used::

        mytable.update().values({
            mytable.c.data[5]: 7,
            mytable.c.data[2:7]: [1, 2, 3]
        })

    Multi-dimensional array index support is provided automatically based on
    either the value specified for the :paramref:`.ARRAY.dimensions` parameter.
    E.g. an :class:`.ARRAY` with dimensions set to 2 would return an expression
    of type :class:`.ARRAY` for a single index operation::

        type = ARRAY(Integer, dimensions=2)

        expr = column('x', type)  # expr is of type ARRAY(Integer, dimensions=2)

        expr = column('x', type)[5]  # expr is of type ARRAY(Integer, dimensions=1)

    An index expression from ``expr`` above would then return an expression
    of type Integer::

        sub_expr = expr[10]  # expr is of type Integer

    .. versionadded:: 1.1 support for index operations on multi-dimensional
       :class:`.postgresql.ARRAY` objects is added.

    :class:`.ARRAY` provides special methods for containment operations,
    e.g.::

        mytable.c.data.contains([1, 2])

    For a full list of special methods see :class:`.ARRAY.Comparator`.

    .. versionadded:: 0.8 Added support for index and slice operations
       to the :class:`.ARRAY` type, including support for UPDATE
       statements, and special array containment operations.

    The :class:`.ARRAY` type may not be supported on all DBAPIs.
    It is known to work on psycopg2 and not pg8000.

    See also:

    :class:`.postgresql.array` - produce a literal array value.

    """
    __visit_name__ = 'ARRAY'

    class Comparator(
            sqltypes.Indexable.Comparator, sqltypes.Concatenable.Comparator):

        """Define comparison operations for :class:`.ARRAY`."""

        def _setup_getitem(self, index):
            if isinstance(index, slice):
                return_type = self.type
            elif self.type.dimensions is None or self.type.dimensions == 1:
                return_type = self.type.item_type
            else:
                adapt_kw = {'dimensions': self.type.dimensions - 1}
                return_type = self.type.adapt(self.type.__class__, **adapt_kw)

            return operators.getitem, index, return_type

        def any(self, other, operator=operators.eq):
            """Return ``other operator ANY (array)`` clause.

            Argument places are switched, because ANY requires array
            expression to be on the right hand-side.

            E.g.::

                from sqlalchemy.sql import operators

                conn.execute(
                    select([table.c.data]).where(
                            table.c.data.any(7, operator=operators.lt)
                        )
                )

            :param other: expression to be compared
            :param operator: an operator object from the
             :mod:`sqlalchemy.sql.operators`
             package, defaults to :func:`.operators.eq`.

            .. seealso::

                :class:`.postgresql.Any`

                :meth:`.postgresql.ARRAY.Comparator.all`

            """
            return Any(other, self.expr, operator=operator)

        def all(self, other, operator=operators.eq):
            """Return ``other operator ALL (array)`` clause.

            Argument places are switched, because ALL requires array
            expression to be on the right hand-side.

            E.g.::

                from sqlalchemy.sql import operators

                conn.execute(
                    select([table.c.data]).where(
                            table.c.data.all(7, operator=operators.lt)
                        )
                )

            :param other: expression to be compared
            :param operator: an operator object from the
             :mod:`sqlalchemy.sql.operators`
             package, defaults to :func:`.operators.eq`.

            .. seealso::

                :class:`.postgresql.All`

                :meth:`.postgresql.ARRAY.Comparator.any`

            """
            return All(other, self.expr, operator=operator)

        def contains(self, other, **kwargs):
            """Boolean expression.  Test if elements are a superset of the
            elements of the argument array expression.
            """
            return self.operate(CONTAINS, other, result_type=sqltypes.Boolean)

        def contained_by(self, other):
            """Boolean expression.  Test if elements are a proper subset of the
            elements of the argument array expression.
            """
            return self.operate(
                CONTAINED_BY, other, result_type=sqltypes.Boolean)

        def overlap(self, other):
            """Boolean expression.  Test if array has elements in common with
            an argument array expression.
            """
            return self.operate(OVERLAP, other, result_type=sqltypes.Boolean)

    comparator_factory = Comparator

    def __init__(self, item_type, as_tuple=False, dimensions=None,
                 zero_indexes=False):
        """Construct an ARRAY.

        E.g.::

          Column('myarray', ARRAY(Integer))

        Arguments are:

        :param item_type: The data type of items of this array. Note that
          dimensionality is irrelevant here, so multi-dimensional arrays like
          ``INTEGER[][]``, are constructed as ``ARRAY(Integer)``, not as
          ``ARRAY(ARRAY(Integer))`` or such.

        :param as_tuple=False: Specify whether return results
          should be converted to tuples from lists. DBAPIs such
          as psycopg2 return lists by default. When tuples are
          returned, the results are hashable.

        :param dimensions: if non-None, the ARRAY will assume a fixed
         number of dimensions.  This will cause the DDL emitted for this
         ARRAY to include the exact number of bracket clauses ``[]``,
         and will also optimize the performance of the type overall.
         Note that PG arrays are always implicitly "non-dimensioned",
         meaning they can store any number of dimensions no matter how
         they were declared.

        :param zero_indexes=False: when True, index values will be converted
         between Python zero-based and Postgresql one-based indexes, e.g.
         a value of one will be added to all index values before passing
         to the database.

         .. versionadded:: 0.9.5


        """
        if isinstance(item_type, ARRAY):
            raise ValueError("Do not nest ARRAY types; ARRAY(basetype) "
                             "handles multi-dimensional arrays of basetype")
        if isinstance(item_type, type):
            item_type = item_type()
        self.item_type = item_type
        self.as_tuple = as_tuple
        self.dimensions = dimensions
        self.zero_indexes = zero_indexes

    @property
    def hashable(self):
        return self.as_tuple

    @property
    def python_type(self):
        return list

    def compare_values(self, x, y):
        return x == y

    def _proc_array(self, arr, itemproc, dim, collection):
        if dim is None:
            arr = list(arr)
        if dim == 1 or dim is None and (
                # this has to be (list, tuple), or at least
                # not hasattr('__iter__'), since Py3K strings
                # etc. have __iter__
                not arr or not isinstance(arr[0], (list, tuple))):
            if itemproc:
                return collection(itemproc(x) for x in arr)
            else:
                return collection(arr)
        else:
            return collection(
                self._proc_array(
                    x, itemproc,
                    dim - 1 if dim is not None else None,
                    collection)
                for x in arr
            )

    def bind_processor(self, dialect):
        item_proc = self.item_type.dialect_impl(dialect).\
            bind_processor(dialect)

        def process(value):
            if value is None:
                return value
            else:
                return self._proc_array(
                    value,
                    item_proc,
                    self.dimensions,
                    list)
        return process

    def result_processor(self, dialect, coltype):
        item_proc = self.item_type.dialect_impl(dialect).\
            result_processor(dialect, coltype)

        def process(value):
            if value is None:
                return value
            else:
                return self._proc_array(
                    value,
                    item_proc,
                    self.dimensions,
                    tuple if self.as_tuple else list)
        return process

ischema_names['_array'] = ARRAY
