# postgresql/array.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .base import colspecs
from .base import ischema_names
from ... import types as sqltypes
from ...sql import expression
from ...sql import operators


try:
    from uuid import UUID as _python_UUID  # noqa
except ImportError:
    _python_UUID = None


def Any(other, arrexpr, operator=operators.eq):
    """A synonym for the :meth:`.ARRAY.Comparator.any` method.

    This method is legacy and is here for backwards-compatibility.

    .. seealso::

        :func:`.expression.any_`

    """

    return arrexpr.any(other, operator)


def All(other, arrexpr, operator=operators.eq):
    """A synonym for the :meth:`.ARRAY.Comparator.all` method.

    This method is legacy and is here for backwards-compatibility.

    .. seealso::

        :func:`.expression.all_`

    """

    return arrexpr.all(other, operator)


class array(expression.Tuple):

    """A PostgreSQL ARRAY literal.

    This is used to produce ARRAY literals in SQL expressions, e.g.::

        from sqlalchemy.dialects.postgresql import array
        from sqlalchemy.dialects import postgresql
        from sqlalchemy import select, func

        stmt = select([
                        array([1,2]) + array([3,4,5])
                    ])

        print(stmt.compile(dialect=postgresql.dialect()))

    Produces the SQL::

        SELECT ARRAY[%(param_1)s, %(param_2)s] ||
            ARRAY[%(param_3)s, %(param_4)s, %(param_5)s]) AS anon_1

    An instance of :class:`.array` will always have the datatype
    :class:`.ARRAY`.  The "inner" type of the array is inferred from
    the values present, unless the ``type_`` keyword argument is passed::

        array(['foo', 'bar'], type_=CHAR)

    Multidimensional arrays are produced by nesting :class:`.array` constructs.
    The dimensionality of the final :class:`.ARRAY` type is calculated by
    recursively adding the dimensions of the inner :class:`.ARRAY` type::

        stmt = select([
            array([
                array([1, 2]), array([3, 4]), array([column('q'), column('x')])
            ])
        ])
        print(stmt.compile(dialect=postgresql.dialect()))

    Produces::

        SELECT ARRAY[ARRAY[%(param_1)s, %(param_2)s],
        ARRAY[%(param_3)s, %(param_4)s], ARRAY[q, x]] AS anon_1

    .. versionadded:: 1.3.6 added support for multidimensional array literals

    .. seealso::

        :class:`.postgresql.ARRAY`

    """

    __visit_name__ = "array"

    def __init__(self, clauses, **kw):
        super(array, self).__init__(*clauses, **kw)
        if isinstance(self.type, ARRAY):
            self.type = ARRAY(
                self.type.item_type,
                dimensions=self.type.dimensions + 1
                if self.type.dimensions is not None
                else 2,
            )
        else:
            self.type = ARRAY(self.type)

    def _bind_param(self, operator, obj, _assume_scalar=False, type_=None):
        if _assume_scalar or operator is operators.getitem:
            # if getitem->slice were called, Indexable produces
            # a Slice object from that
            assert isinstance(obj, int)
            return expression.BindParameter(
                None,
                obj,
                _compared_to_operator=operator,
                type_=type_,
                _compared_to_type=self.type,
                unique=True,
            )

        else:
            return array(
                [
                    self._bind_param(
                        operator, o, _assume_scalar=True, type_=type_
                    )
                    for o in obj
                ]
            )

    def self_group(self, against=None):
        if against in (operators.any_op, operators.all_op, operators.getitem):
            return expression.Grouping(self)
        else:
            return self


CONTAINS = operators.custom_op("@>", precedence=5)

CONTAINED_BY = operators.custom_op("<@", precedence=5)

OVERLAP = operators.custom_op("&&", precedence=5)


class ARRAY(sqltypes.ARRAY):

    """PostgreSQL ARRAY type.

    .. versionchanged:: 1.1 The :class:`.postgresql.ARRAY` type is now
       a subclass of the core :class:`.types.ARRAY` type.

    The :class:`.postgresql.ARRAY` type is constructed in the same way
    as the core :class:`.types.ARRAY` type; a member type is required, and a
    number of dimensions is recommended if the type is to be used for more
    than one dimension::

        from sqlalchemy.dialects import postgresql

        mytable = Table("mytable", metadata,
                Column("data", postgresql.ARRAY(Integer, dimensions=2))
            )

    The :class:`.postgresql.ARRAY` type provides all operations defined on the
    core :class:`.types.ARRAY` type, including support for "dimensions",
    indexed access, and simple matching such as
    :meth:`.types.ARRAY.Comparator.any` and
    :meth:`.types.ARRAY.Comparator.all`.  :class:`.postgresql.ARRAY` class also
    provides PostgreSQL-specific methods for containment operations, including
    :meth:`.postgresql.ARRAY.Comparator.contains`
    :meth:`.postgresql.ARRAY.Comparator.contained_by`, and
    :meth:`.postgresql.ARRAY.Comparator.overlap`, e.g.::

        mytable.c.data.contains([1, 2])

    The :class:`.postgresql.ARRAY` type may not be supported on all
    PostgreSQL DBAPIs; it is currently known to work on psycopg2 only.

    Additionally, the :class:`.postgresql.ARRAY` type does not work directly in
    conjunction with the :class:`.ENUM` type.  For a workaround, see the
    special type at :ref:`postgresql_array_of_enum`.

    .. seealso::

        :class:`.types.ARRAY` - base array type

        :class:`.postgresql.array` - produces a literal array value.

    """

    class Comparator(sqltypes.ARRAY.Comparator):

        """Define comparison operations for :class:`.ARRAY`.

        Note that these operations are in addition to those provided
        by the base :class:`.types.ARRAY.Comparator` class, including
        :meth:`.types.ARRAY.Comparator.any` and
        :meth:`.types.ARRAY.Comparator.all`.

        """

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
                CONTAINED_BY, other, result_type=sqltypes.Boolean
            )

        def overlap(self, other):
            """Boolean expression.  Test if array has elements in common with
            an argument array expression.
            """
            return self.operate(OVERLAP, other, result_type=sqltypes.Boolean)

    comparator_factory = Comparator

    def __init__(
        self, item_type, as_tuple=False, dimensions=None, zero_indexes=False
    ):
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
         between Python zero-based and PostgreSQL one-based indexes, e.g.
         a value of one will be added to all index values before passing
         to the database.

         .. versionadded:: 0.9.5


        """
        if isinstance(item_type, ARRAY):
            raise ValueError(
                "Do not nest ARRAY types; ARRAY(basetype) "
                "handles multi-dimensional arrays of basetype"
            )
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
        if (
            dim == 1
            or dim is None
            and (
                # this has to be (list, tuple), or at least
                # not hasattr('__iter__'), since Py3K strings
                # etc. have __iter__
                not arr
                or not isinstance(arr[0], (list, tuple))
            )
        ):
            if itemproc:
                return collection(itemproc(x) for x in arr)
            else:
                return collection(arr)
        else:
            return collection(
                self._proc_array(
                    x,
                    itemproc,
                    dim - 1 if dim is not None else None,
                    collection,
                )
                for x in arr
            )

    def bind_processor(self, dialect):
        item_proc = self.item_type.dialect_impl(dialect).bind_processor(
            dialect
        )

        def process(value):
            if value is None:
                return value
            else:
                return self._proc_array(
                    value, item_proc, self.dimensions, list
                )

        return process

    def result_processor(self, dialect, coltype):
        item_proc = self.item_type.dialect_impl(dialect).result_processor(
            dialect, coltype
        )

        def process(value):
            if value is None:
                return value
            else:
                return self._proc_array(
                    value,
                    item_proc,
                    self.dimensions,
                    tuple if self.as_tuple else list,
                )

        return process


colspecs[sqltypes.ARRAY] = ARRAY
ischema_names["_array"] = ARRAY
