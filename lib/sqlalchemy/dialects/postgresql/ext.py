# postgresql/ext.py
# Copyright (C) 2005-2017 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from ...sql import expression
from ...sql import elements
from ...sql import functions
from ...sql.schema import ColumnCollectionConstraint
from .array import ARRAY


class aggregate_order_by(expression.ColumnElement):
    """Represent a PostgreSQL aggregate order by expression.

    E.g.::

        from sqlalchemy.dialects.postgresql import aggregate_order_by
        expr = func.array_agg(aggregate_order_by(table.c.a, table.c.b.desc()))
        stmt = select([expr])

    would represent the expression::

        SELECT array_agg(a ORDER BY b DESC) FROM table;

    Similarly::

        expr = func.string_agg(
            table.c.a,
            aggregate_order_by(literal_column("','"), table.c.a)
        )
        stmt = select([expr])

    Would represent::

        SELECT string_agg(a, ',' ORDER BY a) FROM table;

    .. versionadded:: 1.1

    .. seealso::

        :class:`.array_agg`

    """

    __visit_name__ = 'aggregate_order_by'

    def __init__(self, target, order_by):
        self.target = elements._literal_as_binds(target)
        self.order_by = elements._literal_as_binds(order_by)

    def self_group(self, against=None):
        return self

    def get_children(self, **kwargs):
        return self.target, self.order_by

    def _copy_internals(self, clone=elements._clone, **kw):
        self.target = clone(self.target, **kw)
        self.order_by = clone(self.order_by, **kw)

    @property
    def _from_objects(self):
        return self.target._from_objects + self.order_by._from_objects


class ExcludeConstraint(ColumnCollectionConstraint):
    """A table-level EXCLUDE constraint.

    Defines an EXCLUDE constraint as described in the `postgres
    documentation`__.

    __ http://www.postgresql.org/docs/9.0/\
static/sql-createtable.html#SQL-CREATETABLE-EXCLUDE
    """

    __visit_name__ = 'exclude_constraint'

    where = None

    def __init__(self, *elements, **kw):
        r"""
        :param \*elements:
          A sequence of two tuples of the form ``(column, operator)`` where
          column must be a column name or Column object and operator must
          be a string containing the operator to use.

        :param name:
          Optional, the in-database name of this constraint.

        :param deferrable:
          Optional bool.  If set, emit DEFERRABLE or NOT DEFERRABLE when
          issuing DDL for this constraint.

        :param initially:
          Optional string.  If set, emit INITIALLY <value> when issuing DDL
          for this constraint.

        :param using:
          Optional string.  If set, emit USING <index_method> when issuing DDL
          for this constraint. Defaults to 'gist'.

        :param where:
          Optional string.  If set, emit WHERE <predicate> when issuing DDL
          for this constraint.

        """
        columns = []
        render_exprs = []
        self.operators = {}

        expressions, operators = zip(*elements)

        for (expr, column, strname, add_element), operator in zip(
                self._extract_col_expression_collection(expressions),
                operators
        ):
            if add_element is not None:
                columns.append(add_element)

            name = column.name if column is not None else strname

            if name is not None:
                # backwards compat
                self.operators[name] = operator

            expr = expression._literal_as_text(expr)

            render_exprs.append(
                (expr, name, operator)
            )

        self._render_exprs = render_exprs
        ColumnCollectionConstraint.__init__(
            self,
            *columns,
            name=kw.get('name'),
            deferrable=kw.get('deferrable'),
            initially=kw.get('initially')
        )
        self.using = kw.get('using', 'gist')
        where = kw.get('where')
        if where is not None:
            self.where = expression._literal_as_text(where)

    def copy(self, **kw):
        elements = [(col, self.operators[col])
                    for col in self.columns.keys()]
        c = self.__class__(*elements,
                           name=self.name,
                           deferrable=self.deferrable,
                           initially=self.initially)
        c.dispatch._update(self.dispatch)
        return c


def array_agg(*arg, **kw):
    """PostgreSQL-specific form of :class:`.array_agg`, ensures
    return type is :class:`.postgresql.ARRAY` and not
    the plain :class:`.types.ARRAY`.

    .. versionadded:: 1.1

    """
    kw['type_'] = ARRAY(functions._type_from_args(arg))
    return functions.func.array_agg(*arg, **kw)
