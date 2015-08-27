# postgresql/ext.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from ...sql import expression
from ...sql import elements


class aggregate_order_by(expression.ColumnElement):
    """Represent a Postgresql aggregate order by expression.

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
