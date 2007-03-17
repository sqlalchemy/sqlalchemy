import sqlalchemy.sql as sql
import sqlalchemy.orm as orm

class SelectResultsExt(orm.MapperExtension):
    """a MapperExtension that provides SelectResults functionality for the
    results of query.select_by() and query.select()"""
    def select_by(self, query, *args, **params):
        return SelectResults(query, query.join_by(*args, **params))
    def select(self, query, arg=None, **kwargs):
        if hasattr(arg, '_selectable'):
            return orm.EXT_PASS
        else:
            return SelectResults(query, arg, ops=kwargs)

class SelectResults(object):
    """Build a query one component at a time via separate method
    calls, each call transforming the previous ``SelectResults``
    instance into a new ``SelectResults`` instance with further
    limiting criterion added. When interpreted in an iterator context
    (such as via calling ``list(selectresults)``), executes the query.
    """

    def __init__(self, query, clause=None, ops={}, joinpoint=None):
        """Construct a new ``SelectResults`` using the given ``Query``
        object and optional ``WHERE`` clause.  `ops` is an optional
        dictionary of bind parameter values.
        """

        self._query = query
        self._clause = clause
        self._ops = {}
        self._ops.update(ops)
        self._joinpoint = joinpoint or (self._query.table, self._query.mapper)

    def options(self,*args, **kwargs):
        """Apply mapper options to the underlying query.

        See also ``Query.options``.
        """

        new = self.clone()
        new._query = new._query.options(*args, **kwargs)
        return new

    def count(self):
        """Execute the SQL ``count()`` function against the ``SelectResults`` criterion."""

        return self._query.count(self._clause, **self._ops)

    def _col_aggregate(self, col, func):
        """Execute ``func()`` function against the given column.

        For performance, only use subselect if `order_by` attribute is set.
        """

        if self._ops.get('order_by'):
            s1 = sql.select([col], self._clause, **self._ops).alias('u')
            return sql.select([func(s1.corresponding_column(col))]).scalar()
        else:
            return sql.select([func(col)], self._clause, **self._ops).scalar()

    def min(self, col):
        """Execute the SQL ``min()`` function against the given column."""

        return self._col_aggregate(col, sql.func.min)

    def max(self, col):
        """Execute the SQL ``max()`` function against the given column."""

        return self._col_aggregate(col, sql.func.max)

    def sum(self, col):
        """Execute the SQL ``sum()`` function against the given column."""

        return self._col_aggregate(col, sql.func.sum)

    def avg(self, col):
        """Execute the SQL ``avg()`` function against the given column."""

        return self._col_aggregate(col, sql.func.avg)

    def clone(self):
        """Create a copy of this ``SelectResults``."""

        return SelectResults(self._query, self._clause, self._ops.copy(), self._joinpoint)

    def filter(self, clause):
        """Apply an additional ``WHERE`` clause against the query."""

        new = self.clone()
        new._clause = sql.and_(self._clause, clause)
        return new

    def select(self, clause):
        return self.filter(clause)

    def select_by(self, *args, **kwargs):
        return self.filter(self._query._join_by(args, kwargs, start=self._joinpoint[1]))

    def order_by(self, order_by):
        """Apply an ``ORDER BY`` to the query."""

        new = self.clone()
        new._ops['order_by'] = order_by
        return new

    def limit(self, limit):
        """Apply a ``LIMIT`` to the query."""

        return self[:limit]

    def offset(self, offset):
        """Apply an ``OFFSET`` to the query."""

        return self[offset:]

    def distinct(self):
        """Apply a ``DISTINCT`` to the query."""

        new = self.clone()
        new._ops['distinct'] = True
        return new

    def list(self):
        """Return the results represented by this ``SelectResults`` as a list.

        This results in an execution of the underlying query.
        """

        return list(self)

    def select_from(self, from_obj):
        """Set the `from_obj` parameter of the query.

        `from_obj` is a list of one or more tables.
        """

        new = self.clone()
        new._ops['from_obj'] = from_obj
        return new

    def join_to(self, prop):
        """Join the table of this ``SelectResults`` to the table located against the given property name.

        Subsequent calls to join_to or outerjoin_to will join against
        the rightmost table located from the previous `join_to` or
        `outerjoin_to` call, searching for the property starting with
        the rightmost mapper last located.
        """

        new = self.clone()
        (clause, mapper) = self._join_to(prop, outerjoin=False)
        new._ops['from_obj'] = [clause]
        new._joinpoint = (clause, mapper)
        return new

    def outerjoin_to(self, prop):
        """Outer join the table of this ``SelectResults`` to the 
        table located against the given property name.

        Subsequent calls to join_to or outerjoin_to will join against
        the rightmost table located from the previous ``join_to`` or
        ``outerjoin_to`` call, searching for the property starting with
        the rightmost mapper last located.
        """

        new = self.clone()
        (clause, mapper) = self._join_to(prop, outerjoin=True)
        new._ops['from_obj'] = [clause]
        new._joinpoint = (clause, mapper)
        return new

    def _join_to(self, prop, outerjoin=False):
        [keys,p] = self._query._locate_prop(prop, start=self._joinpoint[1])
        clause = self._joinpoint[0]
        mapper = self._joinpoint[1]
        for key in keys:
            prop = mapper.props[key]
            if outerjoin:
                clause = clause.outerjoin(prop.select_table, prop.get_join(mapper))
            else:
                clause = clause.join(prop.select_table, prop.get_join(mapper))
            mapper = prop.mapper
        return (clause, mapper)

    def compile(self):
        return self._query.compile(self._clause, **self._ops)

    def __getitem__(self, item):
        if isinstance(item, slice):
            start = item.start
            stop = item.stop
            if (isinstance(start, int) and start < 0) or \
               (isinstance(stop, int) and stop < 0):
                return list(self)[item]
            else:
                res = self.clone()
                if start is not None and stop is not None:
                    res._ops.update(dict(offset=self._ops.get('offset', 0)+start, limit=stop-start))
                elif start is None and stop is not None:
                    res._ops.update(dict(limit=stop))
                elif start is not None and stop is None:
                    res._ops.update(dict(offset=self._ops.get('offset', 0)+start))
                if item.step is not None:
                    return list(res)[None:None:item.step]
                else:
                    return res
        else:
            return list(self[item:item+1])[0]

    def __iter__(self):
        return iter(self._query.select_whereclause(self._clause, **self._ops))
