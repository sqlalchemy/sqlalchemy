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
    """Builds a query one component at a time via separate method calls, 
    each call transforming the previous SelectResults instance into a new SelectResults 
    instance with further limiting criterion added. When interpreted
    in an iterator context (such as via calling list(selectresults)), executes the query."""
    
    def __init__(self, query, clause=None, ops={}):
        """constructs a new SelectResults using the given Query object and optional WHERE 
        clause.  ops is an optional dictionary of bind parameter values."""
        self._query = query
        self._clause = clause
        self._ops = {}
        self._ops.update(ops)

    def count(self):
        """executes the SQL count() function against the SelectResults criterion."""
        return self._query.count(self._clause, **self._ops)

    def _col_aggregate(self, col, func):
        """executes func() function against the given column

        For performance, only use subselect if order_by attribute is set.
        
        """
        if self._ops.get('order_by'):
            s1 = sql.select([col], self._clause, **self._ops).alias('u')
            return sql.select([func(s1.corresponding_column(col))]).scalar()
        else:
            return sql.select([func(col)], self._clause, **self._ops).scalar()

    def min(self, col):
        """executes the SQL min() function against the given column"""
        return self._col_aggregate(col, sql.func.min)

    def max(self, col):
        """executes the SQL max() function against the given column"""
        return self._col_aggregate(col, sql.func.max)

    def sum(self, col):
        """executes the SQL sum() function against the given column"""
        return self._col_aggregate(col, sql.func.sum)

    def avg(self, col):
        """executes the SQL avg() function against the given column"""
        return self._col_aggregate(col, sql.func.avg)

    def clone(self):
        """creates a copy of this SelectResults."""
        return SelectResults(self._query, self._clause, self._ops.copy())
        
    def filter(self, clause):
        """applies an additional WHERE clause against the query."""
        new = self.clone()
        new._clause = sql.and_(self._clause, clause)
        return new

    def order_by(self, order_by):
        """applies an ORDER BY to the query."""
        new = self.clone()
        new._ops['order_by'] = order_by
        return new

    def limit(self, limit):
        """applies a LIMIT to the query."""
        return self[:limit]

    def offset(self, offset):
        """applies an OFFSET to the query."""
        return self[offset:]

    def list(self):
        """returns the results represented by this SelectResults as a list.  this results in an execution of the underlying query."""
        return list(self)
        
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
