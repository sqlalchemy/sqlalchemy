import sqlalchemy.sql as sql

class SelectResults(object):
    def __init__(self, mapper, clause=None, ops={}):
        self._mapper = mapper
        self._clause = clause
        self._ops = {}
        self._ops.update(ops)

    def count(self):
        return self._mapper.count(self._clause)
    
    def min(self, col):
        return sql.select([sql.func.min(col)], self._clause, **self._ops).scalar()

    def max(self, col):
        return sql.select([sql.func.max(col)], self._clause, **self._ops).scalar()

    def sum(self, col):
        return sql.select([sql.func.sum(col)], self._clause, **self._ops).scalar()

    def avg(self, col):
        return sql.select([sql.func.avg(col)], self._clause, **self._ops).scalar()

    def clone(self):
        return SelectResults(self._mapper, self._clause, self._ops.copy())
        
    def filter(self, clause):
        new = self.clone()
        new._clause = sql.and_(self._clause, clause)
        return new

    def order_by(self, order_by):
        new = self.clone()
        new._ops['order_by'] = order_by
        return new

    def limit(self, limit):
        return self[:limit]

    def offset(self, offset):
        return self[offset:]

    def list(self):
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
                    res._ops.update(dict(offset=start, limit=stop-start))
                elif start is None and stop is not None:
                    res._ops.update(dict(limit=stop))
                elif start is not None and stop is None:
                    res._ops.update(dict(offset=start))
                if item.step is not None:
                    return list(res)[None:None:item.step]
                else:
                    return res
        else:
            return list(self[item:item+1])[0]
    
    def __iter__(self):
        return iter(self._mapper.select_whereclause(self._clause, **self._ops))
        
        
class TableFinder(sql.ClauseVisitor):
    """given a Clause, locates all the Tables within it into a list."""
    def __init__(self, table, check_columns=False):
        self.tables = []
        self.check_columns = check_columns
        if table is not None:
            table.accept_visitor(self)
    def visit_table(self, table):
        self.tables.append(table)
    def __len__(self):
        return len(self.tables)
    def __getitem__(self, i):
        return self.tables[i]
    def __iter__(self):
        return iter(self.tables)
    def __contains__(self, obj):
        return obj in self.tables
    def __add__(self, obj):
        return self.tables + list(obj)
    def visit_column(self, column):
        if self.check_columns:
            column.table.accept_visitor(self)
