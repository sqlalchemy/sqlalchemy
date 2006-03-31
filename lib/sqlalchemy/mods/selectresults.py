import sqlalchemy.sql as sql

import sqlalchemy.mapping as mapping

def install_plugin():
    mapping.extensions.append(SelectResultsExt)
    
class SelectResultsExt(mapping.MapperExtension):
    def select_by(self, mapper, *args, **params):
        return SelectResults(mapper, mapper._by_clause(*args, **params))
    def select(self, mapper, arg=None, **kwargs):
        if arg is not None and isinstance(arg, sql.Selectable):
            return mapping.EXT_PASS
        else:
            return SelectResults(mapper, arg, ops=kwargs)
        
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
