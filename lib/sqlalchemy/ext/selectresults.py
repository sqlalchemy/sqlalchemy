"""SelectResults has been rolled into Query.  This class is now just a placeholder."""

import sqlalchemy.sql as sql
import sqlalchemy.orm as orm

class SelectResultsExt(orm.MapperExtension):
    """a MapperExtension that provides SelectResults functionality for the
    results of query.select_by() and query.select()"""
    
    def select_by(self, query, *args, **params):
        q = query
        for a in args:
            q = q.filter(a)
        return q.filter_by(**params)
        
    def select(self, query, arg=None, **kwargs):
        if isinstance(arg, sql.FromClause) and arg.supports_execution():
            return orm.EXT_CONTINUE
        else:
            if arg is not None:
                query = query.filter(arg)
            return query._legacy_select_kwargs(**kwargs)

def SelectResults(query, clause=None, ops={}):
    if clause is not None:
        query = query.filter(clause)
    query = query.options(orm.extension(SelectResultsExt()))
    return query._legacy_select_kwargs(**ops)
