# orm/query.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, util, exceptions, sql_util, logging, schema
from sqlalchemy.orm import mapper, class_mapper, object_mapper
from sqlalchemy.orm.interfaces import OperationContext, SynonymProperty

__all__ = ['Query', 'QueryContext', 'SelectionContext']

class Query(object):
    """Encapsulates the object-fetching operations provided by Mappers."""

    def __init__(self, class_or_mapper, session=None, entity_name=None, lockmode=None, with_options=None, extension=None, **kwargs):
        if isinstance(class_or_mapper, type):
            self.mapper = mapper.class_mapper(class_or_mapper, entity_name=entity_name)
        else:
            self.mapper = class_or_mapper.compile()
        self.with_options = with_options or []
        self.select_mapper = self.mapper.get_select_mapper().compile()
        self.always_refresh = kwargs.pop('always_refresh', self.mapper.always_refresh)
        self.lockmode = lockmode
        self.extension = mapper._ExtensionCarrier()
        if extension is not None:
            self.extension.append(extension)
        self.extension.append(self.mapper.extension)
        self.is_polymorphic = self.mapper is not self.select_mapper
        self._session = session
        if not hasattr(self.mapper, '_get_clause'):
            _get_clause = sql.and_()
            for primary_key in self.primary_key_columns:
                _get_clause.clauses.append(primary_key == sql.bindparam(primary_key._label, type=primary_key.type, unique=True))
            self.mapper._get_clause = _get_clause
            
        self._entities = []
        self._get_clause = self.mapper._get_clause

        self._order_by = kwargs.pop('order_by', False)
        self._group_by = kwargs.pop('group_by', False)
        self._distinct = kwargs.pop('distinct', False)
        self._offset = kwargs.pop('offset', None)
        self._limit = kwargs.pop('limit', None)
        self._criterion = None
        self._col = None
        self._func = None
        self._joinpoint = self.mapper
        self._from_obj = [self.table]

        for opt in util.flatten_iterator(self.with_options):
            opt.process_query(self)
        
    def _clone(self):
        q = Query.__new__(Query)
        q.mapper = self.mapper
        q.select_mapper = self.select_mapper
        q._order_by = self._order_by
        q._distinct = self._distinct
        q._entities = list(self._entities)
        q.always_refresh = self.always_refresh
        q.with_options = list(self.with_options)
        q._session = self.session
        q.is_polymorphic = self.is_polymorphic
        q.lockmode = self.lockmode
        q.extension = mapper._ExtensionCarrier()
        for ext in self.extension:
            q.extension.append(ext)
        q._offset = self._offset
        q._limit = self._limit
        q._group_by = self._group_by
        q._get_clause = self._get_clause
        q._from_obj = list(self._from_obj)
        q._joinpoint = self._joinpoint
        q._criterion = self._criterion
        q._col = self._col
        q._func = self._func
        return q
    
    def _get_session(self):
        if self._session is None:
            return self.mapper.get_session()
        else:
            return self._session

    table = property(lambda s:s.select_mapper.mapped_table)
    primary_key_columns = property(lambda s:s.select_mapper.pks_by_table[s.select_mapper.mapped_table])
    session = property(_get_session)

    def get(self, ident, **kwargs):
        """Return an instance of the object based on the given
        identifier, or None if not found.

        The `ident` argument is a scalar or tuple of primary key
        column values in the order of the table def's primary key
        columns.
        """

        ret = self.extension.get(self, ident, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret
        key = self.mapper.identity_key(ident)
        return self._get(key, ident, **kwargs)

    def load(self, ident, **kwargs):
        """Return an instance of the object based on the given
        identifier.

        If not found, raises an exception.  The method will **remove
        all pending changes** to the object already existing in the
        Session.  The `ident` argument is a scalar or tuple of primary
        key column values in the order of the table def's primary key
        columns.
        """

        ret = self.extension.load(self, ident, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret
        key = self.mapper.identity_key(ident)
        instance = self._get(key, ident, reload=True, **kwargs)
        if instance is None:
            raise exceptions.InvalidRequestError("No instance found for identity %s" % repr(ident))
        return instance

    def get_by(self, *args, **params):
        """Like ``select_by()``, but only return the first 
        as a scalar, or None if no object found.
        Synonymous with ``selectfirst_by()``.

        The criterion is constructed in the same way as the
        ``select_by()`` method.
        """

        ret = self.extension.get_by(self, *args, **params)
        if ret is not mapper.EXT_PASS:
            return ret
        x = self.select_whereclause(self.join_by(*args, **params), limit=1)
        if x:
            return x[0]
        else:
            return None

    def select_by(self, *args, **params):
        """Return an array of object instances based on the given
        clauses and key/value criterion.

        \*args
            a list of zero or more ``ClauseElements`` which will be
            connected by ``AND`` operators.

        \**params 
            a set of zero or more key/value parameters which
            are converted into ``ClauseElements``.  the keys are mapped to
            property or column names mapped by this mapper's Table, and
            the values are coerced into a ``WHERE`` clause separated by
            ``AND`` operators.  If the local property/column names dont
            contain the key, a search will be performed against this
            mapper's immediate list of relations as well, forming the
            appropriate join conditions if a matching property is located.

            if the located property is a column-based property, the comparison
            value should be a scalar with an appropriate type.  If the 
            property is a relationship-bound property, the comparison value
            should be an instance of the related class.

            E.g.::

              result = usermapper.select_by(user_name = 'fred')

        """

        ret = self.extension.select_by(self, *args, **params)
        if ret is not mapper.EXT_PASS:
            return ret
        return self.select_whereclause(self.join_by(*args, **params))

    def join_by(self, *args, **params):
        """Return a ``ClauseElement`` representing the ``WHERE``
        clause that would normally be sent to ``select_whereclause()``
        by ``select_by()``.

        The criterion is constructed in the same way as the
        ``select_by()`` method.
        """

        return self._join_by(args, params)


    def join_to(self, key):
        """Given the key name of a property, will recursively descend
        through all child properties from this Query's mapper to
        locate the property, and will return a ClauseElement
        representing a join from this Query's mapper to the endmost
        mapper.
        """

        [keys, p] = self._locate_prop(key)
        return self.join_via(keys)

    def join_via(self, keys):
        """Given a list of keys that represents a path from this
        Query's mapper to a related mapper based on names of relations
        from one mapper to the next, return a ClauseElement
        representing a join from this Query's mapper to the endmost
        mapper.
        """

        mapper = self.mapper
        clause = None
        for key in keys:
            prop = mapper.props[key]
            if clause is None:
                clause = prop.get_join(mapper)
            else:
                clause &= prop.get_join(mapper)
            mapper = prop.mapper

        return clause

    def selectfirst_by(self, *args, **params):
        """Like ``select_by()``, but only return the first 
        as a scalar, or None if no object found.
        Synonymous with ``get_by()``.

        The criterion is constructed in the same way as the
        ``select_by()`` method.
        """

        return self.get_by(*args, **params)

    def selectone_by(self, *args, **params):
        """Like ``selectfirst_by()``, but throws an error if not
        exactly one result was returned.

        The criterion is constructed in the same way as the
        ``select_by()`` method.
        """

        ret = self.select_whereclause(self.join_by(*args, **params), limit=2)
        if len(ret) == 1:
            return ret[0]
        elif len(ret) == 0:
            raise exceptions.InvalidRequestError('No rows returned for selectone_by')
        else:
            raise exceptions.InvalidRequestError('Multiple rows returned for selectone_by')

    def count_by(self, *args, **params):
        """Return the count of instances based on the given clauses
        and key/value criterion.

        The criterion is constructed in the same way as the
        ``select_by()`` method.
        """

        return self.count(self.join_by(*args, **params))

    def selectfirst(self, arg=None, **kwargs):
        """Query for a single instance using the given criterion.
        
        Arguments are the same as ``select()``. In the case that 
        the given criterion represents ``WHERE`` criterion only, 
        LIMIT 1 is applied to the fully generated statement.

        """

        if isinstance(arg, sql.FromClause) and arg.supports_execution():
            ret = self.select_statement(arg, **kwargs)
        else:
            kwargs['limit'] = 1
            ret = self.select_whereclause(whereclause=arg, **kwargs)
        if ret:
            return ret[0]
        else:
            return None

    def selectone(self, arg=None, **kwargs):
        """Query for a single instance using the given criterion.
        
        Unlike ``selectfirst``, this method asserts that only one
        row exists.  In the case that the given criterion represents
        ``WHERE`` criterion only, LIMIT 2 is applied to the fully
        generated statement.

        """
        
        if isinstance(arg, sql.FromClause) and arg.supports_execution():
            ret = self.select_statement(arg, **kwargs)
        else:
            kwargs['limit'] = 2
            ret = self.select_whereclause(whereclause=arg, **kwargs)
        if len(ret) == 1:
            return ret[0]
        elif len(ret) == 0:
            raise exceptions.InvalidRequestError('No rows returned for selectone_by')
        else:
            raise exceptions.InvalidRequestError('Multiple rows returned for selectone')

    def select(self, arg=None, **kwargs):
        """Select instances of the object from the database.

        `arg` can be any ClauseElement, which will form the criterion
        with which to load the objects.

        For more advanced usage, arg can also be a Select statement
        object, which will be executed and its resulting rowset used
        to build new object instances.

        In this case, the developer must ensure that an adequate set
        of columns exists in the rowset with which to build new object
        instances.
        """

        ret = self.extension.select(self, arg=arg, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret
        if isinstance(arg, sql.FromClause) and arg.supports_execution():
            return self.select_statement(arg, **kwargs)
        else:
            return self.select_whereclause(whereclause=arg, **kwargs)

    def select_whereclause(self, whereclause=None, params=None, **kwargs):
        """Given a ``WHERE`` criterion, create a ``SELECT`` statement,
        execute and return the resulting instances.
        """
        statement = self.compile(whereclause, **kwargs)
        return self._select_statement(statement, params=params)

    def count(self, whereclause=None, params=None, **kwargs):
        """Given a ``WHERE`` criterion, create a ``SELECT COUNT``
        statement, execute and return the resulting count value.
        """
        if self._criterion:
            if whereclause is not None:
                whereclause = sql.and_(self._criterion, whereclause)
            else:
                whereclause = self._criterion
        from_obj = kwargs.pop('from_obj', self._from_obj)
        kwargs.setdefault('distinct', self._distinct)

        alltables = []
        for l in [sql_util.TableFinder(x) for x in from_obj]:
            alltables += l
        
        if self.table not in alltables:
            from_obj.append(self.table)
        if self._nestable(**kwargs):
            s = sql.select([self.table], whereclause, from_obj=from_obj, **kwargs).alias('getcount').count()
        else:
            primary_key = self.primary_key_columns
            s = sql.select([sql.func.count(list(primary_key)[0])], whereclause, from_obj=from_obj, **kwargs)
        return self.session.scalar(self.mapper, s, params=params)

    def select_statement(self, statement, **params):
        """Given a ``ClauseElement``-based statement, execute and
        return the resulting instances.
        """

        return self._select_statement(statement, params=params)

    def select_text(self, text, **params):
        """Given a literal string-based statement, execute and return
        the resulting instances.
        """

        t = sql.text(text)
        return self.execute(t, params=params)

    def _with_lazy_criterion(cls, instance, prop, reverse=False):
        """extract query criterion from a LazyLoader strategy given a Mapper, 
        source persisted/detached instance and PropertyLoader.
        
        """
        
        from sqlalchemy.orm import strategies
        (criterion, lazybinds, rev) = strategies.LazyLoader._create_lazy_clause(prop, reverse_direction=reverse)
        bind_to_col = dict([(lazybinds[col].key, col) for col in lazybinds])

        class Visitor(sql.ClauseVisitor):
            def visit_bindparam(self, bindparam):
                mapper = reverse and prop.mapper or prop.parent
                bindparam.value = mapper.get_attr_by_column(instance, bind_to_col[bindparam.key])
        Visitor().traverse(criterion)
        return criterion
    _with_lazy_criterion = classmethod(_with_lazy_criterion)
    
        
    def query_from_parent(cls, instance, property, **kwargs):
        """return a newly constructed Query object, with criterion corresponding to 
        a relationship to the given parent instance.

            instance
                a persistent or detached instance which is related to class represented
                by this query.

            property
                string name of the property which relates this query's class to the 
                instance. 
                
            \**kwargs
                all extra keyword arguments are propigated to the constructor of
                Query.
                
        """
        
        mapper = object_mapper(instance)
        prop = mapper.props[property]
        target = prop.mapper
        criterion = cls._with_lazy_criterion(instance, prop)
        return Query(target, **kwargs).filter(criterion)
    query_from_parent = classmethod(query_from_parent)
        
    def with_parent(self, instance, property=None):
        """add a join criterion corresponding to a relationship to the given parent instance.

            instance
                a persistent or detached instance which is related to class represented
                by this query.

            property
                string name of the property which relates this query's class to the 
                instance.  if None, the method will attempt to find a suitable property.

        currently, this method only works with immediate parent relationships, but in the
        future may be enhanced to work across a chain of parent mappers.    
        """

        from sqlalchemy.orm import properties
        mapper = object_mapper(instance)
        if property is None:
            for prop in mapper.props.values():
                if isinstance(prop, properties.PropertyLoader) and prop.mapper is self.mapper:
                    break
            else:
                raise exceptions.InvalidRequestError("Could not locate a property which relates instances of class '%s' to instances of class '%s'" % (self.mapper.class_.__name__, instance.__class__.__name__))
        else:
            prop = mapper.props[property]
        return self.filter(Query._with_lazy_criterion(instance, prop))

    def add_entity(self, entity):
        """add a mapped entity to the list of result columns to be returned.
        
        This will have the effect of all result-returning methods returning a tuple
        of results, the first element being an instance of the primary class for this 
        Query, and subsequent elements matching columns or entities which were
        specified via add_column or add_entity.
        
        When adding entities to the result, its generally desireable to add
        limiting criterion to the query which can associate the primary entity
        of this Query along with the additional entities.  The Query selects
        from all tables with no joining criterion by default.
        
        When tuple-based results are returned, the 'uniquing' of returned entities
        is disabled to maintain grouping.

            entity
                a class or mapper which will be added to the results.
                
        """
        q = self._clone()
        q._entities.append(entity)
        return q
        
    def add_column(self, column):
        """add a SQL ColumnElement to the list of result columns to be returned.
        
        This will have the effect of all result-returning methods returning a tuple
        of results, the first element being an instance of the primary class for this 
        Query, and subsequent elements matching columns or entities which were
        specified via add_column or add_entity.

        When adding columns to the result, its generally desireable to add
        limiting criterion to the query which can associate the primary entity
        of this Query along with the additional columns, if the column is based on a 
        table or selectable that is not the primary mapped selectable.  The Query selects
        from all tables with no joining criterion by default.
        
        When tuple-based results are returned, the 'uniquing' of returned entities
        is disabled to maintain grouping.

            column
                a string column name or sql.ColumnElement to be added to the results.
                
        """
        
        q = self._clone()
        q._entities.append(column)
        return q
        
    def options(self, *args, **kwargs):
        """Return a new Query object, applying the given list of
        MapperOptions.
        """
        q = self._clone()
        for opt in util.flatten_iterator(args):
            q.with_options.append(opt)
            opt.process_query(q)
        for opt in util.flatten_iterator(self.with_options):
            opt.process_query(self)
        return q

    def with_lockmode(self, mode):
        """Return a new Query object with the specified locking mode."""
        q = self._clone()
        q.lockmode = mode
        return q
    
    def filter(self, criterion):
        """apply the given filtering criterion to the query and return the newly resulting ``Query``
        
        the criterion is any sql.ClauseElement applicable to the WHERE clause of a select.
        """
        q = self._clone()
        if q._criterion is not None:
            q._criterion = q._criterion & criterion
        else:
            q._criterion = criterion
        return q

    def filter_by(self, *args, **kwargs):
        """apply the given filtering criterion to the query and return the newly resulting ``Query``

        The criterion is constructed in the same way as the
        ``select_by()`` method.
        """
        return self.filter(self._join_by(args, kwargs, start=self._joinpoint))

    def _join_to(self, prop, outerjoin=False):
        if isinstance(prop, list):
            mapper = self._joinpoint
            keys = []
            for key in prop:
                p = mapper.props[key]
                if p._is_self_referential():
                    raise exceptions.InvalidRequestError("Self-referential query on '%s' property must be constructed manually using an Alias object for the related table." % (str(p)))
                keys.append(key)
                mapper = p.mapper
        else:
            [keys,p] = self._locate_prop(prop, start=self._joinpoint)
        clause = self._from_obj[-1]
        mapper = self._joinpoint
        for key in keys:
            prop = mapper.props[key]
            if prop._is_self_referential():
                raise exceptions.InvalidRequestError("Self-referential query on '%s' property must be constructed manually using an Alias object for the related table." % str(prop))
            if outerjoin:
                if prop.secondary:
                    clause = clause.outerjoin(prop.secondary, prop.get_join(mapper, primary=True, secondary=False))
                    clause = clause.outerjoin(prop.select_table, prop.get_join(mapper, primary=False))
                else:
                    clause = clause.outerjoin(prop.select_table, prop.get_join(mapper))
            else:
                if prop.secondary:
                    clause = clause.join(prop.secondary, prop.get_join(mapper, primary=True, secondary=False))
                    clause = clause.join(prop.select_table, prop.get_join(mapper, primary=False))
                else:
                    clause = clause.join(prop.select_table, prop.get_join(mapper))
            mapper = prop.mapper
        return (clause, mapper)

    def _join_by(self, args, params, start=None):
        """Return a ``ClauseElement`` representing the ``WHERE``
        clause that would normally be sent to ``select_whereclause()``
        by ``select_by()``.

        The criterion is constructed in the same way as the
        ``select_by()`` method.
        """
        import properties
        
        clause = None
        for arg in args:
            if clause is None:
                clause = arg
            else:
                clause &= arg

        for key, value in params.iteritems():
            (keys, prop) = self._locate_prop(key, start=start)
            if isinstance(prop, properties.PropertyLoader):
                c = self._with_lazy_criterion(value, prop, True) & self.join_via(keys[:-1])
            else:
                c = prop.compare(value) & self.join_via(keys)
            if clause is None:
                clause =  c
            else:
                clause &= c
        return clause

    def _locate_prop(self, key, start=None):
        import properties
        keys = []
        seen = util.Set()
        def search_for_prop(mapper_):
            if mapper_ in seen:
                return None
            seen.add(mapper_)
            if mapper_.props.has_key(key):
                prop = mapper_.props[key]
                if isinstance(prop, SynonymProperty):
                    prop = mapper_.props[prop.name]
                if isinstance(prop, properties.PropertyLoader):
                    keys.insert(0, prop.key)
                return prop
            else:
                for prop in mapper_.props.values():
                    if not isinstance(prop, properties.PropertyLoader):
                        continue
                    x = search_for_prop(prop.mapper)
                    if x:
                        keys.insert(0, prop.key)
                        return x
                else:
                    return None
        p = search_for_prop(start or self.mapper)
        if p is None:
            raise exceptions.InvalidRequestError("Can't locate property named '%s'" % key)
        return [keys, p]

    def _generative_col_aggregate(self, col, func):
        """apply the given aggregate function to the query and return the newly
        resulting ``Query``.
        """
        if self._col is not None or self._func is not None:
            raise exceptions.InvalidRequestError("Query already contains an aggregate column or function")
        q = self._clone()
        q._col = col
        q._func = func
        return q

    def apply_min(self, col):
        """apply the SQL ``min()`` function against the given column to the
        query and return the newly resulting ``Query``.
        """
        return self._generative_col_aggregate(col, sql.func.min)

    def apply_max(self, col):
        """apply the SQL ``max()`` function against the given column to the
        query and return the newly resulting ``Query``.
        """
        return self._generative_col_aggregate(col, sql.func.max)

    def apply_sum(self, col):
        """apply the SQL ``sum()`` function against the given column to the
        query and return the newly resulting ``Query``.
        """
        return self._generative_col_aggregate(col, sql.func.sum)

    def apply_avg(self, col):
        """apply the SQL ``avg()`` function against the given column to the
        query and return the newly resulting ``Query``.
        """
        return self._generative_col_aggregate(col, sql.func.avg)

    def _col_aggregate(self, col, func):
        """Execute ``func()`` function against the given column.

        For performance, only use subselect if `order_by` attribute is set.
        """

        ops = {'distinct':self._distinct, 'order_by':self._order_by, 'from_obj':self._from_obj}

        if self._order_by is not False:
            s1 = sql.select([col], self._criterion, **ops).alias('u')
            return sql.select([func(s1.corresponding_column(col))]).scalar()
        else:
            return sql.select([func(col)], self._criterion, **ops).scalar()

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
    
    def order_by(self, criterion):
        """apply one or more ORDER BY criterion to the query and return the newly resulting ``Query``"""

        q = self._clone()
        if q._order_by is False:    
            q._order_by = util.to_list(criterion)
        else:
            q._order_by.extend(util.to_list(criterion))
        return q

    def group_by(self, criterion):
        """apply one or more GROUP BY criterion to the query and return the newly resulting ``Query``"""

        q = self._clone()
        if q._group_by is False:    
            q._group_by = util.to_list(criterion)
        else:
            q._group_by.extend(util.to_list(criterion))
        return q
    
    def join(self, prop):
        """create a join of this ``Query`` object's criterion
        to a relationship and return the newly resulting ``Query``.
        
        'prop' may be a string property name in which it is located
        in the same manner as keyword arguments in ``select_by``, or
        it may be a list of strings in which case the property is located
        by direct traversal of each keyname (i.e. like join_via()).
        """
        
        q = self._clone()
        (clause, mapper) = self._join_to(prop, outerjoin=False)
        q._from_obj = [clause]
        q._joinpoint = mapper
        return q

    def outerjoin(self, prop):
        """create a left outer join of this ``Query`` object's criterion
        to a relationship and return the newly resulting ``Query``.
        
        'prop' may be a string property name in which it is located
        in the same manner as keyword arguments in ``select_by``, or
        it may be a list of strings in which case the property is located
        by direct traversal of each keyname (i.e. like join_via()).
        """
        q = self._clone()
        (clause, mapper) = self._join_to(prop, outerjoin=True)
        q._from_obj = [clause]
        q._joinpoint = mapper
        return q

    def select_from(self, from_obj):
        """Set the `from_obj` parameter of the query.

        `from_obj` is a list of one or more tables.
        """

        new = self._clone()
        new._from_obj = list(new._from_obj) + util.to_list(from_obj)
        return new
        
    def __getattr__(self, key):
        if (key.startswith('select_by_')):
            key = key[10:]
            def foo(arg):
                return self.select_by(**{key:arg})
            return foo
        elif (key.startswith('get_by_')):
            key = key[7:]
            def foo(arg):
                return self.get_by(**{key:arg})
            return foo
        else:
            raise AttributeError(key)

    def __getitem__(self, item):
        if isinstance(item, slice):
            start = item.start
            stop = item.stop
            if (isinstance(start, int) and start < 0) or \
               (isinstance(stop, int) and stop < 0):
                return list(self)[item]
            else:
                res = self._clone()
                if start is not None and stop is not None:
                    res._offset = (self._offset or 0)+ start
                    res._limit = stop-start
                elif start is None and stop is not None:
                    res._limit = stop
                elif start is not None and stop is None:
                    res._offset = (self._offset or 0) + start
                if item.step is not None:
                    return list(res)[None:None:item.step]
                else:
                    return res
        else:
            return list(self[item:item+1])[0]

    def limit(self, limit):
        """Apply a ``LIMIT`` to the query."""

        return self[:limit]

    def offset(self, offset):
        """Apply an ``OFFSET`` to the query."""

        return self[offset:]

    def distinct(self):
        """Apply a ``DISTINCT`` to the query."""

        new = self._clone()
        new._distinct = True
        return new

    def list(self):
        """Return the results represented by this ``Query`` as a list.

        This results in an execution of the underlying query.
        """

        return list(self)

    def scalar(self):
        if self._col is None or self._func is None: 
            return self[0]
        else:
            return self._col_aggregate(self._col, self._func)
    
    def __iter__(self):
        return iter(self.select_whereclause())

    def execute(self, clauseelement, params=None, *args, **kwargs):
        """Execute the given ClauseElement-based statement against
        this Query's session/mapper, return the resulting list of
        instances.

        After execution, close the ResultProxy and its underlying
        resources.  This method is one step above the ``instances()``
        method, which takes the executed statement's ResultProxy
        directly.
        """

        result = self.session.execute(self.mapper, clauseelement, params=params)
        try:
            return self.instances(result, **kwargs)
        finally:
            result.close()

    def instances(self, cursor, *mappers_or_columns, **kwargs):
        """Return a list of mapped instances corresponding to the rows
        in a given *cursor* (i.e. ``ResultProxy``).
        
        \*mappers_or_columns is an optional list containing one or more of
        classes, mappers, strings or sql.ColumnElements which will be
        applied to each row and added horizontally to the result set,
        which becomes a list of tuples. The first element in each tuple
        is the usual result based on the mapper represented by this
        ``Query``. Each additional element in the tuple corresponds to an
        entry in the \*mappers_or_columns list.
        
        For each element in \*mappers_or_columns, if the element is 
        a mapper or mapped class, an additional class instance will be 
        present in the tuple.  If the element is a string or sql.ColumnElement, 
        the corresponding result column from each row will be present in the tuple.
        
        Note that when \*mappers_or_columns is present, "uniquing" for the result set
        is *disabled*, so that the resulting tuples contain entities as they actually
        correspond.  this indicates that multiple results may be present if this 
        option is used.
        """

        self.__log_debug("instances()")

        session = self.session

        context = SelectionContext(self.select_mapper, session, self.extension, with_options=self.with_options, **kwargs)

        process = []
        mappers_or_columns = tuple(self._entities) + mappers_or_columns
        if mappers_or_columns:
            for m in mappers_or_columns:
                if isinstance(m, type):
                    m = mapper.class_mapper(m)
                if isinstance(m, mapper.Mapper):
                    appender = []
                    def proc(context, row):
                        if not m._instance(context, row, appender):
                            appender.append(None)
                    process.append((proc, appender))
                elif isinstance(m, sql.ColumnElement) or isinstance(m, basestring):
                    res = []
                    def proc(context, row):
                        res.append(row[m])
                    process.append((proc, res))
            result = []
        else:
            result = util.UniqueAppender([])
                    
        for row in cursor.fetchall():
            self.select_mapper._instance(context, row, result)
            for proc in process:
                proc[0](context, row)

        # store new stuff in the identity map
        for value in context.identity_map.values():
            session._register_persistent(value)

        if mappers_or_columns:
            return zip(*([result] + [o[1] for o in process]))
        else:
            return result.data


    def _get(self, key, ident=None, reload=False, lockmode=None):
        lockmode = lockmode or self.lockmode
        if not reload and not self.always_refresh and lockmode is None:
            try:
                return self.session._get(key)
            except KeyError:
                pass

        if ident is None:
            ident = key[1]
        else:
            ident = util.to_list(ident)
        params = {}
        for i, primary_key in enumerate(self.primary_key_columns):
            params[primary_key._label] = ident[i]
        try:
            statement = self.compile(self._get_clause, lockmode=lockmode)
            return self._select_statement(statement, params=params, populate_existing=reload, version_check=(lockmode is not None))[0]
        except IndexError:
            return None

    def _select_statement(self, statement, params=None, **kwargs):
        statement.use_labels = True
        if params is None:
            params = {}
        return self.execute(statement, params=params, **kwargs)

    def _should_nest(self, querycontext):
        """Return True if the given statement options indicate that we
        should *nest* the generated query as a subquery inside of a
        larger eager-loading query.  This is used with keywords like
        distinct, limit and offset and the mapper defines eager loads.
        """

        return (
            len(querycontext.eager_loaders) > 0
            and self._nestable(**querycontext.select_args())
        )

    def _nestable(self, **kwargs):
        """Return true if the given statement options imply it should be nested."""

        return (kwargs.get('limit') is not None or kwargs.get('offset') is not None or kwargs.get('distinct', False))

    def compile(self, whereclause = None, **kwargs):
        """Given a WHERE criterion, produce a ClauseElement-based
        statement suitable for usage in the execute() method.
        """

        if self._criterion:
            whereclause = sql.and_(self._criterion, whereclause)

        if whereclause is not None and self.is_polymorphic:
            # adapt the given WHERECLAUSE to adjust instances of this query's mapped 
            # table to be that of our select_table,
            # which may be the "polymorphic" selectable used by our mapper.
            sql_util.ClauseAdapter(self.table).traverse(whereclause)

            # if extra entities, adapt the criterion to those as well
            for m in self._entities:
                if isinstance(m, type):
                    m = mapper.class_mapper(m)
                if isinstance(m, mapper.Mapper):
                    table = m.select_table
                    sql_util.ClauseAdapter(m.select_table).traverse(whereclause)
        
        # get/create query context.  get the ultimate compile arguments
        # from there
        context = kwargs.pop('query_context', None)
        if context is None:
            context = QueryContext(self, kwargs)
        order_by = context.order_by
        group_by = context.group_by
        from_obj = context.from_obj
        lockmode = context.lockmode
        distinct = context.distinct
        limit = context.limit
        offset = context.offset
        if order_by is False:
            order_by = self.mapper.order_by
        if order_by is False:
            if self.table.default_order_by() is not None:
                order_by = self.table.default_order_by()

        try:
            for_update = {'read':'read','update':True,'update_nowait':'nowait',None:False}[lockmode]
        except KeyError:
            raise exceptions.ArgumentError("Unknown lockmode '%s'" % lockmode)

        # if single-table inheritance mapper, add "typecol IN (polymorphic)" criterion so
        # that we only load the appropriate types
        if self.select_mapper.single and self.select_mapper.polymorphic_on is not None and self.select_mapper.polymorphic_identity is not None:
            whereclause = sql.and_(whereclause, self.select_mapper.polymorphic_on.in_(*[m.polymorphic_identity for m in self.select_mapper.polymorphic_iterator()]))

        alltables = []
        for l in [sql_util.TableFinder(x) for x in from_obj]:
            alltables += l

        if self.table not in alltables:
            from_obj.append(self.table)

        if self._should_nest(context):
            # if theres an order by, add those columns to the column list
            # of the "rowcount" query we're going to make
            if order_by:
                order_by = util.to_list(order_by) or []
                cf = sql_util.ColumnFinder()
                for o in order_by:
                    cf.traverse(o)
            else:
                cf = []

            s2 = sql.select(self.table.primary_key + list(cf), whereclause, use_labels=True, from_obj=from_obj, **context.select_args())
            if order_by:
                s2.order_by(*util.to_list(order_by))
            s3 = s2.alias('tbl_row_count')
            crit = s3.primary_key==self.table.primary_key
            statement = sql.select([], crit, use_labels=True, for_update=for_update)
            # now for the order by, convert the columns to their corresponding columns
            # in the "rowcount" query, and tack that new order by onto the "rowcount" query
            if order_by:
                statement.order_by(*sql_util.ClauseAdapter(s3).copy_and_process(order_by))
        else:
            statement = sql.select([], whereclause, from_obj=from_obj, use_labels=True, for_update=for_update, **context.select_args())
            if order_by:
                statement.order_by(*util.to_list(order_by))
            # for a DISTINCT query, you need the columns explicitly specified in order
            # to use it in "order_by".  ensure they are in the column criterion (particularly oid).
            # TODO: this should be done at the SQL level not the mapper level
            if kwargs.get('distinct', False) and order_by:
                [statement.append_column(c) for c in util.to_list(order_by)]

        context.statement = statement
        
        # give all the attached properties a chance to modify the query
        # TODO: doing this off the select_mapper.  if its the polymorphic mapper, then
        # it has no relations() on it.  should we compile those too into the query ?  (i.e. eagerloads)
        for value in self.select_mapper.props.values():
            value.setup(context)

        # additional entities/columns, add those to selection criterion
        for m in self._entities:
            if isinstance(m, type):
                m = mapper.class_mapper(m)
            if isinstance(m, mapper.Mapper):
                for value in m.props.values():
                    value.setup(context)
            elif isinstance(m, sql.ColumnElement):
                statement.append_column(m)
                
        return statement

    def __log_debug(self, msg):
        self.logger.debug(msg)

Query.logger = logging.class_logger(Query)

class QueryContext(OperationContext):
    """Created within the ``Query.compile()`` method to store and
    share state among all the Mappers and MapperProperty objects used
    in a query construction.
    """

    def __init__(self, query, kwargs):
        self.query = query
        self.order_by = kwargs.pop('order_by', query._order_by)
        self.group_by = kwargs.pop('group_by', query._group_by)
        self.from_obj = kwargs.pop('from_obj', query._from_obj)
        self.lockmode = kwargs.pop('lockmode', query.lockmode)
        self.distinct = kwargs.pop('distinct', query._distinct)
        self.limit = kwargs.pop('limit', query._limit)
        self.offset = kwargs.pop('offset', query._offset)
        self.eager_loaders = util.Set([x for x in query.mapper._eager_loaders])
        self.statement = None
        super(QueryContext, self).__init__(query.mapper, query.with_options, **kwargs)

    def select_args(self):
        """Return a dictionary of attributes from this
        ``QueryContext`` that can be applied to a ``sql.Select``
        statement.
        """
        return {'limit':self.limit, 'offset':self.offset, 'distinct':self.distinct, 'group_by':self.group_by}

    def accept_option(self, opt):
        """Accept a ``MapperOption`` which will process (modify) the
        state of this ``QueryContext``.
        """

        opt.process_query_context(self)


class SelectionContext(OperationContext):
    """Created within the ``query.instances()`` method to store and share
    state among all the Mappers and MapperProperty objects used in a
    load operation.

    SelectionContext contains these attributes:

    mapper
      The Mapper which originated the instances() call.

    session
      The Session that is relevant to the instances call.

    identity_map
      A dictionary which stores newly created instances that have not
      yet been added as persistent to the Session.

    attributes
      A dictionary to store arbitrary data; eager loaders use it to
      store additional result lists.

    populate_existing
      Indicates if its OK to overwrite the attributes of instances
      that were already in the Session.

    version_check
      Indicates if mappers that have version_id columns should verify
      that instances existing already within the Session should have
      this attribute compared to the freshly loaded value.
    """

    def __init__(self, mapper, session, extension, **kwargs):
        self.populate_existing = kwargs.pop('populate_existing', False)
        self.version_check = kwargs.pop('version_check', False)
        self.session = session
        self.extension = extension
        self.identity_map = {}
        super(SelectionContext, self).__init__(mapper, kwargs.pop('with_options', []), **kwargs)

    def accept_option(self, opt):
        """Accept a MapperOption which will process (modify) the state
        of this SelectionContext.
        """

        opt.process_selection_context(self)
