# orm/query.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, util, exceptions, sql_util, logging, schema
from sqlalchemy.orm import mapper, class_mapper, object_mapper
from sqlalchemy.orm.interfaces import OperationContext
import operator

__all__ = ['Query', 'QueryContext', 'SelectionContext']

class Query(object):
    """Encapsulates the object-fetching operations provided by Mappers.
    
    """

    def __init__(self, class_or_mapper, session=None, entity_name=None):
        if isinstance(class_or_mapper, type):
            self.mapper = mapper.class_mapper(class_or_mapper, entity_name=entity_name)
        else:
            self.mapper = class_or_mapper.compile()
        self.select_mapper = self.mapper.get_select_mapper().compile()
        
        self._session = session
            
        self._with_options = []
        self._lockmode = None
        self._extension = self.mapper.extension.copy()
        self._entities = []
        self._order_by = False
        self._group_by = False
        self._distinct = False
        self._offset = None
        self._limit = None
        self._statement = None
        self._params = {}
        self._criterion = None
        self._column_aggregate = None
        self._joinpoint = self.mapper
        self._from_obj = [self.table]
        self._populate_existing = False
        self._version_check = False
        
    def _clone(self):
        q = Query.__new__(Query)
        q.__dict__ = self.__dict__.copy()
        return q
    
    def _get_session(self):
        if self._session is None:
            return self.mapper.get_session()
        else:
            return self._session

    table = property(lambda s:s.select_mapper.mapped_table)
    primary_key_columns = property(lambda s:s.select_mapper.primary_key)
    session = property(_get_session)

    def get(self, ident, **kwargs):
        """Return an instance of the object based on the given
        identifier, or None if not found.

        The `ident` argument is a scalar or tuple of primary key
        column values in the order of the table def's primary key
        columns.
        """

        ret = self._extension.get(self, ident, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret

        # convert composite types to individual args
        # TODO: account for the order of columns in the 
        # ColumnProperty it corresponds to
        if hasattr(ident, '__colset__'):
            ident = ident.__colset__()

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

        ret = self._extension.load(self, ident, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret
        key = self.mapper.identity_key(ident)
        instance = self._get(key, ident, reload=True, **kwargs)
        if instance is None:
            raise exceptions.InvalidRequestError("No instance found for identity %s" % repr(ident))
        return instance
        
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
        prop = mapper.get_property(property, resolve_synonyms=True)
        target = prop.mapper
        criterion = prop.compare(operator.eq, instance, value_is_parent=True)
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
            for prop in mapper.iterate_properties:
                if isinstance(prop, properties.PropertyLoader) and prop.mapper is self.mapper:
                    break
            else:
                raise exceptions.InvalidRequestError("Could not locate a property which relates instances of class '%s' to instances of class '%s'" % (self.mapper.class_.__name__, instance.__class__.__name__))
        else:
            prop = mapper.get_property(property, resolve_synonyms=True)
        return self.filter(prop.compare(operator.eq, instance, value_is_parent=True))

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
        
            entity
                a class or mapper which will be added to the results.
                
        """
        q = self._clone()
        q._entities = q._entities + [entity]
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
        
            column
                a string column name or sql.ColumnElement to be added to the results.
                
        """
        
        q = self._clone()

        # alias non-labeled column elements. 
        # TODO: make the generation deterministic
        if isinstance(column, sql.ColumnElement) and not hasattr(column, '_label'):
            column = column.label(None)

        q._entities = q._entities + [column]
        return q
        
    def options(self, *args):
        """Return a new Query object, applying the given list of
        MapperOptions.
        """
        
        q = self._clone()
        opts = [o for o in util.flatten_iterator(args)]
        q._with_options = q._with_options + opts
        for opt in opts:
            opt.process_query(q)
        return q

    def with_lockmode(self, mode):
        """Return a new Query object with the specified locking mode."""
        q = self._clone()
        q._lockmode = mode
        return q

    def params(self, **kwargs):
        """add values for bind parameters which may have been specified in filter()."""
        
        q = self._clone()
        q._params = q._params.copy()
        q._params.update(kwargs)
        return q
        
    def filter(self, criterion):
        """apply the given filtering criterion to the query and return the newly resulting ``Query``
        
        the criterion is any sql.ClauseElement applicable to the WHERE clause of a select.
        """
        
        if isinstance(criterion, basestring):
            criterion = sql.text(criterion)
            
        if criterion is not None and not isinstance(criterion, sql.ClauseElement):
            raise exceptions.ArgumentError("filter() argument must be of type sqlalchemy.sql.ClauseElement or string")
            
        q = self._clone()
        if q._criterion is not None:
            q._criterion = q._criterion & criterion
        else:
            q._criterion = criterion
        return q

    def filter_by(self, *args, **kwargs):
        """apply the given filtering criterion to the query and return the newly resulting ``Query``."""

        import properties
        
        if len(args) > 1:
            raise exceptions.ArgumentError("filter_by() takes either zero positional arguments, or one scalar or list argument indicating a property search path.")
        if len(args) == 1:
            path = args[0]
            (join, joinpoint, alias) = self._join_to(path, outerjoin=False, start=self.mapper, create_aliases=True)
            clause = None
        else:
            alias = None
            join = None
            clause = None
            joinpoint = self._joinpoint

        for key, value in kwargs.iteritems():
            prop = joinpoint.get_property(key, resolve_synonyms=True)
            c = prop.compare(operator.eq, value)

            if alias is not None:
                sql_util.ClauseAdapter(alias).traverse(c)
            if clause is None:
                clause =  c
            else:
                clause &= c
        
        if join is not None:
            return self.select_from(join).filter(clause)
        else:
            return self.filter(clause)

    def _join_to(self, keys, outerjoin=False, start=None, create_aliases=False):
        if start is None:
            start = self._joinpoint
        
        clause = self._from_obj[-1]
        
        currenttables = [clause]
        class FindJoinedTables(sql.NoColumnVisitor):
            def visit_join(self, join):
                currenttables.append(join.left)
                currenttables.append(join.right)
        FindJoinedTables().traverse(clause)
            
        mapper = start
        alias = None
        for key in util.to_list(keys):
            prop = mapper.get_property(key, resolve_synonyms=True)
            if prop._is_self_referential():
                raise exceptions.InvalidRequestError("Self-referential query on '%s' property must be constructed manually using an Alias object for the related table." % str(prop))
            # dont re-join to a table already in our from objects
            if prop.select_table not in currenttables:
                if outerjoin:
                    if prop.secondary:
                        clause = clause.outerjoin(prop.secondary, prop.get_join(mapper, primary=True, secondary=False))
                        clause = clause.outerjoin(prop.select_table, prop.get_join(mapper, primary=False))
                    else:
                        clause = clause.outerjoin(prop.select_table, prop.get_join(mapper))
                else:
                    if prop.secondary:
                        if create_aliases:
                            join = prop.get_join(mapper, primary=True, secondary=False)
                            secondary_alias = prop.secondary.alias()
                            if alias is not None:
                                join = sql_util.ClauseAdapter(alias).traverse(join, clone=True)
                            sql_util.ClauseAdapter(secondary_alias).traverse(join)
                            clause = clause.join(secondary_alias, join)
                            alias = prop.select_table.alias()
                            join = prop.get_join(mapper, primary=False)
                            join = sql_util.ClauseAdapter(secondary_alias).traverse(join, clone=True)
                            sql_util.ClauseAdapter(alias).traverse(join)
                            clause = clause.join(alias, join)
                        else:
                            clause = clause.join(prop.secondary, prop.get_join(mapper, primary=True, secondary=False))
                            clause = clause.join(prop.select_table, prop.get_join(mapper, primary=False))
                    else:
                        if create_aliases:
                            join = prop.get_join(mapper)
                            if alias is not None:
                                join = sql_util.ClauseAdapter(alias).traverse(join, clone=True)
                            alias = prop.select_table.alias()
                            join = sql_util.ClauseAdapter(alias).traverse(join, clone=True)
                            clause = clause.join(alias, join)
                        else:
                            clause = clause.join(prop.select_table, prop.get_join(mapper))
            mapper = prop.mapper
        if create_aliases:
            return (clause, mapper, alias)
        else:
            return (clause, mapper)

    def _generative_col_aggregate(self, col, func):
        """apply the given aggregate function to the query and return the newly
        resulting ``Query``.
        """
        if self._column_aggregate is not None:
            raise exceptions.InvalidRequestError("Query already contains an aggregate column or function")
        q = self._clone()
        q._column_aggregate = (col, func)
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

        ops = {'distinct':self._distinct, 'order_by':self._order_by or None, 'from_obj':self._from_obj}

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
            q._order_by = q._order_by + util.to_list(criterion)
        return q

    def group_by(self, criterion):
        """apply one or more GROUP BY criterion to the query and return the newly resulting ``Query``"""

        q = self._clone()
        if q._group_by is False:    
            q._group_by = util.to_list(criterion)
        else:
            q._group_by = q._group_by + util.to_list(criterion)
        return q

    def join(self, prop):
        """create a join of this ``Query`` object's criterion
        to a relationship and return the newly resulting ``Query``.

        'prop' may be a string property name or a list of string
        property names.
        """
        
        q = self._clone()
        (clause, mapper) = self._join_to(prop, outerjoin=False, start=self.mapper)
        q._from_obj = [clause]
        q._joinpoint = mapper
        return q

    def outerjoin(self, prop):
        """create a left outer join of this ``Query`` object's criterion
        to a relationship and return the newly resulting ``Query``.
        
        'prop' may be a string property name or a list of string
        property names.
        """
        q = self._clone()
        (clause, mapper) = self._join_to(prop, outerjoin=True, start=self.mapper)
        q._from_obj = [clause]
        q._joinpoint = mapper
        return q

    def reset_joinpoint(self):
        """return a new Query reset the 'joinpoint' of this Query reset 
        back to the starting mapper.  Subsequent generative calls will
        be constructed from the new joinpoint.

        Note that each call to join() or outerjoin() also starts from
        the root.
        """

        q = self._clone()
        q._joinpoint = q.mapper
        return q


    def select_from(self, from_obj):
        """Set the `from_obj` parameter of the query and return the newly 
        resulting ``Query``.

        `from_obj` is a list of one or more tables.
        """

        new = self._clone()
        new._from_obj = list(new._from_obj) + util.to_list(from_obj)
        return new
        
    def __getitem__(self, item):
        if isinstance(item, slice):
            start = item.start
            stop = item.stop
            # if we slice from the end we need to execute the query
            if (isinstance(start, int) and start < 0) or \
               (isinstance(stop, int) and stop < 0):
                return list(self)[item]
            else:
                res = self._clone()
                if start is not None and stop is not None:
                    res._offset = (self._offset or 0) + start
                    res._limit = stop - start
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
        """Apply a ``LIMIT`` to the query and return the newly resulting
        ``Query``.
        """

        return self[:limit]

    def offset(self, offset):
        """Apply an ``OFFSET`` to the query and return the newly resulting
        ``Query``.
        """

        return self[offset:]

    def distinct(self):
        """Apply a ``DISTINCT`` to the query and return the newly resulting
        ``Query``.
        """

        new = self._clone()
        new._distinct = True
        return new

    def all(self):
        """Return the results represented by this ``Query`` as a list.

        This results in an execution of the underlying query.
        """
        return list(self)
        
    
    def from_statement(self, statement):
        if isinstance(statement, basestring):
            statement = sql.text(statement)
        q = self._clone()
        q._statement = statement
        return q
        
    def first(self):
        """Return the first result of this ``Query``.

        This results in an execution of the underlying query.
        """

        if self._column_aggregate is not None: 
            return self._col_aggregate(*self._column_aggregate)
        
        ret = list(self[0:1])
        if len(ret) > 0:
            return ret[0]
        else:
            return None

    def one(self):
        """Return the first result of this ``Query``, raising an exception if more than one row exists.

        This results in an execution of the underlying query.
        """

        if self._column_aggregate is not None: 
            return self._col_aggregate(*self._column_aggregate)

        ret = list(self[0:2])
        
        if len(ret) == 1:
            return ret[0]
        elif len(ret) == 0:
            raise exceptions.InvalidRequestError('No rows returned for one()')
        else:
            raise exceptions.InvalidRequestError('Multiple rows returned for one()')
    
    def __iter__(self):
        statement = self.compile()
        statement.use_labels = True
        if self.session.autoflush:
            self.session.flush()
        result = self.session.execute(statement, params=self._params, mapper=self.mapper)
        try:
            return iter(self.instances(result))
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

        kwargs.setdefault('populate_existing', self._populate_existing)
        kwargs.setdefault('version_check', self._version_check)
        
        context = SelectionContext(self.select_mapper, session, self._extension, with_options=self._with_options, **kwargs)

        process = []
        mappers_or_columns = tuple(self._entities) + mappers_or_columns
        if mappers_or_columns:
            for m in mappers_or_columns:
                if isinstance(m, type):
                    m = mapper.class_mapper(m)
                if isinstance(m, mapper.Mapper):
                    def x(m):
                        appender = []
                        def proc(context, row):
                            if not m._instance(context, row, appender):
                                appender.append(None)
                        process.append((proc, appender))
                    x(m)
                elif isinstance(m, (sql.ColumnElement, basestring)):
                    def y(m):
                        res = []
                        def proc(context, row):
                            res.append(row[m])
                        process.append((proc, res))
                    y(m)
            result = []
        else:
            result = util.UniqueAppender([])
                    
        for row in cursor.fetchall():
            self.select_mapper._instance(context, row, result)
            for proc in process:
                proc[0](context, row)

        for instance in context.identity_map.values():
            context.attributes.get(('populating_mapper', instance), object_mapper(instance))._post_instance(context, instance)
        
        # store new stuff in the identity map
        for instance in context.identity_map.values():
            session._register_persistent(instance)

        if mappers_or_columns:
            return list(util.OrderedSet(zip(*([result] + [o[1] for o in process]))))
        else:
            return result.data


    def _get(self, key, ident=None, reload=False, lockmode=None):
        lockmode = lockmode or self._lockmode
        if not reload and not self.mapper.always_refresh and lockmode is None:
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
            try:
                params[primary_key._label] = ident[i]
            except IndexError:
                raise exceptions.InvalidRequestError("Could not find enough values to formulate primary key for query.get(); primary key columns are %s" % ', '.join(["'%s'" % str(c) for c in self.primary_key_columns]))
        try:
            q = self
            if lockmode is not None:
                q = q.with_lockmode(lockmode)
            q = q.filter(self.select_mapper._get_clause)
            q = q.params(**params)._select_context_options(populate_existing=reload, version_check=(lockmode is not None))
            return q.first()
        except IndexError:
            return None

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

    def count(self, whereclause=None, params=None, **kwargs):
        """Apply this query's criterion to a SELECT COUNT statement.

        the whereclause, params and \**kwargs arguments are deprecated.  use filter()
        and other generative methods to establish modifiers.
        """

        q = self
        if whereclause is not None:
            q = q.filter(whereclause)
        if params is not None:
            q = q.params(**params)
        q = q._legacy_select_kwargs(**kwargs)
        return q._count()

    def _count(self):
        """Apply this query's criterion to a SELECT COUNT statement.
        
        this is the purely generative version which will become 
        the public method in version 0.5.
        """

        whereclause = self._criterion

        context = QueryContext(self)
        from_obj = context.from_obj

        alltables = []
        for l in [sql_util.TableFinder(x) for x in from_obj]:
            alltables += l

        if self.table not in alltables:
            from_obj.append(self.table)
        if self._nestable(**context.select_args()):
            s = sql.select([self.table], whereclause, from_obj=from_obj, **context.select_args()).alias('getcount').count()
        else:
            primary_key = self.primary_key_columns
            s = sql.select([sql.func.count(list(primary_key)[0])], whereclause, from_obj=from_obj, **context.select_args())
        return self.session.scalar(s, params=self._params, mapper=self.mapper)
        
    def compile(self):
        """compiles and returns a SQL statement based on the criterion and conditions within this Query."""
        
        if self._statement:
            self._statement.use_labels = True
            return self._statement
        
        whereclause = self._criterion

        if whereclause is not None and (self.mapper is not self.select_mapper):
            # adapt the given WHERECLAUSE to adjust instances of this query's mapped 
            # table to be that of our select_table,
            # which may be the "polymorphic" selectable used by our mapper.
            sql_util.ClauseAdapter(self.table).traverse(whereclause, stop_on=util.Set([self.table]))

            # if extra entities, adapt the criterion to those as well
            for m in self._entities:
                if isinstance(m, type):
                    m = mapper.class_mapper(m)
                if isinstance(m, mapper.Mapper):
                    table = m.select_table
                    sql_util.ClauseAdapter(m.select_table).traverse(whereclause, stop_on=util.Set([m.select_table]))
        
        # get/create query context.  get the ultimate compile arguments
        # from there
        context = QueryContext(self)
        order_by = context.order_by
        from_obj = context.from_obj
        lockmode = context.lockmode
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
                order_by = [sql._literal_as_text(o) for o in util.to_list(order_by) or []]
                cf = sql_util.ColumnFinder()
                for o in order_by:
                    cf.traverse(o)
            else:
                cf = []

            s2 = sql.select(self.primary_key_columns + list(cf), whereclause, use_labels=True, from_obj=from_obj, correlate=False, **context.select_args())
            if order_by:
                s2 = s2.order_by(*util.to_list(order_by))
            s3 = s2.alias('tbl_row_count')
            crit = s3.primary_key==self.primary_key_columns
            statement = sql.select([], crit, use_labels=True, for_update=for_update)
            # now for the order by, convert the columns to their corresponding columns
            # in the "rowcount" query, and tack that new order by onto the "rowcount" query
            if order_by:
                statement.append_order_by(*sql_util.ClauseAdapter(s3).copy_and_process(order_by))
        else:
            statement = sql.select([], whereclause, from_obj=from_obj, use_labels=True, for_update=for_update, **context.select_args())
            if order_by:
                statement.append_order_by(*util.to_list(order_by))
                
            # for a DISTINCT query, you need the columns explicitly specified in order
            # to use it in "order_by".  ensure they are in the column criterion (particularly oid).
            # TODO: this should be done at the SQL level not the mapper level
            # TODO: need test coverage for this 
            if context.distinct and order_by:
                [statement.append_column(c) for c in util.to_list(order_by)]

        context.statement = statement
        
        # give all the attached properties a chance to modify the query
        # TODO: doing this off the select_mapper.  if its the polymorphic mapper, then
        # it has no relations() on it.  should we compile those too into the query ?  (i.e. eagerloads)
        for value in self.select_mapper.iterate_properties:
            value.setup(context)
        
        # additional entities/columns, add those to selection criterion
        for m in self._entities:
            if isinstance(m, type):
                m = mapper.class_mapper(m)
            if isinstance(m, mapper.Mapper):
                for value in m.iterate_properties:
                    value.setup(context)
            elif isinstance(m, sql.ColumnElement):
                statement.append_column(m)
                
        return statement

    def __log_debug(self, msg):
        self.logger.debug(msg)

    # DEPRECATED LAND !

    def list(self):
        """DEPRECATED.  use all()"""

        return list(self)

    def scalar(self):
        """DEPRECATED.  use first()"""

        return self.first()

    def _legacy_filter_by(self, *args, **kwargs):
        return self.filter(self._legacy_join_by(args, kwargs, start=self._joinpoint))

    def count_by(self, *args, **params):
        """DEPRECATED.  use query.filter_by(\**params).count()"""

        return self.count(self.join_by(*args, **params))


    def select_whereclause(self, whereclause=None, params=None, **kwargs):
        """DEPRECATED.  use query.filter(whereclause).all()"""

        q = self.filter(whereclause)._legacy_select_kwargs(**kwargs)
        if params is not None:
            q = q.params(**params)
        return list(q)
        
    def _legacy_select_kwargs(self, **kwargs):
        q = self
        if "order_by" in kwargs and kwargs['order_by']:
            q = q.order_by(kwargs['order_by'])
        if "group_by" in kwargs:
            q = q.group_by(kwargs['group_by'])
        if "from_obj" in kwargs:
            q = q.select_from(kwargs['from_obj'])
        if "lockmode" in kwargs:
            q = q.with_lockmode(kwargs['lockmode'])
        if "distinct" in kwargs:
            q = q.distinct()
        if "limit" in kwargs:
            q = q.limit(kwargs['limit'])
        if "offset" in kwargs:
            q = q.offset(kwargs['offset'])
        return q


    def get_by(self, *args, **params):
        """DEPRECATED.  use query.filter_by(\**params).first()"""

        ret = self._extension.get_by(self, *args, **params)
        if ret is not mapper.EXT_PASS:
            return ret

        return self._legacy_filter_by(*args, **params).first()

    def select_by(self, *args, **params):
        """DEPRECATED. use use query.filter_by(\**params).all()."""

        ret = self._extension.select_by(self, *args, **params)
        if ret is not mapper.EXT_PASS:
            return ret

        return self._legacy_filter_by(*args, **params).list()

    def join_by(self, *args, **params):
        """DEPRECATED. use join() to construct joins based on attribute names."""

        return self._legacy_join_by(args, params, start=self._joinpoint)

    def _build_select(self, arg=None, params=None, **kwargs):
        if isinstance(arg, sql.FromClause) and arg.supports_execution():
            return self.from_statement(arg)
        else:
            return self.filter(arg)._legacy_select_kwargs(**kwargs)

    def selectfirst(self, arg=None, **kwargs):
        """DEPRECATED.  use query.filter(whereclause).first()"""

        return self._build_select(arg, **kwargs).first()

    def selectone(self, arg=None, **kwargs):
        """DEPRECATED.  use query.filter(whereclause).one()"""

        return self._build_select(arg, **kwargs).one()

    def select(self, arg=None, **kwargs):
        """DEPRECATED.  use query.filter(whereclause).all(), or query.from_statement(statement).all()"""

        ret = self._extension.select(self, arg=arg, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret
        return self._build_select(arg, **kwargs).all()

    def execute(self, clauseelement, params=None, *args, **kwargs):
        """DEPRECATED.  use query.from_statement().all()"""

        return self._select_statement(statement, params, **kwargs)

    def select_statement(self, statement, **params):
        """DEPRECATED.  Use query.from_statement(statement)"""
        
        return self._select_statement(statement, params)

    def select_text(self, text, **params):
        """DEPRECATED.  Use query.from_statement(statement)"""

        return self._select_statement(statement, params)

    def _select_statement(self, statement, params=None, **kwargs):
        q = self.from_statement(statement)
        if params is not None:
            q = q.params(**params)
        q._select_context_options(**kwargs)
        return list(q)

    def _select_context_options(self, populate_existing=None, version_check=None):
        if populate_existing is not None:
            self._populate_existing = populate_existing
        if version_check is not None:
            self._version_check = version_check
        return self
        
    def join_to(self, key):
        """DEPRECATED. use join() to create joins based on property names."""

        [keys, p] = self._locate_prop(key)
        return self.join_via(keys)

    def join_via(self, keys):
        """DEPRECATED. use join() to create joins based on property names."""

        mapper = self._joinpoint
        clause = None
        for key in keys:
            prop = mapper.get_property(key, resolve_synonyms=True)
            if clause is None:
                clause = prop.get_join(mapper)
            else:
                clause &= prop.get_join(mapper)
            mapper = prop.mapper

        return clause

    def _legacy_join_by(self, args, params, start=None):
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
                c = prop.compare(operator.eq, value) & self.join_via(keys[:-1])
            else:
                c = prop.compare(operator.eq, value) & self.join_via(keys)
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
            
            prop = mapper_.get_property(key, resolve_synonyms=True, raiseerr=False)
            if prop is not None:
                if isinstance(prop, properties.PropertyLoader):
                    keys.insert(0, prop.key)
                return prop
            else:
                for prop in mapper_.iterate_properties:
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

    def selectfirst_by(self, *args, **params):
        """DEPRECATED. Use query.filter_by(\**kwargs).first()"""

        return self._legacy_filter_by(*args, **params).first()

    def selectone_by(self, *args, **params):
        """DEPRECATED. Use query.filter_by(\**kwargs).one()"""

        return self._legacy_filter_by(*args, **params).one()



Query.logger = logging.class_logger(Query)

class QueryContext(OperationContext):
    """Created within the ``Query.compile()`` method to store and
    share state among all the Mappers and MapperProperty objects used
    in a query construction.
    """

    def __init__(self, query):
        self.query = query
        self.order_by = query._order_by
        self.group_by = query._group_by
        self.from_obj = query._from_obj
        self.lockmode = query._lockmode
        self.distinct = query._distinct
        self.limit = query._limit
        self.offset = query._offset
        self.eager_loaders = util.Set([x for x in query.mapper._eager_loaders])
        self.statement = None
        super(QueryContext, self).__init__(query.mapper, query._with_options)

    def select_args(self):
        """Return a dictionary of attributes from this
        ``QueryContext`` that can be applied to a ``sql.Select``
        statement.
        """
        return {'limit':self.limit, 'offset':self.offset, 'distinct':self.distinct, 'group_by':self.group_by or None}

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
      A dictionary to store arbitrary data; mappers, strategies, and
      options all store various state information here in order
      to communicate with each other and to themselves.
      

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
