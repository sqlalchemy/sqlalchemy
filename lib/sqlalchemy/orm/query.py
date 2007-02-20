# orm/query.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, util, exceptions, sql_util, logging, schema
from sqlalchemy.orm import mapper, class_mapper
from sqlalchemy.orm.interfaces import OperationContext, SynonymProperty

__all__ = ['Query', 'QueryContext', 'SelectionContext']

class Query(object):
    """encapsulates the object-fetching operations provided by Mappers."""
    def __init__(self, class_or_mapper, session=None, entity_name=None, lockmode=None, with_options=None, extension=None, **kwargs):
        if isinstance(class_or_mapper, type):
            self.mapper = mapper.class_mapper(class_or_mapper, entity_name=entity_name)
        else:
            self.mapper = class_or_mapper.compile()
        self.with_options = with_options or []
        self.select_mapper = self.mapper.get_select_mapper().compile()
        self.always_refresh = kwargs.pop('always_refresh', self.mapper.always_refresh)
        self.order_by = kwargs.pop('order_by', self.mapper.order_by)
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
                _get_clause.clauses.append(primary_key == sql.bindparam(primary_key._label, type=primary_key.type))
            self.mapper._get_clause = _get_clause
        self._get_clause = self.mapper._get_clause
        for opt in util.flatten_iterator(self.with_options):
            opt.process_query(self)
    
    def _insert_extension(self, ext):
        self.extension.insert(ext)
              
    def _get_session(self):
        if self._session is None:
            return self.mapper.get_session()
        else:
            return self._session
    table = property(lambda s:s.select_mapper.mapped_table)
    primary_key_columns = property(lambda s:s.select_mapper.pks_by_table[s.select_mapper.mapped_table])
    session = property(_get_session)
    
    def get(self, ident, **kwargs):
        """return an instance of the object based on the given identifier, or None if not found.  
        
        The ident argument is a scalar or tuple of primary key column values
        in the order of the table def's primary key columns."""
        ret = self.extension.get(self, ident, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret
        key = self.mapper.identity_key(ident)
        return self._get(key, ident, **kwargs)

    def load(self, ident, **kwargs):
        """return an instance of the object based on the given identifier. 
        
        If not found, raises an exception.  The method will *remove all pending changes* to the object
        already existing in the Session.  The ident argument is a scalar or tuple of primary
        key column values in the order of the table def's primary key columns."""
        ret = self.extension.load(self, ident, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret
        key = self.mapper.identity_key(ident)
        instance = self._get(key, ident, reload=True, **kwargs)
        if instance is None:
            raise exceptions.InvalidRequestError("No instance found for identity %s" % repr(ident))
        return instance
        
    def get_by(self, *args, **params):
        """return a single object instance based on the given key/value criterion.
         
        this is either the first value in the result list, or None if the list is 
        empty.

        the keys are mapped to property or column names mapped by this mapper's Table, and the values
        are coerced into a WHERE clause separated by AND operators.  If the local property/column
        names dont contain the key, a search will be performed against this mapper's immediate
        list of relations as well, forming the appropriate join conditions if a matching property
        is located.

        e.g.   u = usermapper.get_by(user_name = 'fred')
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
        """return an array of object instances based on the given clauses and key/value criterion. 

        *args is a list of zero or more ClauseElements which will be connected by AND operators.

        **params is a set of zero or more key/value parameters which are converted into ClauseElements.
        the keys are mapped to property or column names mapped by this mapper's Table, and the values
        are coerced into a WHERE clause separated by AND operators.  If the local property/column
        names dont contain the key, a search will be performed against this mapper's immediate
        list of relations as well, forming the appropriate join conditions if a matching property
        is located.

        e.g.   result = usermapper.select_by(user_name = 'fred')
        """
        ret = self.extension.select_by(self, *args, **params)
        if ret is not mapper.EXT_PASS:
            return ret
        return self.select_whereclause(self.join_by(*args, **params))

    def join_by(self, *args, **params):
        """return a ClauseElement representing the WHERE clause that would normally be sent to select_whereclause() by select_by()."""
        return self._join_by(args, params)

    def _join_by(self, args, params, start=None):
        """return a ClauseElement representing the WHERE clause that would normally be sent to select_whereclause() by select_by()."""
        clause = None
        for arg in args:
            if clause is None:
                clause = arg
            else:
                clause &= arg

        for key, value in params.iteritems():
            (keys, prop) = self._locate_prop(key, start=start)
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
            raise exceptions.InvalidRequestError("Cant locate property named '%s'" % key)
        return [keys, p]

    def join_to(self, key):
        """given the key name of a property, will recursively descend through all child properties
        from this Query's mapper to locate the property, and will return a ClauseElement
        representing a join from this Query's mapper to the endmost mapper."""
        [keys, p] = self._locate_prop(key)
        return self.join_via(keys)

    def join_via(self, keys):
        """given a list of keys that represents a path from this Query's mapper to a related mapper
        based on names of relations from one mapper to the next, returns a 
        ClauseElement representing a join from this Query's mapper to the endmost mapper.
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
        """works like select_by(), but only returns the first result by itself, or None if no 
        objects returned.  Synonymous with get_by()"""
        return self.get_by(*args, **params)

    def selectone_by(self, *args, **params):
        """works like selectfirst_by(), but throws an error if not exactly one result was returned."""
        ret = self.select_whereclause(self.join_by(*args, **params), limit=2)
        if len(ret) == 1:
            return ret[0]
        elif len(ret) == 0:
            raise exceptions.InvalidRequestError('No rows returned for selectone_by')
        else:
            raise exceptions.InvalidRequestError('Multiple rows returned for selectone_by')

    def count_by(self, *args, **params):
        """returns the count of instances based on the given clauses and key/value criterion.
        The criterion is constructed in the same way as the select_by() method."""
        return self.count(self.join_by(*args, **params))

    def selectfirst(self, *args, **params):
        """works like select(), but only returns the first result by itself, or None if no 
        objects returned."""
        params['limit'] = 1
        ret = self.select_whereclause(*args, **params)
        if ret:
            return ret[0]
        else:
            return None

    def selectone(self, *args, **params):
        """works like selectfirst(), but throws an error if not exactly one result was returned."""
        ret = list(self.select(*args, **params)[0:2])
        if len(ret) == 1:
            return ret[0]
        elif len(ret) == 0:
            raise exceptions.InvalidRequestError('No rows returned for selectone_by')
        else:
            raise exceptions.InvalidRequestError('Multiple rows returned for selectone')

    def select(self, arg=None, **kwargs):
        """selects instances of the object from the database.  

        arg can be any ClauseElement, which will form the criterion with which to
        load the objects.

        For more advanced usage, arg can also be a Select statement object, which
        will be executed and its resulting rowset used to build new object instances.  
        in this case, the developer must ensure that an adequate set of columns exists in the 
        rowset with which to build new object instances."""

        ret = self.extension.select(self, arg=arg, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret
        if isinstance(arg, sql.FromClause):
            return self.select_statement(arg, **kwargs)
        else:
            return self.select_whereclause(whereclause=arg, **kwargs)

    def select_whereclause(self, whereclause=None, params=None, **kwargs):
        """given a WHERE criterion, create a SELECT statement, execute and return the resulting instances."""
        statement = self.compile(whereclause, **kwargs)
        return self._select_statement(statement, params=params)

    def count(self, whereclause=None, params=None, **kwargs):
        """given a WHERE criterion, create a SELECT COUNT statement, execute and return the resulting count value."""

        from_obj = kwargs.pop('from_obj', [])
        alltables = []
        for l in [sql_util.TableFinder(x) for x in from_obj]:
            alltables += l
            
        if self.table not in alltables:
            from_obj.append(self.table)

        if self._nestable(**kwargs):
            s = sql.select([self.table], whereclause, **kwargs).alias('getcount').count()
        else:
            primary_key = self.primary_key_columns
            s = sql.select([sql.func.count(list(primary_key)[0])], whereclause, from_obj=from_obj, **kwargs)
        return self.session.scalar(self.mapper, s, params=params)

    def select_statement(self, statement, **params):
        """given a ClauseElement-based statement, execute and return the resulting instances."""
        return self._select_statement(statement, params=params)

    def select_text(self, text, **params):
        """given a literal string-based statement, execute and return the resulting instances."""
        t = sql.text(text)
        return self.execute(t, params=params)

    def options(self, *args, **kwargs):
        """return a new Query object, applying the given list of MapperOptions."""
        return Query(self.mapper, self._session, with_options=args)
    
    def with_lockmode(self, mode):
        """return a new Query object with the specified locking mode."""
        return Query(self.mapper, self._session, lockmode=mode)
        
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

    def execute(self, clauseelement, params=None, *args, **kwargs):
        """execute the given ClauseElement-based statement against this Query's session/mapper, return the resulting list of instances.
        
        After execution, closes the ResultProxy and its underlying resources.  
        This method is one step above the instances() method, which takes the executed statement's ResultProxy directly."""
        result = self.session.execute(self.mapper, clauseelement, params=params)
        try:
            return self.instances(result, **kwargs)
        finally:
            result.close()

    def instances(self, cursor, *mappers, **kwargs):
        """return a list of mapped instances corresponding to the rows in a given "cursor" (i.e. ResultProxy)."""
        self.__log_debug("instances()")

        session = self.session
        
        context = SelectionContext(self.select_mapper, session, self.extension, with_options=self.with_options, **kwargs)

        result = util.UniqueAppender([])
        if mappers:
            otherresults = []
            for m in mappers:
                otherresults.append(util.UniqueAppender([]))

        for row in cursor.fetchall():
            self.select_mapper._instance(context, row, result)
            i = 0
            for m in mappers:
                m._instance(context, row, otherresults[i])
                i+=1

        # store new stuff in the identity map
        for value in context.identity_map.values():
            session._register_persistent(value)

        if mappers:
            return [result.data] + [o.data for o in otherresults]
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
        i = 0
        params = {}
        for primary_key in self.primary_key_columns:
            params[primary_key._label] = ident[i]
            # if there are not enough elements in the given identifier, then 
            # use the previous identifier repeatedly.  this is a workaround for the issue 
            # in [ticket:185], where a mapper that uses joined table inheritance needs to specify
            # all primary keys of the joined relationship, which includes even if the join is joining
            # two primary key (and therefore synonymous) columns together, the usual case for joined table inheritance.
            if len(ident) > i + 1:
                i += 1
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
        """return True if the given statement options indicate that we should "nest" the
        generated query as a subquery inside of a larger eager-loading query.  this is used
        with keywords like distinct, limit and offset and the mapper defines eager loads."""
        return (
            len(querycontext.eager_loaders) > 0
            and self._nestable(**querycontext.select_args())
        )

    def _nestable(self, **kwargs):
        """return true if the given statement options imply it should be nested."""
        return (kwargs.get('limit') is not None or kwargs.get('offset') is not None or kwargs.get('distinct', False))
        
    def compile(self, whereclause = None, **kwargs):
        """given a WHERE criterion, produce a ClauseElement-based statement suitable for usage in the execute() method."""
        
        if whereclause is not None and self.is_polymorphic:
            # adapt the given WHERECLAUSE to adjust instances of this query's mapped table to be that of our select_table,
            # which may be the "polymorphic" selectable used by our mapper.
            whereclause.accept_visitor(sql_util.ClauseAdapter(self.table))
        
        context = kwargs.pop('query_context', None)
        if context is None:
            context = QueryContext(self, kwargs)
        order_by = context.order_by
        from_obj = context.from_obj
        lockmode = context.lockmode
        distinct = context.distinct
        limit = context.limit
        offset = context.offset
        if order_by is False:
            order_by = self.order_by
        if order_by is False:
            if self.table.default_order_by() is not None:
                order_by = self.table.default_order_by()

        try:
            for_update = {'read':'read','update':True,'update_nowait':'nowait',None:False}[lockmode]
        except KeyError:
            raise exceptions.ArgumentError("Unknown lockmode '%s'" % lockmode)
        
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
                    o.accept_visitor(cf)
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
        
        return statement

    def __log_debug(self, msg):
        self.logger.debug(msg)

Query.logger = logging.class_logger(Query)

class QueryContext(OperationContext):
    """created within the Query.compile() method to store and share
    state among all the Mappers and MapperProperty objects used in a query construction."""
    def __init__(self, query, kwargs):
        self.query = query
        self.order_by = kwargs.pop('order_by', False)
        self.from_obj = kwargs.pop('from_obj', [])
        self.lockmode = kwargs.pop('lockmode', query.lockmode)
        self.distinct = kwargs.pop('distinct', False)
        self.limit = kwargs.pop('limit', None)
        self.offset = kwargs.pop('offset', None)
        self.eager_loaders = util.Set([x for x in query.mapper._eager_loaders])
        self.statement = None
        super(QueryContext, self).__init__(query.mapper, query.with_options, **kwargs)
    def select_args(self):
        """return a dictionary of attributes from this QueryContext that can be applied to a sql.Select statement."""
        return {'limit':self.limit, 'offset':self.offset, 'distinct':self.distinct}
    def accept_option(self, opt):
        """accept a MapperOption which will process (modify) the state of this QueryContext."""
        opt.process_query_context(self)


class SelectionContext(OperationContext):
    """created within the query.instances() method to store and share
    state among all the Mappers and MapperProperty objects used in a load operation.

    SelectionContext contains these attributes:

    mapper - the Mapper which originated the instances() call.

    session - the Session that is relevant to the instances call.

    identity_map - a dictionary which stores newly created instances that have
    not yet been added as persistent to the Session.

    attributes - a dictionary to store arbitrary data; eager loaders use it to
    store additional result lists

    populate_existing - indicates if its OK to overwrite the attributes of instances
    that were already in the Session

    version_check - indicates if mappers that have version_id columns should verify
    that instances existing already within the Session should have this attribute compared
    to the freshly loaded value

    """
    def __init__(self, mapper, session, extension, **kwargs):
        self.populate_existing = kwargs.pop('populate_existing', False)
        self.version_check = kwargs.pop('version_check', False)
        self.session = session
        self.extension = extension
        self.identity_map = {}
        super(SelectionContext, self).__init__(mapper, kwargs.pop('with_options', []), **kwargs)
    def accept_option(self, opt):
        """accept a MapperOption which will process (modify) the state of this SelectionContext."""
        opt.process_selection_context(self)
        
