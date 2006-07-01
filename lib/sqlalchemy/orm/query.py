# orm/query.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import session as sessionlib
from sqlalchemy import sql, util, exceptions, sql_util

import mapper


class Query(object):
    """encapsulates the object-fetching operations provided by Mappers."""
    def __init__(self, class_or_mapper, session=None, entity_name=None, **kwargs):
        if isinstance(class_or_mapper, type):
            self.mapper = mapper.class_mapper(class_or_mapper, entity_name=entity_name)
        else:
            self.mapper = class_or_mapper.compile()
        self.mapper = self.mapper.get_select_mapper()

        self.always_refresh = kwargs.pop('always_refresh', self.mapper.always_refresh)
        self.order_by = kwargs.pop('order_by', self.mapper.order_by)
        self.extension = kwargs.pop('extension', self.mapper.extension)
        self._session = session
        if not hasattr(self.mapper, '_get_clause'):
            _get_clause = sql.and_()
            for primary_key in self.mapper.pks_by_table[self.table]:
                _get_clause.clauses.append(primary_key == sql.bindparam("pk_"+primary_key._label, type=primary_key.type))
            self.mapper._get_clause = _get_clause
        self._get_clause = self.mapper._get_clause
    def _get_session(self):
        if self._session is None:
            return self.mapper.get_session()
        else:
            return self._session
    table = property(lambda s:s.mapper.select_table)
    session = property(_get_session)
    
    def get(self, ident, **kwargs):
        """returns an instance of the object based on the given identifier, or None
        if not found.  The ident argument is a scalar or tuple of primary key column values
        in the order of the table def's primary key columns."""
        key = self.mapper.identity_key(ident)
        return self._get(key, ident, **kwargs)

    def load(self, ident, **kwargs):
        """returns an instance of the object based on the given identifier. If not found,
        raises an exception.  The method will *remove all pending changes* to the object
        already existing in the Session.  The ident argument is a scalar or tuple of primary
        key column values in the order of the table def's primary key columns."""
        key = self.mapper.identity_key(ident)
        instance = self._get(key, ident, reload=True, **kwargs)
        if instance is None:
            raise exceptions.InvalidRequestError("No instance found for identity %s" % repr(ident))
        return instance
        
    def get_by(self, *args, **params):
        """returns a single object instance based on the given key/value criterion. 
        this is either the first value in the result list, or None if the list is 
        empty.

        the keys are mapped to property or column names mapped by this mapper's Table, and the values
        are coerced into a WHERE clause separated by AND operators.  If the local property/column
        names dont contain the key, a search will be performed against this mapper's immediate
        list of relations as well, forming the appropriate join conditions if a matching property
        is located.

        e.g.   u = usermapper.get_by(user_name = 'fred')
        """
        x = self.select_whereclause(self.join_by(*args, **params), limit=1)
        if x:
            return x[0]
        else:
            return None

    def select_by(self, *args, **params):
        """returns an array of object instances based on the given clauses and key/value criterion. 

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
        """like select_by, but returns a ClauseElement representing the WHERE clause that would normally
        be sent to select_whereclause by select_by."""
        clause = None
        for arg in args:
            if clause is None:
                clause = arg
            else:
                clause &= arg

        for key, value in params.iteritems():
            (keys, prop) = self._locate_prop(key)
            c = (prop.columns[0]==value) & self.join_via(keys)
            if clause is None:
                clause =  c
            else:                
                clause &= c
        return clause

    def _locate_prop(self, key):
        import properties
        keys = []
        seen = util.Set()
        def search_for_prop(mapper):
            if mapper in seen:
                return None
            seen.add(mapper)
            if mapper.props.has_key(key):
                prop = mapper.props[key]
                if isinstance(prop, properties.PropertyLoader):
                    keys.insert(0, prop.key)
                return prop
            else:
                for prop in mapper.props.values():
                    if not isinstance(prop, properties.PropertyLoader):
                        continue
                    x = search_for_prop(prop.mapper)
                    if x:
                        keys.insert(0, prop.key)
                        return x
                else:
                    return None
        p = search_for_prop(self.mapper)
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
                clause = prop.get_join()
            else:
                clause &= prop.get_join()
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
        in this case, the developer must insure that an adequate set of columns exists in the 
        rowset with which to build new object instances."""

        ret = self.extension.select(self, arg=arg, **kwargs)
        if ret is not mapper.EXT_PASS:
            return ret
        elif arg is not None and isinstance(arg, sql.Selectable):
            return self.select_statement(arg, **kwargs)
        else:
            return self.select_whereclause(whereclause=arg, **kwargs)

    def select_whereclause(self, whereclause=None, params=None, **kwargs):
        statement = self.compile(whereclause, **kwargs)
        return self._select_statement(statement, params=params)

    def count(self, whereclause=None, params=None, **kwargs):
        s = self.table.count(whereclause)
        return self.session.scalar(self.mapper, s, params=params)

    def select_statement(self, statement, **params):
        return self._select_statement(statement, params=params)

    def select_text(self, text, **params):
        t = sql.text(text)
        return self.instances(t, params=params)

    def options(self, *args, **kwargs):
        """returns a new Query object using the given MapperOptions."""
        return self.mapper.options(*args, **kwargs).using(session=self._session)

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

    def instances(self, clauseelement, params=None, *args, **kwargs):
        result = self.session.execute(self.mapper, clauseelement, params=params)
        try:
            return self.mapper.instances(result, self.session, **kwargs)
        finally:
            result.close()
        
    def _get(self, key, ident=None, reload=False):
        if not reload and not self.always_refresh:
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
        for primary_key in self.mapper.pks_by_table[self.table]:
            params["pk_"+primary_key._label] = ident[i]
            # if there are not enough elements in the given identifier, then 
            # use the previous identifier repeatedly.  this is a workaround for the issue 
            # in [ticket:185], where a mapper that uses joined table inheritance needs to specify
            # all primary keys of the joined relationship, which includes even if the join is joining
            # two primary key (and therefore synonymous) columns together, the usual case for joined table inheritance.
            if len(ident) > i + 1:
                i += 1
        try:
            statement = self.compile(self._get_clause)
            return self._select_statement(statement, params=params, populate_existing=reload)[0]
        except IndexError:
            return None

    def _select_statement(self, statement, params=None, **kwargs):
        statement.use_labels = True
        if params is None:
            params = {}
        return self.instances(statement, params=params, **kwargs)

    def _should_nest(self, **kwargs):
        """returns True if the given statement options indicate that we should "nest" the
        generated query as a subquery inside of a larger eager-loading query.  this is used
        with keywords like distinct, limit and offset and the mapper defines eager loads."""
        return (
            self.mapper.has_eager()
            and (kwargs.has_key('limit') or kwargs.has_key('offset') or kwargs.get('distinct', False))
        )

    def compile(self, whereclause = None, **kwargs):
        order_by = kwargs.pop('order_by', False)
        from_obj = kwargs.pop('from_obj', [])
        if order_by is False:
            order_by = self.order_by
        if order_by is False:
            if self.table.default_order_by() is not None:
                order_by = self.table.default_order_by()

        if self._should_nest(**kwargs):
            from_obj.append(self.table)
            
            # if theres an order by, add those columns to the column list
            # of the "rowcount" query we're going to make
            if order_by:
                order_by = util.to_list(order_by) or []
                cf = sql_util.ColumnFinder()
                [o.accept_visitor(cf) for o in order_by]
            else:
                cf = []
                
            s2 = sql.select(self.table.primary_key + list(cf), whereclause, use_labels=True, from_obj=from_obj, **kwargs)
#            raise "ok first thing", str(s2)
            if not kwargs.get('distinct', False) and order_by:
                s2.order_by(*util.to_list(order_by))
            s3 = s2.alias('tbl_row_count')
            crit = []
            for i in range(0, len(self.table.primary_key)):
                crit.append(s3.primary_key[i] == self.table.primary_key[i])
            statement = sql.select([], sql.and_(*crit), from_obj=[self.table], use_labels=True)
 #           raise "OK statement", str(statement)
 
            # now for the order by, convert the columns to their corresponding columns
            # in the "rowcount" query, and tack that new order by onto the "rowcount" query
            if order_by:
                class Aliasizer(sql_util.Aliasizer):
                    def get_alias(self, table):
                        return s3
                order_by = [o.copy_container() for o in order_by]
                aliasizer = Aliasizer(*[t for t in sql_util.TableFinder(s3)])
                [o.accept_visitor(aliasizer) for  o in order_by]
                statement.order_by(*util.to_list(order_by))
        else:
            from_obj.append(self.table)
            statement = sql.select([], whereclause, from_obj=from_obj, use_labels=True, **kwargs)
            if order_by:
                statement.order_by(*util.to_list(order_by))
            # for a DISTINCT query, you need the columns explicitly specified in order
            # to use it in "order_by".  insure they are in the column criterion (particularly oid).
            # TODO: this should be done at the SQL level not the mapper level
            if kwargs.get('distinct', False) and order_by:
                statement.append_column(*util.to_list(order_by))
        # plugin point

        # give all the attached properties a chance to modify the query
        for key, value in self.mapper.props.iteritems():
            value.setup(key, statement, **kwargs) 
        return statement

