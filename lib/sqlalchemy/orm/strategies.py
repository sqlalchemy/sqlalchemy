# strategies.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""sqlalchemy.orm.interfaces.LoaderStrategy implementations, and related MapperOptions."""

from sqlalchemy import sql, util, exceptions, logging
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors, expression, operators
from sqlalchemy.orm import mapper, attributes
from sqlalchemy.orm.interfaces import LoaderStrategy, StrategizedOption, MapperOption, PropertyOption, serialize_path, deserialize_path
from sqlalchemy.orm import session as sessionlib
from sqlalchemy.orm import util as mapperutil


class ColumnLoader(LoaderStrategy):
    """Default column loader."""
    
    def init(self):
        super(ColumnLoader, self).init()
        self.columns = self.parent_property.columns
        self._should_log_debug = logging.is_debug_enabled(self.logger)
        self.is_composite = hasattr(self.parent_property, 'composite_class')
        
    def setup_query(self, context, parentclauses=None, **kwargs):
        for c in self.columns:
            if parentclauses is not None:
                context.secondary_columns.append(parentclauses.aliased_column(c))
            else:
                context.primary_columns.append(c)
        
    def init_class_attribute(self):
        self.is_class_level = True
        if self.is_composite:
            self._init_composite_attribute()
        else:
            self._init_scalar_attribute()

    def _init_composite_attribute(self):
        self.logger.info("register managed composite attribute %s on class %s" % (self.key, self.parent.class_.__name__))
        def copy(obj):
            return self.parent_property.composite_class(
                *obj.__composite_values__())
        def compare(a, b):
            for col, aprop, bprop in zip(self.columns,
                                         a.__composite_values__(),
                                         b.__composite_values__()):
                if not col.type.compare_values(aprop, bprop):
                    return False
            else:
                return True
        sessionlib.register_attribute(self.parent.class_, self.key, uselist=False, useobject=False, copy_function=copy, compare_function=compare, mutable_scalars=True, comparator=self.parent_property.comparator)

    def _init_scalar_attribute(self):
        self.logger.info("register managed attribute %s on class %s" % (self.key, self.parent.class_.__name__))
        coltype = self.columns[0].type
        sessionlib.register_attribute(self.parent.class_, self.key, uselist=False, useobject=False, copy_function=coltype.copy_value, compare_function=coltype.compare_values, mutable_scalars=self.columns[0].type.is_mutable(), comparator=self.parent_property.comparator)
        
    def create_row_processor(self, selectcontext, mapper, row):
        if self.is_composite:
            for c in self.columns:
                if c not in row:
                    break
            else:
                def new_execute(instance, row, **flags):
                    if self._should_log_debug:
                        self.logger.debug("populating %s with %s/%s..." % (mapperutil.attribute_str(instance, self.key), row.__class__.__name__, self.columns[0].key))
                    instance.__dict__[self.key] = self.parent_property.composite_class(*[row[c] for c in self.columns])
                if self._should_log_debug:
                    self.logger.debug("Returning active composite column fetcher for %s %s" % (mapper, self.key))
                return (new_execute, None, None)
                
        elif self.columns[0] in row:
            def new_execute(instance, row, **flags):
                if self._should_log_debug:
                    self.logger.debug("populating %s with %s/%s" % (mapperutil.attribute_str(instance, self.key), row.__class__.__name__, self.columns[0].key))
                instance.__dict__[self.key] = row[self.columns[0]]
            if self._should_log_debug:
                self.logger.debug("Returning active column fetcher for %s %s" % (mapper, self.key))
            return (new_execute, None, None)
        else:
            def new_execute(instance, row, isnew, **flags):
                if isnew:
                    instance._state.expire_attributes([self.key])
            if self._should_log_debug:
                self.logger.debug("Deferring load for %s %s" % (mapper, self.key))
            return (new_execute, None, None)

ColumnLoader.logger = logging.class_logger(ColumnLoader)

class DeferredColumnLoader(LoaderStrategy):
    """Deferred column loader, a per-column or per-column-group lazy loader."""
    
    def create_row_processor(self, selectcontext, mapper, row):
        if self.columns[0] in row:
            return self.parent_property._get_strategy(ColumnLoader).create_row_processor(selectcontext, mapper, row)
        elif not self.is_class_level or len(selectcontext.options):
            def new_execute(instance, row, **flags):
                if self._should_log_debug:
                    self.logger.debug("set deferred callable on %s" % mapperutil.attribute_str(instance, self.key))
                instance._state.set_callable(self.key, self.setup_loader(instance))
            return (new_execute, None, None)
        else:
            def new_execute(instance, row, **flags):
                if self._should_log_debug:
                    self.logger.debug("set deferred callable on %s" % mapperutil.attribute_str(instance, self.key))
                instance._state.reset(self.key)
            return (new_execute, None, None)

    def init(self):
        super(DeferredColumnLoader, self).init()
        if hasattr(self.parent_property, 'composite_class'):
            raise NotImplementedError("Deferred loading for composite types not implemented yet")
        self.columns = self.parent_property.columns
        self.group = self.parent_property.group
        self._should_log_debug = logging.is_debug_enabled(self.logger)

    def init_class_attribute(self):
        self.is_class_level = True
        self.logger.info("register managed attribute %s on class %s" % (self.key, self.parent.class_.__name__))
        sessionlib.register_attribute(self.parent.class_, self.key, uselist=False, useobject=False, callable_=self.class_level_loader, copy_function=self.columns[0].type.copy_value, compare_function=self.columns[0].type.compare_values, mutable_scalars=self.columns[0].type.is_mutable(), comparator=self.parent_property.comparator)

    def setup_query(self, context, only_load_props=None, **kwargs):
        if \
            (self.group is not None and context.attributes.get(('undefer', self.group), False)) or \
            (only_load_props and self.key in only_load_props):
            
            self.parent_property._get_strategy(ColumnLoader).setup_query(context, **kwargs)
    
    def class_level_loader(self, instance, props=None):
        if not mapper.has_mapper(instance):
            return None
            
        localparent = mapper.object_mapper(instance)

        # adjust for the ColumnProperty associated with the instance
        # not being our own ColumnProperty.  This can occur when entity_name
        # mappers are used to map different versions of the same ColumnProperty
        # to the class.
        prop = localparent.get_property(self.key)
        if prop is not self.parent_property:
            return prop._get_strategy(DeferredColumnLoader).setup_loader(instance)

        return LoadDeferredColumns(instance, self.key, props)
        
    def setup_loader(self, instance, props=None, create_statement=None):
        return LoadDeferredColumns(instance, self.key, props, optimizing_statement=create_statement)
                
DeferredColumnLoader.logger = logging.class_logger(DeferredColumnLoader)

class LoadDeferredColumns(object):
    """callable, serializable loader object used by DeferredColumnLoader"""
    
    def __init__(self, instance, key, keys, optimizing_statement=None):
        self.instance = instance
        self.key = key
        self.keys = keys
        self.optimizing_statement = optimizing_statement

    def __getstate__(self):
        return {'instance':self.instance, 'key':self.key, 'keys':self.keys}
    
    def __setstate__(self, state):
        self.instance = state['instance']
        self.key = state['key']
        self.keys = state['keys']
        self.optimizing_statement = None
        
    def __call__(self):
        if not mapper.has_identity(self.instance):
            return None
            
        localparent = mapper.object_mapper(self.instance, raiseerror=False)
        
        prop = localparent.get_property(self.key)
        strategy = prop._get_strategy(DeferredColumnLoader)

        if self.keys:
            toload = self.keys
        elif strategy.group:
            toload = [p.key for p in localparent.iterate_properties if isinstance(p.strategy, DeferredColumnLoader) and p.group==strategy.group]
        else:
            toload = [self.key]

        # narrow the keys down to just those which have no history
        group = [k for k in toload if k in self.instance._state.unmodified]

        if strategy._should_log_debug:
            strategy.logger.debug("deferred load %s group %s" % (mapperutil.attribute_str(self.instance, self.key), group and ','.join(group) or 'None'))

        session = sessionlib.object_session(self.instance)
        if session is None:
            raise exceptions.UnboundExecutionError("Parent instance %s is not bound to a Session; deferred load operation of attribute '%s' cannot proceed" % (self.instance.__class__, self.key))

        query = session.query(localparent)
        if not self.optimizing_statement:
            ident = self.instance._instance_key[1]
            query._get(None, ident=ident, only_load_props=group, refresh_instance=self.instance._state)
        else:
            statement, params = self.optimizing_statement(self.instance)
            query.from_statement(statement).params(params)._get(None, only_load_props=group, refresh_instance=self.instance._state)
        return attributes.ATTR_WAS_SET

class DeferredOption(StrategizedOption):
    def __init__(self, key, defer=False):
        super(DeferredOption, self).__init__(key)
        self.defer = defer

    def get_strategy_class(self):
        if self.defer:
            return DeferredColumnLoader
        else:
            return ColumnLoader

class UndeferGroupOption(MapperOption):
    def __init__(self, group):
        self.group = group
    def process_query(self, query):
        query._attributes[('undefer', self.group)] = True

class AbstractRelationLoader(LoaderStrategy):
    def init(self):
        super(AbstractRelationLoader, self).init()
        for attr in ['primaryjoin', 'secondaryjoin', 'secondary', 'foreign_keys', 'mapper', 'target', 'table', 'uselist', 'cascade', 'attributeext', 'order_by', 'remote_side', 'direction']:
            setattr(self, attr, getattr(self.parent_property, attr))
        self._should_log_debug = logging.is_debug_enabled(self.logger)
        
    def _init_instance_attribute(self, instance, callable_=None):
        if callable_:
            instance._state.set_callable(self.key, callable_)
        else:
            instance._state.initialize(self.key)
        
    def _register_attribute(self, class_, callable_=None, **kwargs):
        self.logger.info("register managed %s attribute %s on class %s" % ((self.uselist and "list-holding" or "scalar"), self.key, self.parent.class_.__name__))
        sessionlib.register_attribute(class_, self.key, uselist=self.uselist, useobject=True, extension=self.attributeext, cascade=self.cascade,  trackparent=True, typecallable=self.parent_property.collection_class, callable_=callable_, comparator=self.parent_property.comparator, **kwargs)

class NoLoader(AbstractRelationLoader):
    def init_class_attribute(self):
        self.is_class_level = True
        self._register_attribute(self.parent.class_)

    def create_row_processor(self, selectcontext, mapper, row):
        def new_execute(instance, row, ispostselect, **flags):
            if not ispostselect:
                if self._should_log_debug:
                    self.logger.debug("initializing blank scalar/collection on %s" % mapperutil.attribute_str(instance, self.key))
                self._init_instance_attribute(instance)
        return (new_execute, None, None)

NoLoader.logger = logging.class_logger(NoLoader)
        
class LazyLoader(AbstractRelationLoader):
    def init(self):
        super(LazyLoader, self).init()
        (self.__lazywhere, self.__bind_to_col, self._equated_columns) = self.__create_lazy_clause(self.parent_property)
        
        self.logger.info(str(self.parent_property) + " lazy loading clause " + str(self.__lazywhere))

        # determine if our "lazywhere" clause is the same as the mapper's
        # get() clause.  then we can just use mapper.get()
        #from sqlalchemy.orm import query
        self.use_get = not self.uselist and self.mapper._get_clause[0].compare(self.__lazywhere)
        if self.use_get:
            self.logger.info(str(self.parent_property) + " will use query.get() to optimize instance loads")

    def init_class_attribute(self):
        self.is_class_level = True
        self._register_attribute(self.parent.class_, callable_=self.class_level_loader)

    def lazy_clause(self, instance, reverse_direction=False, alias_secondary=False):
        if instance is None:
            return self._lazy_none_clause(reverse_direction)
            
        if not reverse_direction:
            (criterion, bind_to_col, rev) = (self.__lazywhere, self.__bind_to_col, self._equated_columns)
        else:
            (criterion, bind_to_col, rev) = LazyLoader.__create_lazy_clause(self.parent_property, reverse_direction=reverse_direction)

        def visit_bindparam(bindparam):
            mapper = reverse_direction and self.parent_property.mapper or self.parent_property.parent
            if bindparam.key in bind_to_col:
                # use the "committed" (database) version to get query column values
                # also its a deferred value; so that when used by Query, the committed value is used
                # after an autoflush occurs
                bindparam.value = lambda: mapper._get_committed_attr_by_column(instance, bind_to_col[bindparam.key])
                
        if self.secondary and alias_secondary:
            criterion = sql_util.ClauseAdapter(self.secondary.alias()).traverse(criterion)
            
        return visitors.traverse(criterion, clone=True, visit_bindparam=visit_bindparam)
    
    def _lazy_none_clause(self, reverse_direction=False):
        if not reverse_direction:
            (criterion, bind_to_col, rev) = (self.__lazywhere, self.__bind_to_col, self._equated_columns)
        else:
            (criterion, bind_to_col, rev) = LazyLoader.__create_lazy_clause(self.parent_property, reverse_direction=reverse_direction)

        def visit_binary(binary):
            mapper = reverse_direction and self.parent_property.mapper or self.parent_property.parent
            if isinstance(binary.left, expression._BindParamClause) and binary.left.key in bind_to_col:
                # reverse order if the NULL is on the left side
                binary.left = binary.right
                binary.right = expression.null()
                binary.operator = operators.is_
            elif isinstance(binary.right, expression._BindParamClause) and binary.right.key in bind_to_col:
                binary.right = expression.null()
                binary.operator = operators.is_
        
        return visitors.traverse(criterion, clone=True, visit_binary=visit_binary)
        
    def class_level_loader(self, instance, options=None, path=None):
        if not mapper.has_mapper(instance):
            return None

        localparent = mapper.object_mapper(instance)

        # adjust for the PropertyLoader associated with the instance
        # not being our own PropertyLoader.  This can occur when entity_name
        # mappers are used to map different versions of the same PropertyLoader
        # to the class.
        prop = localparent.get_property(self.key)
        if prop is not self.parent_property:
            return prop._get_strategy(LazyLoader).setup_loader(instance)
        
        return LoadLazyAttribute(instance, self.key, options, path)

    def setup_loader(self, instance, options=None, path=None):
        return LoadLazyAttribute(instance, self.key, options, path)

    def create_row_processor(self, selectcontext, mapper, row):
        if not self.is_class_level or len(selectcontext.options):
            def new_execute(instance, row, ispostselect, **flags):
                if not ispostselect:
                    if self._should_log_debug:
                        self.logger.debug("set instance-level lazy loader on %s" % mapperutil.attribute_str(instance, self.key))
                    # we are not the primary manager for this attribute on this class - set up a per-instance lazyloader,
                    # which will override the class-level behavior
                    
                    self._init_instance_attribute(instance, callable_=self.setup_loader(instance, selectcontext.options, selectcontext.query._current_path + selectcontext.path))
            return (new_execute, None, None)
        else:
            def new_execute(instance, row, ispostselect, **flags):
                if not ispostselect:
                    if self._should_log_debug:
                        self.logger.debug("set class-level lazy loader on %s" % mapperutil.attribute_str(instance, self.key))
                    # we are the primary manager for this attribute on this class - reset its per-instance attribute state, 
                    # so that the class-level lazy loader is executed when next referenced on this instance.
                    # this usually is not needed unless the constructor of the object referenced the attribute before we got 
                    # to load data into it.
                    instance._state.reset(self.key)
            return (new_execute, None, None)

    def __create_lazy_clause(cls, prop, reverse_direction=False):
        binds = {}
        lookup = {}
        equated_columns = {}

        if reverse_direction and not prop.secondaryjoin:
            for l, r in prop.local_remote_pairs:
                _list = lookup.setdefault(r, [])
                _list.append((r, l))
                equated_columns[l] = r
        else:
            for l, r in prop.local_remote_pairs:
                _list = lookup.setdefault(l, [])
                _list.append((l, r))
                equated_columns[r] = l
                
        def col_to_bind(col):
            if col in lookup:
                for tobind, equated in lookup[col]:
                    if equated in binds:
                        return None
                if col not in binds:
                    binds[col] = sql.bindparam(None, None, type_=col.type)
                return binds[col]
            return None
                    
        lazywhere = prop.primaryjoin
        
        if not prop.secondaryjoin or not reverse_direction:
            lazywhere = visitors.traverse(lazywhere, before_clone=col_to_bind, clone=True) 
        
        if prop.secondaryjoin is not None:
            secondaryjoin = prop.secondaryjoin
            if reverse_direction:
                secondaryjoin = visitors.traverse(secondaryjoin, before_clone=col_to_bind, clone=True)
            lazywhere = sql.and_(lazywhere, secondaryjoin)
    
        bind_to_col = dict([(binds[col].key, col) for col in binds])
        
        return (lazywhere, bind_to_col, equated_columns)
    __create_lazy_clause = classmethod(__create_lazy_clause)
    
LazyLoader.logger = logging.class_logger(LazyLoader)

class LoadLazyAttribute(object):
    """callable, serializable loader object used by LazyLoader"""

    def __init__(self, instance, key, options, path):
        self.instance = instance
        self.key = key
        self.options = options
        self.path = path
        
    def __getstate__(self):
        return {'instance':self.instance, 'key':self.key, 'options':self.options, 'path':serialize_path(self.path)}

    def __setstate__(self, state):
        self.instance = state['instance']
        self.key = state['key']
        self.options= state['options']
        self.path = deserialize_path(state['path'])
        
    def __call__(self):
        instance = self.instance
        
        if not mapper.has_identity(instance):
            return None

        instance_mapper = mapper.object_mapper(instance)
        prop = instance_mapper.get_property(self.key)
        strategy = prop._get_strategy(LazyLoader)
        
        if strategy._should_log_debug:
            strategy.logger.debug("lazy load attribute %s on instance %s" % (self.key, mapperutil.instance_str(instance)))

        session = sessionlib.object_session(instance)
        if session is None:
            try:
                session = instance_mapper.get_session()
            except exceptions.InvalidRequestError:
                raise exceptions.UnboundExecutionError("Parent instance %s is not bound to a Session, and no contextual session is established; lazy load operation of attribute '%s' cannot proceed" % (instance.__class__, self.key))

        q = session.query(prop.mapper).autoflush(False)
        if self.path:
            q = q._with_current_path(self.path)
            
        # if we have a simple primary key load, use mapper.get()
        # to possibly save a DB round trip
        if strategy.use_get:
            ident = []
            allnulls = True
            for primary_key in prop.mapper.primary_key: 
                val = instance_mapper._get_committed_attr_by_column(instance, strategy._equated_columns[primary_key])
                allnulls = allnulls and val is None
                ident.append(val)
            if allnulls:
                return None
            if self.options:
                q = q._conditional_options(*self.options)
            return q.get(ident)
            
        if strategy.order_by is not False:
            q = q.order_by(strategy.order_by)
        elif strategy.secondary is not None and strategy.secondary.default_order_by() is not None:
            q = q.order_by(strategy.secondary.default_order_by())

        if self.options:
            q = q._conditional_options(*self.options)
        q = q.filter(strategy.lazy_clause(instance))

        result = q.all()
        if strategy.uselist:
            return result
        else:
            if result:
                return result[0]
            else:
                return None
        

class EagerLoader(AbstractRelationLoader):
    """Loads related objects inline with a parent query."""
    
    def init(self):
        super(EagerLoader, self).init()
        self.clauses = {}
        self.join_depth = self.parent_property.join_depth

    def init_class_attribute(self):
        # class-level eager strategy; add the PropertyLoader
        # to the parent's list of "eager loaders"; this tells the Query
        # that eager loaders will be used in a normal query
        self.parent._eager_loaders.add(self.parent_property)
        
        # initialize a lazy loader on the class level attribute
        self.parent_property._get_strategy(LazyLoader).init_class_attribute()
        
    def setup_query(self, context, parentclauses=None, parentmapper=None, **kwargs):
        """Add a left outer join to the statement thats being constructed."""
        
        path = context.path
        
        # check for join_depth or basic recursion,
        # if the current path was not explicitly stated as 
        # a desired "loaderstrategy" (i.e. via query.options())
        if ("loaderstrategy", path) not in context.attributes:
            if self.join_depth:
                if len(path) / 2 > self.join_depth:
                    return
            else:
                if self.mapper.base_mapper in path:
                    return

        if ("eager_row_processor", path) in context.attributes:
            # if user defined eager_row_processor, that's contains_eager().
            # don't render LEFT OUTER JOIN, generate an AliasedClauses from 
            # the decorator (this is a hack here, cleaned up in 0.5)
            cl = context.attributes[("eager_row_processor", path)]
            if cl:
                row = cl(None)
                class ActsLikeAliasedClauses(object):
                    def aliased_column(self, col):
                        return row.map[col]
                clauses = ActsLikeAliasedClauses()
            else:
                clauses = None
        else:
            clauses = self.__create_eager_join(context, path, parentclauses, parentmapper, **kwargs)
            if not clauses:
                return

        for value in self.mapper._iterate_polymorphic_properties():
            context.exec_with_path(self.mapper, value.key, value.setup, context, parentclauses=clauses, parentmapper=self.mapper)

    def __create_eager_join(self, context, path, parentclauses, parentmapper, **kwargs):
        if parentmapper is None:
            localparent = context.mapper
        else:
            localparent = parentmapper
        
        if context.eager_joins:
            towrap = context.eager_joins
        else:
            towrap = context.from_clause
        
        # create AliasedClauses object to build up the eager query.  this is cached after 1st creation.    
        try:
            clauses = self.clauses[path]
        except KeyError:
            clauses = mapperutil.PropertyAliasedClauses(self.parent_property, self.parent_property.primaryjoin, self.parent_property.secondaryjoin, parentclauses)
            self.clauses[path] = clauses

        # place the "row_decorator" from the AliasedClauses into the QueryContext, where it will
        # be picked up in create_row_processor() when results are fetched
        context.attributes[("eager_row_processor", path)] = clauses.row_decorator
        
        if self.secondaryjoin is not None:
            context.eager_joins = sql.outerjoin(towrap, clauses.secondary, clauses.primaryjoin).outerjoin(clauses.alias, clauses.secondaryjoin)
            
            # TODO: check for "deferred" cols on parent/child tables here ?  this would only be
            # useful if the primary/secondaryjoin are against non-PK columns on the tables (and therefore might be deferred)
            
            if self.order_by is False and self.secondary.default_order_by() is not None:
                context.eager_order_by += clauses.secondary.default_order_by()
        else:
            context.eager_joins = towrap.outerjoin(clauses.alias, clauses.primaryjoin)
            # ensure all the cols on the parent side are actually in the
            # columns clause (i.e. are not deferred), so that aliasing applied by the Query propagates 
            # those columns outward.  This has the effect of "undefering" those columns.
            for col in sql_util.find_columns(clauses.primaryjoin):
                if localparent.mapped_table.c.contains_column(col):
                    context.primary_columns.append(col)
                
            if self.order_by is False and clauses.alias.default_order_by() is not None:
                context.eager_order_by += clauses.alias.default_order_by()

        if clauses.order_by:
            context.eager_order_by += util.to_list(clauses.order_by)
        
        return clauses
        
    def _create_row_decorator(self, selectcontext, row, path):
        """Create a *row decorating* function that will apply eager
        aliasing to the row.
        
        Also check that an identity key can be retrieved from the row,
        else return None.
        """
        
        #print "creating row decorator for path ", "->".join([str(s) for s in path])
        
        if ("eager_row_processor", path) in selectcontext.attributes:
            decorator = selectcontext.attributes[("eager_row_processor", path)]
            if decorator is None:
                decorator = lambda row: row
        else:
            if self._should_log_debug:
                self.logger.debug("Could not locate aliased clauses for key: " + str(path))
            return None

        try:
            decorated_row = decorator(row)
            # check for identity key
            identity_key = self.mapper.identity_key_from_row(decorated_row)
            # and its good
            return decorator
        except KeyError, k:
            # no identity key - dont return a row processor, will cause a degrade to lazy
            if self._should_log_debug:
                self.logger.debug("could not locate identity key from row '%s'; missing column '%s'" % (repr(decorated_row), str(k)))
            return None

    def create_row_processor(self, selectcontext, mapper, row):

        row_decorator = self._create_row_decorator(selectcontext, row, selectcontext.path)
        pathstr = ','.join([str(x) for x in selectcontext.path])
        if row_decorator is not None:
            def execute(instance, row, isnew, **flags):
                decorated_row = row_decorator(row)

                if not self.uselist:
                    if self._should_log_debug:
                        self.logger.debug("eagerload scalar instance on %s" % mapperutil.attribute_str(instance, self.key))
                    if isnew:
                        # set a scalar object instance directly on the
                        # parent object, bypassing InstrumentedAttribute
                        # event handlers.
                        #
                        instance.__dict__[self.key] = self.mapper._instance(selectcontext, decorated_row, None)
                    else:
                        # call _instance on the row, even though the object has been created,
                        # so that we further descend into properties
                        self.mapper._instance(selectcontext, decorated_row, None)
                else:
                    if isnew or self.key not in instance._state.appenders:
                        # appender_key can be absent from selectcontext.attributes with isnew=False
                        # when self-referential eager loading is used; the same instance may be present
                        # in two distinct sets of result columns
                        
                        if self._should_log_debug:
                            self.logger.debug("initialize UniqueAppender on %s" % mapperutil.attribute_str(instance, self.key))

                        collection = attributes.init_collection(instance, self.key)
                        appender = util.UniqueAppender(collection, 'append_without_event')

                        instance._state.appenders[self.key] = appender
                    
                    result_list = instance._state.appenders[self.key]
                    if self._should_log_debug:
                        self.logger.debug("eagerload list instance on %s" % mapperutil.attribute_str(instance, self.key))
                    
                    self.mapper._instance(selectcontext, decorated_row, result_list)

            if self._should_log_debug:
                self.logger.debug("Returning eager instance loader for %s" % str(self))

            return (execute, execute, None)
        else:
            if self._should_log_debug:
                self.logger.debug("eager loader %s degrading to lazy loader" % str(self))
            return self.parent_property._get_strategy(LazyLoader).create_row_processor(selectcontext, mapper, row)

    def __str__(self):
        return str(self.parent) + "." + self.key
        
EagerLoader.logger = logging.class_logger(EagerLoader)

class EagerLazyOption(StrategizedOption):
    def __init__(self, key, lazy=True, chained=False, mapper=None):
        super(EagerLazyOption, self).__init__(key, mapper)
        self.lazy = lazy
        self.chained = chained
        
    def is_chained(self):
        return not self.lazy and self.chained
        
    def process_query_property(self, query, paths):
        if self.lazy:
            if paths[-1] in query._eager_loaders:
                query._eager_loaders = query._eager_loaders.difference(util.Set([paths[-1]]))
        else:
            if not self.chained:
                paths = [paths[-1]]
            res = util.Set()
            for path in paths:
                if len(path) - len(query._current_path) == 2:
                    res.add(path)
            query._eager_loaders = query._eager_loaders.union(res)
        super(EagerLazyOption, self).process_query_property(query, paths)

    def get_strategy_class(self):
        if self.lazy:
            return LazyLoader
        elif self.lazy is False:
            return EagerLoader
        elif self.lazy is None:
            return NoLoader

EagerLazyOption.logger = logging.class_logger(EagerLazyOption)

class RowDecorateOption(PropertyOption):
    def __init__(self, key, decorator=None, alias=None):
        super(RowDecorateOption, self).__init__(key)
        self.decorator = decorator
        self.alias = alias

    def process_query_property(self, query, paths):
        if self.alias is not None and self.decorator is None:
            (mapper, propname) = paths[-1][-2:]

            prop = mapper.get_property(propname, resolve_synonyms=True)
            if isinstance(self.alias, basestring):
                self.alias = prop.target.alias(self.alias)

            self.decorator = mapperutil.create_row_adapter(self.alias)
        query._attributes[("eager_row_processor", paths[-1])] = self.decorator

RowDecorateOption.logger = logging.class_logger(RowDecorateOption)
        
