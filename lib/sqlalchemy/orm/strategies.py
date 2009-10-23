# strategies.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""sqlalchemy.orm.interfaces.LoaderStrategy implementations, and related MapperOptions."""

import sqlalchemy.exceptions as sa_exc
from sqlalchemy import sql, util, log
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors, expression, operators
from sqlalchemy.orm import mapper, attributes, interfaces
from sqlalchemy.orm.interfaces import (
    LoaderStrategy, StrategizedOption, MapperOption, PropertyOption,
    serialize_path, deserialize_path, StrategizedProperty
    )
from sqlalchemy.orm import session as sessionlib
from sqlalchemy.orm import util as mapperutil

def _register_attribute(strategy, mapper, useobject,
        compare_function=None, 
        typecallable=None,
        copy_function=None, 
        mutable_scalars=False, 
        uselist=False,
        callable_=None, 
        proxy_property=None, 
        active_history=False,
        impl_class=None, 
        **kw        
):

    prop = strategy.parent_property
    attribute_ext = list(util.to_list(prop.extension, default=[]))
        
    if useobject and prop.single_parent:
        attribute_ext.append(_SingleParentValidator(prop))

    if getattr(prop, 'backref', None):
        attribute_ext.append(prop.backref.extension)
    
    if prop.key in prop.parent._validators:
        attribute_ext.append(mapperutil.Validator(prop.key, prop.parent._validators[prop.key]))
    
    if useobject:
        attribute_ext.append(sessionlib.UOWEventHandler(prop.key))
    
    for m in mapper.polymorphic_iterator():
        if prop is m._props.get(prop.key):
            
            attributes.register_attribute_impl(
                m.class_, 
                prop.key, 
                parent_token=prop,
                mutable_scalars=mutable_scalars,
                uselist=uselist, 
                copy_function=copy_function, 
                compare_function=compare_function, 
                useobject=useobject, 
                extension=attribute_ext, 
                trackparent=useobject, 
                typecallable=typecallable,
                callable_=callable_, 
                active_history=active_history,
                impl_class=impl_class,
                **kw
                )

class UninstrumentedColumnLoader(LoaderStrategy):
    """Represent the strategy for a MapperProperty that doesn't instrument the class.
    
    The polymorphic_on argument of mapper() often results in this,
    if the argument is against the with_polymorphic selectable.
    
    """
    def init(self):
        self.columns = self.parent_property.columns

    def setup_query(self, context, entity, path, adapter, column_collection=None, **kwargs):
        for c in self.columns:
            if adapter:
                c = adapter.columns[c]
            column_collection.append(c)

    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        return (None, None)

class ColumnLoader(LoaderStrategy):
    """Strategize the loading of a plain column-based MapperProperty."""
    
    def init(self):
        self.columns = self.parent_property.columns
        self.is_composite = hasattr(self.parent_property, 'composite_class')
        
    def setup_query(self, context, entity, path, adapter, column_collection=None, **kwargs):
        for c in self.columns:
            if adapter:
                c = adapter.columns[c]
            column_collection.append(c)
        
    def init_class_attribute(self, mapper):
        self.is_class_level = True
        coltype = self.columns[0].type
        active_history = self.columns[0].primary_key  # TODO: check all columns ?  check for foreign Key as well?

        _register_attribute(self, mapper, useobject=False,
            compare_function=coltype.compare_values,
            copy_function=coltype.copy_value,
            mutable_scalars=self.columns[0].type.is_mutable(),
            active_history = active_history
       )
        
    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        key, col = self.key, self.columns[0]
        if adapter:
            col = adapter.columns[col]
        if col is not None and col in row:
            def new_execute(state, dict_, row, **flags):
                dict_[key] = row[col]
                
            if self._should_log_debug:
                new_execute = self.debug_callable(new_execute, self.logger,
                    "%s returning active column fetcher" % self,
                    lambda state, dict_, row, **flags: "%s populating %s" % \
                                                      (self,
                                                       mapperutil.state_attribute_str(state, key))
                )
            return (new_execute, None)
        else:
            def new_execute(state, dict_, row, isnew, **flags):
                if isnew:
                    state.expire_attributes([key])
            if self._should_log_debug:
                self.logger.debug("%s deferring load" % self)
            return (new_execute, None)

log.class_logger(ColumnLoader)

class CompositeColumnLoader(ColumnLoader):
    """Strategize the loading of a composite column-based MapperProperty."""

    def init_class_attribute(self, mapper):
        self.is_class_level = True
        self.logger.info("%s register managed composite attribute" % self)

        def copy(obj):
            if obj is None:
                return None
            return self.parent_property.composite_class(*obj.__composite_values__())
            
        def compare(a, b):
            if a is None or b is None:
                return a is b
                
            for col, aprop, bprop in zip(self.columns,
                                         a.__composite_values__(),
                                         b.__composite_values__()):
                if not col.type.compare_values(aprop, bprop):
                    return False
            else:
                return True

        _register_attribute(self, mapper, useobject=False,
            compare_function=compare,
            copy_function=copy,
            mutable_scalars=True
            #active_history ?
        )

    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        key, columns, composite_class = self.key, self.columns, self.parent_property.composite_class
        if adapter:
            columns = [adapter.columns[c] for c in columns]
        for c in columns:
            if c not in row:
                def new_execute(state, dict_, row, isnew, **flags):
                    if isnew:
                        state.expire_attributes([key])
                if self._should_log_debug:
                    self.logger.debug("%s deferring load" % self)
                return (new_execute, None)
        else:
            def new_execute(state, dict_, row, **flags):
                dict_[key] = composite_class(*[row[c] for c in columns])

            if self._should_log_debug:
                new_execute = self.debug_callable(new_execute, self.logger,
                    "%s returning active composite column fetcher" % self,
                    lambda state, dict_, row, **flags: "populating %s" % \
                                                      (mapperutil.state_attribute_str(state, key))
                )

            return (new_execute, None)

log.class_logger(CompositeColumnLoader)
    
class DeferredColumnLoader(LoaderStrategy):
    """Strategize the loading of a deferred column-based MapperProperty."""
    
    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        col = self.columns[0]
        if adapter:
            col = adapter.columns[col]
        if col in row:
            return self.parent_property._get_strategy(ColumnLoader).create_row_processor(selectcontext, path, mapper, row, adapter)

        elif not self.is_class_level:
            def new_execute(state, dict_, row, **flags):
                state.set_callable(self.key, LoadDeferredColumns(state, self.key))
        else:
            def new_execute(state, dict_, row, **flags):
                # reset state on the key so that deferred callables
                # fire off on next access.
                state.reset(self.key, dict_)

        if self._should_log_debug:
            new_execute = self.debug_callable(new_execute, self.logger, None,
                lambda state, dict_, row, **flags: "set deferred callable on %s" % \
                                                  mapperutil.state_attribute_str(state, self.key)
            )
        return (new_execute, None)

    def init(self):
        if hasattr(self.parent_property, 'composite_class'):
            raise NotImplementedError("Deferred loading for composite types not implemented yet")
        self.columns = self.parent_property.columns
        self.group = self.parent_property.group

    def init_class_attribute(self, mapper):
        self.is_class_level = True
    
        _register_attribute(self, mapper, useobject=False,
             compare_function=self.columns[0].type.compare_values,
             copy_function=self.columns[0].type.copy_value,
             mutable_scalars=self.columns[0].type.is_mutable(),
             callable_=self._class_level_loader,
             dont_expire_missing=True
        )

    def setup_query(self, context, entity, path, adapter, only_load_props=None, **kwargs):
        if \
            (self.group is not None and context.attributes.get(('undefer', self.group), False)) or \
            (only_load_props and self.key in only_load_props):
            
            self.parent_property._get_strategy(ColumnLoader).setup_query(context, entity, path, adapter, **kwargs)
    
    def _class_level_loader(self, state):
        if not mapperutil._state_has_identity(state):
            return None
            
        return LoadDeferredColumns(state, self.key)
        
                
log.class_logger(DeferredColumnLoader)

class LoadDeferredColumns(object):
    """serializable loader object used by DeferredColumnLoader"""
    
    def __init__(self, state, key):
        self.state, self.key = state, key

    def __call__(self):
        state = self.state
        
        
        localparent = mapper._state_mapper(state)
        
        prop = localparent.get_property(self.key)
        strategy = prop._get_strategy(DeferredColumnLoader)

        if strategy.group:
            toload = [
                    p.key for p in 
                    localparent.iterate_properties 
                    if isinstance(p, StrategizedProperty) and 
                      isinstance(p.strategy, DeferredColumnLoader) and 
                      p.group==strategy.group
                    ]
        else:
            toload = [self.key]

        # narrow the keys down to just those which have no history
        group = [k for k in toload if k in state.unmodified]

        if strategy._should_log_debug:
            strategy.logger.debug(
                    "deferred load %s group %s" % 
                    (mapperutil.state_attribute_str(state, self.key), group and ','.join(group) or 'None')
                )

        session = sessionlib._state_session(state)
        if session is None:
            raise sa_exc.UnboundExecutionError(
                        "Parent instance %s is not bound to a Session; "
                        "deferred load operation of attribute '%s' cannot proceed" % 
                        (mapperutil.state_str(state), self.key)
                    )

        query = session.query(localparent)
        ident = state.key[1]
        query._get(None, ident=ident, only_load_props=group, refresh_state=state)
        return attributes.ATTR_WAS_SET

class DeferredOption(StrategizedOption):
    propagate_to_loaders = True
    
    def __init__(self, key, defer=False):
        super(DeferredOption, self).__init__(key)
        self.defer = defer

    def get_strategy_class(self):
        if self.defer:
            return DeferredColumnLoader
        else:
            return ColumnLoader

class UndeferGroupOption(MapperOption):
    propagate_to_loaders = True

    def __init__(self, group):
        self.group = group
    def process_query(self, query):
        query._attributes[('undefer', self.group)] = True

class AbstractRelationLoader(LoaderStrategy):
    """LoaderStratgies which deal with related objects as opposed to scalars."""

    def init(self):
        for attr in ['mapper', 'target', 'table', 'uselist']:
            setattr(self, attr, getattr(self.parent_property, attr))
        
    def _init_instance_attribute(self, state, callable_=None):
        if callable_:
            state.set_callable(self.key, callable_)
        else:
            state.initialize(self.key)

class NoLoader(AbstractRelationLoader):
    """Strategize a relation() that doesn't load data automatically."""

    def init_class_attribute(self, mapper):
        self.is_class_level = True

        _register_attribute(self, mapper,
            useobject=True, 
            uselist=self.parent_property.uselist,
            typecallable = self.parent_property.collection_class,
        )

    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        def new_execute(state, dict_, row, **flags):
            self._init_instance_attribute(state)

        if self._should_log_debug:
            new_execute = self.debug_callable(new_execute, self.logger, None,
                lambda state, dict_, row, **flags: "initializing blank scalar/collection on %s" % \
                                                  mapperutil.state_attribute_str(state, self.key)
            )
        return (new_execute, None)

log.class_logger(NoLoader)
        
class LazyLoader(AbstractRelationLoader):
    """Strategize a relation() that loads when first accessed."""

    def init(self):
        super(LazyLoader, self).init()
        (self.__lazywhere, self.__bind_to_col, self._equated_columns) = self._create_lazy_clause(self.parent_property)
        
        self.logger.info("%s lazy loading clause %s" % (self, self.__lazywhere))

        # determine if our "lazywhere" clause is the same as the mapper's
        # get() clause.  then we can just use mapper.get()
        #from sqlalchemy.orm import query
        self.use_get = not self.uselist and self.mapper._get_clause[0].compare(self.__lazywhere)
        if self.use_get:
            self.logger.info("%s will use query.get() to optimize instance loads" % self)

    def init_class_attribute(self, mapper):
        self.is_class_level = True
        
        # MANYTOONE currently only needs the "old" value for delete-orphan
        # cascades.  the required _SingleParentValidator will enable active_history
        # in that case.  otherwise we don't need the "old" value during backref operations.
        _register_attribute(self, 
                mapper,
                useobject=True,
                callable_=self._class_level_loader,
                uselist = self.parent_property.uselist,
                typecallable = self.parent_property.collection_class,
                active_history = self.parent_property.direction is not interfaces.MANYTOONE, 
                )

    def lazy_clause(self, state, reverse_direction=False, alias_secondary=False, adapt_source=None):
        if state is None:
            return self._lazy_none_clause(reverse_direction, adapt_source=adapt_source)
            
        if not reverse_direction:
            (criterion, bind_to_col, rev) = (self.__lazywhere, self.__bind_to_col, self._equated_columns)
        else:
            (criterion, bind_to_col, rev) = LazyLoader._create_lazy_clause(self.parent_property, reverse_direction=reverse_direction)

        def visit_bindparam(bindparam):
            mapper = reverse_direction and self.parent_property.mapper or self.parent_property.parent
            if bindparam.key in bind_to_col:
                # use the "committed" (database) version to get query column values
                # also its a deferred value; so that when used by Query, the committed value is used
                # after an autoflush occurs
                o = state.obj() # strong ref
                bindparam.value = lambda: mapper._get_committed_attr_by_column(o, bind_to_col[bindparam.key])

        if self.parent_property.secondary and alias_secondary:
            criterion = sql_util.ClauseAdapter(self.parent_property.secondary.alias()).traverse(criterion)

        criterion = visitors.cloned_traverse(criterion, {}, {'bindparam':visit_bindparam})
        if adapt_source:
            criterion = adapt_source(criterion)
        return criterion
        
    def _lazy_none_clause(self, reverse_direction=False, adapt_source=None):
        if not reverse_direction:
            (criterion, bind_to_col, rev) = (self.__lazywhere, self.__bind_to_col, self._equated_columns)
        else:
            (criterion, bind_to_col, rev) = LazyLoader._create_lazy_clause(self.parent_property, reverse_direction=reverse_direction)

        def visit_binary(binary):
            mapper = reverse_direction and self.parent_property.mapper or self.parent_property.parent
            if isinstance(binary.left, expression._BindParamClause) and binary.left.key in bind_to_col:
                # reverse order if the NULL is on the left side
                binary.left = binary.right
                binary.right = expression.null()
                binary.operator = operators.is_
                binary.negate = operators.isnot
            elif isinstance(binary.right, expression._BindParamClause) and binary.right.key in bind_to_col:
                binary.right = expression.null()
                binary.operator = operators.is_
                binary.negate = operators.isnot

        criterion = visitors.cloned_traverse(criterion, {}, {'binary':visit_binary})
        if adapt_source:
            criterion = adapt_source(criterion)
        return criterion
        
    def _class_level_loader(self, state):
        if not mapperutil._state_has_identity(state):
            return None

        return LoadLazyAttribute(state, self.key)

    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        if not self.is_class_level:
            def new_execute(state, dict_, row, **flags):
                # we are not the primary manager for this attribute on this class - set up a
                # per-instance lazyloader, which will override the class-level behavior.
                # this currently only happens when using a "lazyload" option on a "no load"
                # attribute - "eager" attributes always have a class-level lazyloader
                # installed.
                self._init_instance_attribute(state, callable_=LoadLazyAttribute(state, self.key))

            if self._should_log_debug:
                new_execute = self.debug_callable(new_execute, self.logger, None,
                    lambda state, dict_, row, **flags: "set instance-level lazy loader on %s" % \
                                                      mapperutil.state_attribute_str(state,
                                                                                     self.key)
                )

            return (new_execute, None)
        else:
            def new_execute(state, dict_, row, **flags):
                # we are the primary manager for this attribute on this class - reset its
                # per-instance attribute state, so that the class-level lazy loader is
                # executed when next referenced on this instance.  this is needed in
                # populate_existing() types of scenarios to reset any existing state.
                state.reset(self.key, dict_)

            if self._should_log_debug:
                new_execute = self.debug_callable(new_execute, self.logger, None,
                    lambda state, dict_, row, **flags: "set class-level lazy loader on %s" % \
                                                      mapperutil.state_attribute_str(state,
                                                                                     self.key)
                )

            return (new_execute, None)
            
    def _create_lazy_clause(cls, prop, reverse_direction=False):
        binds = util.column_dict()
        lookup = util.column_dict()
        equated_columns = util.column_dict()

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
            lazywhere = visitors.replacement_traverse(lazywhere, {}, col_to_bind) 
        
        if prop.secondaryjoin is not None:
            secondaryjoin = prop.secondaryjoin
            if reverse_direction:
                secondaryjoin = visitors.replacement_traverse(secondaryjoin, {}, col_to_bind)
            lazywhere = sql.and_(lazywhere, secondaryjoin)
    
        bind_to_col = dict((binds[col].key, col) for col in binds)
        
        return (lazywhere, bind_to_col, equated_columns)
    _create_lazy_clause = classmethod(_create_lazy_clause)
    
log.class_logger(LazyLoader)

class LoadLazyAttribute(object):
    """serializable loader object used by LazyLoader"""

    def __init__(self, state, key):
        self.state, self.key = state, key
        
    def __getstate__(self):
        return (self.state, self.key)

    def __setstate__(self, state):
        self.state, self.key = state
        
    def __call__(self):
        state = self.state

        instance_mapper = mapper._state_mapper(state)
        prop = instance_mapper.get_property(self.key)
        strategy = prop._get_strategy(LazyLoader)
        
        if strategy._should_log_debug:
            strategy.logger.debug("loading %s" % mapperutil.state_attribute_str(state, self.key))

        session = sessionlib._state_session(state)
        if session is None:
            raise sa_exc.UnboundExecutionError(
                "Parent instance %s is not bound to a Session; "
                "lazy load operation of attribute '%s' cannot proceed" % 
                (mapperutil.state_str(state), self.key)
            )
        
        q = session.query(prop.mapper)._adapt_all_clauses()
        
        if state.load_path:
            q = q._with_current_path(state.load_path + (self.key,))
            
        # if we have a simple primary key load, use mapper.get()
        # to possibly save a DB round trip
        if strategy.use_get:
            ident = []
            allnulls = True
            for primary_key in prop.mapper.primary_key: 
                val = instance_mapper._get_committed_state_attr_by_column(state, strategy._equated_columns[primary_key])
                allnulls = allnulls and val is None
                ident.append(val)
            if allnulls:
                return None
            if state.load_options:
                q = q._conditional_options(*state.load_options)
            return q.get(ident)

        if prop.order_by:
            q = q.order_by(*util.to_list(prop.order_by))

        if state.load_options:
            q = q._conditional_options(*state.load_options)
        q = q.filter(strategy.lazy_clause(state))

        result = q.all()
        if strategy.uselist:
            return result
        else:
            if result:
                return result[0]
            else:
                return None

class EagerLoader(AbstractRelationLoader):
    """Strategize a relation() that loads within the process of the parent object being selected."""
    
    def init(self):
        super(EagerLoader, self).init()
        self.join_depth = self.parent_property.join_depth

    def init_class_attribute(self, mapper):
        self.parent_property._get_strategy(LazyLoader).init_class_attribute(mapper)
        
    def setup_query(self, context, entity, path, adapter, column_collection=None, parentmapper=None, **kwargs):
        """Add a left outer join to the statement thats being constructed."""

        if not context.enable_eagerloads:
            return
            
        path = path + (self.key,)

        # check for user-defined eager alias
        if ("user_defined_eager_row_processor", path) in context.attributes:
            clauses = context.attributes[("user_defined_eager_row_processor", path)]
            
            adapter = entity._get_entity_clauses(context.query, context)
            if adapter and clauses:
                context.attributes[("user_defined_eager_row_processor", path)] = clauses = clauses.wrap(adapter)
            elif adapter:
                context.attributes[("user_defined_eager_row_processor", path)] = clauses = adapter
            
            add_to_collection = context.primary_columns
            
        else:
            clauses = self._create_eager_join(context, entity, path, adapter, parentmapper)
            if not clauses:
                return

            context.attributes[("eager_row_processor", path)] = clauses

            add_to_collection = context.secondary_columns
            
        for value in self.mapper._iterate_polymorphic_properties():
            value.setup(context, entity, path + (self.mapper.base_mapper,), clauses, parentmapper=self.mapper, column_collection=add_to_collection)
    
    def _create_eager_join(self, context, entity, path, adapter, parentmapper):
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

        if parentmapper is None:
            localparent = entity.mapper
        else:
            localparent = parentmapper
    
        # whether or not the Query will wrap the selectable in a subquery,
        # and then attach eager load joins to that (i.e., in the case of LIMIT/OFFSET etc.)
        should_nest_selectable = context.query._should_nest_selectable

        if entity in context.eager_joins:
            entity_key, default_towrap = entity, entity.selectable

        elif should_nest_selectable or not context.from_clause:
            # if no from_clause, or a subquery is going to be generated, 
            # store eager joins per _MappedEntity; Query._compile_context will 
            # add them as separate selectables to the select(), or splice them together
            # after the subquery is generated
            entity_key, default_towrap = entity, entity.selectable
        else:
            index, clause = sql_util.find_join_source(context.from_clause, entity.selectable)
            if clause:
                # join to an existing FROM clause on the query.
                # key it to its list index in the eager_joins dict.
                # Query._compile_context will adapt as needed and append to the
                # FROM clause of the select().
                entity_key, default_towrap = index, clause
            else:
                # if no from_clause to join to,
                # store eager joins per _MappedEntity
                entity_key, default_towrap = entity, entity.selectable
                
        towrap = context.eager_joins.setdefault(entity_key, default_towrap)

        # create AliasedClauses object to build up the eager query.  
        clauses = mapperutil.ORMAdapter(mapperutil.AliasedClass(self.mapper), 
                    equivalents=self.mapper._equivalent_columns, adapt_required=True)

        join_to_left = False
        if adapter:
            if getattr(adapter, 'aliased_class', None):
                onclause = getattr(adapter.aliased_class, self.key, self.parent_property)
            else:
                onclause = getattr(mapperutil.AliasedClass(self.parent, adapter.selectable), self.key, self.parent_property)
                
            if onclause is self.parent_property:
                # TODO: this is a temporary hack to account for polymorphic eager loads where
                # the eagerload is referencing via of_type().
                join_to_left = True
        else:
            onclause = self.parent_property
            
        context.eager_joins[entity_key] = eagerjoin = mapperutil.outerjoin(towrap, clauses.aliased_class, onclause, join_to_left=join_to_left)
        
        # send a hint to the Query as to where it may "splice" this join
        eagerjoin.stop_on = entity.selectable
        
        if not self.parent_property.secondary and context.query._should_nest_selectable and not parentmapper:
            # for parentclause that is the non-eager end of the join,
            # ensure all the parent cols in the primaryjoin are actually in the
            # columns clause (i.e. are not deferred), so that aliasing applied by the Query propagates 
            # those columns outward.  This has the effect of "undefering" those columns.
            for col in sql_util.find_columns(self.parent_property.primaryjoin):
                if localparent.mapped_table.c.contains_column(col):
                    if adapter:
                        col = adapter.columns[col]
                    context.primary_columns.append(col)
        
        if self.parent_property.order_by:
            context.eager_order_by += eagerjoin._target_adapter.copy_and_process(util.to_list(self.parent_property.order_by))
            
        return clauses
        
    def _create_eager_adapter(self, context, row, adapter, path):
        if ("user_defined_eager_row_processor", path) in context.attributes:
            decorator = context.attributes[("user_defined_eager_row_processor", path)]
            # user defined eagerloads are part of the "primary" portion of the load.
            # the adapters applied to the Query should be honored.
            if context.adapter and decorator:
                decorator = decorator.wrap(context.adapter)
            elif context.adapter:
                decorator = context.adapter
        elif ("eager_row_processor", path) in context.attributes:
            decorator = context.attributes[("eager_row_processor", path)]
        else:
            if self._should_log_debug:
                self.logger.debug("Could not locate aliased clauses for key: " + str(path))
            return False

        try:
            identity_key = self.mapper.identity_key_from_row(row, decorator)
            return decorator
        except KeyError, k:
            # no identity key - dont return a row processor, will cause a degrade to lazy
            if self._should_log_debug:
                self.logger.debug("could not locate identity key from row; missing column '%s'" % k)
            return False

    def create_row_processor(self, context, path, mapper, row, adapter):
        path = path + (self.key,)
            
        eager_adapter = self._create_eager_adapter(context, row, adapter, path)
        
        if eager_adapter is not False:
            key = self.key
            _instance = self.mapper._instance_processor(context, path + (self.mapper.base_mapper,), eager_adapter)
            
            if not self.uselist:
                def execute(state, dict_, row, isnew, **flags):
                    if isnew:
                        # set a scalar object instance directly on the
                        # parent object, bypassing InstrumentedAttribute
                        # event handlers.
                        dict_[key] = _instance(row, None)
                    else:
                        # call _instance on the row, even though the object has been created,
                        # so that we further descend into properties
                        _instance(row, None)
            else:
                def execute(state, dict_, row, isnew, **flags):
                    if isnew or (state, key) not in context.attributes:
                        # appender_key can be absent from context.attributes with isnew=False
                        # when self-referential eager loading is used; the same instance may be present
                        # in two distinct sets of result columns

                        collection = attributes.init_state_collection(state, dict_, key)
                        appender = util.UniqueAppender(collection, 'append_without_event')

                        context.attributes[(state, key)] = appender

                    result_list = context.attributes[(state, key)]
                    
                    _instance(row, result_list)

            if self._should_log_debug:
                execute = self.debug_callable(execute, self.logger, 
                    "%s returning eager instance loader" % self,
                    lambda state, dict_, row, isnew, **flags: "%s eagerload %s" % \
                                                  (self,
                                                   self.uselist and "scalar attribute"
                                                   or "collection")
                )

            return (execute, execute)
        else:
            if self._should_log_debug:
                self.logger.debug("%s degrading to lazy loader" % self)
            return self.parent_property._get_strategy(LazyLoader).create_row_processor(context, path, mapper, row, adapter)

log.class_logger(EagerLoader)

class EagerLazyOption(StrategizedOption):

    def __init__(self, key, lazy=True, chained=False, mapper=None, propagate_to_loaders=True):
        super(EagerLazyOption, self).__init__(key, mapper)
        self.lazy = lazy
        self.chained = chained
        self.propagate_to_loaders = propagate_to_loaders
        
    def is_chained(self):
        return not self.lazy and self.chained
        
    def get_strategy_class(self):
        if self.lazy:
            return LazyLoader
        elif self.lazy is False:
            return EagerLoader
        elif self.lazy is None:
            return NoLoader

class LoadEagerFromAliasOption(PropertyOption):
    
    def __init__(self, key, alias=None):
        super(LoadEagerFromAliasOption, self).__init__(key)
        if alias:
            if not isinstance(alias, basestring):
                m, alias, is_aliased_class = mapperutil._entity_info(alias)
        self.alias = alias

    def process_query_property(self, query, paths, mappers):
        if self.alias:
            if isinstance(self.alias, basestring):
                mapper = mappers[-1]
                (root_mapper, propname) = paths[-1][-2:]
                prop = mapper.get_property(propname, resolve_synonyms=True)
                self.alias = prop.target.alias(self.alias)
            query._attributes[("user_defined_eager_row_processor", paths[-1])] = sql_util.ColumnAdapter(self.alias)
        else:
            (root_mapper, propname) = paths[-1][-2:]
            mapper = mappers[-1]
            prop = mapper.get_property(propname, resolve_synonyms=True)
            adapter = query._polymorphic_adapters.get(prop.mapper, None)
            query._attributes[("user_defined_eager_row_processor", paths[-1])] = adapter

class _SingleParentValidator(interfaces.AttributeExtension):
    def __init__(self, prop):
        self.prop = prop

    def _do_check(self, state, value, oldvalue, initiator):
        if value is not None:
            hasparent = initiator.hasparent(attributes.instance_state(value))
            if hasparent and oldvalue is not value: 
                raise sa_exc.InvalidRequestError("Instance %s is already associated with an instance "
                    "of %s via its %s attribute, and is only allowed a single parent." % 
                    (mapperutil.instance_str(value), state.class_, self.prop)
                )
        return value
        
    def append(self, state, value, initiator):
        return self._do_check(state, value, None, initiator)

    def set(self, state, value, oldvalue, initiator):
        return self._do_check(state, value, oldvalue, initiator)


