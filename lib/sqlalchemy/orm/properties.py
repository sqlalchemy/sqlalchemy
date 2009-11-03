# properties.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""MapperProperty implementations.

This is a private module which defines the behavior of invidual ORM-mapped
attributes.

"""

from sqlalchemy import sql, util, log
import sqlalchemy.exceptions as sa_exc
from sqlalchemy.sql.util import ClauseAdapter, criterion_as_pairs, join_condition
from sqlalchemy.sql import operators, expression
from sqlalchemy.orm import (
    attributes, dependency, mapper, object_mapper, strategies,
    )
from sqlalchemy.orm.util import CascadeOptions, _class_to_mapper, _orm_annotate, _orm_deannotate
from sqlalchemy.orm.interfaces import (
    MANYTOMANY, MANYTOONE, MapperProperty, ONETOMANY, PropComparator,
    StrategizedProperty,
    )

__all__ = ('ColumnProperty', 'CompositeProperty', 'SynonymProperty',
           'ComparableProperty', 'RelationProperty', 'BackRef')


class ColumnProperty(StrategizedProperty):
    """Describes an object attribute that corresponds to a table column."""

    def __init__(self, *columns, **kwargs):
        """Construct a ColumnProperty.

        :param \*columns: The list of `columns` describes a single
          object property. If there are multiple tables joined
          together for the mapper, this list represents the equivalent
          column as it appears across each table.

        :param group:

        :param deferred:

        :param comparator_factory:

        :param descriptor:

        :param extension:

        """
        self.columns = [expression._labeled(c) for c in columns]
        self.group = kwargs.pop('group', None)
        self.deferred = kwargs.pop('deferred', False)
        self.no_instrument = kwargs.pop('_no_instrument', False)
        self.comparator_factory = kwargs.pop('comparator_factory', self.__class__.Comparator)
        self.descriptor = kwargs.pop('descriptor', None)
        self.extension = kwargs.pop('extension', None)
        if kwargs:
            raise TypeError(
                "%s received unexpected keyword argument(s): %s" % (
                    self.__class__.__name__, ', '.join(sorted(kwargs.keys()))))

        util.set_creation_order(self)
        if self.no_instrument:
            self.strategy_class = strategies.UninstrumentedColumnLoader
        elif self.deferred:
            self.strategy_class = strategies.DeferredColumnLoader
        else:
            self.strategy_class = strategies.ColumnLoader
    
    def instrument_class(self, mapper):
        if self.no_instrument:
            return
        
        attributes.register_descriptor(
            mapper.class_, 
            self.key, 
            comparator=self.comparator_factory(self, mapper), 
            parententity=mapper,
            property_=self
            )
        
    def do_init(self):
        super(ColumnProperty, self).do_init()
        if len(self.columns) > 1 and self.parent.primary_key.issuperset(self.columns):
            util.warn(
                ("On mapper %s, primary key column '%s' is being combined "
                 "with distinct primary key column '%s' in attribute '%s'.  "
                 "Use explicit properties to give each column its own mapped "
                 "attribute name.") % (self.parent, self.columns[1],
                                       self.columns[0], self.key))

    def copy(self):
        return ColumnProperty(deferred=self.deferred, group=self.group, *self.columns)

    def getattr(self, state, column):
        return state.get_impl(self.key).get(state, state.dict)

    def getcommitted(self, state, column, passive=False):
        return state.get_impl(self.key).get_committed_value(state, state.dict, passive=passive)

    def setattr(self, state, value, column):
        state.get_impl(self.key).set(state, state.dict, value, None)

    def merge(self, session, source, dest, dont_load, _recursive):
        value = attributes.instance_state(source).value_as_iterable(
            self.key, passive=True)
        if value:
            setattr(dest, self.key, value[0])
        else:
            attributes.instance_state(dest).expire_attributes([self.key])

    def get_col_value(self, column, value):
        return value

    class Comparator(PropComparator):
        @util.memoized_instancemethod
        def __clause_element__(self):
            if self.adapter:
                return self.adapter(self.prop.columns[0])
            else:
                return self.prop.columns[0]._annotate({"parententity": self.mapper, "parentmapper":self.mapper})
                
        def operate(self, op, *other, **kwargs):
            return op(self.__clause_element__(), *other, **kwargs)

        def reverse_operate(self, op, other, **kwargs):
            col = self.__clause_element__()
            return op(col._bind_param(other), col, **kwargs)
    
    # TODO: legacy..do we need this ? (0.5)
    ColumnComparator = Comparator
    
    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key

log.class_logger(ColumnProperty)

class CompositeProperty(ColumnProperty):
    """subclasses ColumnProperty to provide composite type support."""
    
    def __init__(self, class_, *columns, **kwargs):
        if 'comparator' in kwargs:
            util.warn_deprecated("The 'comparator' argument to CompositeProperty is deprecated.  Use comparator_factory.")
            kwargs['comparator_factory'] = kwargs['comparator']
        super(CompositeProperty, self).__init__(*columns, **kwargs)
        self._col_position_map = util.column_dict((c, i) for i, c in enumerate(columns))
        self.composite_class = class_
        self.strategy_class = strategies.CompositeColumnLoader

    def copy(self):
        return CompositeProperty(deferred=self.deferred, group=self.group, composite_class=self.composite_class, *self.columns)

    def do_init(self):
        # skip over ColumnProperty's do_init(),
        # which issues assertions that do not apply to CompositeColumnProperty
        super(ColumnProperty, self).do_init()

    def getattr(self, state, column):
        obj = state.get_impl(self.key).get(state, state.dict)
        return self.get_col_value(column, obj)

    def getcommitted(self, state, column, passive=False):
        # TODO: no coverage here
        obj = state.get_impl(self.key).get_committed_value(state, state.dict, passive=passive)
        return self.get_col_value(column, obj)

    def setattr(self, state, value, column):

        obj = state.get_impl(self.key).get(state, state.dict)
        if obj is None:
            obj = self.composite_class(*[None for c in self.columns])
            state.get_impl(self.key).set(state, state.dict, obj, None)

        if hasattr(obj, '__set_composite_values__'):
            values = list(obj.__composite_values__())
            values[self._col_position_map[column]] = value
            obj.__set_composite_values__(*values)
        else:
            setattr(obj, column.key, value)
            
    def get_col_value(self, column, value):
        if value is None:
            return None
        for a, b in zip(self.columns, value.__composite_values__()):
            if a is column:
                return b

    class Comparator(PropComparator):
        def __clause_element__(self):
            if self.adapter:
                # TODO: test coverage for adapted composite comparison
                return expression.ClauseList(*[self.adapter(x) for x in self.prop.columns])
            else:
                return expression.ClauseList(*self.prop.columns)
        
        __hash__ = None
        
        def __eq__(self, other):
            if other is None:
                values = [None] * len(self.prop.columns)
            else:
                values = other.__composite_values__()
            return sql.and_(*[a==b for a, b in zip(self.prop.columns, values)])
            
        def __ne__(self, other):
            return sql.not_(self.__eq__(other))

    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key

class ConcreteInheritedProperty(MapperProperty):
    extension = None

    def setup(self, context, entity, path, adapter, **kwargs):
        pass

    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        return (None, None)

    def instrument_class(self, mapper):
        def warn():
            raise AttributeError("Concrete %s does not implement attribute %r at "
                "the instance level.  Add this property explicitly to %s." % 
                (self.parent, self.key, self.parent))

        class NoninheritedConcreteProp(object):
            def __set__(s, obj, value):
                warn()
            def __delete__(s, obj):
                warn()
            def __get__(s, obj, owner):
                warn()

        comparator_callable = None
        # TODO: put this process into a deferred callable?
        for m in self.parent.iterate_to_root():
            p = m._get_property(self.key)
            if not isinstance(p, ConcreteInheritedProperty):
                comparator_callable = p.comparator_factory
                break

        attributes.register_descriptor(
            mapper.class_, 
            self.key, 
            comparator=comparator_callable(self, mapper), 
            parententity=mapper,
            property_=self,
            proxy_property=NoninheritedConcreteProp()
            )


class SynonymProperty(MapperProperty):

    extension = None

    def __init__(self, name, map_column=None, descriptor=None, comparator_factory=None):
        self.name = name
        self.map_column = map_column
        self.descriptor = descriptor
        self.comparator_factory = comparator_factory
        util.set_creation_order(self)

    def setup(self, context, entity, path, adapter, **kwargs):
        pass

    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        return (None, None)

    def instrument_class(self, mapper):
        class_ = self.parent.class_

        if self.descriptor is None:
            class SynonymProp(object):
                def __set__(s, obj, value):
                    setattr(obj, self.name, value)
                def __delete__(s, obj):
                    delattr(obj, self.name)
                def __get__(s, obj, owner):
                    if obj is None:
                        return s
                    return getattr(obj, self.name)

            self.descriptor = SynonymProp()

        def comparator_callable(prop, mapper):
            def comparator():
                prop = self.parent._get_property(self.key, resolve_synonyms=True)
                if self.comparator_factory:
                    return self.comparator_factory(prop, mapper)
                else:
                    return prop.comparator_factory(prop, mapper)
            return comparator

        attributes.register_descriptor(
            mapper.class_, 
            self.key, 
            comparator=comparator_callable(self, mapper), 
            parententity=mapper,
            property_=self,
            proxy_property=self.descriptor
            )

    def merge(self, session, source, dest, dont_load, _recursive):
        pass
        
log.class_logger(SynonymProperty)

class ComparableProperty(MapperProperty):
    """Instruments a Python property for use in query expressions."""

    extension = None
    
    def __init__(self, comparator_factory, descriptor=None):
        self.descriptor = descriptor
        self.comparator_factory = comparator_factory
        util.set_creation_order(self)

    def instrument_class(self, mapper):
        """Set up a proxy to the unmanaged descriptor."""

        attributes.register_descriptor(
            mapper.class_, 
            self.key, 
            comparator=self.comparator_factory(self, mapper), 
            parententity=mapper,
            property_=self,
            proxy_property=self.descriptor
            )

    def setup(self, context, entity, path, adapter, **kwargs):
        pass

    def create_row_processor(self, selectcontext, path, mapper, row, adapter):
        return (None, None)

    def merge(self, session, source, dest, dont_load, _recursive):
        pass


class RelationProperty(StrategizedProperty):
    """Describes an object property that holds a single item or list
    of items that correspond to a related database table.
    """

    def __init__(self, argument,
        secondary=None, primaryjoin=None,
        secondaryjoin=None, 
        foreign_keys=None,
        uselist=None,
        order_by=False,
        backref=None,
        back_populates=None,
        post_update=False,
        cascade=False, extension=None,
        viewonly=False, lazy=True,
        collection_class=None, passive_deletes=False,
        passive_updates=True, remote_side=None,
        enable_typechecks=True, join_depth=None,
        comparator_factory=None,
        single_parent=False,
        strategy_class=None, _local_remote_pairs=None, query_class=None):

        self.uselist = uselist
        self.argument = argument
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.post_update = post_update
        self.direction = None
        self.viewonly = viewonly
        self.lazy = lazy
        self.single_parent = single_parent
        self._foreign_keys = foreign_keys
        self.collection_class = collection_class
        self.passive_deletes = passive_deletes
        self.passive_updates = passive_updates
        self.remote_side = remote_side
        self.enable_typechecks = enable_typechecks
        self.query_class = query_class

        self.join_depth = join_depth
        self.local_remote_pairs = _local_remote_pairs
        self.extension = extension
        self.comparator_factory = comparator_factory or RelationProperty.Comparator
        self.comparator = self.comparator_factory(self, None)
        util.set_creation_order(self)

        if strategy_class:
            self.strategy_class = strategy_class
        elif self.lazy == 'dynamic':
            from sqlalchemy.orm import dynamic
            self.strategy_class = dynamic.DynaLoader
        elif self.lazy is False:
            self.strategy_class = strategies.EagerLoader
        elif self.lazy is None:
            self.strategy_class = strategies.NoLoader
        else:
            self.strategy_class = strategies.LazyLoader

        self._reverse_property = set()

        if cascade is not False:
            self.cascade = CascadeOptions(cascade)
        else:
            self.cascade = CascadeOptions("save-update, merge")

        if self.passive_deletes == 'all' and ("delete" in self.cascade or "delete-orphan" in self.cascade):
            raise sa_exc.ArgumentError("Can't set passive_deletes='all' in conjunction with 'delete' or 'delete-orphan' cascade")

        self.order_by = order_by

        self.back_populates = back_populates

        if self.back_populates:
            if backref:
                raise sa_exc.ArgumentError("backref and back_populates keyword arguments are mutually exclusive")
            self.backref = None
        elif isinstance(backref, basestring):
            # propagate explicitly sent primary/secondary join conditions to the BackRef object if
            # just a string was sent
            if secondary is not None:
                # reverse primary/secondary in case of a many-to-many
                self.backref = BackRef(backref, primaryjoin=secondaryjoin, 
                                    secondaryjoin=primaryjoin, passive_updates=self.passive_updates)
            else:
                self.backref = BackRef(backref, primaryjoin=primaryjoin, 
                                    secondaryjoin=secondaryjoin, passive_updates=self.passive_updates)
        else:
            self.backref = backref

    def instrument_class(self, mapper):
        attributes.register_descriptor(
            mapper.class_, 
            self.key, 
            comparator=self.comparator_factory(self, mapper), 
            parententity=mapper,
            property_=self
            )

    class Comparator(PropComparator):
        def __init__(self, prop, mapper, of_type=None, adapter=None):
            self.prop = prop
            self.mapper = mapper
            self.adapter = adapter
            if of_type:
                self._of_type = _class_to_mapper(of_type)

        def adapted(self, adapter):
            """Return a copy of this PropComparator which will use the given adaption function
            on the local side of generated expressions.

            """
            return self.__class__(self.property, self.mapper, getattr(self, '_of_type', None), adapter)
        
        @property
        def parententity(self):
            return self.property.parent

        def __clause_element__(self):
            elem = self.property.parent._with_polymorphic_selectable
            if self.adapter:
                return self.adapter(elem)
            else:
                return elem

        def operate(self, op, *other, **kwargs):
            return op(self, *other, **kwargs)

        def reverse_operate(self, op, other, **kwargs):
            return op(self, *other, **kwargs)

        def of_type(self, cls):
            return RelationProperty.Comparator(self.property, self.mapper, cls, adapter=self.adapter)

        def in_(self, other):
            raise NotImplementedError("in_() not yet supported for relations.  For a "
                    "simple many-to-one, use in_() against the set of foreign key values.")
            
        __hash__ = None
        
        def __eq__(self, other):
            if other is None:
                if self.property.direction in [ONETOMANY, MANYTOMANY]:
                    return ~self._criterion_exists()
                else:
                    return _orm_annotate(self.property._optimized_compare(None, adapt_source=self.adapter))
            elif self.property.uselist:
                raise sa_exc.InvalidRequestError("Can't compare a collection to an object or collection; use contains() to test for membership.")
            else:
                return _orm_annotate(self.property._optimized_compare(other, adapt_source=self.adapter))

        def _criterion_exists(self, criterion=None, **kwargs):
            if getattr(self, '_of_type', None):
                target_mapper = self._of_type
                to_selectable = target_mapper._with_polymorphic_selectable
                if self.property._is_self_referential():
                    to_selectable = to_selectable.alias()

                single_crit = target_mapper._single_table_criterion
                if single_crit:
                    if criterion is not None:
                        criterion = single_crit & criterion
                    else:
                        criterion = single_crit
            else:
                to_selectable = None

            if self.adapter:
                source_selectable = self.__clause_element__()
            else:
                source_selectable = None
            
            pj, sj, source, dest, secondary, target_adapter = \
                self.property._create_joins(dest_polymorphic=True, dest_selectable=to_selectable, source_selectable=source_selectable)

            for k in kwargs:
                crit = self.property.mapper.class_manager[k] == kwargs[k]
                if criterion is None:
                    criterion = crit
                else:
                    criterion = criterion & crit
            
            # annotate the *local* side of the join condition, in the case of pj + sj this
            # is the full primaryjoin, in the case of just pj its the local side of
            # the primaryjoin.  
            if sj:
                j = _orm_annotate(pj) & sj
            else:
                j = _orm_annotate(pj, exclude=self.property.remote_side)
            
            if criterion and target_adapter:
                # limit this adapter to annotated only?
                criterion = target_adapter.traverse(criterion)

            # only have the "joined left side" of what we return be subject to Query adaption.  The right
            # side of it is used for an exists() subquery and should not correlate or otherwise reach out
            # to anything in the enclosing query.
            if criterion:
                criterion = criterion._annotate({'_halt_adapt': True})
            
            crit = j & criterion
            
            return sql.exists([1], crit, from_obj=dest).correlate(source)

        def any(self, criterion=None, **kwargs):
            if not self.property.uselist:
                raise sa_exc.InvalidRequestError("'any()' not implemented for scalar attributes. Use has().")

            return self._criterion_exists(criterion, **kwargs)

        def has(self, criterion=None, **kwargs):
            if self.property.uselist:
                raise sa_exc.InvalidRequestError("'has()' not implemented for collections.  Use any().")
            return self._criterion_exists(criterion, **kwargs)

        def contains(self, other, **kwargs):
            if not self.property.uselist:
                raise sa_exc.InvalidRequestError("'contains' not implemented for scalar attributes.  Use ==")
            clause = self.property._optimized_compare(other, adapt_source=self.adapter)

            if self.property.secondaryjoin:
                clause.negation_clause = self.__negated_contains_or_equals(other)

            return clause

        def __negated_contains_or_equals(self, other):
            if self.property.direction == MANYTOONE:
                state = attributes.instance_state(other)
                strategy = self.property._get_strategy(strategies.LazyLoader)
                
                def state_bindparam(state, col):
                    o = state.obj() # strong ref
                    return lambda: self.property.mapper._get_committed_attr_by_column(o, col)
                
                def adapt(col):
                    if self.adapter:
                        return self.adapter(col)
                    else:
                        return col
                        
                if strategy.use_get:
                    return sql.and_(*[
                        sql.or_(
                        adapt(x) != state_bindparam(state, y),
                        adapt(x) == None)
                        for (x, y) in self.property.local_remote_pairs])
                    
            criterion = sql.and_(*[x==y for (x, y) in zip(self.property.mapper.primary_key, self.property.mapper.primary_key_from_instance(other))])
            return ~self._criterion_exists(criterion)

        def __ne__(self, other):
            if other is None:
                if self.property.direction == MANYTOONE:
                    return sql.or_(*[x!=None for x in self.property._foreign_keys])
                else:
                    return self._criterion_exists()
            elif self.property.uselist:
                raise sa_exc.InvalidRequestError("Can't compare a collection to an object or collection; use contains() to test for membership.")
            else:
                return self.__negated_contains_or_equals(other)

        @util.memoized_property
        def property(self):
            self.prop.parent.compile()
            return self.prop

    def compare(self, op, value, value_is_parent=False):
        if op == operators.eq:
            if value is None:
                if self.uselist:
                    return ~sql.exists([1], self.primaryjoin)
                else:
                    return self._optimized_compare(None, value_is_parent=value_is_parent)
            else:
                return self._optimized_compare(value, value_is_parent=value_is_parent)
        else:
            return op(self.comparator, value)

    def _optimized_compare(self, value, value_is_parent=False, adapt_source=None):
        if value is not None:
            value = attributes.instance_state(value)
        return self._get_strategy(strategies.LazyLoader).\
                lazy_clause(value, reverse_direction=not value_is_parent, alias_secondary=True, adapt_source=adapt_source)

    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key

    def merge(self, session, source, dest, dont_load, _recursive):
        if not dont_load:
            # TODO: no test coverage for recursive check
            for r in self._reverse_property:
                if (source, r) in _recursive:
                    return

        source_state = attributes.instance_state(source)
        dest_state, dest_dict = attributes.instance_state(dest), attributes.instance_dict(dest)

        if not "merge" in self.cascade:
            dest_state.expire_attributes([self.key])
            return

        instances = source_state.value_as_iterable(self.key, passive=True)

        if not instances:
            return

        if self.uselist:
            dest_list = []
            for current in instances:
                _recursive[(current, self)] = True
                obj = session._merge(current, dont_load=dont_load, _recursive=_recursive)
                if obj is not None:
                    dest_list.append(obj)
            if dont_load:
                coll = attributes.init_collection(dest_state, self.key)
                for c in dest_list:
                    coll.append_without_event(c)
            else:
                getattr(dest.__class__, self.key).impl._set_iterable(dest_state, dest_dict, dest_list)
        else:
            current = instances[0]
            if current is not None:
                _recursive[(current, self)] = True
                obj = session._merge(current, dont_load=dont_load, _recursive=_recursive)
                if obj is not None:
                    if dont_load:
                        dest_state.dict[self.key] = obj
                    else:
                        setattr(dest, self.key, obj)

    def cascade_iterator(self, type_, state, visited_instances, halt_on=None):
        if not type_ in self.cascade:
            return

        # only actively lazy load on the 'delete' cascade
        if type_ != 'delete' or self.passive_deletes:
            passive = attributes.PASSIVE_NO_INITIALIZE
        else:
            passive = attributes.PASSIVE_OFF

        mapper = self.mapper.primary_mapper()
        instances = state.value_as_iterable(self.key, passive=passive)
        if instances:
            for c in instances:
                if c is not None and c not in visited_instances and (halt_on is None or not halt_on(c)):
                    if not isinstance(c, self.mapper.class_):
                        raise AssertionError("Attribute '%s' on class '%s' doesn't handle objects "
                                    "of type '%s'" % (self.key, str(self.parent.class_), str(c.__class__)))
                    visited_instances.add(c)

                    # cascade using the mapper local to this object, so that its individual properties are located
                    instance_mapper = object_mapper(c)
                    yield (c, instance_mapper, attributes.instance_state(c))

    def _add_reverse_property(self, key):
        other = self.mapper._get_property(key)
        self._reverse_property.add(other)
        other._reverse_property.add(self)
        
        if not other._get_target().common_parent(self.parent):
            raise sa_exc.ArgumentError("reverse_property %r on relation %s references "
                    "relation %s, which does not reference mapper %s" % (key, self, other, self.parent))
        
        if self.direction in (ONETOMANY, MANYTOONE) and self.direction == other.direction:
            raise sa_exc.ArgumentError("%s and back-reference %s are both of the same direction %r."
                "  Did you mean to set remote_side on the many-to-one side ?" % (self, other, self.direction))
        
    def do_init(self):
        self._get_target()
        self._process_dependent_arguments()
        self._determine_joins()
        self._determine_synchronize_pairs()
        self._determine_direction()
        self._determine_local_remote_pairs()
        self._post_init()
        super(RelationProperty, self).do_init()

    def _get_target(self):
        if not hasattr(self, 'mapper'):
            if isinstance(self.argument, type):
                self.mapper = mapper.class_mapper(self.argument, compile=False)
            elif isinstance(self.argument, mapper.Mapper):
                self.mapper = self.argument
            elif util.callable(self.argument):
                # accept a callable to suit various deferred-configurational schemes
                self.mapper = mapper.class_mapper(self.argument(), compile=False)
            else:
                raise sa_exc.ArgumentError("relation '%s' expects a class or a mapper argument (received: %s)" % (self.key, type(self.argument)))
            assert isinstance(self.mapper, mapper.Mapper), self.mapper
        return self.mapper
        
    def _process_dependent_arguments(self):

        # accept callables for other attributes which may require deferred initialization
        for attr in ('order_by', 'primaryjoin', 'secondaryjoin', 'secondary', '_foreign_keys', 'remote_side'):
            if util.callable(getattr(self, attr)):
                setattr(self, attr, getattr(self, attr)())

        # in the case that InstrumentedAttributes were used to construct
        # primaryjoin or secondaryjoin, remove the "_orm_adapt" annotation so these
        # interact with Query in the same way as the original Table-bound Column objects
        for attr in ('primaryjoin', 'secondaryjoin'):
            val = getattr(self, attr)
            if val is not None:
                util.assert_arg_type(val, sql.ClauseElement, attr)
                setattr(self, attr, _orm_deannotate(val))
        
        if self.order_by:
            self.order_by = [expression._literal_as_column(x) for x in util.to_list(self.order_by)]
        
        self._foreign_keys = util.column_set(expression._literal_as_column(x) for x in util.to_column_set(self._foreign_keys))
        self.remote_side = util.column_set(expression._literal_as_column(x) for x in util.to_column_set(self.remote_side))

        if not self.parent.concrete:
            for inheriting in self.parent.iterate_to_root():
                if inheriting is not self.parent and inheriting._get_property(self.key, raiseerr=False):
                    util.warn(
                        ("Warning: relation '%s' on mapper '%s' supercedes "
                         "the same relation on inherited mapper '%s'; this "
                         "can cause dependency issues during flush") %
                        (self.key, self.parent, inheriting))

        # TODO: remove 'self.table'
        self.target = self.table = self.mapper.mapped_table

        if self.cascade.delete_orphan:
            if self.parent.class_ is self.mapper.class_:
                raise sa_exc.ArgumentError("In relationship '%s', can't establish 'delete-orphan' cascade "
                            "rule on a self-referential relationship.  "
                            "You probably want cascade='all', which includes delete cascading but not orphan detection." %(str(self)))
            self.mapper.primary_mapper().delete_orphans.append((self.key, self.parent.class_))

    def _determine_joins(self):
        if self.secondaryjoin is not None and self.secondary is None:
            raise sa_exc.ArgumentError("Property '" + self.key + "' specified with secondary join condition but no secondary argument")
        # if join conditions were not specified, figure them out based on foreign keys

        def _search_for_join(mapper, table):
            # find a join between the given mapper's mapped table and the given table.
            # will try the mapper's local table first for more specificity, then if not
            # found will try the more general mapped table, which in the case of inheritance
            # is a join.
            try:
                return join_condition(mapper.local_table, table)
            except sa_exc.ArgumentError, e:
                return join_condition(mapper.mapped_table, table)

        try:
            if self.secondary is not None:
                if self.secondaryjoin is None:
                    self.secondaryjoin = _search_for_join(self.mapper, self.secondary)
                if self.primaryjoin is None:
                    self.primaryjoin = _search_for_join(self.parent, self.secondary)
            else:
                if self.primaryjoin is None:
                    self.primaryjoin = _search_for_join(self.parent, self.target)
        except sa_exc.ArgumentError, e:
            raise sa_exc.ArgumentError("Could not determine join condition between "
                        "parent/child tables on relation %s.  "
                        "Specify a 'primaryjoin' expression.  If this is a "
                        "many-to-many relation, 'secondaryjoin' is needed as well." % (self))

    def _col_is_part_of_mappings(self, column):
        if self.secondary is None:
            return self.parent.mapped_table.c.contains_column(column) or \
                self.target.c.contains_column(column)
        else:
            return self.parent.mapped_table.c.contains_column(column) or \
                self.target.c.contains_column(column) or \
                self.secondary.c.contains_column(column) is not None

    def _determine_synchronize_pairs(self):

        if self.local_remote_pairs:
            if not self._foreign_keys:
                raise sa_exc.ArgumentError("foreign_keys argument is required with _local_remote_pairs argument")

            self.synchronize_pairs = []

            for l, r in self.local_remote_pairs:
                if r in self._foreign_keys:
                    self.synchronize_pairs.append((l, r))
                elif l in self._foreign_keys:
                    self.synchronize_pairs.append((r, l))
        else:
            eq_pairs = criterion_as_pairs(
                            self.primaryjoin, 
                            consider_as_foreign_keys=self._foreign_keys, 
                            any_operator=self.viewonly
                        )
            eq_pairs = [
                            (l, r) for l, r in eq_pairs if 
                            (self._col_is_part_of_mappings(l) and 
                                self._col_is_part_of_mappings(r)) 
                                or self.viewonly and r in self._foreign_keys
                        ]

            if not eq_pairs:
                if not self.viewonly and criterion_as_pairs(self.primaryjoin, consider_as_foreign_keys=self._foreign_keys, any_operator=True):
                    raise sa_exc.ArgumentError("Could not locate any equated, locally "
                        "mapped column pairs for primaryjoin condition '%s' on relation %s. "
                        "For more relaxed rules on join conditions, the relation may be "
                        "marked as viewonly=True." % (self.primaryjoin, self)
                    )
                else:
                    if self._foreign_keys:
                        raise sa_exc.ArgumentError("Could not determine relation direction for "
                            "primaryjoin condition '%s', on relation %s. "
                            "Do the columns in 'foreign_keys' represent only the 'foreign' columns "
                            "in this join condition ?" % (self.primaryjoin, self))
                    else:
                        raise sa_exc.ArgumentError("Could not determine relation direction for "
                            "primaryjoin condition '%s', on relation %s. "
                            "Specify the 'foreign_keys' argument to indicate which columns "
                            "on the relation are foreign." % (self.primaryjoin, self))

            self.synchronize_pairs = eq_pairs

        if self.secondaryjoin:
            sq_pairs = criterion_as_pairs(self.secondaryjoin, consider_as_foreign_keys=self._foreign_keys, any_operator=self.viewonly)
            sq_pairs = [(l, r) for l, r in sq_pairs if (self._col_is_part_of_mappings(l) and self._col_is_part_of_mappings(r)) or r in self._foreign_keys]

            if not sq_pairs:
                if not self.viewonly and criterion_as_pairs(self.secondaryjoin, consider_as_foreign_keys=self._foreign_keys, any_operator=True):
                    raise sa_exc.ArgumentError("Could not locate any equated, locally mapped "
                        "column pairs for secondaryjoin condition '%s' on relation %s. "
                        "For more relaxed rules on join conditions, the "
                        "relation may be marked as viewonly=True." % (self.secondaryjoin, self)
                    )
                else:
                    raise sa_exc.ArgumentError("Could not determine relation direction "
                    "for secondaryjoin condition '%s', on relation %s. "
                    "Specify the foreign_keys argument to indicate which "
                    "columns on the relation are foreign." % (self.secondaryjoin, self))

            self.secondary_synchronize_pairs = sq_pairs
        else:
            self.secondary_synchronize_pairs = None

        self._foreign_keys = util.column_set(r for l, r in self.synchronize_pairs)
        if self.secondary_synchronize_pairs:
            self._foreign_keys.update(r for l, r in self.secondary_synchronize_pairs)

    def _determine_direction(self):
        if self.secondaryjoin is not None:
            self.direction = MANYTOMANY
        elif self._refers_to_parent_table():
            # self referential defaults to ONETOMANY unless the "remote" side is present
            # and does not reference any foreign key columns

            if self.local_remote_pairs:
                remote = [r for l, r in self.local_remote_pairs]
            elif self.remote_side:
                remote = self.remote_side
            else:
                remote = None

            if not remote or self._foreign_keys.\
                                    difference(l for l, r in self.synchronize_pairs).\
                                    intersection(remote):
                self.direction = ONETOMANY
            else:
                self.direction = MANYTOONE

        else:
            foreign_keys = [f for c, f in self.synchronize_pairs]

            parentcols = util.column_set(self.parent.mapped_table.c)
            targetcols = util.column_set(self.mapper.mapped_table.c)

            # fk collection which suggests ONETOMANY.
            onetomany_fk = targetcols.intersection(foreign_keys)

            # fk collection which suggests MANYTOONE.
            manytoone_fk = parentcols.intersection(foreign_keys)
            
            if not onetomany_fk and not manytoone_fk:
                raise sa_exc.ArgumentError(
                    "Can't determine relation direction for relationship '%s' "
                    "- foreign key columns are present in neither the "
                    "parent nor the child's mapped tables" % self )

            elif onetomany_fk and manytoone_fk: 
                # fks on both sides.  do the same
                # test only based on the local side.
                referents = [c for c, f in self.synchronize_pairs]
                onetomany_local = parentcols.intersection(referents)
                manytoone_local = targetcols.intersection(referents)

                if onetomany_local and not manytoone_local:
                    self.direction = ONETOMANY
                elif manytoone_local and not onetomany_local:
                    self.direction = MANYTOONE
            elif onetomany_fk:
                self.direction = ONETOMANY
            elif manytoone_fk:
                self.direction = MANYTOONE
                
            if not self.direction:
                raise sa_exc.ArgumentError(
                    "Can't determine relation direction for relationship '%s' "
                    "- foreign key columns are present in both the parent and "
                    "the child's mapped tables.  Specify 'foreign_keys' "
                    "argument." % self)
        
        if self.cascade.delete_orphan and not self.single_parent and \
            (self.direction is MANYTOMANY or self.direction is MANYTOONE):
            util.warn("On %s, delete-orphan cascade is not supported on a "
                    "many-to-many or many-to-one relationship when single_parent is not set.  "
                    " Set single_parent=True on the relation()." % self)
        
    def _determine_local_remote_pairs(self):
        if not self.local_remote_pairs:
            if self.remote_side:
                if self.direction is MANYTOONE:
                    self.local_remote_pairs = [
                        (r, l) for l, r in
                        criterion_as_pairs(self.primaryjoin, consider_as_referenced_keys=self.remote_side, any_operator=True)
                    ]
                else:
                    self.local_remote_pairs = criterion_as_pairs(self.primaryjoin, consider_as_foreign_keys=self.remote_side, any_operator=True)

                if not self.local_remote_pairs:
                    raise sa_exc.ArgumentError("Relation %s could not determine any local/remote column pairs from remote side argument %r" % (self, self.remote_side))

            else:
                if self.viewonly:
                    eq_pairs = self.synchronize_pairs
                    if self.secondaryjoin:
                        eq_pairs += self.secondary_synchronize_pairs
                else:
                    eq_pairs = criterion_as_pairs(self.primaryjoin, consider_as_foreign_keys=self._foreign_keys, any_operator=True)
                    if self.secondaryjoin:
                        eq_pairs += criterion_as_pairs(self.secondaryjoin, consider_as_foreign_keys=self._foreign_keys, any_operator=True)
                    eq_pairs = [(l, r) for l, r in eq_pairs if self._col_is_part_of_mappings(l) and self._col_is_part_of_mappings(r)]

                if self.direction is MANYTOONE:
                    self.local_remote_pairs = [(r, l) for l, r in eq_pairs]
                else:
                    self.local_remote_pairs = eq_pairs
        elif self.remote_side:
            raise sa_exc.ArgumentError("remote_side argument is redundant against more detailed _local_remote_side argument.")
        
        for l, r in self.local_remote_pairs:

            if self.direction is ONETOMANY and not self._col_is_part_of_mappings(l):
                raise sa_exc.ArgumentError("Local column '%s' is not part of mapping %s.  "
                        "Specify remote_side argument to indicate which column "
                        "lazy join condition should compare against." % (l, self.parent))

            elif self.direction is MANYTOONE and not self._col_is_part_of_mappings(r):
                raise sa_exc.ArgumentError("Remote column '%s' is not part of mapping %s. "
                        "Specify remote_side argument to indicate which column lazy "
                        "join condition should bind." % (r, self.mapper))

        self.local_side, self.remote_side = [util.ordered_column_set(x) for x in zip(*list(self.local_remote_pairs))]


    def _post_init(self):
        if self._should_log_info:
            self.logger.info(str(self) + " setup primary join %s" % self.primaryjoin)
            self.logger.info(str(self) + " setup secondary join %s" % self.secondaryjoin)
            self.logger.info(str(self) + " synchronize pairs [%s]" % ",".join("(%s => %s)" % (l, r) for l, r in self.synchronize_pairs))
            self.logger.info(str(self) + " secondary synchronize pairs [%s]" % ",".join(("(%s => %s)" % (l, r) for l, r in self.secondary_synchronize_pairs or [])))
            self.logger.info(str(self) + " local/remote pairs [%s]" % ",".join("(%s / %s)" % (l, r) for l, r in self.local_remote_pairs))
            self.logger.info(str(self) + " relation direction %s" % self.direction)

        if self.uselist is None and self.direction is MANYTOONE:
            self.uselist = False

        if self.uselist is None:
            self.uselist = True

        if not self.viewonly:
            self._dependency_processor = dependency.create_dependency_processor(self)

        # primary property handler, set up class attributes
        if self.is_primary():
            if self.back_populates:
                self.extension = list(util.to_list(self.extension, default=[]))
                self.extension.append(attributes.GenericBackrefExtension(self.back_populates))
                self._add_reverse_property(self.back_populates)
            
            if self.backref is not None:
                self.backref.compile(self)
        elif not mapper.class_mapper(self.parent.class_, compile=False)._get_property(self.key, raiseerr=False):
            raise sa_exc.ArgumentError("Attempting to assign a new relation '%s' to "
                "a non-primary mapper on class '%s'.  New relations can only be "
                "added to the primary mapper, i.e. the very first "
                "mapper created for class '%s' " % (self.key, self.parent.class_.__name__, self.parent.class_.__name__))
        

    def _refers_to_parent_table(self):
        for c, f in self.synchronize_pairs:
            if c.table is f.table:
                return True
        else:
            return False

    def _is_self_referential(self):
        return self.mapper.common_parent(self.parent)

    def _create_joins(self, source_polymorphic=False, source_selectable=None, dest_polymorphic=False, dest_selectable=None, of_type=None):
        if source_selectable is None:
            if source_polymorphic and self.parent.with_polymorphic:
                source_selectable = self.parent._with_polymorphic_selectable

        aliased = False
        if dest_selectable is None:
            if dest_polymorphic and self.mapper.with_polymorphic:
                dest_selectable = self.mapper._with_polymorphic_selectable
                aliased = True
            else:
                dest_selectable = self.mapper.mapped_table

            if self._is_self_referential() and source_selectable is None:
                dest_selectable = dest_selectable.alias()
                aliased = True
        else:
            aliased = True

        aliased = aliased or bool(source_selectable)

        primaryjoin, secondaryjoin, secondary = self.primaryjoin, self.secondaryjoin, self.secondary
        
        # adjust the join condition for single table inheritance,
        # in the case that the join is to a subclass
        # this is analgous to the "_adjust_for_single_table_inheritance()"
        # method in Query.

        dest_mapper = of_type or self.mapper
        
        single_crit = dest_mapper._single_table_criterion
        if single_crit:
            if secondaryjoin:
                secondaryjoin = secondaryjoin & single_crit
            else:
                primaryjoin = primaryjoin & single_crit
            

        if aliased:
            if secondary:
                secondary = secondary.alias()
                primary_aliasizer = ClauseAdapter(secondary)
                if dest_selectable:
                    secondary_aliasizer = ClauseAdapter(dest_selectable, equivalents=self.mapper._equivalent_columns).chain(primary_aliasizer)
                else:
                    secondary_aliasizer = primary_aliasizer

                if source_selectable:
                    primary_aliasizer = ClauseAdapter(secondary).chain(ClauseAdapter(source_selectable, equivalents=self.parent._equivalent_columns))

                secondaryjoin = secondary_aliasizer.traverse(secondaryjoin)
            else:
                if dest_selectable:
                    primary_aliasizer = ClauseAdapter(dest_selectable, exclude=self.local_side, equivalents=self.mapper._equivalent_columns)
                    if source_selectable:
                        primary_aliasizer.chain(ClauseAdapter(source_selectable, exclude=self.remote_side, equivalents=self.parent._equivalent_columns))
                elif source_selectable:
                    primary_aliasizer = ClauseAdapter(source_selectable, exclude=self.remote_side, equivalents=self.parent._equivalent_columns)

                secondary_aliasizer = None

            primaryjoin = primary_aliasizer.traverse(primaryjoin)
            target_adapter = secondary_aliasizer or primary_aliasizer
            target_adapter.include = target_adapter.exclude = None
        else:
            target_adapter = None

        return (primaryjoin, secondaryjoin, 
                (source_selectable or self.parent.local_table), 
                (dest_selectable or self.mapper.local_table), secondary, target_adapter)

    def _get_join(self, parent, primary=True, secondary=True, polymorphic_parent=True):
        """deprecated.  use primary_join_against(), secondary_join_against(), full_join_against()"""

        pj, sj, source, dest, secondarytable, adapter = self._create_joins(source_polymorphic=polymorphic_parent)

        if primary and secondary:
            return pj & sj
        elif primary:
            return pj
        elif secondary:
            return sj
        else:
            raise AssertionError("illegal condition")

    def register_dependencies(self, uowcommit):
        if not self.viewonly:
            self._dependency_processor.register_dependencies(uowcommit)

    def register_processors(self, uowcommit):
        if not self.viewonly:
            self._dependency_processor.register_processors(uowcommit)

PropertyLoader = RelationProperty
log.class_logger(RelationProperty)

class BackRef(object):
    """Attached to a RelationProperty to indicate a complementary reverse relationship.

    Handles the job of creating the opposite RelationProperty according to configuration.
    
    Alternatively, two explicit RelationProperty objects can be associated bidirectionally
    using the back_populates keyword argument on each.
    
    """

    def __init__(self, key, _prop=None, **kwargs):
        self.key = key
        self.kwargs = kwargs
        self.prop = _prop
        self.extension = attributes.GenericBackrefExtension(self.key)

    def compile(self, prop):
        if self.prop:
            return

        self.prop = prop

        mapper = prop.mapper.primary_mapper()
        if mapper._get_property(self.key, raiseerr=False) is None:
            if prop.secondary:
                pj = self.kwargs.pop('primaryjoin', prop.secondaryjoin)
                sj = self.kwargs.pop('secondaryjoin', prop.primaryjoin)
            else:
                pj = self.kwargs.pop('primaryjoin', prop.primaryjoin)
                sj = self.kwargs.pop('secondaryjoin', None)
                if sj:
                    raise sa_exc.InvalidRequestError(
                        "Can't assign 'secondaryjoin' on a backref against "
                        "a non-secondary relation.")
            
            foreign_keys = self.kwargs.pop('foreign_keys', prop._foreign_keys)
            
            parent = prop.parent.primary_mapper()
            self.kwargs.setdefault('viewonly', prop.viewonly)
            self.kwargs.setdefault('post_update', prop.post_update)

            relation = RelationProperty(parent, prop.secondary, pj, sj, foreign_keys=foreign_keys,
                                      backref=BackRef(prop.key, _prop=prop),
                                      **self.kwargs)

            mapper._configure_property(self.key, relation);

            prop._add_reverse_property(self.key)

        else:
            raise sa_exc.ArgumentError("Error creating backref '%s' on relation '%s': "
                "property of that name exists on mapper '%s'" % (self.key, prop, mapper))

mapper.ColumnProperty = ColumnProperty
mapper.SynonymProperty = SynonymProperty
mapper.ComparableProperty = ComparableProperty
mapper.RelationProperty = RelationProperty
mapper.ConcreteInheritedProperty = ConcreteInheritedProperty
