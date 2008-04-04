# properties.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""MapperProperty implementations.

This is a private module which defines the behavior of
invidual ORM-mapped attributes.
"""

from sqlalchemy import sql, schema, util, exceptions, logging
from sqlalchemy.sql.util import ClauseAdapter, criterion_as_pairs, find_columns
from sqlalchemy.sql import visitors, operators, ColumnElement
from sqlalchemy.orm import mapper, sync, strategies, attributes, dependency, object_mapper
from sqlalchemy.orm import session as sessionlib
from sqlalchemy.orm.mapper import _class_to_mapper
from sqlalchemy.orm.util import CascadeOptions, PropertyAliasedClauses
from sqlalchemy.orm.interfaces import StrategizedProperty, PropComparator, MapperProperty, ONETOMANY, MANYTOONE, MANYTOMANY
from sqlalchemy.exceptions import ArgumentError

__all__ = ('ColumnProperty', 'CompositeProperty', 'SynonymProperty',
           'ComparableProperty', 'PropertyLoader', 'BackRef')


class ColumnProperty(StrategizedProperty):
    """Describes an object attribute that corresponds to a table column."""

    def __init__(self, *columns, **kwargs):
        """The list of `columns` describes a single object
        property. If there are multiple tables joined together for the
        mapper, this list represents the equivalent column as it
        appears across each table.
        """

        self.columns = list(columns)
        self.group = kwargs.pop('group', None)
        self.deferred = kwargs.pop('deferred', False)
        self.comparator = ColumnProperty.ColumnComparator(self)
        if self.deferred:
            self.strategy_class = strategies.DeferredColumnLoader
        else:
            self.strategy_class = strategies.ColumnLoader
        # sanity check
        for col in columns:
            if not isinstance(col, ColumnElement):
                raise ArgumentError('column_property() must be given a ColumnElement as its argument.  Try .label() or .as_scalar() for Selectables to fix this.')

    def do_init(self):
        super(ColumnProperty, self).do_init()
        if len(self.columns) > 1 and self.parent.primary_key.issuperset(self.columns):
            util.warn(
                ("On mapper %s, primary key column '%s' is being combined "
                 "with distinct primary key column '%s' in attribute '%s'.  "
                 "Use explicit properties to give each column its own mapped "
                 "attribute name.") % (str(self.parent), str(self.columns[1]),
                                       str(self.columns[0]), self.key))

    def copy(self):
        return ColumnProperty(deferred=self.deferred, group=self.group, *self.columns)

    def getattr(self, state, column):
        return getattr(state.class_, self.key).impl.get(state)

    def getcommitted(self, state, column):
        return getattr(state.class_, self.key).impl.get_committed_value(state)

    def setattr(self, state, value, column):
        getattr(state.class_, self.key).impl.set(state, value, None)

    def merge(self, session, source, dest, dont_load, _recursive):
        value = attributes.get_as_list(source._state, self.key, passive=True)
        if value:
            setattr(dest, self.key, value[0])
        else:
            # TODO: lazy callable should merge to the new instance
            dest._state.expire_attributes([self.key])

    def get_col_value(self, column, value):
        return value

    class ColumnComparator(PropComparator):
        def clause_element(self):
            return self.prop.columns[0]

        def operate(self, op, *other, **kwargs):
            return op(self.prop.columns[0], *other, **kwargs)

        def reverse_operate(self, op, other, **kwargs):
            col = self.prop.columns[0]
            return op(col._bind_param(other), col, **kwargs)

ColumnProperty.logger = logging.class_logger(ColumnProperty)

class CompositeProperty(ColumnProperty):
    """subclasses ColumnProperty to provide composite type support."""

    def __init__(self, class_, *columns, **kwargs):
        super(CompositeProperty, self).__init__(*columns, **kwargs)
        self.composite_class = class_
        self.comparator = kwargs.pop('comparator', CompositeProperty.Comparator)(self)

    def do_init(self):
        super(ColumnProperty, self).do_init()
        # TODO: similar PK check as ColumnProperty does ?

    def copy(self):
        return CompositeProperty(deferred=self.deferred, group=self.group, composite_class=self.composite_class, *self.columns)

    def getattr(self, state, column):
        obj = getattr(state.class_, self.key).impl.get(state)
        return self.get_col_value(column, obj)

    def getcommitted(self, state, column):
        obj = getattr(state.class_, self.key).impl.get_committed_value(state)
        return self.get_col_value(column, obj)

    def setattr(self, state, value, column):
        # TODO: test coverage for this method
        obj = getattr(state.class_, self.key).impl.get(state)
        if obj is None:
            obj = self.composite_class(*[None for c in self.columns])
            getattr(state.class_, self.key).impl.set(state, obj, None)

        for a, b in zip(self.columns, value.__composite_values__()):
            if a is column:
                setattr(obj, b, value)

    def get_col_value(self, column, value):
        for a, b in zip(self.columns, value.__composite_values__()):
            if a is column:
                return b

    class Comparator(PropComparator):
        def __eq__(self, other):
            if other is None:
                return sql.and_(*[a==None for a in self.prop.columns])
            else:
                return sql.and_(*[a==b for a, b in
                                  zip(self.prop.columns,
                                      other.__composite_values__())])

        def __ne__(self, other):
            return sql.or_(*[a!=b for a, b in
                             zip(self.prop.columns,
                                 other.__composite_values__())])

class SynonymProperty(MapperProperty):
    def __init__(self, name, map_column=None, descriptor=None):
        self.name = name
        self.map_column=map_column
        self.descriptor = descriptor

    def setup(self, querycontext, **kwargs):
        pass

    def create_row_processor(self, selectcontext, mapper, row):
        return (None, None, None)

    def do_init(self):
        class_ = self.parent.class_
        def comparator():
            return self.parent._get_property(self.key, resolve_synonyms=True).comparator
        self.logger.info("register managed attribute %s on class %s" % (self.key, class_.__name__))
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
        sessionlib.register_attribute(class_, self.key, uselist=False, proxy_property=self.descriptor, useobject=False, comparator=comparator)

    def merge(self, session, source, dest, _recursive):
        pass
SynonymProperty.logger = logging.class_logger(SynonymProperty)


class ComparableProperty(MapperProperty):
    """Instruments a Python property for use in query expressions."""

    def __init__(self, comparator_factory, descriptor=None):
        self.descriptor = descriptor
        self.comparator = comparator_factory(self)

    def do_init(self):
        """Set up a proxy to the unmanaged descriptor."""

        class_ = self.parent.class_
        # refactor me
        sessionlib.register_attribute(class_, self.key, uselist=False,
                                      proxy_property=self.descriptor,
                                      useobject=False,
                                      comparator=self.comparator)

    def setup(self, querycontext, **kwargs):
        pass

    def create_row_processor(self, selectcontext, mapper, row):
        return (None, None, None)


class PropertyLoader(StrategizedProperty):
    """Describes an object property that holds a single item or list
    of items that correspond to a related database table.
    """

    def __init__(self, argument, secondary=None, primaryjoin=None, secondaryjoin=None, entity_name=None, foreign_keys=None, foreignkey=None, uselist=None, private=False, association=None, order_by=False, attributeext=None, backref=None, is_backref=False, post_update=False, cascade=None, viewonly=False, lazy=True, collection_class=None, passive_deletes=False, passive_updates=True, remote_side=None, enable_typechecks=True, join_depth=None, strategy_class=None):
        self.uselist = uselist
        self.argument = argument
        self.entity_name = entity_name
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.post_update = post_update
        self.direction = None
        self.viewonly = viewonly
        self.lazy = lazy
        self.foreign_keys = util.to_set(foreign_keys)
        self._legacy_foreignkey = util.to_set(foreignkey)
        if foreignkey:
            util.warn_deprecated('foreignkey option is deprecated; see docs for details')
        self.collection_class = collection_class
        self.passive_deletes = passive_deletes
        self.passive_updates = passive_updates
        self.remote_side = util.to_set(remote_side)
        self.enable_typechecks = enable_typechecks
        self.comparator = PropertyLoader.Comparator(self)
        self.join_depth = join_depth
        
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

        self._reverse_property = None
        
        if cascade is not None:
            self.cascade = CascadeOptions(cascade)
        else:
            if private:
                util.warn_deprecated('private option is deprecated; see docs for details')
                self.cascade = CascadeOptions("all, delete-orphan")
            else:
                self.cascade = CascadeOptions("save-update, merge")

        if self.passive_deletes == 'all' and ("delete" in self.cascade or "delete-orphan" in self.cascade):
            raise exceptions.ArgumentError("Can't set passive_deletes='all' in conjunction with 'delete' or 'delete-orphan' cascade")

        self.association = association
        if association:
            util.warn_deprecated('association option is deprecated; see docs for details')
        self.order_by = order_by
        self.attributeext=attributeext
        if isinstance(backref, str):
            # propigate explicitly sent primary/secondary join conditions to the BackRef object if
            # just a string was sent
            if secondary is not None:
                # reverse primary/secondary in case of a many-to-many
                self.backref = BackRef(backref, primaryjoin=secondaryjoin, secondaryjoin=primaryjoin, passive_updates=self.passive_updates)
            else:
                self.backref = BackRef(backref, primaryjoin=primaryjoin, secondaryjoin=secondaryjoin, passive_updates=self.passive_updates)
        else:
            self.backref = backref
        self.is_backref = is_backref

    class Comparator(PropComparator):
        def __init__(self, prop, of_type=None):
            self.prop = self.property = prop
            if of_type:
                self._of_type = _class_to_mapper(of_type)
        
        def of_type(self, cls):
            return PropertyLoader.Comparator(self.prop, cls)
            
        def __eq__(self, other):
            if other is None:
                if self.prop.direction == ONETOMANY:
                    return ~sql.exists([1], self.prop.primaryjoin)
                else:
                    return self.prop._optimized_compare(None)
            elif self.prop.uselist:
                if not hasattr(other, '__iter__'):
                    raise exceptions.InvalidRequestError("Can only compare a collection to an iterable object.  Use contains().")
                else:
                    j = self.prop.primaryjoin
                    if self.prop.secondaryjoin:
                        j = j & self.prop.secondaryjoin
                    clauses = []
                    for o in other:
                        clauses.append(
                            sql.exists([1], j & sql.and_(*[x==y for (x, y) in zip(self.prop.mapper.primary_key, self.prop.mapper.primary_key_from_instance(o))]))
                        )
                    return sql.and_(*clauses)
            else:
                return self.prop._optimized_compare(other)
        
        def _join_and_criterion(self, criterion=None, **kwargs):
            adapt_against = None

            if getattr(self, '_of_type', None):
                target_mapper = self._of_type
                to_selectable = target_mapper.mapped_table
                adapt_against = to_selectable
            else:
                target_mapper = self.prop.mapper
                to_selectable = None
                adapt_against = None
                
            if self.prop._is_self_referential():
                pj = self.prop.primary_join_against(self.prop.parent, None)
                sj = self.prop.secondary_join_against(self.prop.parent, toselectable=to_selectable)

                pac = PropertyAliasedClauses(self.prop, pj, sj)
                j = pac.primaryjoin
                if pac.secondaryjoin:
                    j = j & pac.secondaryjoin
            else:
                j = self.prop.full_join_against(self.prop.parent, None, toselectable=to_selectable)

            for k in kwargs:
                crit = (getattr(self.prop.mapper.class_, k) == kwargs[k])
                if criterion is None:
                    criterion = crit
                else:
                    criterion = criterion & crit
            
            if criterion:
                if adapt_against:
                    criterion = ClauseAdapter(adapt_against).traverse(criterion)
                if self.prop._is_self_referential():
                    criterion = pac.adapt_clause(criterion)
            
            return j, criterion, to_selectable
            
        def any(self, criterion=None, **kwargs):
            if not self.prop.uselist:
                raise exceptions.InvalidRequestError("'any()' not implemented for scalar attributes. Use has().")
            j, criterion, from_obj = self._join_and_criterion(criterion, **kwargs)

            return sql.exists([1], j & criterion, from_obj=from_obj)

        def has(self, criterion=None, **kwargs):
            if self.prop.uselist:
                raise exceptions.InvalidRequestError("'has()' not implemented for collections.  Use any().")
            j, criterion, from_obj = self._join_and_criterion(criterion, **kwargs)

            return sql.exists([1], j & criterion, from_obj=from_obj)

        def contains(self, other):
            if not self.prop.uselist:
                raise exceptions.InvalidRequestError("'contains' not implemented for scalar attributes.  Use ==")
            clause = self.prop._optimized_compare(other)

            if self.prop.secondaryjoin:
                clause.negation_clause = self._negated_contains_or_equals(other)

            return clause

        def _negated_contains_or_equals(self, other):
            criterion = sql.and_(*[x==y for (x, y) in zip(self.prop.mapper.primary_key, self.prop.mapper.primary_key_from_instance(other))])
            j, criterion, from_obj = self._join_and_criterion(criterion)
            return ~sql.exists([1], j & criterion, from_obj=from_obj)
            
        def __ne__(self, other):
            if other is None:
                if self.prop.direction == MANYTOONE:
                    return sql.or_(*[x!=None for x in self.prop.foreign_keys])
                elif self.prop.uselist:
                    return self.any()
                else:
                    return self.has()

            if self.prop.uselist and not hasattr(other, '__iter__'):
                raise exceptions.InvalidRequestError("Can only compare a collection to an iterable object")

            return self._negated_contains_or_equals(other)

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

    def _optimized_compare(self, value, value_is_parent=False):
        return self._get_strategy(strategies.LazyLoader).lazy_clause(value, reverse_direction=not value_is_parent)

    def private(self):
        return self.cascade.delete_orphan
    private = property(private)

    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key + " (" + str(self.mapper.class_.__name__)  + ")"

    def merge(self, session, source, dest, dont_load, _recursive):
        if not dont_load and self._reverse_property and (source, self._reverse_property) in _recursive:
            return
            
        if not "merge" in self.cascade:
            dest._state.expire_attributes([self.key])
            return

        instances = attributes.get_as_list(source._state, self.key, passive=True)
        if not instances:
            return
        
        if self.uselist:
            dest_list = []
            for current in instances:
                _recursive[(current, self)] = True
                obj = session.merge(current, entity_name=self.mapper.entity_name, dont_load=dont_load, _recursive=_recursive)
                if obj is not None:
                    dest_list.append(obj)
            if dont_load:
                coll = attributes.init_collection(dest, self.key)
                for c in dest_list:
                    coll.append_without_event(c) 
            else:
                getattr(dest.__class__, self.key).impl._set_iterable(dest._state, dest_list)
        else:
            current = instances[0]
            if current is not None:
                _recursive[(current, self)] = True
                obj = session.merge(current, entity_name=self.mapper.entity_name, dont_load=dont_load, _recursive=_recursive)
                if obj is not None:
                    if dont_load:
                        dest.__dict__[self.key] = obj
                    else:
                        setattr(dest, self.key, obj)

    def cascade_iterator(self, type_, state, visited_instances, halt_on=None):
        if not type_ in self.cascade:
            return
        passive = type_ != 'delete' or self.passive_deletes
        mapper = self.mapper.primary_mapper()
        instances = attributes.get_as_list(state, self.key, passive=passive)
        if instances:
            for c in instances:
                if c is not None and c not in visited_instances and (halt_on is None or not halt_on(c)):
                    if not isinstance(c, self.mapper.class_):
                        raise exceptions.AssertionError("Attribute '%s' on class '%s' doesn't handle objects of type '%s'" % (self.key, str(self.parent.class_), str(c.__class__)))
                    visited_instances.add(c)

                    # cascade using the mapper local to this object, so that its individual properties are located
                    instance_mapper = object_mapper(c, entity_name=mapper.entity_name)
                    yield (c, instance_mapper, c._state)

    def _get_target_class(self):
        """Return the target class of the relation, even if the
        property has not been initialized yet.

        """
        if isinstance(self.argument, type):
            return self.argument
        else:
            return self.argument.class_

    def do_init(self):
        self.__determine_targets()
        self.__determine_joins()
        self.__determine_fks()
        self.__determine_direction()
        self.__determine_remote_side()
        self._post_init()

    def __determine_targets(self):
        if isinstance(self.argument, type):
            self.mapper = mapper.class_mapper(self.argument, entity_name=self.entity_name, compile=False)
        elif isinstance(self.argument, mapper.Mapper):
            self.mapper = self.argument
        elif callable(self.argument):
            # accept a callable to suit various deferred-configurational schemes
            self.mapper = mapper.class_mapper(self.argument(), entity_name=self.entity_name, compile=False)
        else:
            raise exceptions.ArgumentError("relation '%s' expects a class or a mapper argument (received: %s)" % (self.key, type(self.argument)))

        if not self.parent.concrete:
            for inheriting in self.parent.iterate_to_root():
                if inheriting is not self.parent and inheriting._get_property(self.key, raiseerr=False):
                    util.warn(
                        ("Warning: relation '%s' on mapper '%s' supercedes "
                         "the same relation on inherited mapper '%s'; this "
                         "can cause dependency issues during flush") %
                        (self.key, self.parent, inheriting))

        self.target = self.mapper.mapped_table
        self.table = self.mapper.mapped_table

        if self.cascade.delete_orphan:
            if self.parent.class_ is self.mapper.class_:
                raise exceptions.ArgumentError("In relationship '%s', can't establish 'delete-orphan' cascade "
                            "rule on a self-referential relationship.  "
                            "You probably want cascade='all', which includes delete cascading but not orphan detection." %(str(self)))
            self.mapper.primary_mapper().delete_orphans.append((self.key, self.parent.class_))

    def __determine_joins(self):
        if self.secondaryjoin is not None and self.secondary is None:
            raise exceptions.ArgumentError("Property '" + self.key + "' specified with secondary join condition but no secondary argument")
        # if join conditions were not specified, figure them out based on foreign keys

        def _search_for_join(mapper, table):
            """find a join between the given mapper's mapped table and the given table.
            will try the mapper's local table first for more specificity, then if not
            found will try the more general mapped table, which in the case of inheritance
            is a join."""
            try:
                return sql.join(mapper.local_table, table)
            except exceptions.ArgumentError, e:
                return sql.join(mapper.mapped_table, table)

        try:
            if self.secondary is not None:
                if self.secondaryjoin is None:
                    self.secondaryjoin = _search_for_join(self.mapper, self.secondary).onclause
                if self.primaryjoin is None:
                    self.primaryjoin = _search_for_join(self.parent, self.secondary).onclause
            else:
                if self.primaryjoin is None:
                    self.primaryjoin = _search_for_join(self.parent, self.target).onclause
        except exceptions.ArgumentError, e:
            raise exceptions.ArgumentError("Could not determine join condition between parent/child tables on relation %s.  "
                        "Specify a 'primaryjoin' expression.  If this is a many-to-many relation, 'secondaryjoin' is needed as well." % (self))


    def __col_is_part_of_mappings(self, column):
        if self.secondary is None:
            return self.parent.mapped_table.c.contains_column(column) or \
                self.target.c.contains_column(column)
        else:
            return self.parent.mapped_table.c.contains_column(column) or \
                self.target.c.contains_column(column) or \
                self.secondary.c.contains_column(column) is not None
        
    def __determine_fks(self):
        if self._legacy_foreignkey and not self._refers_to_parent_table():
            self.foreign_keys = self._legacy_foreignkey

        arg_foreign_keys = self.foreign_keys
        
        eq_pairs = criterion_as_pairs(self.primaryjoin, consider_as_foreign_keys=arg_foreign_keys, any_operator=self.viewonly)
        eq_pairs = [(l, r) for l, r in eq_pairs if self.__col_is_part_of_mappings(l) and self.__col_is_part_of_mappings(r)]

        if not eq_pairs:
            if not self.viewonly and criterion_as_pairs(self.primaryjoin, consider_as_foreign_keys=arg_foreign_keys, any_operator=True):
                raise exceptions.ArgumentError("Could not locate any equated column pairs for primaryjoin condition '%s' on relation %s. "
                    "If no equated pairs exist, the relation must be marked as viewonly=True." % (self.primaryjoin, self)
                )
            else:
                raise exceptions.ArgumentError("Could not determine relation direction for primaryjoin condition '%s', on relation %s. "
                "Specify the foreign_keys argument to indicate which columns on the relation are foreign." % (self.primaryjoin, self))
        
        self.foreign_keys = util.OrderedSet([r for l, r in eq_pairs])
        self._opposite_side = util.OrderedSet([l for l, r in eq_pairs])
        self.synchronize_pairs = eq_pairs
        
        if self.secondaryjoin:
            sq_pairs = criterion_as_pairs(self.secondaryjoin, consider_as_foreign_keys=arg_foreign_keys)
            sq_pairs = [(l, r) for l, r in sq_pairs if self.__col_is_part_of_mappings(l) and self.__col_is_part_of_mappings(r)]
            
            if not sq_pairs:
                if not self.viewonly and criterion_as_pairs(self.secondaryjoin, consider_as_foreign_keys=arg_foreign_keys, any_operator=True):
                    raise exceptions.ArgumentError("Could not locate any equated column pairs for secondaryjoin condition '%s' on relation %s. "
                        "If no equated pairs exist, the relation must be marked as viewonly=True." % (self.secondaryjoin, self)
                    )
                else:
                    raise exceptions.ArgumentError("Could not determine relation direction for secondaryjoin condition '%s', on relation %s. "
                    "Specify the foreign_keys argument to indicate which columns on the relation are foreign." % (self.secondaryjoin, self))

            self.foreign_keys.update([r for l, r in sq_pairs])
            self._opposite_side.update([l for l, r in sq_pairs])
            self.secondary_synchronize_pairs = sq_pairs
        else:
            self.secondary_synchronize_pairs = None
    
    def local_remote_pairs(self):
        return zip(self.local_side, self.remote_side)
    local_remote_pairs = property(local_remote_pairs)
    
    def __determine_remote_side(self):
        if self.remote_side:
            if self.direction is MANYTOONE:
                eq_pairs = criterion_as_pairs(self.primaryjoin, consider_as_referenced_keys=self.remote_side, any_operator=True)
            else:
                eq_pairs = criterion_as_pairs(self.primaryjoin, consider_as_foreign_keys=self.remote_side, any_operator=True)

            if self.secondaryjoin:
                sq_pairs = criterion_as_pairs(self.secondaryjoin, consider_as_foreign_keys=self.foreign_keys, any_operator=True)
                sq_pairs = [(l, r) for l, r in sq_pairs if self.__col_is_part_of_mappings(l) and self.__col_is_part_of_mappings(r)]
                eq_pairs += sq_pairs
        else:
            eq_pairs = zip(self._opposite_side, self.foreign_keys)

        if self.direction is MANYTOONE:
            self.remote_side, self.local_side = [util.OrderedSet(s) for s in zip(*eq_pairs)]
        else:
            self.local_side, self.remote_side = [util.OrderedSet(s) for s in zip(*eq_pairs)]
            
    def __determine_direction(self):
        """Determine our *direction*, i.e. do we represent one to
        many, many to many, etc.
        """

        if self.secondaryjoin is not None:
            self.direction = MANYTOMANY
        elif self._refers_to_parent_table():
            # for a self referential mapper, if the "foreignkey" is a single or composite primary key,
            # then we are "many to one", since the remote site of the relationship identifies a singular entity.
            # otherwise we are "one to many".
            if self._legacy_foreignkey:
                for f in self._legacy_foreignkey:
                    if not f.primary_key:
                        self.direction = ONETOMANY
                    else:
                        self.direction = MANYTOONE

            elif self.remote_side:
                for f in self.foreign_keys:
                    if f in self.remote_side:
                        self.direction = ONETOMANY
                        return
                else:
                    self.direction = MANYTOONE
            else:
                self.direction = ONETOMANY
        else:
            for mappedtable, parenttable in [(self.mapper.mapped_table, self.parent.mapped_table), (self.mapper.local_table, self.parent.local_table)]:
                onetomany = [c for c in self.foreign_keys if mappedtable.c.contains_column(c)]
                manytoone = [c for c in self.foreign_keys if parenttable.c.contains_column(c)]

                if not onetomany and not manytoone:
                    raise exceptions.ArgumentError(
                        "Can't determine relation direction for relationship '%s' "
                        "- foreign key columns are present in neither the "
                        "parent nor the child's mapped tables" %(str(self)))
                elif onetomany and manytoone:
                    continue
                elif onetomany:
                    self.direction = ONETOMANY
                    break
                elif manytoone:
                    self.direction = MANYTOONE
                    break
            else:
                raise exceptions.ArgumentError(
                    "Can't determine relation direction for relationship '%s' "
                    "- foreign key columns are present in both the parent and "
                    "the child's mapped tables.  Specify 'foreign_keys' "
                    "argument." % (str(self)))

    def _post_init(self):
        if logging.is_info_enabled(self.logger):
            self.logger.info(str(self) + " setup primary join %s" % self.primaryjoin)
            self.logger.info(str(self) + " setup secondary join %s" % self.secondaryjoin)
            self.logger.info(str(self) + " synchronize pairs [%s]" % ",".join(["(%s => %s)" % (l, r) for l, r in self.synchronize_pairs]))
            self.logger.info(str(self) + " secondary synchronize pairs [%s]" % ",".join(["(%s => %s)" % (l, r) for l, r in self.secondary_synchronize_pairs or []]))
            self.logger.info(str(self) + " local/remote pairs [%s]" % ",".join(["(%s / %s)" % (l, r) for l, r in self.local_remote_pairs]))
            self.logger.info(str(self) + " relation direction %s" % self.direction)

        if self.uselist is None and self.direction is MANYTOONE:
            self.uselist = False

        if self.uselist is None:
            self.uselist = True

        if not self.viewonly:
            self._dependency_processor = dependency.create_dependency_processor(self)

        # primary property handler, set up class attributes
        if self.is_primary():
            # if a backref name is defined, set up an extension to populate
            # attributes in the other direction
            if self.backref is not None:
                self.attributeext = self.backref.get_extension()

            if self.backref is not None:
                self.backref.compile(self)
        elif not mapper.class_mapper(self.parent.class_, compile=False)._get_property(self.key, raiseerr=False):
            raise exceptions.ArgumentError("Attempting to assign a new relation '%s' to a non-primary mapper on class '%s'.  New relations can only be added to the primary mapper, i.e. the very first mapper created for class '%s' " % (self.key, self.parent.class_.__name__, self.parent.class_.__name__))

        super(PropertyLoader, self).do_init()

    def _refers_to_parent_table(self):
        return self.parent.mapped_table is self.target or self.parent.mapped_table is self.target
    
    def _is_self_referential(self):
        return self.mapper.common_parent(self.parent)
        
    def primary_join_against(self, mapper, selectable=None, toselectable=None):
        return self.__join_against(mapper, selectable, toselectable, True, False)
        
    def secondary_join_against(self, mapper, toselectable=None):
        return self.__join_against(mapper, None, toselectable, False, True)
        
    def full_join_against(self, mapper, selectable=None, toselectable=None):
        return self.__join_against(mapper, selectable, toselectable, True, True)
    
    def __join_against(self, frommapper, fromselectable, toselectable, primary, secondary):
        if fromselectable is None:
            fromselectable = frommapper.local_table
            
        parent_equivalents = frommapper._equivalent_columns
        
        if primary:
            primaryjoin = self.primaryjoin
            
            if fromselectable is not frommapper.local_table:
                if self.direction is ONETOMANY:
                    primaryjoin = ClauseAdapter(fromselectable, exclude=self.foreign_keys, equivalents=parent_equivalents).traverse(primaryjoin)
                elif self.direction is MANYTOONE:
                    primaryjoin = ClauseAdapter(fromselectable, include=self.foreign_keys, equivalents=parent_equivalents).traverse(primaryjoin)
                elif self.secondaryjoin:
                    primaryjoin = ClauseAdapter(fromselectable, exclude=self.foreign_keys, equivalents=parent_equivalents).traverse(primaryjoin)
                
            if secondary:
                secondaryjoin = self.secondaryjoin
                return primaryjoin & secondaryjoin
            else:
                return primaryjoin
        elif secondary:
            return self.secondaryjoin
        else:
            raise AssertionError("illegal condition")
        
    def get_join(self, parent, primary=True, secondary=True, polymorphic_parent=True):
        """deprecated.  use primary_join_against(), secondary_join_against(), full_join_against()"""
        
        if primary and secondary:
            return self.full_join_against(parent, parent.mapped_table)
        elif primary:
            return self.primary_join_against(parent, parent.mapped_table)
        elif secondary:
            return self.secondary_join_against(parent)
        else:
            raise AssertionError("illegal condition")

    def register_dependencies(self, uowcommit):
        if not self.viewonly:
            self._dependency_processor.register_dependencies(uowcommit)

PropertyLoader.logger = logging.class_logger(PropertyLoader)

class BackRef(object):
    """Attached to a PropertyLoader to indicate a complementary reverse relationship.

    Can optionally create the complementing PropertyLoader if one does not exist already."""

    def __init__(self, key, _prop=None, **kwargs):
        self.key = key
        self.kwargs = kwargs
        self.prop = _prop

    def compile(self, prop):
        if self.prop:
            return

        self.prop = prop

        mapper = prop.mapper.primary_mapper()
        if mapper._get_property(self.key, raiseerr=False) is None:
            pj = self.kwargs.pop('primaryjoin', None)
            sj = self.kwargs.pop('secondaryjoin', None)

            parent = prop.parent.primary_mapper()
            self.kwargs.setdefault('viewonly', prop.viewonly)
            self.kwargs.setdefault('post_update', prop.post_update)

            relation = PropertyLoader(parent, prop.secondary, pj, sj,
                                      backref=BackRef(prop.key, _prop=prop),
                                      is_backref=True,
                                      **self.kwargs)

            mapper._compile_property(self.key, relation);

            prop._reverse_property = mapper._get_property(self.key)
            mapper._get_property(self.key)._reverse_property = prop

        else:
            raise exceptions.ArgumentError("Error creating backref '%s' on relation '%s': property of that name exists on mapper '%s'" % (self.key, prop, mapper))

    def get_extension(self):
        """Return an attribute extension to use with this backreference."""

        return attributes.GenericBackrefExtension(self.key)

mapper.ColumnProperty = ColumnProperty
mapper.SynonymProperty = SynonymProperty
mapper.ComparableProperty = ComparableProperty
