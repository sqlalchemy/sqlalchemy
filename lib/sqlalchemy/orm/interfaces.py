# orm/interfaces.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

Contains various base classes used throughout the ORM.

Defines some key base classes prominent within the internals,
as well as the now-deprecated ORM extension classes.

Other than the deprecated extensions, this module and the
classes within are mostly private, though some attributes
are exposed when inspecting mappings.

"""

from __future__ import absolute_import

from .. import util
from ..sql import operators
from .base import (ONETOMANY, MANYTOONE, MANYTOMANY,
                   EXT_CONTINUE, EXT_STOP, NOT_EXTENSION)
from .base import (InspectionAttr, InspectionAttr,
    InspectionAttrInfo, _MappedAttribute)
import collections
from .. import inspect
from . import path_registry

# imported later
MapperExtension = SessionExtension = AttributeExtension = None

__all__ = (
    'AttributeExtension',
    'EXT_CONTINUE',
    'EXT_STOP',
    'ONETOMANY',
    'MANYTOMANY',
    'MANYTOONE',
    'NOT_EXTENSION',
    'LoaderStrategy',
    'MapperExtension',
    'MapperOption',
    'MapperProperty',
    'PropComparator',
    'SessionExtension',
    'StrategizedProperty',
)


class MapperProperty(_MappedAttribute, InspectionAttr, util.MemoizedSlots):
    """Represent a particular class attribute mapped by :class:`.Mapper`.

    The most common occurrences of :class:`.MapperProperty` are the
    mapped :class:`.Column`, which is represented in a mapping as
    an instance of :class:`.ColumnProperty`,
    and a reference to another class produced by :func:`.relationship`,
    represented in the mapping as an instance of
    :class:`.RelationshipProperty`.

    """

    __slots__ = (
        '_configure_started', '_configure_finished', 'parent', 'key',
        'info'
    )

    cascade = frozenset()
    """The set of 'cascade' attribute names.

    This collection is checked before the 'cascade_iterator' method is called.

    The collection typically only applies to a RelationshipProperty.

    """

    is_property = True
    """Part of the InspectionAttr interface; states this object is a
    mapper property.

    """

    def _memoized_attr_info(self):
        """Info dictionary associated with the object, allowing user-defined
        data to be associated with this :class:`.InspectionAttr`.

        The dictionary is generated when first accessed.  Alternatively,
        it can be specified as a constructor argument to the
        :func:`.column_property`, :func:`.relationship`, or :func:`.composite`
        functions.

        .. versionadded:: 0.8  Added support for .info to all
           :class:`.MapperProperty` subclasses.

        .. versionchanged:: 1.0.0 :attr:`.MapperProperty.info` is also
           available on extension types via the
           :attr:`.InspectionAttrInfo.info` attribute, so that it can apply
           to a wider variety of ORM and extension constructs.

        .. seealso::

            :attr:`.QueryableAttribute.info`

            :attr:`.SchemaItem.info`

        """
        return {}

    def setup(self, context, entity, path, adapter, **kwargs):
        """Called by Query for the purposes of constructing a SQL statement.

        Each MapperProperty associated with the target mapper processes the
        statement referenced by the query context, adding columns and/or
        criterion as appropriate.

        """

    def create_row_processor(self, context, path,
                             mapper, result, adapter, populators):
        """Produce row processing functions and append to the given
        set of populators lists.

        """

    def cascade_iterator(self, type_, state, visited_instances=None,
                         halt_on=None):
        """Iterate through instances related to the given instance for
        a particular 'cascade', starting with this MapperProperty.

        Return an iterator3-tuples (instance, mapper, state).

        Note that the 'cascade' collection on this MapperProperty is
        checked first for the given type before cascade_iterator is called.

        This method typically only applies to RelationshipProperty.

        """

        return iter(())

    def set_parent(self, parent, init):
        """Set the parent mapper that references this MapperProperty.

        This method is overridden by some subclasses to perform extra
        setup when the mapper is first known.

        """
        self.parent = parent

    def instrument_class(self, mapper):
        """Hook called by the Mapper to the property to initiate
        instrumentation of the class attribute managed by this
        MapperProperty.

        The MapperProperty here will typically call out to the
        attributes module to set up an InstrumentedAttribute.

        This step is the first of two steps to set up an InstrumentedAttribute,
        and is called early in the mapper setup process.

        The second step is typically the init_class_attribute step,
        called from StrategizedProperty via the post_instrument_class()
        hook.  This step assigns additional state to the InstrumentedAttribute
        (specifically the "impl") which has been determined after the
        MapperProperty has determined what kind of persistence
        management it needs to do (e.g. scalar, object, collection, etc).

        """

    def __init__(self):
        self._configure_started = False
        self._configure_finished = False

    def init(self):
        """Called after all mappers are created to assemble
        relationships between mappers and perform other post-mapper-creation
        initialization steps.

        """
        self._configure_started = True
        self.do_init()
        self._configure_finished = True

    @property
    def class_attribute(self):
        """Return the class-bound descriptor corresponding to this
        :class:`.MapperProperty`.

        This is basically a ``getattr()`` call::

            return getattr(self.parent.class_, self.key)

        I.e. if this :class:`.MapperProperty` were named ``addresses``,
        and the class to which it is mapped is ``User``, this sequence
        is possible::

            >>> from sqlalchemy import inspect
            >>> mapper = inspect(User)
            >>> addresses_property = mapper.attrs.addresses
            >>> addresses_property.class_attribute is User.addresses
            True
            >>> User.addresses.property is addresses_property
            True


        """

        return getattr(self.parent.class_, self.key)

    def do_init(self):
        """Perform subclass-specific initialization post-mapper-creation
        steps.

        This is a template method called by the ``MapperProperty``
        object's init() method.

        """

    def post_instrument_class(self, mapper):
        """Perform instrumentation adjustments that need to occur
        after init() has completed.

        The given Mapper is the Mapper invoking the operation, which
        may not be the same Mapper as self.parent in an inheritance
        scenario; however, Mapper will always at least be a sub-mapper of
        self.parent.

        This method is typically used by StrategizedProperty, which delegates
        it to LoaderStrategy.init_class_attribute() to perform final setup
        on the class-bound InstrumentedAttribute.

        """

    def merge(self, session, source_state, source_dict, dest_state,
              dest_dict, load, _recursive, _resolve_conflict_map):
        """Merge the attribute represented by this ``MapperProperty``
        from source to destination object.

        """

    def __repr__(self):
        return '<%s at 0x%x; %s>' % (
            self.__class__.__name__,
            id(self), getattr(self, 'key', 'no key'))


class PropComparator(operators.ColumnOperators):
    r"""Defines SQL operators for :class:`.MapperProperty` objects.

    SQLAlchemy allows for operators to
    be redefined at both the Core and ORM level.  :class:`.PropComparator`
    is the base class of operator redefinition for ORM-level operations,
    including those of :class:`.ColumnProperty`,
    :class:`.RelationshipProperty`, and :class:`.CompositeProperty`.

    .. note:: With the advent of Hybrid properties introduced in SQLAlchemy
       0.7, as well as Core-level operator redefinition in
       SQLAlchemy 0.8, the use case for user-defined :class:`.PropComparator`
       instances is extremely rare.  See :ref:`hybrids_toplevel` as well
       as :ref:`types_operators`.

    User-defined subclasses of :class:`.PropComparator` may be created. The
    built-in Python comparison and math operator methods, such as
    :meth:`.operators.ColumnOperators.__eq__`,
    :meth:`.operators.ColumnOperators.__lt__`, and
    :meth:`.operators.ColumnOperators.__add__`, can be overridden to provide
    new operator behavior. The custom :class:`.PropComparator` is passed to
    the :class:`.MapperProperty` instance via the ``comparator_factory``
    argument. In each case,
    the appropriate subclass of :class:`.PropComparator` should be used::

        # definition of custom PropComparator subclasses

        from sqlalchemy.orm.properties import \
                                ColumnProperty,\
                                CompositeProperty,\
                                RelationshipProperty

        class MyColumnComparator(ColumnProperty.Comparator):
            def __eq__(self, other):
                return self.__clause_element__() == other

        class MyRelationshipComparator(RelationshipProperty.Comparator):
            def any(self, expression):
                "define the 'any' operation"
                # ...

        class MyCompositeComparator(CompositeProperty.Comparator):
            def __gt__(self, other):
                "redefine the 'greater than' operation"

                return sql.and_(*[a>b for a, b in
                                  zip(self.__clause_element__().clauses,
                                      other.__composite_values__())])


        # application of custom PropComparator subclasses

        from sqlalchemy.orm import column_property, relationship, composite
        from sqlalchemy import Column, String

        class SomeMappedClass(Base):
            some_column = column_property(Column("some_column", String),
                                comparator_factory=MyColumnComparator)

            some_relationship = relationship(SomeOtherClass,
                                comparator_factory=MyRelationshipComparator)

            some_composite = composite(
                    Column("a", String), Column("b", String),
                    comparator_factory=MyCompositeComparator
                )

    Note that for column-level operator redefinition, it's usually
    simpler to define the operators at the Core level, using the
    :attr:`.TypeEngine.comparator_factory` attribute.  See
    :ref:`types_operators` for more detail.

    See also:

    :class:`.ColumnProperty.Comparator`

    :class:`.RelationshipProperty.Comparator`

    :class:`.CompositeProperty.Comparator`

    :class:`.ColumnOperators`

    :ref:`types_operators`

    :attr:`.TypeEngine.comparator_factory`

    """

    __slots__ = 'prop', 'property', '_parententity', '_adapt_to_entity'

    def __init__(self, prop, parentmapper, adapt_to_entity=None):
        self.prop = self.property = prop
        self._parententity = adapt_to_entity or parentmapper
        self._adapt_to_entity = adapt_to_entity

    def __clause_element__(self):
        raise NotImplementedError("%r" % self)

    def _query_clause_element(self):
        return self.__clause_element__()

    def _bulk_update_tuples(self, value):
        return [(self.__clause_element__(), value)]

    def adapt_to_entity(self, adapt_to_entity):
        """Return a copy of this PropComparator which will use the given
        :class:`.AliasedInsp` to produce corresponding expressions.
        """
        return self.__class__(self.prop, self._parententity, adapt_to_entity)

    @property
    def _parentmapper(self):
        """legacy; this is renamed to _parententity to be
        compatible with QueryableAttribute."""
        return inspect(self._parententity).mapper

    @property
    def adapter(self):
        """Produce a callable that adapts column expressions
        to suit an aliased version of this comparator.

        """
        if self._adapt_to_entity is None:
            return None
        else:
            return self._adapt_to_entity._adapt_element

    @property
    def info(self):
        return self.property.info

    @staticmethod
    def any_op(a, b, **kwargs):
        return a.any(b, **kwargs)

    @staticmethod
    def has_op(a, b, **kwargs):
        return a.has(b, **kwargs)

    @staticmethod
    def of_type_op(a, class_):
        return a.of_type(class_)

    def of_type(self, class_):
        r"""Redefine this object in terms of a polymorphic subclass.

        Returns a new PropComparator from which further criterion can be
        evaluated.

        e.g.::

            query.join(Company.employees.of_type(Engineer)).\
               filter(Engineer.name=='foo')

        :param \class_: a class or mapper indicating that criterion will be
            against this specific subclass.

        .. seealso::

            :ref:`inheritance_of_type`

        """

        return self.operate(PropComparator.of_type_op, class_)

    def any(self, criterion=None, **kwargs):
        r"""Return true if this collection contains any member that meets the
        given criterion.

        The usual implementation of ``any()`` is
        :meth:`.RelationshipProperty.Comparator.any`.

        :param criterion: an optional ClauseElement formulated against the
          member class' table or attributes.

        :param \**kwargs: key/value pairs corresponding to member class
          attribute names which will be compared via equality to the
          corresponding values.

        """

        return self.operate(PropComparator.any_op, criterion, **kwargs)

    def has(self, criterion=None, **kwargs):
        r"""Return true if this element references a member which meets the
        given criterion.

        The usual implementation of ``has()`` is
        :meth:`.RelationshipProperty.Comparator.has`.

        :param criterion: an optional ClauseElement formulated against the
          member class' table or attributes.

        :param \**kwargs: key/value pairs corresponding to member class
          attribute names which will be compared via equality to the
          corresponding values.

        """

        return self.operate(PropComparator.has_op, criterion, **kwargs)


class StrategizedProperty(MapperProperty):
    """A MapperProperty which uses selectable strategies to affect
    loading behavior.

    There is a single strategy selected by default.  Alternate
    strategies can be selected at Query time through the usage of
    ``StrategizedOption`` objects via the Query.options() method.

    The mechanics of StrategizedProperty are used for every Query
    invocation for every mapped attribute participating in that Query,
    to determine first how the attribute will be rendered in SQL
    and secondly how the attribute will retrieve a value from a result
    row and apply it to a mapped object.  The routines here are very
    performance-critical.

    """

    __slots__ = (
        '_strategies', 'strategy',
        '_wildcard_token', '_default_path_loader_key'
    )

    strategy_wildcard_key = None

    def _memoized_attr__wildcard_token(self):
        return ("%s:%s" % (
            self.strategy_wildcard_key, path_registry._WILDCARD_TOKEN), )

    def _memoized_attr__default_path_loader_key(self):
        return (
            "loader",
            ("%s:%s" % (
                self.strategy_wildcard_key, path_registry._DEFAULT_TOKEN), )
        )

    def _get_context_loader(self, context, path):
        load = None

        # use EntityRegistry.__getitem__()->PropRegistry here so
        # that the path is stated in terms of our base
        search_path = dict.__getitem__(path, self)

        # search among: exact match, "attr.*", "default" strategy
        # if any.
        for path_key in (
            search_path._loader_key,
            search_path._wildcard_path_loader_key,
            search_path._default_path_loader_key
        ):
            if path_key in context.attributes:
                load = context.attributes[path_key]
                break

        return load

    def _get_strategy(self, key):
        try:
            return self._strategies[key]
        except KeyError:
            cls = self._strategy_lookup(*key)
            self._strategies[key] = self._strategies[
                cls] = strategy = cls(self, key)
            return strategy

    def setup(
            self, context, entity, path, adapter, **kwargs):
        loader = self._get_context_loader(context, path)
        if loader and loader.strategy:
            strat = self._get_strategy(loader.strategy)
        else:
            strat = self.strategy
        strat.setup_query(context, entity, path, loader, adapter, **kwargs)

    def create_row_processor(
            self, context, path, mapper,
            result, adapter, populators):
        loader = self._get_context_loader(context, path)
        if loader and loader.strategy:
            strat = self._get_strategy(loader.strategy)
        else:
            strat = self.strategy
        strat.create_row_processor(
            context, path, loader,
            mapper, result, adapter, populators)

    def do_init(self):
        self._strategies = {}
        self.strategy = self._get_strategy(self.strategy_key)

    def post_instrument_class(self, mapper):
        if not self.parent.non_primary and \
                not mapper.class_manager._attr_has_impl(self.key):
            self.strategy.init_class_attribute(mapper)

    _all_strategies = collections.defaultdict(dict)

    @classmethod
    def strategy_for(cls, **kw):
        def decorate(dec_cls):
            # ensure each subclass of the strategy has its
            # own _strategy_keys collection
            if '_strategy_keys' not in dec_cls.__dict__:
                dec_cls._strategy_keys = []
            key = tuple(sorted(kw.items()))
            cls._all_strategies[cls][key] = dec_cls
            dec_cls._strategy_keys.append(key)
            return dec_cls
        return decorate

    @classmethod
    def _strategy_lookup(cls, *key):
        for prop_cls in cls.__mro__:
            if prop_cls in cls._all_strategies:
                strategies = cls._all_strategies[prop_cls]
                try:
                    return strategies[key]
                except KeyError:
                    pass
        raise Exception("can't locate strategy for %s %s" % (cls, key))


class MapperOption(object):
    """Describe a modification to a Query."""

    propagate_to_loaders = False
    """if True, indicate this option should be carried along
    to "secondary" Query objects produced during lazy loads
    or refresh operations.

    """

    def process_query(self, query):
        """Apply a modification to the given :class:`.Query`."""

    def process_query_conditionally(self, query):
        """same as process_query(), except that this option may not
        apply to the given query.

        This is typically used during a lazy load or scalar refresh
        operation to propagate options stated in the original Query to the
        new Query being used for the load.  It occurs for those options that
        specify propagate_to_loaders=True.

        """

        self.process_query(query)

    def _generate_cache_key(self, path):
        """Used by the baked loader to see if this option can be cached.

        A given MapperOption that returns a cache key must return a key
        that uniquely identifies the complete state of this option, which
        will match any other MapperOption that itself retains the identical
        state.  This includes path options, flags, etc.

        If the MapperOption does not apply to the given path and would
        not affect query results on such a path, it should return None.

        if the MapperOption **does** apply to the give path, however cannot
        produce a safe cache key, it should return False; this will cancel
        caching of the result.   An unsafe cache key is one that includes
        an ad-hoc user object, typically an AliasedClass object.  As these
        are usually created per-query, they don't work as cache keys.


        """

        return None


class LoaderStrategy(object):
    """Describe the loading behavior of a StrategizedProperty object.

    The ``LoaderStrategy`` interacts with the querying process in three
    ways:

    * it controls the configuration of the ``InstrumentedAttribute``
      placed on a class to handle the behavior of the attribute.  this
      may involve setting up class-level callable functions to fire
      off a select operation when the attribute is first accessed
      (i.e. a lazy load)

    * it processes the ``QueryContext`` at statement construction time,
      where it can modify the SQL statement that is being produced.
      For example, simple column attributes will add their represented
      column to the list of selected columns, a joined eager loader
      may establish join clauses to add to the statement.

    * It produces "row processor" functions at result fetching time.
      These "row processor" functions populate a particular attribute
      on a particular mapped instance.

    """

    __slots__ = 'parent_property', 'is_class_level', 'parent', 'key', \
        'strategy_key', 'strategy_opts'

    def __init__(self, parent, strategy_key):
        self.parent_property = parent
        self.is_class_level = False
        self.parent = self.parent_property.parent
        self.key = self.parent_property.key
        self.strategy_key = strategy_key
        self.strategy_opts = dict(strategy_key)

    def init_class_attribute(self, mapper):
        pass

    def setup_query(self, context, entity, path, loadopt, adapter, **kwargs):
        """Establish column and other state for a given QueryContext.

        This method fulfills the contract specified by MapperProperty.setup().

        StrategizedProperty delegates its setup() method
        directly to this method.

        """

    def create_row_processor(self, context, path, loadopt, mapper,
                             result, adapter, populators):
        """Establish row processing functions for a given QueryContext.

        This method fulfills the contract specified by
        MapperProperty.create_row_processor().

        StrategizedProperty delegates its create_row_processor() method
        directly to this method.

        """

    def __str__(self):
        return str(self.parent_property)
