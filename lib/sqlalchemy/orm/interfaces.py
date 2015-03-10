# orm/interfaces.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

Contains various base classes used throughout the ORM.

Defines the now deprecated ORM extension classes as well
as ORM internals.

Other than the deprecated extensions, this module and the
classes within should be considered mostly private.

"""

from __future__ import absolute_import

from .. import exc as sa_exc, util, inspect
from ..sql import operators
from collections import deque
from .base import (ONETOMANY, MANYTOONE, MANYTOMANY,
                   EXT_CONTINUE, EXT_STOP, NOT_EXTENSION)
from .base import _InspectionAttr, _MappedAttribute
from .path_registry import PathRegistry
import collections


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


class MapperProperty(_MappedAttribute, _InspectionAttr):
    """Manage the relationship of a ``Mapper`` to a single class
    attribute, as well as that attribute as it appears on individual
    instances of the class, including attribute instrumentation,
    attribute access, loading behavior, and dependency calculations.

    The most common occurrences of :class:`.MapperProperty` are the
    mapped :class:`.Column`, which is represented in a mapping as
    an instance of :class:`.ColumnProperty`,
    and a reference to another class produced by :func:`.relationship`,
    represented in the mapping as an instance of
    :class:`.RelationshipProperty`.

    """

    cascade = frozenset()
    """The set of 'cascade' attribute names.

    This collection is checked before the 'cascade_iterator' method is called.

    """

    is_property = True

    def setup(self, context, entity, path, adapter, **kwargs):
        """Called by Query for the purposes of constructing a SQL statement.

        Each MapperProperty associated with the target mapper processes the
        statement referenced by the query context, adding columns and/or
        criterion as appropriate.
        """

        pass

    def create_row_processor(self, context, path,
                             mapper, row, adapter):
        """Return a 3-tuple consisting of three row processing functions.

        """
        return None, None, None

    def cascade_iterator(self, type_, state, visited_instances=None,
                         halt_on=None):
        """Iterate through instances related to the given instance for
        a particular 'cascade', starting with this MapperProperty.

        Return an iterator3-tuples (instance, mapper, state).

        Note that the 'cascade' collection on this MapperProperty is
        checked first for the given type before cascade_iterator is called.

        See PropertyLoader for the related instance implementation.
        """

        return iter(())

    def set_parent(self, parent, init):
        self.parent = parent

    def instrument_class(self, mapper):  # pragma: no-coverage
        raise NotImplementedError()

    @util.memoized_property
    def info(self):
        """Info dictionary associated with the object, allowing user-defined
        data to be associated with this :class:`.MapperProperty`.

        The dictionary is generated when first accessed.  Alternatively,
        it can be specified as a constructor argument to the
        :func:`.column_property`, :func:`.relationship`, or :func:`.composite`
        functions.

        .. versionadded:: 0.8  Added support for .info to all
           :class:`.MapperProperty` subclasses.

        .. seealso::

            :attr:`.QueryableAttribute.info`

            :attr:`.SchemaItem.info`

        """
        return {}

    _configure_started = False
    _configure_finished = False

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

        pass

    def post_instrument_class(self, mapper):
        """Perform instrumentation adjustments that need to occur
        after init() has completed.

        """
        pass

    def is_primary(self):
        """Return True if this ``MapperProperty``'s mapper is the
        primary mapper for its class.

        This flag is used to indicate that the ``MapperProperty`` can
        define attribute instrumentation for the class at the class
        level (as opposed to the individual instance level).
        """

        return not self.parent.non_primary

    def merge(self, session, source_state, source_dict, dest_state,
              dest_dict, load, _recursive):
        """Merge the attribute represented by this ``MapperProperty``
        from source to destination object"""

        pass

    def compare(self, operator, value, **kw):
        """Return a compare operation for the columns represented by
        this ``MapperProperty`` to the given value, which may be a
        column value or an instance.  'operator' is an operator from
        the operators module, or from sql.Comparator.

        By default uses the PropComparator attached to this MapperProperty
        under the attribute name "comparator".
        """

        return operator(self.comparator, value)

    def __repr__(self):
        return '<%s at 0x%x; %s>' % (
            self.__class__.__name__,
            id(self), getattr(self, 'key', 'no key'))


class PropComparator(operators.ColumnOperators):
    """Defines boolean, comparison, and other operators for
    :class:`.MapperProperty` objects.

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

        from sqlalchemy.orm.properties import \\
                                ColumnProperty,\\
                                CompositeProperty,\\
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

    def __init__(self, prop, parentmapper, adapt_to_entity=None):
        self.prop = self.property = prop
        self._parentmapper = parentmapper
        self._adapt_to_entity = adapt_to_entity

    def __clause_element__(self):
        raise NotImplementedError("%r" % self)

    def _query_clause_element(self):
        return self.__clause_element__()

    def adapt_to_entity(self, adapt_to_entity):
        """Return a copy of this PropComparator which will use the given
        :class:`.AliasedInsp` to produce corresponding expressions.
        """
        return self.__class__(self.prop, self._parentmapper, adapt_to_entity)

    @property
    def adapter(self):
        """Produce a callable that adapts column expressions
        to suit an aliased version of this comparator.

        """
        if self._adapt_to_entity is None:
            return None
        else:
            return self._adapt_to_entity._adapt_element

    @util.memoized_property
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
        """Redefine this object in terms of a polymorphic subclass.

        Returns a new PropComparator from which further criterion can be
        evaluated.

        e.g.::

            query.join(Company.employees.of_type(Engineer)).\\
               filter(Engineer.name=='foo')

        :param \class_: a class or mapper indicating that criterion will be
            against this specific subclass.


        """

        return self.operate(PropComparator.of_type_op, class_)

    def any(self, criterion=None, **kwargs):
        """Return true if this collection contains any member that meets the
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
        """Return true if this element references a member which meets the
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

    """

    strategy_wildcard_key = None

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
                cls] = strategy = cls(self)
            return strategy

    def _get_strategy_by_cls(self, cls):
        return self._get_strategy(cls._strategy_keys[0])

    def setup(self, context, entity, path, adapter, **kwargs):
        loader = self._get_context_loader(context, path)
        if loader and loader.strategy:
            strat = self._get_strategy(loader.strategy)
        else:
            strat = self.strategy
        strat.setup_query(context, entity, path, loader, adapter, **kwargs)

    def create_row_processor(self, context, path, mapper, row, adapter):
        loader = self._get_context_loader(context, path)
        if loader and loader.strategy:
            strat = self._get_strategy(loader.strategy)
        else:
            strat = self.strategy
        return strat.create_row_processor(context, path, loader,
                                          mapper, row, adapter)

    def do_init(self):
        self._strategies = {}
        self.strategy = self._get_strategy_by_cls(self.strategy_class)

    def post_instrument_class(self, mapper):
        if self.is_primary() and \
                not mapper.class_manager._attr_has_impl(self.key):
            self.strategy.init_class_attribute(mapper)

    _strategies = collections.defaultdict(dict)

    @classmethod
    def strategy_for(cls, **kw):
        def decorate(dec_cls):
            dec_cls._strategy_keys = []
            key = tuple(sorted(kw.items()))
            cls._strategies[cls][key] = dec_cls
            dec_cls._strategy_keys.append(key)
            return dec_cls
        return decorate

    @classmethod
    def _strategy_lookup(cls, *key):
        for prop_cls in cls.__mro__:
            if prop_cls in cls._strategies:
                strategies = cls._strategies[prop_cls]
                try:
                    return strategies[key]
                except KeyError:
                    pass
        raise Exception("can't locate strategy for %s %s" % (cls, key))


class MapperOption(object):
    """Describe a modification to a Query."""

    propagate_to_loaders = False
    """if True, indicate this option should be carried along
    Query object generated by scalar or object lazy loaders.
    """

    def process_query(self, query):
        pass

    def process_query_conditionally(self, query):
        """same as process_query(), except that this option may not
        apply to the given query.

        Used when secondary loaders resend existing options to a new
        Query."""

        self.process_query(query)


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
      Simple column attributes may add their represented column to the
      list of selected columns, *eager loading* properties may add
      ``LEFT OUTER JOIN`` clauses to the statement.

    * It produces "row processor" functions at result fetching time.
      These "row processor" functions populate a particular attribute
      on a particular mapped instance.

    """

    def __init__(self, parent):
        self.parent_property = parent
        self.is_class_level = False
        self.parent = self.parent_property.parent
        self.key = self.parent_property.key

    def init_class_attribute(self, mapper):
        pass

    def setup_query(self, context, entity, path, loadopt, adapter, **kwargs):
        pass

    def create_row_processor(self, context, path, loadopt, mapper,
                             row, adapter):
        """Return row processing functions which fulfill the contract
        specified by MapperProperty.create_row_processor.

        StrategizedProperty delegates its create_row_processor method
        directly to this method. """

        return None, None, None

    def __str__(self):
        return str(self.parent_property)
