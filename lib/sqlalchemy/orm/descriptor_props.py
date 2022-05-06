# orm/descriptor_props.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Descriptor properties are more "auxiliary" properties
that exist as configurational elements, but don't participate
as actively in the load/persist ORM loop.

"""
from __future__ import annotations

from dataclasses import is_dataclass
import inspect
import itertools
import operator
import typing
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

from . import attributes
from . import util as orm_util
from .base import Mapped
from .interfaces import _IntrospectsAnnotations
from .interfaces import _MapsColumns
from .interfaces import MapperProperty
from .interfaces import PropComparator
from .util import _extract_mapped_subtype
from .util import _none_set
from .. import event
from .. import exc as sa_exc
from .. import schema
from .. import sql
from .. import util
from ..sql import expression
from ..sql import operators
from ..util.typing import Protocol

if typing.TYPE_CHECKING:
    from .attributes import InstrumentedAttribute
    from .properties import MappedColumn
    from ..sql._typing import _InfoType
    from ..sql.schema import Column

_T = TypeVar("_T", bound=Any)
_PT = TypeVar("_PT", bound=Any)


class _CompositeClassProto(Protocol):
    def __composite_values__(self) -> Tuple[Any, ...]:
        ...


class DescriptorProperty(MapperProperty[_T]):
    """:class:`.MapperProperty` which proxies access to a
    user-defined descriptor."""

    doc = None

    uses_objects = False
    _links_to_entity = False

    def instrument_class(self, mapper):
        prop = self

        class _ProxyImpl:
            accepts_scalar_loader = False
            load_on_unexpire = True
            collection = False

            @property
            def uses_objects(self):
                return prop.uses_objects

            def __init__(self, key):
                self.key = key

            if hasattr(prop, "get_history"):

                def get_history(
                    self, state, dict_, passive=attributes.PASSIVE_OFF
                ):
                    return prop.get_history(state, dict_, passive)

        if self.descriptor is None:
            desc = getattr(mapper.class_, self.key, None)
            if mapper._is_userland_descriptor(self.key, desc):
                self.descriptor = desc

        if self.descriptor is None:

            def fset(obj, value):
                setattr(obj, self.name, value)

            def fdel(obj):
                delattr(obj, self.name)

            def fget(obj):
                return getattr(obj, self.name)

            self.descriptor = property(fget=fget, fset=fset, fdel=fdel)

        proxy_attr = attributes.create_proxied_attribute(self.descriptor)(
            self.parent.class_,
            self.key,
            self.descriptor,
            lambda: self._comparator_factory(mapper),
            doc=self.doc,
            original_property=self,
        )
        proxy_attr.impl = _ProxyImpl(self.key)
        mapper.class_manager.instrument_attribute(self.key, proxy_attr)


_CompositeAttrType = Union[
    str,
    "Column[_T]",
    "MappedColumn[_T]",
    "InstrumentedAttribute[_T]",
    "Mapped[_T]",
]


class Composite(
    _MapsColumns[_T], _IntrospectsAnnotations, DescriptorProperty[_T]
):
    """Defines a "composite" mapped attribute, representing a collection
    of columns as one attribute.

    :class:`.Composite` is constructed using the :func:`.composite`
    function.

    .. versionchanged:: 2.0 Renamed :class:`_orm.CompositeProperty`
       to :class:`_orm.Composite`.  The old name
       :class:`_orm.CompositeProperty` remains as an alias.

    .. seealso::

        :ref:`mapper_composite`

    """

    composite_class: Union[
        Type[_CompositeClassProto], Callable[..., Type[_CompositeClassProto]]
    ]
    attrs: Tuple[_CompositeAttrType, ...]

    def __init__(
        self,
        class_: Union[None, _CompositeClassProto, _CompositeAttrType] = None,
        *attrs: _CompositeAttrType,
        active_history: bool = False,
        deferred: bool = False,
        group: Optional[str] = None,
        comparator_factory: Optional[Type[Comparator]] = None,
        info: Optional[_InfoType] = None,
    ):
        super().__init__()

        if isinstance(class_, (Mapped, str, sql.ColumnElement)):
            self.attrs = (class_,) + attrs
            # will initialize within declarative_scan
            self.composite_class = None  # type: ignore
        else:
            self.composite_class = class_
            self.attrs = attrs

        self.active_history = active_history
        self.deferred = deferred
        self.group = group
        self.comparator_factory = (
            comparator_factory
            if comparator_factory is not None
            else self.__class__.Comparator
        )
        self._generated_composite_accessor = None
        if info is not None:
            self.info = info

        util.set_creation_order(self)
        self._create_descriptor()

    def instrument_class(self, mapper):
        super().instrument_class(mapper)
        self._setup_event_handlers()

    def _composite_values_from_instance(
        self, value: _CompositeClassProto
    ) -> Tuple[Any, ...]:
        if self._generated_composite_accessor:
            return self._generated_composite_accessor(value)
        else:
            try:
                accessor = value.__composite_values__
            except AttributeError as ae:
                raise sa_exc.InvalidRequestError(
                    f"Composite class {self.composite_class.__name__} is not "
                    f"a dataclass and does not define a __composite_values__()"
                    " method; can't get state"
                ) from ae
            else:
                return accessor()

    def do_init(self):
        """Initialization which occurs after the :class:`.Composite`
        has been associated with its parent mapper.

        """
        self._setup_arguments_on_columns()

    _COMPOSITE_FGET = object()

    def _create_descriptor(self):
        """Create the Python descriptor that will serve as
        the access point on instances of the mapped class.

        """

        def fget(instance):
            dict_ = attributes.instance_dict(instance)
            state = attributes.instance_state(instance)

            if self.key not in dict_:
                # key not present.  Iterate through related
                # attributes, retrieve their values.  This
                # ensures they all load.
                values = [
                    getattr(instance, key) for key in self._attribute_keys
                ]

                # current expected behavior here is that the composite is
                # created on access if the object is persistent or if
                # col attributes have non-None.  This would be better
                # if the composite were created unconditionally,
                # but that would be a behavioral change.
                if self.key not in dict_ and (
                    state.key is not None or not _none_set.issuperset(values)
                ):
                    dict_[self.key] = self.composite_class(*values)
                    state.manager.dispatch.refresh(
                        state, self._COMPOSITE_FGET, [self.key]
                    )

            return dict_.get(self.key, None)

        def fset(instance, value):
            dict_ = attributes.instance_dict(instance)
            state = attributes.instance_state(instance)
            attr = state.manager[self.key]
            previous = dict_.get(self.key, attributes.NO_VALUE)
            for fn in attr.dispatch.set:
                value = fn(state, value, previous, attr.impl)
            dict_[self.key] = value
            if value is None:
                for key in self._attribute_keys:
                    setattr(instance, key, None)
            else:
                for key, value in zip(
                    self._attribute_keys,
                    self._composite_values_from_instance(value),
                ):
                    setattr(instance, key, value)

        def fdel(instance):
            state = attributes.instance_state(instance)
            dict_ = attributes.instance_dict(instance)
            previous = dict_.pop(self.key, attributes.NO_VALUE)
            attr = state.manager[self.key]
            attr.dispatch.remove(state, previous, attr.impl)
            for key in self._attribute_keys:
                setattr(instance, key, None)

        self.descriptor = property(fget, fset, fdel)

    @util.preload_module("sqlalchemy.orm.properties")
    def declarative_scan(
        self, registry, cls, key, annotation, is_dataclass_field
    ):
        MappedColumn = util.preloaded.orm_properties.MappedColumn

        argument = _extract_mapped_subtype(
            annotation,
            cls,
            key,
            MappedColumn,
            self.composite_class is None,
            is_dataclass_field,
        )

        if argument and self.composite_class is None:
            if isinstance(argument, str) or hasattr(
                argument, "__forward_arg__"
            ):
                raise sa_exc.ArgumentError(
                    f"Can't use forward ref {argument} for composite "
                    f"class argument"
                )
            self.composite_class = argument

        if is_dataclass(self.composite_class):
            self._setup_for_dataclass(registry, cls, key)

    @util.preload_module("sqlalchemy.orm.properties")
    @util.preload_module("sqlalchemy.orm.decl_base")
    def _setup_for_dataclass(self, registry, cls, key):
        MappedColumn = util.preloaded.orm_properties.MappedColumn

        decl_base = util.preloaded.orm_decl_base

        insp = inspect.signature(self.composite_class)
        for param, attr in itertools.zip_longest(
            insp.parameters.values(), self.attrs
        ):
            if param is None or attr is None:
                raise sa_exc.ArgumentError(
                    f"number of arguments to {self.composite_class.__name__} "
                    f"class and number of attributes don't match"
                )
            if isinstance(attr, MappedColumn):
                attr.declarative_scan_for_composite(
                    registry, cls, key, param.name, param.annotation
                )
            elif isinstance(attr, schema.Column):
                decl_base._undefer_column_name(param.name, attr)

        if not hasattr(self.composite_class, "__composite_values__"):
            getter = operator.attrgetter(
                *[p.name for p in insp.parameters.values()]
            )
            if len(insp.parameters) == 1:
                self._generated_composite_accessor = lambda obj: (getter(obj),)
            else:
                self._generated_composite_accessor = getter

    @util.memoized_property
    def _comparable_elements(self):
        return [getattr(self.parent.class_, prop.key) for prop in self.props]

    @util.memoized_property
    @util.preload_module("orm.properties")
    def props(self):
        props = []
        MappedColumn = util.preloaded.orm_properties.MappedColumn

        for attr in self.attrs:
            if isinstance(attr, str):
                prop = self.parent.get_property(attr, _configure_mappers=False)
            elif isinstance(attr, schema.Column):
                prop = self.parent._columntoproperty[attr]
            elif isinstance(attr, MappedColumn):
                prop = self.parent._columntoproperty[attr.column]
            elif isinstance(attr, attributes.InstrumentedAttribute):
                prop = attr.property
            else:
                raise sa_exc.ArgumentError(
                    "Composite expects Column objects or mapped "
                    "attributes/attribute names as arguments, got: %r"
                    % (attr,)
                )
            props.append(prop)
        return props

    @property
    @util.preload_module("orm.properties")
    def columns(self):
        MappedColumn = util.preloaded.orm_properties.MappedColumn
        return [
            a.column if isinstance(a, MappedColumn) else a
            for a in self.attrs
            if isinstance(a, (schema.Column, MappedColumn))
        ]

    @property
    def mapper_property_to_assign(self) -> Optional["MapperProperty[_T]"]:
        return self

    @property
    def columns_to_assign(self) -> List[schema.Column]:
        return [c for c in self.columns if c.table is None]

    def _setup_arguments_on_columns(self):
        """Propagate configuration arguments made on this composite
        to the target columns, for those that apply.

        """
        for prop in self.props:
            prop.active_history = self.active_history
            if self.deferred:
                prop.deferred = self.deferred
                prop.strategy_key = (("deferred", True), ("instrument", True))
            prop.group = self.group

    def _setup_event_handlers(self):
        """Establish events that populate/expire the composite attribute."""

        def load_handler(state, context):
            _load_refresh_handler(state, context, None, is_refresh=False)

        def refresh_handler(state, context, to_load):
            # note this corresponds to sqlalchemy.ext.mutable load_attrs()

            if not to_load or (
                {self.key}.union(self._attribute_keys)
            ).intersection(to_load):
                _load_refresh_handler(state, context, to_load, is_refresh=True)

        def _load_refresh_handler(state, context, to_load, is_refresh):
            dict_ = state.dict

            # if context indicates we are coming from the
            # fget() handler, this already set the value; skip the
            # handler here. (other handlers like mutablecomposite will still
            # want to catch it)
            # there's an insufficiency here in that the fget() handler
            # really should not be using the refresh event and there should
            # be some other event that mutablecomposite can subscribe
            # towards for this.

            if (
                not is_refresh or context is self._COMPOSITE_FGET
            ) and self.key in dict_:
                return

            # if column elements aren't loaded, skip.
            # __get__() will initiate a load for those
            # columns
            for k in self._attribute_keys:
                if k not in dict_:
                    return

            dict_[self.key] = self.composite_class(
                *[state.dict[key] for key in self._attribute_keys]
            )

        def expire_handler(state, keys):
            if keys is None or set(self._attribute_keys).intersection(keys):
                state.dict.pop(self.key, None)

        def insert_update_handler(mapper, connection, state):
            """After an insert or update, some columns may be expired due
            to server side defaults, or re-populated due to client side
            defaults.  Pop out the composite value here so that it
            recreates.

            """

            state.dict.pop(self.key, None)

        event.listen(
            self.parent, "after_insert", insert_update_handler, raw=True
        )
        event.listen(
            self.parent, "after_update", insert_update_handler, raw=True
        )
        event.listen(
            self.parent, "load", load_handler, raw=True, propagate=True
        )
        event.listen(
            self.parent, "refresh", refresh_handler, raw=True, propagate=True
        )
        event.listen(
            self.parent, "expire", expire_handler, raw=True, propagate=True
        )

        # TODO: need a deserialize hook here

    @util.memoized_property
    def _attribute_keys(self):
        return [prop.key for prop in self.props]

    def get_history(self, state, dict_, passive=attributes.PASSIVE_OFF):
        """Provided for userland code that uses attributes.get_history()."""

        added = []
        deleted = []

        has_history = False
        for prop in self.props:
            key = prop.key
            hist = state.manager[key].impl.get_history(state, dict_)
            if hist.has_changes():
                has_history = True

            non_deleted = hist.non_deleted()
            if non_deleted:
                added.extend(non_deleted)
            else:
                added.append(None)
            if hist.deleted:
                deleted.extend(hist.deleted)
            else:
                deleted.append(None)

        if has_history:
            return attributes.History(
                [self.composite_class(*added)],
                (),
                [self.composite_class(*deleted)],
            )
        else:
            return attributes.History((), [self.composite_class(*added)], ())

    def _comparator_factory(self, mapper):
        return self.comparator_factory(self, mapper)

    class CompositeBundle(orm_util.Bundle):
        def __init__(self, property_, expr):
            self.property = property_
            super().__init__(property_.key, *expr)

        def create_row_processor(self, query, procs, labels):
            def proc(row):
                return self.property.composite_class(
                    *[proc(row) for proc in procs]
                )

            return proc

    class Comparator(PropComparator[_PT]):
        """Produce boolean, comparison, and other operators for
        :class:`.Composite` attributes.

        See the example in :ref:`composite_operations` for an overview
        of usage , as well as the documentation for :class:`.PropComparator`.

        .. seealso::

            :class:`.PropComparator`

            :class:`.ColumnOperators`

            :ref:`types_operators`

            :attr:`.TypeEngine.comparator_factory`

        """

        # https://github.com/python/mypy/issues/4266
        __hash__ = None  # type: ignore

        @util.memoized_property
        def clauses(self):
            return expression.ClauseList(
                group=False, *self._comparable_elements
            )

        def __clause_element__(self):
            return self.expression

        @util.memoized_property
        def expression(self):
            clauses = self.clauses._annotate(
                {
                    "parententity": self._parententity,
                    "parentmapper": self._parententity,
                    "proxy_key": self.prop.key,
                }
            )
            return Composite.CompositeBundle(self.prop, clauses)

        def _bulk_update_tuples(self, value):
            if isinstance(value, sql.elements.BindParameter):
                value = value.value

            if value is None:
                values = [None for key in self.prop._attribute_keys]
            elif isinstance(value, self.prop.composite_class):
                values = self.prop._composite_values_from_instance(value)
            else:
                raise sa_exc.ArgumentError(
                    "Can't UPDATE composite attribute %s to %r"
                    % (self.prop, value)
                )

            return zip(self._comparable_elements, values)

        @util.memoized_property
        def _comparable_elements(self):
            if self._adapt_to_entity:
                return [
                    getattr(self._adapt_to_entity.entity, prop.key)
                    for prop in self.prop._comparable_elements
                ]
            else:
                return self.prop._comparable_elements

        def __eq__(self, other):
            if other is None:
                values = [None] * len(self.prop._comparable_elements)
            else:
                values = self.prop._composite_values_from_instance(other)
            comparisons = [
                a == b for a, b in zip(self.prop._comparable_elements, values)
            ]
            if self._adapt_to_entity:
                comparisons = [self.adapter(x) for x in comparisons]
            return sql.and_(*comparisons)

        def __ne__(self, other):
            return sql.not_(self.__eq__(other))

    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key


class ConcreteInheritedProperty(DescriptorProperty[_T]):
    """A 'do nothing' :class:`.MapperProperty` that disables
    an attribute on a concrete subclass that is only present
    on the inherited mapper, not the concrete classes' mapper.

    Cases where this occurs include:

    * When the superclass mapper is mapped against a
      "polymorphic union", which includes all attributes from
      all subclasses.
    * When a relationship() is configured on an inherited mapper,
      but not on the subclass mapper.  Concrete mappers require
      that relationship() is configured explicitly on each
      subclass.

    """

    def _comparator_factory(self, mapper):
        comparator_callable = None

        for m in self.parent.iterate_to_root():
            p = m._props[self.key]
            if not isinstance(p, ConcreteInheritedProperty):
                comparator_callable = p.comparator_factory
                break
        return comparator_callable

    def __init__(self):
        super().__init__()

        def warn():
            raise AttributeError(
                "Concrete %s does not implement "
                "attribute %r at the instance level.  Add "
                "this property explicitly to %s."
                % (self.parent, self.key, self.parent)
            )

        class NoninheritedConcreteProp:
            def __set__(s, obj, value):
                warn()

            def __delete__(s, obj):
                warn()

            def __get__(s, obj, owner):
                if obj is None:
                    return self.descriptor
                warn()

        self.descriptor = NoninheritedConcreteProp()


class Synonym(DescriptorProperty[_T]):
    """Denote an attribute name as a synonym to a mapped property,
    in that the attribute will mirror the value and expression behavior
    of another attribute.

    :class:`.Synonym` is constructed using the :func:`_orm.synonym`
    function.

    .. versionchanged:: 2.0 Renamed :class:`_orm.SynonymProperty`
       to :class:`_orm.Synonym`.  The old name
       :class:`_orm.SynonymProperty` remains as an alias.

    .. seealso::

        :ref:`synonyms` - Overview of synonyms

    """

    def __init__(
        self,
        name,
        map_column=None,
        descriptor=None,
        comparator_factory=None,
        doc=None,
        info=None,
    ):
        super().__init__()

        self.name = name
        self.map_column = map_column
        self.descriptor = descriptor
        self.comparator_factory = comparator_factory
        self.doc = doc or (descriptor and descriptor.__doc__) or None
        if info:
            self.info = info

        util.set_creation_order(self)

    @property
    def uses_objects(self):
        return getattr(self.parent.class_, self.name).impl.uses_objects

    # TODO: when initialized, check _proxied_object,
    # emit a warning if its not a column-based property

    @util.memoized_property
    def _proxied_object(self):
        attr = getattr(self.parent.class_, self.name)
        if not hasattr(attr, "property") or not isinstance(
            attr.property, MapperProperty
        ):
            # attribute is a non-MapperProprerty proxy such as
            # hybrid or association proxy
            if isinstance(attr, attributes.QueryableAttribute):
                return attr.comparator
            elif isinstance(attr, operators.ColumnOperators):
                return attr

            raise sa_exc.InvalidRequestError(
                """synonym() attribute "%s.%s" only supports """
                """ORM mapped attributes, got %r"""
                % (self.parent.class_.__name__, self.name, attr)
            )
        return attr.property

    def _comparator_factory(self, mapper):
        prop = self._proxied_object

        if isinstance(prop, MapperProperty):
            if self.comparator_factory:
                comp = self.comparator_factory(prop, mapper)
            else:
                comp = prop.comparator_factory(prop, mapper)
            return comp
        else:
            return prop

    def get_history(self, *arg, **kw):
        attr = getattr(self.parent.class_, self.name)
        return attr.impl.get_history(*arg, **kw)

    @util.preload_module("sqlalchemy.orm.properties")
    def set_parent(self, parent, init):
        properties = util.preloaded.orm_properties

        if self.map_column:
            # implement the 'map_column' option.
            if self.key not in parent.persist_selectable.c:
                raise sa_exc.ArgumentError(
                    "Can't compile synonym '%s': no column on table "
                    "'%s' named '%s'"
                    % (
                        self.name,
                        parent.persist_selectable.description,
                        self.key,
                    )
                )
            elif (
                parent.persist_selectable.c[self.key]
                in parent._columntoproperty
                and parent._columntoproperty[
                    parent.persist_selectable.c[self.key]
                ].key
                == self.name
            ):
                raise sa_exc.ArgumentError(
                    "Can't call map_column=True for synonym %r=%r, "
                    "a ColumnProperty already exists keyed to the name "
                    "%r for column %r"
                    % (self.key, self.name, self.name, self.key)
                )
            p = properties.ColumnProperty(
                parent.persist_selectable.c[self.key]
            )
            parent._configure_property(self.name, p, init=init, setparent=True)
            p._mapped_by_synonym = self.key

        self.parent = parent
