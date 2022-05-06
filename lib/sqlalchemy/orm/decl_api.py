# ext/declarative/api.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
"""Public API functions and helpers for declarative."""

from __future__ import annotations

import itertools
import re
import typing
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Mapping
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union
import weakref

from . import attributes
from . import clsregistry
from . import exc as orm_exc
from . import instrumentation
from . import interfaces
from . import mapperlib
from .attributes import InstrumentedAttribute
from .base import _inspect_mapped_class
from .base import Mapped
from .decl_base import _add_attribute
from .decl_base import _as_declarative
from .decl_base import _declarative_constructor
from .decl_base import _DeferredMapperConfig
from .decl_base import _del_attribute
from .decl_base import _mapper
from .descriptor_props import Synonym as _orm_synonym
from .mapper import Mapper
from .. import exc
from .. import inspection
from .. import util
from ..sql.elements import SQLCoreOperations
from ..sql.schema import MetaData
from ..sql.selectable import FromClause
from ..sql.type_api import TypeEngine
from ..util import hybridmethod
from ..util import hybridproperty
from ..util import typing as compat_typing


_T = TypeVar("_T", bound=Any)

_TypeAnnotationMapType = Mapping[Type, Union[Type[TypeEngine], TypeEngine]]


def has_inherited_table(cls):
    """Given a class, return True if any of the classes it inherits from has a
    mapped table, otherwise return False.

    This is used in declarative mixins to build attributes that behave
    differently for the base class vs. a subclass in an inheritance
    hierarchy.

    .. seealso::

        :ref:`decl_mixin_inheritance`

    """
    for class_ in cls.__mro__[1:]:
        if getattr(class_, "__table__", None) is not None:
            return True
    return False


class _DynamicAttributesType(type):
    def __setattr__(cls, key, value):
        if "__mapper__" in cls.__dict__:
            _add_attribute(cls, key, value)
        else:
            type.__setattr__(cls, key, value)

    def __delattr__(cls, key):
        if "__mapper__" in cls.__dict__:
            _del_attribute(cls, key)
        else:
            type.__delattr__(cls, key)


class DeclarativeAttributeIntercept(
    _DynamicAttributesType, inspection.Inspectable["Mapper[Any]"]
):
    """Metaclass that may be used in conjunction with the
    :class:`_orm.DeclarativeBase` class to support addition of class
    attributes dynamically.

    """


class DeclarativeMeta(
    _DynamicAttributesType, inspection.Inspectable["Mapper[Any]"]
):
    metadata: MetaData
    registry: "RegistryType"

    def __init__(
        cls, classname: Any, bases: Any, dict_: Any, **kw: Any
    ) -> None:
        # use cls.__dict__, which can be modified by an
        # __init_subclass__() method (#7900)
        dict_ = cls.__dict__

        # early-consume registry from the initial declarative base,
        # assign privately to not conflict with subclass attributes named
        # "registry"
        reg = getattr(cls, "_sa_registry", None)
        if reg is None:
            reg = dict_.get("registry", None)
            if not isinstance(reg, registry):
                raise exc.InvalidRequestError(
                    "Declarative base class has no 'registry' attribute, "
                    "or registry is not a sqlalchemy.orm.registry() object"
                )
            else:
                cls._sa_registry = reg

        if not cls.__dict__.get("__abstract__", False):
            _as_declarative(reg, cls, dict_)
        type.__init__(cls, classname, bases, dict_)


def synonym_for(name, map_column=False):
    """Decorator that produces an :func:`_orm.synonym`
    attribute in conjunction with a Python descriptor.

    The function being decorated is passed to :func:`_orm.synonym` as the
    :paramref:`.orm.synonym.descriptor` parameter::

        class MyClass(Base):
            __tablename__ = 'my_table'

            id = Column(Integer, primary_key=True)
            _job_status = Column("job_status", String(50))

            @synonym_for("job_status")
            @property
            def job_status(self):
                return "Status: %s" % self._job_status

    The :ref:`hybrid properties <mapper_hybrids>` feature of SQLAlchemy
    is typically preferred instead of synonyms, which is a more legacy
    feature.

    .. seealso::

        :ref:`synonyms` - Overview of synonyms

        :func:`_orm.synonym` - the mapper-level function

        :ref:`mapper_hybrids` - The Hybrid Attribute extension provides an
        updated approach to augmenting attribute behavior more flexibly than
        can be achieved with synonyms.

    """

    def decorate(fn):
        return _orm_synonym(name, map_column=map_column, descriptor=fn)

    return decorate


class declared_attr(interfaces._MappedAttribute[_T]):
    """Mark a class-level method as representing the definition of
    a mapped property or special declarative member name.

    :class:`_orm.declared_attr` is typically applied as a decorator to a class
    level method, turning the attribute into a scalar-like property that can be
    invoked from the uninstantiated class. The Declarative mapping process
    looks for these :class:`_orm.declared_attr` callables as it scans classes,
    and assumes any attribute marked with :class:`_orm.declared_attr` will be a
    callable that will produce an object specific to the Declarative mapping or
    table configuration.

    :class:`_orm.declared_attr` is usually applicable to mixins, to define
    relationships that are to be applied to different implementors of the
    class. It is also used to define :class:`_schema.Column` objects that
    include the :class:`_schema.ForeignKey` construct, as these cannot be
    easily reused across different mappings.  The example below illustrates
    both::

        class ProvidesUser:
            "A mixin that adds a 'user' relationship to classes."

            @declared_attr
            def user_id(self):
                return Column(ForeignKey("user_account.id"))

            @declared_attr
            def user(self):
                return relationship("User")

    :class:`_orm.declared_attr` can also be applied to mapped classes, such as
    to provide a "polymorphic" scheme for inheritance::

        class Employee(Base):
            id = Column(Integer, primary_key=True)
            type = Column(String(50), nullable=False)

            @declared_attr
            def __tablename__(cls):
                return cls.__name__.lower()

            @declared_attr
            def __mapper_args__(cls):
                if cls.__name__ == 'Employee':
                    return {
                            "polymorphic_on":cls.type,
                            "polymorphic_identity":"Employee"
                    }
                else:
                    return {"polymorphic_identity":cls.__name__}

    To use :class:`_orm.declared_attr` inside of a Python dataclass
    as discussed at :ref:`orm_declarative_dataclasses_declarative_table`,
    it may be placed directly inside the field metadata using a lambda::

        @dataclass
        class AddressMixin:
            __sa_dataclass_metadata_key__ = "sa"

            user_id: int = field(
                init=False, metadata={"sa": declared_attr(lambda: Column(ForeignKey("user.id")))}
            )
            user: User = field(
                init=False, metadata={"sa": declared_attr(lambda: relationship(User))}
            )

    :class:`_orm.declared_attr` also may be omitted from this form using a
    lambda directly, as in::

        user: User = field(
            init=False, metadata={"sa": lambda: relationship(User)}
        )

    .. seealso::

        :ref:`orm_mixins_toplevel` - illustrates how to use Declarative Mixins
        which is the primary use case for :class:`_orm.declared_attr`

        :ref:`orm_declarative_dataclasses_mixin` - illustrates special forms
        for use with Python dataclasses

    """  # noqa: E501

    if typing.TYPE_CHECKING:

        def __set__(self, instance, value):
            ...

        def __delete__(self, instance: Any):
            ...

    def __init__(
        self,
        fn: Callable[..., Union[Mapped[_T], SQLCoreOperations[_T]]],
        cascading=False,
    ):
        self.fget = fn
        self._cascading = cascading
        self.__doc__ = fn.__doc__

    def _collect_return_annotation(self) -> Optional[Type[Any]]:
        return util.get_annotations(self.fget).get("return")

    def __get__(self, instance, owner) -> InstrumentedAttribute[_T]:
        # the declared_attr needs to make use of a cache that exists
        # for the span of the declarative scan_attributes() phase.
        # to achieve this we look at the class manager that's configured.
        cls = owner
        manager = attributes.opt_manager_of_class(cls)
        if manager is None:
            if not re.match(r"^__.+__$", self.fget.__name__):
                # if there is no manager at all, then this class hasn't been
                # run through declarative or mapper() at all, emit a warning.
                util.warn(
                    "Unmanaged access of declarative attribute %s from "
                    "non-mapped class %s" % (self.fget.__name__, cls.__name__)
                )
            return self.fget(cls)
        elif manager.is_mapped:
            # the class is mapped, which means we're outside of the declarative
            # scan setup, just run the function.
            return self.fget(cls)

        # here, we are inside of the declarative scan.  use the registry
        # that is tracking the values of these attributes.
        declarative_scan = manager.declarative_scan()
        assert declarative_scan is not None
        reg = declarative_scan.declared_attr_reg

        if self in reg:
            return reg[self]
        else:
            reg[self] = obj = self.fget(cls)
            return obj

    @hybridmethod
    def _stateful(cls, **kw):
        return _stateful_declared_attr(**kw)

    @hybridproperty
    def cascading(cls):
        """Mark a :class:`.declared_attr` as cascading.

        This is a special-use modifier which indicates that a column
        or MapperProperty-based declared attribute should be configured
        distinctly per mapped subclass, within a mapped-inheritance scenario.

        .. warning::

            The :attr:`.declared_attr.cascading` modifier has several
            limitations:

            * The flag **only** applies to the use of :class:`.declared_attr`
              on declarative mixin classes and ``__abstract__`` classes; it
              currently has no effect when used on a mapped class directly.

            * The flag **only** applies to normally-named attributes, e.g.
              not any special underscore attributes such as ``__tablename__``.
              On these attributes it has **no** effect.

            * The flag currently **does not allow further overrides** down
              the class hierarchy; if a subclass tries to override the
              attribute, a warning is emitted and the overridden attribute
              is skipped.  This is a limitation that it is hoped will be
              resolved at some point.

        Below, both MyClass as well as MySubClass will have a distinct
        ``id`` Column object established::

            class HasIdMixin:
                @declared_attr.cascading
                def id(cls):
                    if has_inherited_table(cls):
                        return Column(
                            ForeignKey('myclass.id'), primary_key=True
                        )
                    else:
                        return Column(Integer, primary_key=True)

            class MyClass(HasIdMixin, Base):
                __tablename__ = 'myclass'
                # ...

            class MySubClass(MyClass):
                ""
                # ...

        The behavior of the above configuration is that ``MySubClass``
        will refer to both its own ``id`` column as well as that of
        ``MyClass`` underneath the attribute named ``some_id``.

        .. seealso::

            :ref:`declarative_inheritance`

            :ref:`mixin_inheritance_columns`


        """
        return cls._stateful(cascading=True)


class _stateful_declared_attr(declared_attr):
    def __init__(self, **kw):
        self.kw = kw

    def _stateful(self, **kw):
        new_kw = self.kw.copy()
        new_kw.update(kw)
        return _stateful_declared_attr(**new_kw)

    def __call__(self, fn):
        return declared_attr(fn, **self.kw)


def declarative_mixin(cls):
    """Mark a class as providing the feature of "declarative mixin".

    E.g.::

        from sqlalchemy.orm import declared_attr
        from sqlalchemy.orm import declarative_mixin

        @declarative_mixin
        class MyMixin:

            @declared_attr
            def __tablename__(cls):
                return cls.__name__.lower()

            __table_args__ = {'mysql_engine': 'InnoDB'}
            __mapper_args__= {'always_refresh': True}

            id =  Column(Integer, primary_key=True)

        class MyModel(MyMixin, Base):
            name = Column(String(1000))

    The :func:`_orm.declarative_mixin` decorator currently does not modify
    the given class in any way; it's current purpose is strictly to assist
    the :ref:`Mypy plugin <mypy_toplevel>` in being able to identify
    SQLAlchemy declarative mixin classes when no other context is present.

    .. versionadded:: 1.4.6

    .. seealso::

        :ref:`orm_mixins_toplevel`

        :ref:`mypy_declarative_mixins` - in the
        :ref:`Mypy plugin documentation <mypy_toplevel>`

    """  # noqa: E501

    return cls


def _setup_declarative_base(cls):
    if "metadata" in cls.__dict__:
        metadata = cls.metadata
    else:
        metadata = None

    if "type_annotation_map" in cls.__dict__:
        type_annotation_map = cls.__dict__["type_annotation_map"]
    else:
        type_annotation_map = None

    reg = cls.__dict__.get("registry", None)
    if reg is not None:
        if not isinstance(reg, registry):
            raise exc.InvalidRequestError(
                "Declarative base class has a 'registry' attribute that is "
                "not an instance of sqlalchemy.orm.registry()"
            )
        elif type_annotation_map is not None:
            raise exc.InvalidRequestError(
                "Declarative base class has both a 'registry' attribute and a "
                "type_annotation_map entry.  Per-base type_annotation_maps "
                "are not supported.  Please apply the type_annotation_map "
                "to this registry directly."
            )

    else:
        reg = registry(
            metadata=metadata, type_annotation_map=type_annotation_map
        )
        cls.registry = reg

    cls._sa_registry = reg

    if "metadata" not in cls.__dict__:
        cls.metadata = cls.registry.metadata


class DeclarativeBaseNoMeta(inspection.Inspectable["Mapper"]):
    """Same as :class:`_orm.DeclarativeBase`, but does not use a metaclass
    to intercept new attributes.

    The :class:`_orm.DeclarativeBaseNoMeta` base may be used when use of
    custom metaclasses is desirable.

    .. versionadded:: 2.0


    """

    registry: ClassVar["registry"]
    _sa_registry: ClassVar["registry"]
    metadata: ClassVar[MetaData]
    __mapper__: ClassVar[Mapper]
    __table__: Optional[FromClause]

    if typing.TYPE_CHECKING:

        def __init__(self, **kw: Any):
            ...

    def __init_subclass__(cls) -> None:
        if DeclarativeBaseNoMeta in cls.__bases__:
            _setup_declarative_base(cls)
        else:
            cls._sa_registry.map_declaratively(cls)


class DeclarativeBase(
    inspection.Inspectable["InstanceState"],
    metaclass=DeclarativeAttributeIntercept,
):
    """Base class used for declarative class definitions.

    The :class:`_orm.DeclarativeBase` allows for the creation of new
    declarative bases in such a way that is compatible with type checkers::


        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass


    The above ``Base`` class is now usable as the base for new declarative
    mappings.  The superclass makes use of the ``__init_subclass__()``
    method to set up new classes and metaclasses aren't used.

    When first used, the :class:`_orm.DeclarativeBase` class instantiates a new
    :class:`_orm.registry` to be used with the base, assuming one was not
    provided explicitly. The :class:`_orm.DeclarativeBase` class supports
    class-level attributes which act as parameters for the construction of this
    registry; such as to indicate a specific :class:`_schema.MetaData`
    collection as well as a specific value for
    :paramref:`_orm.registry.type_annotation_map`::

        from typing import Annotation

        from sqlalchemy import BigInteger
        from sqlalchemy import MetaData
        from sqlalchemy import String
        from sqlalchemy.orm import DeclarativeBase

        bigint = Annotation(int, "bigint")
        my_metadata = MetaData()

        class Base(DeclarativeBase):
            metadata = my_metadata
            type_annotation_map = {
                str: String().with_variant(String(255), "mysql", "mariadb"),
                bigint: BigInteger()
            }

    Class-level attributes which may be specified include:

    :param metadata: optional :class:`_schema.MetaData` collection.
     If a :class:`_orm.registry` is constructed automatically, this
     :class:`_schema.MetaData` collection will be used to construct it.
     Otherwise, the local :class:`_schema.MetaData` collection will supercede
     that used by an existing :class:`_orm.registry` passed using the
     :paramref:`_orm.DeclarativeBase.registry` parameter.
    :param type_annotation_map: optional type annotation map that will be
     passed to the :class:`_orm.registry` as
     :paramref:`_orm.registry.type_annotation_map`.
    :param registry: supply a pre-existing :class:`_orm.registry` directly.

    .. versionadded:: 2.0

    """

    registry: ClassVar["registry"]
    _sa_registry: ClassVar["registry"]
    metadata: ClassVar[MetaData]
    __mapper__: ClassVar[Mapper]
    __table__: Optional[FromClause]

    if typing.TYPE_CHECKING:

        def __init__(self, **kw: Any):
            ...

    def __init_subclass__(cls) -> None:
        if DeclarativeBase in cls.__bases__:
            _setup_declarative_base(cls)
        else:
            cls._sa_registry.map_declaratively(cls)


def add_mapped_attribute(target, key, attr):
    """Add a new mapped attribute to an ORM mapped class.

    E.g.::

        add_mapped_attribute(User, "addresses", relationship(Address))

    This may be used for ORM mappings that aren't using a declarative
    metaclass that intercepts attribute set operations.

    .. versionadded:: 2.0


    """
    _add_attribute(target, key, attr)


def declarative_base(
    metadata: Optional[MetaData] = None,
    mapper=None,
    cls=object,
    name="Base",
    class_registry: Optional[clsregistry._ClsRegistryType] = None,
    type_annotation_map: Optional[_TypeAnnotationMapType] = None,
    constructor: Callable[..., None] = _declarative_constructor,
    metaclass=DeclarativeMeta,
) -> Any:
    r"""Construct a base class for declarative class definitions.

    The new base class will be given a metaclass that produces
    appropriate :class:`~sqlalchemy.schema.Table` objects and makes
    the appropriate :class:`_orm.Mapper` calls based on the
    information provided declaratively in the class and any subclasses
    of the class.

    The :func:`_orm.declarative_base` function is a shorthand version
    of using the :meth:`_orm.registry.generate_base`
    method.  That is, the following::

        from sqlalchemy.orm import declarative_base

        Base = declarative_base()

    Is equivalent to::

        from sqlalchemy.orm import registry

        mapper_registry = registry()
        Base = mapper_registry.generate_base()

    See the docstring for :class:`_orm.registry`
    and :meth:`_orm.registry.generate_base`
    for more details.

    .. versionchanged:: 1.4  The :func:`_orm.declarative_base`
       function is now a specialization of the more generic
       :class:`_orm.registry` class.  The function also moves to the
       ``sqlalchemy.orm`` package from the ``declarative.ext`` package.


    :param metadata:
      An optional :class:`~sqlalchemy.schema.MetaData` instance.  All
      :class:`~sqlalchemy.schema.Table` objects implicitly declared by
      subclasses of the base will share this MetaData.  A MetaData instance
      will be created if none is provided.  The
      :class:`~sqlalchemy.schema.MetaData` instance will be available via the
      ``metadata`` attribute of the generated declarative base class.

    :param mapper:
      An optional callable, defaults to :class:`_orm.Mapper`. Will
      be used to map subclasses to their Tables.

    :param cls:
      Defaults to :class:`object`. A type to use as the base for the generated
      declarative base class. May be a class or tuple of classes.

    :param name:
      Defaults to ``Base``.  The display name for the generated
      class.  Customizing this is not required, but can improve clarity in
      tracebacks and debugging.

    :param constructor:
      Specify the implementation for the ``__init__`` function on a mapped
      class that has no ``__init__`` of its own.  Defaults to an
      implementation that assigns \**kwargs for declared
      fields and relationships to an instance.  If ``None`` is supplied,
      no __init__ will be provided and construction will fall back to
      cls.__init__ by way of the normal Python semantics.

    :param class_registry: optional dictionary that will serve as the
      registry of class names-> mapped classes when string names
      are used to identify classes inside of :func:`_orm.relationship`
      and others.  Allows two or more declarative base classes
      to share the same registry of class names for simplified
      inter-base relationships.

    :param type_annotation_map: optional dictionary of Python types to
        SQLAlchemy :class:`_types.TypeEngine` classes or instances.  This
        is used exclusively by the :class:`_orm.MappedColumn` construct
        to produce column types based on annotations within the
        :class:`_orm.Mapped` type.

        .. versionadded:: 2.0

    :param metaclass:
      Defaults to :class:`.DeclarativeMeta`.  A metaclass or __metaclass__
      compatible callable to use as the meta type of the generated
      declarative base class.

    .. seealso::

        :class:`_orm.registry`

    """

    return registry(
        metadata=metadata,
        class_registry=class_registry,
        constructor=constructor,
        type_annotation_map=type_annotation_map,
    ).generate_base(
        mapper=mapper,
        cls=cls,
        name=name,
        metaclass=metaclass,
    )


class registry:
    """Generalized registry for mapping classes.

    The :class:`_orm.registry` serves as the basis for maintaining a collection
    of mappings, and provides configurational hooks used to map classes.

    The three general kinds of mappings supported are Declarative Base,
    Declarative Decorator, and Imperative Mapping.   All of these mapping
    styles may be used interchangeably:

    * :meth:`_orm.registry.generate_base` returns a new declarative base
      class, and is the underlying implementation of the
      :func:`_orm.declarative_base` function.

    * :meth:`_orm.registry.mapped` provides a class decorator that will
      apply declarative mapping to a class without the use of a declarative
      base class.

    * :meth:`_orm.registry.map_imperatively` will produce a
      :class:`_orm.Mapper` for a class without scanning the class for
      declarative class attributes. This method suits the use case historically
      provided by the ``sqlalchemy.orm.mapper()`` classical mapping function,
      which is removed as of SQLAlchemy 2.0.

    .. versionadded:: 1.4

    .. seealso::

        :ref:`orm_mapping_classes_toplevel` - overview of class mapping
        styles.

    """

    def __init__(
        self,
        metadata: Optional[MetaData] = None,
        class_registry: Optional[clsregistry._ClsRegistryType] = None,
        type_annotation_map: Optional[_TypeAnnotationMapType] = None,
        constructor: Callable[..., None] = _declarative_constructor,
    ):
        r"""Construct a new :class:`_orm.registry`

        :param metadata:
          An optional :class:`_schema.MetaData` instance.  All
          :class:`_schema.Table` objects generated using declarative
          table mapping will make use of this :class:`_schema.MetaData`
          collection.  If this argument is left at its default of ``None``,
          a blank :class:`_schema.MetaData` collection is created.

        :param constructor:
          Specify the implementation for the ``__init__`` function on a mapped
          class that has no ``__init__`` of its own.  Defaults to an
          implementation that assigns \**kwargs for declared
          fields and relationships to an instance.  If ``None`` is supplied,
          no __init__ will be provided and construction will fall back to
          cls.__init__ by way of the normal Python semantics.

        :param class_registry: optional dictionary that will serve as the
          registry of class names-> mapped classes when string names
          are used to identify classes inside of :func:`_orm.relationship`
          and others.  Allows two or more declarative base classes
          to share the same registry of class names for simplified
          inter-base relationships.

        :param type_annotation_map: optional dictionary of Python types to
          SQLAlchemy :class:`_types.TypeEngine` classes or instances.  This
          is used exclusively by the :class:`_orm.MappedColumn` construct
          to produce column types based on annotations within the
          :class:`_orm.Mapped` type.

          .. versionadded:: 2.0

        """
        lcl_metadata = metadata or MetaData()

        if class_registry is None:
            class_registry = weakref.WeakValueDictionary()

        self._class_registry = class_registry
        self._managers = weakref.WeakKeyDictionary()
        self._non_primary_mappers = weakref.WeakKeyDictionary()
        self.metadata = lcl_metadata
        self.constructor = constructor
        self.type_annotation_map = {}
        if type_annotation_map is not None:
            self.update_type_annotation_map(type_annotation_map)
        self._dependents = set()
        self._dependencies = set()

        self._new_mappers = False

        with mapperlib._CONFIGURE_MUTEX:
            mapperlib._mapper_registries[self] = True

    def update_type_annotation_map(
        self,
        type_annotation_map: Mapping[
            Type, Union[Type[TypeEngine], TypeEngine]
        ],
    ) -> None:
        """update the :paramref:`_orm.registry.type_annotation_map` with new
        values."""

        self.type_annotation_map.update(
            {
                sub_type: sqltype
                for typ, sqltype in type_annotation_map.items()
                for sub_type in compat_typing.expand_unions(
                    typ, include_union=True, discard_none=True
                )
            }
        )

    @property
    def mappers(self):
        """read only collection of all :class:`_orm.Mapper` objects."""

        return frozenset(manager.mapper for manager in self._managers).union(
            self._non_primary_mappers
        )

    def _set_depends_on(self, registry):
        if registry is self:
            return
        registry._dependents.add(self)
        self._dependencies.add(registry)

    def _flag_new_mapper(self, mapper):
        mapper._ready_for_configure = True
        if self._new_mappers:
            return

        for reg in self._recurse_with_dependents({self}):
            reg._new_mappers = True

    @classmethod
    def _recurse_with_dependents(cls, registries):
        todo = registries
        done = set()
        while todo:
            reg = todo.pop()
            done.add(reg)

            # if yielding would remove dependents, make sure we have
            # them before
            todo.update(reg._dependents.difference(done))
            yield reg

            # if yielding would add dependents, make sure we have them
            # after
            todo.update(reg._dependents.difference(done))

    @classmethod
    def _recurse_with_dependencies(cls, registries):
        todo = registries
        done = set()
        while todo:
            reg = todo.pop()
            done.add(reg)

            # if yielding would remove dependencies, make sure we have
            # them before
            todo.update(reg._dependencies.difference(done))

            yield reg

            # if yielding would remove dependencies, make sure we have
            # them before
            todo.update(reg._dependencies.difference(done))

    def _mappers_to_configure(self):
        return itertools.chain(
            (
                manager.mapper
                for manager in list(self._managers)
                if manager.is_mapped
                and not manager.mapper.configured
                and manager.mapper._ready_for_configure
            ),
            (
                npm
                for npm in list(self._non_primary_mappers)
                if not npm.configured and npm._ready_for_configure
            ),
        )

    def _add_non_primary_mapper(self, np_mapper):
        self._non_primary_mappers[np_mapper] = True

    def _dispose_cls(self, cls):
        clsregistry.remove_class(cls.__name__, cls, self._class_registry)

    def _add_manager(self, manager):
        self._managers[manager] = True
        if manager.is_mapped:
            raise exc.ArgumentError(
                "Class '%s' already has a primary mapper defined. "
                % manager.class_
            )
        assert manager.registry is None
        manager.registry = self

    def configure(self, cascade=False):
        """Configure all as-yet unconfigured mappers in this
        :class:`_orm.registry`.

        The configure step is used to reconcile and initialize the
        :func:`_orm.relationship` linkages between mapped classes, as well as
        to invoke configuration events such as the
        :meth:`_orm.MapperEvents.before_configured` and
        :meth:`_orm.MapperEvents.after_configured`, which may be used by ORM
        extensions or user-defined extension hooks.

        If one or more mappers in this registry contain
        :func:`_orm.relationship` constructs that refer to mapped classes in
        other registries, this registry is said to be *dependent* on those
        registries. In order to configure those dependent registries
        automatically, the :paramref:`_orm.registry.configure.cascade` flag
        should be set to ``True``. Otherwise, if they are not configured, an
        exception will be raised.  The rationale behind this behavior is to
        allow an application to programmatically invoke configuration of
        registries while controlling whether or not the process implicitly
        reaches other registries.

        As an alternative to invoking :meth:`_orm.registry.configure`, the ORM
        function :func:`_orm.configure_mappers` function may be used to ensure
        configuration is complete for all :class:`_orm.registry` objects in
        memory. This is generally simpler to use and also predates the usage of
        :class:`_orm.registry` objects overall. However, this function will
        impact all mappings throughout the running Python process and may be
        more memory/time consuming for an application that has many registries
        in use for different purposes that may not be needed immediately.

        .. seealso::

            :func:`_orm.configure_mappers`


        .. versionadded:: 1.4.0b2

        """
        mapperlib._configure_registries({self}, cascade=cascade)

    def dispose(self, cascade=False):
        """Dispose of all mappers in this :class:`_orm.registry`.

        After invocation, all the classes that were mapped within this registry
        will no longer have class instrumentation associated with them. This
        method is the per-:class:`_orm.registry` analogue to the
        application-wide :func:`_orm.clear_mappers` function.

        If this registry contains mappers that are dependencies of other
        registries, typically via :func:`_orm.relationship` links, then those
        registries must be disposed as well. When such registries exist in
        relation to this one, their :meth:`_orm.registry.dispose` method will
        also be called, if the :paramref:`_orm.registry.dispose.cascade` flag
        is set to ``True``; otherwise, an error is raised if those registries
        were not already disposed.

        .. versionadded:: 1.4.0b2

        .. seealso::

            :func:`_orm.clear_mappers`

        """

        mapperlib._dispose_registries({self}, cascade=cascade)

    def _dispose_manager_and_mapper(self, manager):
        if "mapper" in manager.__dict__:
            mapper = manager.mapper

            mapper._set_dispose_flags()

        class_ = manager.class_
        self._dispose_cls(class_)
        instrumentation._instrumentation_factory.unregister(class_)

    def generate_base(
        self,
        mapper=None,
        cls=object,
        name="Base",
        metaclass=DeclarativeMeta,
    ):
        """Generate a declarative base class.

        Classes that inherit from the returned class object will be
        automatically mapped using declarative mapping.

        E.g.::

            from sqlalchemy.orm import registry

            mapper_registry = registry()

            Base = mapper_registry.generate_base()

            class MyClass(Base):
                __tablename__ = "my_table"
                id = Column(Integer, primary_key=True)

        The above dynamically generated class is equivalent to the
        non-dynamic example below::

            from sqlalchemy.orm import registry
            from sqlalchemy.orm.decl_api import DeclarativeMeta

            mapper_registry = registry()

            class Base(metaclass=DeclarativeMeta):
                __abstract__ = True
                registry = mapper_registry
                metadata = mapper_registry.metadata

                __init__ = mapper_registry.constructor

        The :meth:`_orm.registry.generate_base` method provides the
        implementation for the :func:`_orm.declarative_base` function, which
        creates the :class:`_orm.registry` and base class all at once.

        See the section :ref:`orm_declarative_mapping` for background and
        examples.

        :param mapper:
          An optional callable, defaults to :class:`_orm.Mapper`.
          This function is used to generate new :class:`_orm.Mapper` objects.

        :param cls:
          Defaults to :class:`object`. A type to use as the base for the
          generated declarative base class. May be a class or tuple of classes.

        :param name:
          Defaults to ``Base``.  The display name for the generated
          class.  Customizing this is not required, but can improve clarity in
          tracebacks and debugging.

        :param metaclass:
          Defaults to :class:`.DeclarativeMeta`.  A metaclass or __metaclass__
          compatible callable to use as the meta type of the generated
          declarative base class.

        .. seealso::

            :ref:`orm_declarative_mapping`

            :func:`_orm.declarative_base`

        """
        metadata = self.metadata

        bases = not isinstance(cls, tuple) and (cls,) or cls

        class_dict = dict(registry=self, metadata=metadata)
        if isinstance(cls, type):
            class_dict["__doc__"] = cls.__doc__

        if self.constructor:
            class_dict["__init__"] = self.constructor

        class_dict["__abstract__"] = True
        if mapper:
            class_dict["__mapper_cls__"] = mapper

        if hasattr(cls, "__class_getitem__"):

            def __class_getitem__(cls, key):
                # allow generic classes in py3.9+
                return cls

            class_dict["__class_getitem__"] = __class_getitem__

        return metaclass(name, bases, class_dict)

    def mapped(self, cls):
        """Class decorator that will apply the Declarative mapping process
        to a given class.

        E.g.::

            from sqlalchemy.orm import registry

            mapper_registry = registry()

            @mapper_registry.mapped
            class Foo:
                __tablename__ = 'some_table'

                id = Column(Integer, primary_key=True)
                name = Column(String)

        See the section :ref:`orm_declarative_mapping` for complete
        details and examples.

        :param cls: class to be mapped.

        :return: the class that was passed.

        .. seealso::

            :ref:`orm_declarative_mapping`

            :meth:`_orm.registry.generate_base` - generates a base class
            that will apply Declarative mapping to subclasses automatically
            using a Python metaclass.

        """
        _as_declarative(self, cls, cls.__dict__)
        return cls

    def as_declarative_base(self, **kw):
        """
        Class decorator which will invoke
        :meth:`_orm.registry.generate_base`
        for a given base class.

        E.g.::

            from sqlalchemy.orm import registry

            mapper_registry = registry()

            @mapper_registry.as_declarative_base()
            class Base:
                @declared_attr
                def __tablename__(cls):
                    return cls.__name__.lower()
                id = Column(Integer, primary_key=True)

            class MyMappedClass(Base):
                # ...

        All keyword arguments passed to
        :meth:`_orm.registry.as_declarative_base` are passed
        along to :meth:`_orm.registry.generate_base`.

        """

        def decorate(cls):
            kw["cls"] = cls
            kw["name"] = cls.__name__
            return self.generate_base(**kw)

        return decorate

    def map_declaratively(self, cls):
        """Map a class declaratively.

        In this form of mapping, the class is scanned for mapping information,
        including for columns to be associated with a table, and/or an
        actual table object.

        Returns the :class:`_orm.Mapper` object.

        E.g.::

            from sqlalchemy.orm import registry

            mapper_registry = registry()

            class Foo:
                __tablename__ = 'some_table'

                id = Column(Integer, primary_key=True)
                name = Column(String)

            mapper = mapper_registry.map_declaratively(Foo)

        This function is more conveniently invoked indirectly via either the
        :meth:`_orm.registry.mapped` class decorator or by subclassing a
        declarative metaclass generated from
        :meth:`_orm.registry.generate_base`.

        See the section :ref:`orm_declarative_mapping` for complete
        details and examples.

        :param cls: class to be mapped.

        :return: a :class:`_orm.Mapper` object.

        .. seealso::

            :ref:`orm_declarative_mapping`

            :meth:`_orm.registry.mapped` - more common decorator interface
            to this function.

            :meth:`_orm.registry.map_imperatively`

        """
        return _as_declarative(self, cls, cls.__dict__)

    def map_imperatively(self, class_, local_table=None, **kw):
        r"""Map a class imperatively.

        In this form of mapping, the class is not scanned for any mapping
        information.  Instead, all mapping constructs are passed as
        arguments.

        This method is intended to be fully equivalent to the now-removed
        SQLAlchemy ``mapper()`` function, except that it's in terms of
        a particular registry.

        E.g.::

            from sqlalchemy.orm import registry

            mapper_registry = registry()

            my_table = Table(
                "my_table",
                mapper_registry.metadata,
                Column('id', Integer, primary_key=True)
            )

            class MyClass:
                pass

            mapper_registry.map_imperatively(MyClass, my_table)

        See the section :ref:`orm_imperative_mapping` for complete background
        and usage examples.

        :param class\_: The class to be mapped.  Corresponds to the
         :paramref:`_orm.Mapper.class_` parameter.

        :param local_table: the :class:`_schema.Table` or other
         :class:`_sql.FromClause` object that is the subject of the mapping.
         Corresponds to the
         :paramref:`_orm.Mapper.local_table` parameter.

        :param \**kw: all other keyword arguments are passed to the
         :class:`_orm.Mapper` constructor directly.

        .. seealso::

            :ref:`orm_imperative_mapping`

            :ref:`orm_declarative_mapping`

        """
        return _mapper(self, class_, local_table, kw)


RegistryType = registry


def as_declarative(**kw):
    """
    Class decorator which will adapt a given class into a
    :func:`_orm.declarative_base`.

    This function makes use of the :meth:`_orm.registry.as_declarative_base`
    method, by first creating a :class:`_orm.registry` automatically
    and then invoking the decorator.

    E.g.::

        from sqlalchemy.orm import as_declarative

        @as_declarative()
        class Base:
            @declared_attr
            def __tablename__(cls):
                return cls.__name__.lower()
            id = Column(Integer, primary_key=True)

        class MyMappedClass(Base):
            # ...

    .. seealso::

        :meth:`_orm.registry.as_declarative_base`

    """
    metadata, class_registry = (
        kw.pop("metadata", None),
        kw.pop("class_registry", None),
    )

    return registry(
        metadata=metadata, class_registry=class_registry
    ).as_declarative_base(**kw)


@inspection._inspects(
    DeclarativeMeta, DeclarativeBase, DeclarativeAttributeIntercept
)
def _inspect_decl_meta(cls: Type[Any]) -> Mapper[Any]:
    mp: Mapper[Any] = _inspect_mapped_class(cls)
    if mp is None:
        if _DeferredMapperConfig.has_cls(cls):
            _DeferredMapperConfig.raise_unmapped_for_cls(cls)
            raise orm_exc.UnmappedClassError(
                cls,
                msg="Class %s has a deferred mapping on it.  It is not yet "
                "usable as a mapped class." % orm_exc._safe_cls_name(cls),
            )
    return mp
