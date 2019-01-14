# ext/declarative/api.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Public API functions and helpers for declarative."""


import re
import weakref

from .base import _add_attribute
from .base import _as_declarative
from .base import _declarative_constructor
from .base import _DeferredMapperConfig
from .clsregistry import _class_resolver
from ... import exc
from ... import util
from ...orm import attributes
from ...orm import comparable_property
from ...orm import interfaces
from ...orm import properties
from ...orm import synonym as _orm_synonym
from ...orm.base import _mapper_or_none
from ...orm.util import polymorphic_union
from ...schema import MetaData
from ...schema import Table
from ...util import hybridmethod
from ...util import hybridproperty
from ...util import OrderedDict


def instrument_declarative(cls, registry, metadata):
    """Given a class, configure the class declaratively,
    using the given registry, which can be any dictionary, and
    MetaData object.

    """
    if "_decl_class_registry" in cls.__dict__:
        raise exc.InvalidRequestError(
            "Class %r already has been " "instrumented declaratively" % cls
        )
    cls._decl_class_registry = registry
    cls.metadata = metadata
    _as_declarative(cls, cls.__name__, cls.__dict__)


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


class DeclarativeMeta(type):
    def __init__(cls, classname, bases, dict_):
        if "_decl_class_registry" not in cls.__dict__:
            _as_declarative(cls, classname, cls.__dict__)
        type.__init__(cls, classname, bases, dict_)

    def __setattr__(cls, key, value):
        _add_attribute(cls, key, value)


def synonym_for(name, map_column=False):
    """Decorator that produces an :func:`.orm.synonym` attribute in conjunction
    with a Python descriptor.

    The function being decorated is passed to :func:`.orm.synonym` as the
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

        :func:`.orm.synonym` - the mapper-level function

        :ref:`mapper_hybrids` - The Hybrid Attribute extension provides an
        updated approach to augmenting attribute behavior more flexibly than
        can be achieved with synonyms.

    """

    def decorate(fn):
        return _orm_synonym(name, map_column=map_column, descriptor=fn)

    return decorate


def comparable_using(comparator_factory):
    """Decorator, allow a Python @property to be used in query criteria.

    This is a  decorator front end to
    :func:`~sqlalchemy.orm.comparable_property` that passes
    through the comparator_factory and the function being decorated::

      @comparable_using(MyComparatorType)
      @property
      def prop(self):
          return 'special sauce'

    The regular ``comparable_property()`` is also usable directly in a
    declarative setting and may be convenient for read/write properties::

      prop = comparable_property(MyComparatorType)

    """

    def decorate(fn):
        return comparable_property(comparator_factory, fn)

    return decorate


class declared_attr(interfaces._MappedAttribute, property):
    """Mark a class-level method as representing the definition of
    a mapped property or special declarative member name.

    @declared_attr turns the attribute into a scalar-like
    property that can be invoked from the uninstantiated class.
    Declarative treats attributes specifically marked with
    @declared_attr as returning a construct that is specific
    to mapping or declarative table configuration.  The name
    of the attribute is that of what the non-dynamic version
    of the attribute would be.

    @declared_attr is more often than not applicable to mixins,
    to define relationships that are to be applied to different
    implementors of the class::

        class ProvidesUser(object):
            "A mixin that adds a 'user' relationship to classes."

            @declared_attr
            def user(self):
                return relationship("User")

    It also can be applied to mapped classes, such as to provide
    a "polymorphic" scheme for inheritance::

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

    """

    def __init__(self, fget, cascading=False):
        super(declared_attr, self).__init__(fget)
        self.__doc__ = fget.__doc__
        self._cascading = cascading

    def __get__(desc, self, cls):
        reg = cls.__dict__.get("_sa_declared_attr_reg", None)
        if reg is None:
            if (
                not re.match(r"^__.+__$", desc.fget.__name__)
                and attributes.manager_of_class(cls) is None
            ):
                util.warn(
                    "Unmanaged access of declarative attribute %s from "
                    "non-mapped class %s" % (desc.fget.__name__, cls.__name__)
                )
            return desc.fget(cls)
        elif desc in reg:
            return reg[desc]
        else:
            reg[desc] = obj = desc.fget(cls)
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

            class HasIdMixin(object):
                @declared_attr.cascading
                def id(cls):
                    if has_inherited_table(cls):
                        return Column(
                            ForeignKey('myclass.id'), primary_key=True)
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


def declarative_base(
    bind=None,
    metadata=None,
    mapper=None,
    cls=object,
    name="Base",
    constructor=_declarative_constructor,
    class_registry=None,
    metaclass=DeclarativeMeta,
):
    r"""Construct a base class for declarative class definitions.

    The new base class will be given a metaclass that produces
    appropriate :class:`~sqlalchemy.schema.Table` objects and makes
    the appropriate :func:`~sqlalchemy.orm.mapper` calls based on the
    information provided declaratively in the class and any subclasses
    of the class.

    :param bind: An optional
      :class:`~sqlalchemy.engine.Connectable`, will be assigned
      the ``bind`` attribute on the :class:`~sqlalchemy.schema.MetaData`
      instance.

    :param metadata:
      An optional :class:`~sqlalchemy.schema.MetaData` instance.  All
      :class:`~sqlalchemy.schema.Table` objects implicitly declared by
      subclasses of the base will share this MetaData.  A MetaData instance
      will be created if none is provided.  The
      :class:`~sqlalchemy.schema.MetaData` instance will be available via the
      `metadata` attribute of the generated declarative base class.

    :param mapper:
      An optional callable, defaults to :func:`~sqlalchemy.orm.mapper`. Will
      be used to map subclasses to their Tables.

    :param cls:
      Defaults to :class:`object`. A type to use as the base for the generated
      declarative base class. May be a class or tuple of classes.

    :param name:
      Defaults to ``Base``.  The display name for the generated
      class.  Customizing this is not required, but can improve clarity in
      tracebacks and debugging.

    :param constructor:
      Defaults to
      :func:`~sqlalchemy.ext.declarative.base._declarative_constructor`, an
      __init__ implementation that assigns \**kwargs for declared
      fields and relationships to an instance.  If ``None`` is supplied,
      no __init__ will be provided and construction will fall back to
      cls.__init__ by way of the normal Python semantics.

    :param class_registry: optional dictionary that will serve as the
      registry of class names-> mapped classes when string names
      are used to identify classes inside of :func:`.relationship`
      and others.  Allows two or more declarative base classes
      to share the same registry of class names for simplified
      inter-base relationships.

    :param metaclass:
      Defaults to :class:`.DeclarativeMeta`.  A metaclass or __metaclass__
      compatible callable to use as the meta type of the generated
      declarative base class.

    .. versionchanged:: 1.1 if :paramref:`.declarative_base.cls` is a
         single class (rather than a tuple), the constructed base class will
         inherit its docstring.

    .. seealso::

        :func:`.as_declarative`

    """
    lcl_metadata = metadata or MetaData()
    if bind:
        lcl_metadata.bind = bind

    if class_registry is None:
        class_registry = weakref.WeakValueDictionary()

    bases = not isinstance(cls, tuple) and (cls,) or cls
    class_dict = dict(
        _decl_class_registry=class_registry, metadata=lcl_metadata
    )

    if isinstance(cls, type):
        class_dict["__doc__"] = cls.__doc__

    if constructor:
        class_dict["__init__"] = constructor
    if mapper:
        class_dict["__mapper_cls__"] = mapper

    return metaclass(name, bases, class_dict)


def as_declarative(**kw):
    """
    Class decorator for :func:`.declarative_base`.

    Provides a syntactical shortcut to the ``cls`` argument
    sent to :func:`.declarative_base`, allowing the base class
    to be converted in-place to a "declarative" base::

        from sqlalchemy.ext.declarative import as_declarative

        @as_declarative()
        class Base(object):
            @declared_attr
            def __tablename__(cls):
                return cls.__name__.lower()
            id = Column(Integer, primary_key=True)

        class MyMappedClass(Base):
            # ...

    All keyword arguments passed to :func:`.as_declarative` are passed
    along to :func:`.declarative_base`.

    .. seealso::

        :func:`.declarative_base`

    """

    def decorate(cls):
        kw["cls"] = cls
        kw["name"] = cls.__name__
        return declarative_base(**kw)

    return decorate


class ConcreteBase(object):
    """A helper class for 'concrete' declarative mappings.

    :class:`.ConcreteBase` will use the :func:`.polymorphic_union`
    function automatically, against all tables mapped as a subclass
    to this class.   The function is called via the
    ``__declare_last__()`` function, which is essentially
    a hook for the :meth:`.after_configured` event.

    :class:`.ConcreteBase` produces a mapped
    table for the class itself.  Compare to :class:`.AbstractConcreteBase`,
    which does not.

    Example::

        from sqlalchemy.ext.declarative import ConcreteBase

        class Employee(ConcreteBase, Base):
            __tablename__ = 'employee'
            employee_id = Column(Integer, primary_key=True)
            name = Column(String(50))
            __mapper_args__ = {
                            'polymorphic_identity':'employee',
                            'concrete':True}

        class Manager(Employee):
            __tablename__ = 'manager'
            employee_id = Column(Integer, primary_key=True)
            name = Column(String(50))
            manager_data = Column(String(40))
            __mapper_args__ = {
                            'polymorphic_identity':'manager',
                            'concrete':True}

    .. seealso::

        :class:`.AbstractConcreteBase`

        :ref:`concrete_inheritance`

        :ref:`inheritance_concrete_helpers`


    """

    @classmethod
    def _create_polymorphic_union(cls, mappers):
        return polymorphic_union(
            OrderedDict(
                (mp.polymorphic_identity, mp.local_table) for mp in mappers
            ),
            "type",
            "pjoin",
        )

    @classmethod
    def __declare_first__(cls):
        m = cls.__mapper__
        if m.with_polymorphic:
            return

        mappers = list(m.self_and_descendants)
        pjoin = cls._create_polymorphic_union(mappers)
        m._set_with_polymorphic(("*", pjoin))
        m._set_polymorphic_on(pjoin.c.type)


class AbstractConcreteBase(ConcreteBase):
    """A helper class for 'concrete' declarative mappings.

    :class:`.AbstractConcreteBase` will use the :func:`.polymorphic_union`
    function automatically, against all tables mapped as a subclass
    to this class.   The function is called via the
    ``__declare_last__()`` function, which is essentially
    a hook for the :meth:`.after_configured` event.

    :class:`.AbstractConcreteBase` does produce a mapped class
    for the base class, however it is not persisted to any table; it
    is instead mapped directly to the "polymorphic" selectable directly
    and is only used for selecting.  Compare to :class:`.ConcreteBase`,
    which does create a persisted table for the base class.

    Example::

        from sqlalchemy.ext.declarative import AbstractConcreteBase

        class Employee(AbstractConcreteBase, Base):
            pass

        class Manager(Employee):
            __tablename__ = 'manager'
            employee_id = Column(Integer, primary_key=True)
            name = Column(String(50))
            manager_data = Column(String(40))

            __mapper_args__ = {
                'polymorphic_identity':'manager',
                'concrete':True}

    The abstract base class is handled by declarative in a special way;
    at class configuration time, it behaves like a declarative mixin
    or an ``__abstract__`` base class.   Once classes are configured
    and mappings are produced, it then gets mapped itself, but
    after all of its descendants.  This is a very unique system of mapping
    not found in any other SQLAlchemy system.

    Using this approach, we can specify columns and properties
    that will take place on mapped subclasses, in the way that
    we normally do as in :ref:`declarative_mixins`::

        class Company(Base):
            __tablename__ = 'company'
            id = Column(Integer, primary_key=True)

        class Employee(AbstractConcreteBase, Base):
            employee_id = Column(Integer, primary_key=True)

            @declared_attr
            def company_id(cls):
                return Column(ForeignKey('company.id'))

            @declared_attr
            def company(cls):
                return relationship("Company")

        class Manager(Employee):
            __tablename__ = 'manager'

            name = Column(String(50))
            manager_data = Column(String(40))

            __mapper_args__ = {
                'polymorphic_identity':'manager',
                'concrete':True}

    When we make use of our mappings however, both ``Manager`` and
    ``Employee`` will have an independently usable ``.company`` attribute::

        session.query(Employee).filter(Employee.company.has(id=5))

    .. versionchanged:: 1.0.0 - The mechanics of :class:`.AbstractConcreteBase`
       have been reworked to support relationships established directly
       on the abstract base, without any special configurational steps.

    .. seealso::

        :class:`.ConcreteBase`

        :ref:`concrete_inheritance`

        :ref:`inheritance_concrete_helpers`

    """

    __no_table__ = True

    @classmethod
    def __declare_first__(cls):
        cls._sa_decl_prepare_nocascade()

    @classmethod
    def _sa_decl_prepare_nocascade(cls):
        if getattr(cls, "__mapper__", None):
            return

        to_map = _DeferredMapperConfig.config_for_cls(cls)

        # can't rely on 'self_and_descendants' here
        # since technically an immediate subclass
        # might not be mapped, but a subclass
        # may be.
        mappers = []
        stack = list(cls.__subclasses__())
        while stack:
            klass = stack.pop()
            stack.extend(klass.__subclasses__())
            mn = _mapper_or_none(klass)
            if mn is not None:
                mappers.append(mn)
        pjoin = cls._create_polymorphic_union(mappers)

        # For columns that were declared on the class, these
        # are normally ignored with the "__no_table__" mapping,
        # unless they have a different attribute key vs. col name
        # and are in the properties argument.
        # In that case, ensure we update the properties entry
        # to the correct column from the pjoin target table.
        declared_cols = set(to_map.declared_columns)
        for k, v in list(to_map.properties.items()):
            if v in declared_cols:
                to_map.properties[k] = pjoin.c[v.key]

        to_map.local_table = pjoin

        m_args = to_map.mapper_args_fn or dict

        def mapper_args():
            args = m_args()
            args["polymorphic_on"] = pjoin.c.type
            return args

        to_map.mapper_args_fn = mapper_args

        m = to_map.map()

        for scls in cls.__subclasses__():
            sm = _mapper_or_none(scls)
            if sm and sm.concrete and cls in scls.__bases__:
                sm._set_concrete_base(m)


class DeferredReflection(object):
    """A helper class for construction of mappings based on
    a deferred reflection step.

    Normally, declarative can be used with reflection by
    setting a :class:`.Table` object using autoload=True
    as the ``__table__`` attribute on a declarative class.
    The caveat is that the :class:`.Table` must be fully
    reflected, or at the very least have a primary key column,
    at the point at which a normal declarative mapping is
    constructed, meaning the :class:`.Engine` must be available
    at class declaration time.

    The :class:`.DeferredReflection` mixin moves the construction
    of mappers to be at a later point, after a specific
    method is called which first reflects all :class:`.Table`
    objects created so far.   Classes can define it as such::

        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy.ext.declarative import DeferredReflection
        Base = declarative_base()

        class MyClass(DeferredReflection, Base):
            __tablename__ = 'mytable'

    Above, ``MyClass`` is not yet mapped.   After a series of
    classes have been defined in the above fashion, all tables
    can be reflected and mappings created using
    :meth:`.prepare`::

        engine = create_engine("someengine://...")
        DeferredReflection.prepare(engine)

    The :class:`.DeferredReflection` mixin can be applied to individual
    classes, used as the base for the declarative base itself,
    or used in a custom abstract class.   Using an abstract base
    allows that only a subset of classes to be prepared for a
    particular prepare step, which is necessary for applications
    that use more than one engine.  For example, if an application
    has two engines, you might use two bases, and prepare each
    separately, e.g.::

        class ReflectedOne(DeferredReflection, Base):
            __abstract__ = True

        class ReflectedTwo(DeferredReflection, Base):
            __abstract__ = True

        class MyClass(ReflectedOne):
            __tablename__ = 'mytable'

        class MyOtherClass(ReflectedOne):
            __tablename__ = 'myothertable'

        class YetAnotherClass(ReflectedTwo):
            __tablename__ = 'yetanothertable'

        # ... etc.

    Above, the class hierarchies for ``ReflectedOne`` and
    ``ReflectedTwo`` can be configured separately::

        ReflectedOne.prepare(engine_one)
        ReflectedTwo.prepare(engine_two)

    """

    @classmethod
    def prepare(cls, engine):
        """Reflect all :class:`.Table` objects for all current
        :class:`.DeferredReflection` subclasses"""

        to_map = _DeferredMapperConfig.classes_for_base(cls)
        for thingy in to_map:
            cls._sa_decl_prepare(thingy.local_table, engine)
            thingy.map()
            mapper = thingy.cls.__mapper__
            metadata = mapper.class_.metadata
            for rel in mapper._props.values():
                if (
                    isinstance(rel, properties.RelationshipProperty)
                    and rel.secondary is not None
                ):
                    if isinstance(rel.secondary, Table):
                        cls._reflect_table(rel.secondary, engine)
                    elif isinstance(rel.secondary, _class_resolver):
                        rel.secondary._resolvers += (
                            cls._sa_deferred_table_resolver(engine, metadata),
                        )

    @classmethod
    def _sa_deferred_table_resolver(cls, engine, metadata):
        def _resolve(key):
            t1 = Table(key, metadata)
            cls._reflect_table(t1, engine)
            return t1

        return _resolve

    @classmethod
    def _sa_decl_prepare(cls, local_table, engine):
        # autoload Table, which is already
        # present in the metadata.  This
        # will fill in db-loaded columns
        # into the existing Table object.
        if local_table is not None:
            cls._reflect_table(local_table, engine)

    @classmethod
    def _reflect_table(cls, table, engine):
        Table(
            table.name,
            table.metadata,
            extend_existing=True,
            autoload_replace=False,
            autoload=True,
            autoload_with=engine,
            schema=table.schema,
        )
