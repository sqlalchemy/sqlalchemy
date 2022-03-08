.. _orm_declarative_styles_toplevel:

==========================
Declarative Mapping Styles
==========================

As introduced at :ref:`orm_declarative_mapping`, the **Declarative Mapping** is
the typical way that mappings are constructed in modern SQLAlchemy.   This
section will provide an overview of forms that may be used for Declarative
mapper configuration.


.. _orm_declarative_generated_base_class:

Using a Generated Base Class
----------------------------

The most common approach is to generate a "base" class using the
:func:`_orm.declarative_base` function::

    from sqlalchemy.orm import declarative_base

    # declarative base class
    Base = declarative_base()


The declarative base class may also be created from an existing
:class:`_orm.registry`, by using the :meth:`_orm.registry.generate_base`
method::

    from sqlalchemy.orm import registry

    reg = registry()

    # declarative base class
    Base = reg.generate_base()

With the declarative base class, new mapped classes are declared as subclasses
of the base::

    from sqlalchemy import Column, Integer, String, ForeignKey
    from sqlalchemy.orm import declarative_base

    # declarative base class
    Base = declarative_base()

    # an example mapping using the base
    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)
        nickname = Column(String)

Above, the :func:`_orm.declarative_base` function returns a new base class from
which new classes to be mapped may inherit from, as above a new mapped
class ``User`` is constructed.

For each subclass constructed, the body of the class then follows the
declarative mapping approach which defines both a :class:`_schema.Table`
as well as a :class:`_orm.Mapper` object behind the scenes which comprise
a full mapping.

.. seealso::

    :ref:`orm_declarative_table_config_toplevel`

    :ref:`orm_declarative_mapper_config_toplevel`


.. _orm_explicit_declarative_base:

Creating an Explicit Base Non-Dynamically (for use with mypy, similar)
----------------------------------------------------------------------

SQLAlchemy includes a :ref:`Mypy plugin <mypy_toplevel>` that automatically
accommodates for the dynamically generated ``Base`` class delivered by
SQLAlchemy functions like :func:`_orm.declarative_base`. For the **SQLAlchemy
1.4 series only**, this plugin works along with a new set of typing stubs
published at `sqlalchemy2-stubs <https://pypi.org/project/sqlalchemy2-stubs>`_.

When this plugin is not in use, or when using other :pep:`484` tools which
may not know how to interpret this class, the declarative base class may
be produced in a fully explicit fashion using the
:class:`_orm.DeclarativeMeta` directly as follows::

    from sqlalchemy.orm import registry
    from sqlalchemy.orm.decl_api import DeclarativeMeta

    mapper_registry = registry()

    class Base(metaclass=DeclarativeMeta):
        __abstract__ = True

        registry = mapper_registry
        metadata = mapper_registry.metadata

        __init__ = mapper_registry.constructor

The above ``Base`` is equivalent to one created using the
:meth:`_orm.registry.generate_base` method and will be fully understood by
type analysis tools without the use of plugins.

.. seealso::

    :ref:`mypy_toplevel` - background on the Mypy plugin which applies the
    above structure automatically when running Mypy.


.. _orm_declarative_decorator:

Declarative Mapping using a Decorator (no declarative base)
------------------------------------------------------------

As an alternative to using the "declarative base" class is to apply
declarative mapping to a class explicitly, using either an imperative technique
similar to that of a "classical" mapping, or more succinctly by using
a decorator.  The :meth:`_orm.registry.mapped` function is a class decorator
that can be applied to any Python class with no hierarchy in place.  The
Python class otherwise is configured in declarative style normally::

    from sqlalchemy import Column, Integer, String, Text, ForeignKey

    from sqlalchemy.orm import registry
    from sqlalchemy.orm import relationship

    mapper_registry = registry()

    @mapper_registry.mapped
    class User:
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address", back_populates="user")

    @mapper_registry.mapped
    class Address:
        __tablename__ = 'address'

        id = Column(Integer, primary_key=True)
        user_id = Column(ForeignKey("user.id"))
        email_address = Column(String)

        user = relationship("User", back_populates="addresses")

Above, the same :class:`_orm.registry` that we'd use to generate a declarative
base class via its :meth:`_orm.registry.generate_base` method may also apply
a declarative-style mapping to a class without using a base.   When using
the above style, the mapping of a particular class will **only** proceed
if the decorator is applied to that class directly.   For inheritance
mappings, the decorator should be applied to each subclass::

    from sqlalchemy.orm import registry
    mapper_registry = registry()

    @mapper_registry.mapped
    class Person:
        __tablename__ = "person"

        person_id = Column(Integer, primary_key=True)
        type = Column(String, nullable=False)

        __mapper_args__ = {

            "polymorphic_on": type,
            "polymorphic_identity": "person"
        }


    @mapper_registry.mapped
    class Employee(Person):
        __tablename__ = "employee"

        person_id = Column(ForeignKey("person.person_id"), primary_key=True)

        __mapper_args__ = {
            "polymorphic_identity": "employee"
        }

Both the "declarative table" and "imperative table" styles of declarative
mapping may be used with the above mapping style.

The decorator form of mapping is particularly useful when combining a
SQLAlchemy declarative mapping with other forms of class declaration, notably
the Python ``dataclasses`` module.  See the next section.

.. _orm_declarative_dataclasses:

Declarative Mapping with Dataclasses and Attrs
----------------------------------------------

The dataclasses_ module, added in Python 3.7, provides a ``@dataclass`` class
decorator to automatically generate boilerplate definitions of ``__init__()``,
``__eq__()``, ``__repr()__``, etc. methods. Another very popular library that does
the same, and much more, is attrs_.  Both libraries make use of class
decorators in order to scan a class for attributes that define the class'
behavior, which are then used to generate methods, documentation, and annotations.

The :meth:`_orm.registry.mapped` class decorator allows the declarative mapping
of a class to occur after the class has been fully constructed, allowing the
class to be processed by other class decorators first.  The ``@dataclass``
and ``@attr.s`` decorators may therefore be applied first before the
ORM mapping process proceeds via the :meth:`_orm.registry.mapped` decorator
or via the :meth:`_orm.registry.map_imperatively` method discussed in a
later section.

Mapping with ``@dataclass`` or ``@attr.s`` may be used in a straightforward
way with :ref:`orm_imperative_table_configuration` style, where the
the :class:`_schema.Table`, which means that it is defined separately and
associated with the class via the ``__table__``.   For dataclasses specifically,
:ref:`orm_declarative_table` is also supported.

.. versionadded:: 1.4.0b2 Added support for full declarative mapping when using
   dataclasses.

When attributes are defined using ``dataclasses``, the ``@dataclass``
decorator consumes them but leaves them in place on the class.
SQLAlchemy's mapping process, when it encounters an attribute that normally
is to be mapped to a :class:`_schema.Column`, checks explicitly if the
attribute is part of a Dataclasses setup, and if so will **replace**
the class-bound dataclass attribute with its usual mapped
properties.  The ``__init__`` method created by ``@dataclass`` is left
intact.   In contrast, the ``@attr.s`` decorator actually removes its
own class-bound attributes after the decorator runs, so that SQLAlchemy's
mapping process takes over these attributes without any issue.

.. versionadded:: 1.4 Added support for direct mapping of Python dataclasses,
   where the :class:`_orm.Mapper` will now detect attributes that are specific
   to the ``@dataclasses`` module and replace them at mapping time, rather
   than skipping them as is the default behavior for any class attribute
   that's not part of the mapping.

.. _orm_declarative_dataclasses_imperative_table:

Example One - Dataclasses with Imperative Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example of a mapping using ``@dataclass`` using
:ref:`orm_imperative_table_configuration` is as follows::

    from __future__ import annotations

    from dataclasses import dataclass
    from dataclasses import field
    from typing import List
    from typing import Optional

    from sqlalchemy import Column
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy import String
    from sqlalchemy import Table
    from sqlalchemy.orm import registry
    from sqlalchemy.orm import relationship

    mapper_registry = registry()


    @mapper_registry.mapped
    @dataclass
    class User:
        __table__ = Table(
            "user",
            mapper_registry.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("fullname", String(50)),
            Column("nickname", String(12)),
        )
        id: int = field(init=False)
        name: Optional[str] = None
        fullname: Optional[str] = None
        nickname: Optional[str] = None
        addresses: List[Address] = field(default_factory=list)

        __mapper_args__ = {   # type: ignore
            "properties" : {
                "addresses": relationship("Address")
            }
        }

    @mapper_registry.mapped
    @dataclass
    class Address:
        __table__ = Table(
            "address",
            mapper_registry.metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer, ForeignKey("user.id")),
            Column("email_address", String(50)),
        )
        id: int = field(init=False)
        user_id: int = field(init=False)
        email_address: Optional[str] = None

In the above example, the ``User.id``, ``Address.id``, and ``Address.user_id``
attributes are defined as ``field(init=False)``. This means that parameters for
these won't be added to ``__init__()`` methods, but
:class:`.Session` will still be able to set them after getting their values
during flush from autoincrement or other default value generator.   To
allow them to be specified in the constructor explicitly, they would instead
be given a default value of ``None``.

For a :func:`_orm.relationship` to be declared separately, it needs to be
specified directly within the :paramref:`_orm.Mapper.properties` dictionary
which itself is specified within the ``__mapper_args__`` dictionary, so that it
is passed to the constructor for :class:`_orm.Mapper`. An alternative to this
approach is in the next example.

.. _orm_declarative_dataclasses_declarative_table:

Example Two - Dataclasses with Declarative Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The fully declarative approach requires that :class:`_schema.Column` objects
are declared as class attributes, which when using dataclasses would conflict
with the dataclass-level attributes.  An approach to combine these together
is to make use of the ``metadata`` attribute on the ``dataclass.field``
object, where SQLAlchemy-specific mapping information may be supplied.
Declarative supports extraction of these parameters when the class
specifies the attribute ``__sa_dataclass_metadata_key__``.  This also
provides a more succinct method of indicating the :func:`_orm.relationship`
association::


    from __future__ import annotations

    from dataclasses import dataclass
    from dataclasses import field
    from typing import List

    from sqlalchemy import Column
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy import String
    from sqlalchemy.orm import registry
    from sqlalchemy.orm import relationship

    mapper_registry = registry()


    @mapper_registry.mapped
    @dataclass
    class User:
        __tablename__ = "user"

        __sa_dataclass_metadata_key__ = "sa"
        id: int = field(
            init=False, metadata={"sa": Column(Integer, primary_key=True)}
        )
        name: str = field(default=None, metadata={"sa": Column(String(50))})
        fullname: str = field(default=None, metadata={"sa": Column(String(50))})
        nickname: str = field(default=None, metadata={"sa": Column(String(12))})
        addresses: List[Address] = field(
            default_factory=list, metadata={"sa": relationship("Address")}
        )


    @mapper_registry.mapped
    @dataclass
    class Address:
        __tablename__ = "address"
        __sa_dataclass_metadata_key__ = "sa"
        id: int = field(
            init=False, metadata={"sa": Column(Integer, primary_key=True)}
        )
        user_id: int = field(
            init=False, metadata={"sa": Column(ForeignKey("user.id"))}
        )
        email_address: str = field(
            default=None, metadata={"sa": Column(String(50))}
        )

.. _orm_declarative_dataclasses_mixin:

Using Declarative Mixins with Dataclasses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the section :ref:`orm_mixins_toplevel`, Declarative Mixin classes
are introduced.  One requirement of declarative mixins is that certain
constructs that can't be easily duplicated must be given as callables,
using the :class:`_orm.declared_attr` decorator, such as in the
example at :ref:`orm_declarative_mixins_relationships`::

    class RefTargetMixin:
        @declared_attr
        def target_id(cls):
            return Column('target_id', ForeignKey('target.id'))

        @declared_attr
        def target(cls):
            return relationship("Target")

This form is supported within the Dataclasses ``field()`` object by using
a lambda to indicate the SQLAlchemy construct inside the ``field()``.
Using :func:`_orm.declared_attr` to surround the lambda is optional.
If we wanted to produce our ``User`` class above where the ORM fields
came from a mixin that is itself a dataclass, the form would be::

    @dataclass
    class UserMixin:
        __tablename__ = "user"

        __sa_dataclass_metadata_key__ = "sa"

        id: int = field(
            init=False, metadata={"sa": Column(Integer, primary_key=True)}
        )

        addresses: List[Address] = field(
            default_factory=list, metadata={"sa": lambda: relationship("Address")}
        )

    @dataclass
    class AddressMixin:
        __tablename__ = "address"
        __sa_dataclass_metadata_key__ = "sa"
        id: int = field(
            init=False, metadata={"sa": Column(Integer, primary_key=True)}
        )
        user_id: int = field(
            init=False, metadata={"sa": lambda: Column(ForeignKey("user.id"))}
        )
        email_address: str = field(
            default=None, metadata={"sa": Column(String(50))}
        )

    @mapper_registry.mapped
    class User(UserMixin):
        pass

    @mapper_registry.mapped
    class Address(AddressMixin):
      pass

.. versionadded:: 1.4.2  Added support for "declared attr" style mixin attributes,
   namely :func:`_orm.relationship` constructs as well as :class:`_schema.Column`
   objects with foreign key declarations, to be used within "Dataclasses
   with Declarative Table" style mappings.

.. _orm_declarative_attrs_imperative_table:

Example Three - attrs with Imperative Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A mapping using ``@attr.s``, in conjunction with imperative table::

    import attr

    # other imports

    from sqlalchemy.orm import registry

    mapper_registry = registry()


    @mapper_registry.mapped
    @attr.s
    class User:
        __table__ = Table(
            "user",
            mapper_registry.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("fullname", String(50)),
            Column("nickname", String(12)),
        )
        id = attr.ib()
        name = attr.ib()
        fullname = attr.ib()
        nickname = attr.ib()
        addresses = attr.ib()

    # other classes...

``@dataclass`` and attrs_ mappings may also be used with classical mappings, i.e.
with the :meth:`_orm.registry.map_imperatively` function.  See the section
:ref:`orm_imperative_dataclasses` for a similar example.

.. note:: The ``attrs`` ``slots=True`` option, which enables ``__slots__`` on
   a mapped class, cannot be used with SQLAlchemy mappings without fully
   implementing alternative
   :ref:`attribute instrumentation <examples_instrumentation>`, as mapped
   classes normally rely upon direct access to ``__dict__`` for state storage.
   Behavior is undefined when this option is present.

.. _dataclasses: https://docs.python.org/3/library/dataclasses.html
.. _attrs: https://pypi.org/project/attrs/
