.. _orm_mapping_classes_toplevel:

=======================
Mapping Python Classes
=======================

SQLAlchemy historically features two distinct styles of mapper configuration.
The original mapping API is commonly referred to as "classical" style,
whereas the more automated style of mapping is known as "declarative" style.
SQLAlchemy now refers to these two mapping styles as **imperative mapping**
and **declarative mapping**.

Both styles may be used interchangeably, as the end result of each is exactly
the same - a user-defined class that has a :class:`_orm.Mapper` configured
against a selectable unit, typically represented by a :class:`_schema.Table`
object.

Both imperative and declarative mapping begin with an ORM :class:`_orm.registry`
object, which maintains a set of classes that are mapped.    This registry
is present for all mappings.

.. versionchanged:: 1.4  Declarative and classical mapping are now referred
   to as "declarative" and "imperative" mapping, and are unified internally,
   all originating from the :class:`_orm.registry` construct that represents
   a collection of related mappings.

The full suite of styles can be hierarchically organized as follows:

* :ref:`orm_declarative_mapping`
    * Using :func:`_orm.declarative_base` Base class w/ metaclass
        * :ref:`orm_declarative_table`
        * :ref:`Imperative Table (a.k.a. "hybrid table") <orm_imperative_table_configuration>`
    * Using :meth:`_orm.registry.mapped` Declarative Decorator
        * :ref:`Declarative Table <orm_declarative_decorator>` - combine :meth:`_orm.registry.mapped`
          with ``__tablename__``
        * Imperative Table (Hybrid) - combine :meth:`_orm.registry.mapped` with ``__table__``
        * :ref:`orm_declarative_dataclasses`
            * :ref:`orm_declarative_dataclasses_imperative_table`
            * :ref:`orm_declarative_dataclasses_declarative_table`
            * :ref:`orm_declarative_attrs_imperative_table`
* :ref:`Imperative (a.k.a. "classical" mapping) <orm_imperative_mapping>`
    * Using :meth:`_orm.registry.map_imperatively`
        * :ref:`orm_imperative_dataclasses`

.. _orm_declarative_mapping:

Declarative Mapping
===================

The **Declarative Mapping** is the typical way that
mappings are constructed in modern SQLAlchemy.   The most common pattern
is to first construct a base class using the :func:`_orm.declarative_base`
function, which will apply the declarative mapping process to all subclasses
that derive from it.  Below features a declarative base which is then
used in a declarative table mapping::

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

Above, the :func:`_orm.declarative_base` callable returns a new base class from
which new classes to be mapped may inherit from, as above a new mapped
class ``User`` is constructed.

The base class refers to a
:class:`_orm.registry` object that maintains a collection of related mapped
classes.   The :func:`_orm.declarative_base` function is in fact shorthand
for first creating the registry with the :class:`_orm.registry`
constructor, and then generating a base class using the
:meth:`_orm.registry.generate_base` method::

    from sqlalchemy.orm import registry

    # equivalent to Base = declarative_base()

    mapper_registry = registry()
    Base = mapper_registry.generate_base()

The :class:`_orm.registry` is used directly in order to access a variety
of mapping styles to suit different use cases:

* :ref:`orm_declarative_decorator` - declarative mapping using a decorator,
  rather than a base class.

* :ref:`orm_imperative_mapping` - imperative mapping, specifying all mapping
  arguments directly rather than scanning a class.

Documentation for Declarative mapping continues at :ref:`declarative_config_toplevel`.

.. seealso::

    :ref:`declarative_config_toplevel`


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

        __mapper_args__ = {
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

For a :func:`_orm.relationship` to be declared separately, it needs to
be specified directly within the :paramref:`_orm.mapper.properties`
dictionary passed to the :func:`_orm.mapper`.   An alternative to this
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

.. sidebar:: Using MyPy with SQLAlchemy models

    If you are using PEP 484 static type checkers for Python, a `MyPy
    <http://mypy-lang.org/>`_ plugin is included with `type stubs for
    SQLAlchemy <https://github.com/dropbox/sqlalchemy-stubs>`_.  The plugin is
    tailored towards SQLAlchemy declarative models.   SQLAlchemy hopes to include
    more comprehensive PEP 484 support in future releases.


``@dataclass`` and attrs_ mappings may also be used with classical mappings, i.e.
with the :meth:`_orm.registry.map_imperatively` function.  See the section
:ref:`orm_imperative_dataclasses` for a similar example.

.. _dataclasses: https://docs.python.org/3/library/dataclasses.html
.. _attrs: https://pypi.org/project/attrs/

.. _orm_imperative_mapping:

.. _classical_mapping:

Imperative (a.k.a. Classical) Mappings
======================================

An **imperative** or **classical** mapping refers to the configuration of a
mapped class using the :meth:`_orm.registry.map_imperatively` method,
where the target class does not include any declarative class attributes.
The "map imperative" style has historically been achieved using the
:func:`_orm.mapper` function directly, however this function now expects
that a :meth:`_orm.registry` is present.

.. deprecated:: 1.4  Using the :func:`_orm.mapper` function directly to
   achieve a classical mapping directly is deprecated.   The
   :meth:`_orm.registry.map_imperatively` method retains the identical
   functionality while also allowing for string-based resolution of
   other mapped classes from within the registry.


In "classical" form, the table metadata is created separately with the
:class:`_schema.Table` construct, then associated with the ``User`` class via
the :meth:`_orm.registry.map_imperatively` method::

    from sqlalchemy import Table, Column, Integer, String, ForeignKey
    from sqlalchemy.orm import registry

    mapper_registry = registry()

    user_table = Table(
        'user',
        mapper_registry.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('fullname', String(50)),
        Column('nickname', String(12))
    )

    class User:
        pass

    mapper_registry.map_imperatively(User, user_table)



Information about mapped attributes, such as relationships to other classes, are provided
via the ``properties`` dictionary.  The example below illustrates a second :class:`_schema.Table`
object, mapped to a class called ``Address``, then linked to ``User`` via :func:`_orm.relationship`::

    address = Table('address', metadata,
                Column('id', Integer, primary_key=True),
                Column('user_id', Integer, ForeignKey('user.id')),
                Column('email_address', String(50))
                )

    mapper(User, user, properties={
        'addresses' : relationship(Address, backref='user', order_by=address.c.id)
    })

    mapper(Address, address)

When using classical mappings, classes must be provided directly without the benefit
of the "string lookup" system provided by Declarative.  SQL expressions are typically
specified in terms of the :class:`_schema.Table` objects, i.e. ``address.c.id`` above
for the ``Address`` relationship, and not ``Address.id``, as ``Address`` may not
yet be linked to table metadata, nor can we specify a string here.

Some examples in the documentation still use the classical approach, but note that
the classical as well as Declarative approaches are **fully interchangeable**.  Both
systems ultimately create the same configuration, consisting of a :class:`_schema.Table`,
user-defined class, linked together with a :func:`.mapper`.  When we talk about
"the behavior of :func:`.mapper`", this includes when using the Declarative system
as well - it's still used, just behind the scenes.




.. _orm_imperative_dataclasses:

Imperative Mapping with Dataclasses and Attrs
---------------------------------------------

As described in the section :ref:`orm_declarative_dataclasses`, the
``@dataclass`` decorator and the attrs_ library both work as class
decorators that are applied to a class first, before it is passed to
SQLAlchemy for mapping.   Just like we can use the
:meth:`_orm.registry.mapped` decorator in order to apply declarative-style
mapping to the class, we can also pass it to the :meth:`_orm.registry.map_imperatively`
method so that we may pass all :class:`_schema.Table` and :class:`_orm.Mapper`
configuration imperatively to the function rather than having them defined
on the class itself as declarative class variables::

    from __future__ import annotations

    from dataclasses import dataclass
    from dataclasses import field
    from typing import List

    from sqlalchemy import Column
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy import MetaData
    from sqlalchemy import String
    from sqlalchemy import Table
    from sqlalchemy.orm import registry
    from sqlalchemy.orm import relationship

    mapper_registry = registry()

    @dataclass
    class User:
        id: int = field(init=False)
        name: str = None
        fullname: str = None
        nickname: str = None
        addresses: List[Address] = field(default_factory=list)

    @dataclass
    class Address:
        id: int = field(init=False)
        user_id: int = field(init=False)
        email_address: str = None

    metadata = MetaData()

    user = Table(
        'user',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('fullname', String(50)),
        Column('nickname', String(12)),
    )

    address = Table(
        'address',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('user_id', Integer, ForeignKey('user.id')),
        Column('email_address', String(50)),
    )

    mapper_registry.map_imperatively(User, user, properties={
        'addresses': relationship(Address, backref='user', order_by=address.c.id),
    })

    mapper_registry.map_imperatively(Address, address)

.. _orm_mapper_configuration_overview:

Mapper Configuration Overview
=============================

With all mapping forms, the mapping of the class can be
configured in many ways by passing construction arguments that become
part of the :class:`_orm.Mapper` object.   The function which ultimately
receives these arguments is the :func:`_orm.mapper` function, which are delivered
to it originating from one of the front-facing mapping functions defined
on the :class:`_orm.registry` object.

There are four general classes of configuration information that the
:func:`_orm.mapper` function looks for:

The class to be mapped
----------------------

This is a class that we construct in our application.
There are generally no restrictions on the structure of this class. [1]_
When a Python class is mapped, there can only be **one** :class:`_orm.Mapper`
object for the class. [2]_

When mapping with the :ref:`declarative <orm_declarative_mapping>` mapping
style, the class to be mapped is either a subclass of the declarative base class,
or is handled by a decorator or function such as :meth:`_orm.registry.mapped`.

When mapping with the :ref:`imperative <orm_imperative_mapping>` style, the
class is passed directly as the
:paramref:`_orm.registry.map_imperatively.class_` argument.

The table, or other from clause object
--------------------------------------

In the vast majority of common cases this is an instance of
:class:`_schema.Table`.  For more advanced use cases, it may also refer
to any kind of :class:`_sql.FromClause` object, the most common
alternative objects being the :class:`_sql.Subquery` and :class:`_sql.Join`
object.

When mapping with the :ref:`declarative <orm_declarative_mapping>` mapping
style, the subject table is either generated by the declarative system based
on the ``__tablename__`` attribute and the :class:`_schema.Column` objects
presented, or it is established via the ``__table__`` attribute.  These
two styles of configuration are presented at
:ref:`orm_declarative_table` and :ref:`orm_imperative_table_configuration`.

When mapping with the :ref:`imperative <orm_imperative_mapping>` style, the
subject table is passed positionally as the
:paramref:`_orm.registry.map_imperatively.local_table` argument.

In contrast to the "one mapper per class" requirement of a mapped class,
the :class:`_schema.Table` or other :class:`_sql.FromClause` object that
is the subject of the mapping may be associated with any number of mappings.
The :class:`_orm.Mapper` applies modifications directly to the user-defined
class, but does not modify the given :class:`_schema.Table` or other
:class:`_sql.FromClause` in any way.

.. _orm_mapping_properties:

The properties dictionary
-------------------------

This is a dictionary of all of the attributes
that will be associated with the mapped class.    By default, the
:class:`_orm.Mapper` generates entries for this dictionary derived from the
given :class:`_schema.Table`, in the form of :class:`_orm.ColumnProperty`
objects which each refer to an individual :class:`_schema.Column` of the
mapped table.  The properties dictionary will also contain all the other
kinds of :class:`_orm.MapperProperty` objects to be configured, most
commonly instances generated by the :func:`_orm.relationship` construct.

When mapping with the :ref:`declarative <orm_declarative_mapping>` mapping
style, the properties dictionary is generated by the declarative system
by scanning the class to be mapped for appropriate attributes.  See
the section :ref:`orm_declarative_properties` for notes on this process.

When mapping with the :ref:`imperative <orm_imperative_mapping>` style, the
properties dictionary is passed directly as the ``properties`` argument
to :meth:`_orm.registry.map_imperatively`, which will pass it along to the
:paramref:`_orm.mapper.properties` parameter.

Other mapper configuration parameters
-------------------------------------

These flags are documented at  :func:`_orm.mapper`.

When mapping with the :ref:`declarative <orm_declarative_mapping>` mapping
style, additional mapper configuration arguments are configured via the
``__mapper_args__`` class attribute, documented at
:ref:`orm_declarative_mapper_options`

When mapping with the :ref:`imperative <orm_imperative_mapping>` style,
keyword arguments are passed to the to :meth:`_orm.registry.map_imperatively`
method which passes them along to the :func:`_orm.mapper` function.


.. [1] When running under Python 2, a Python 2 "old style" class is the only
       kind of class that isn't compatible.    When running code on Python 2,
       all classes must extend from the Python ``object`` class.  Under
       Python 3 this is always the case.

.. [2] There is a legacy feature known as a "non primary mapper", where
       additional :class:`_orm.Mapper` objects may be associated with a class
       that's already mapped, however they don't apply instrumentation
       to the class.  This feature is deprecated as of SQLAlchemy 1.3.


Mapped Class Behavior
=====================

Across all styles of mapping using the :class:`_orm.registry` object,
the following behaviors are common:

.. _mapped_class_default_constructor:

Default Constructor
-------------------

The :class:`_orm.registry` applies a default constructor, i.e. ``__init__``
method, to all mapped classes that don't explicitly have their own
``__init__`` method.   The behavior of this method is such that it provides
a convenient keyword constructor that will accept as optional keyword arguments
all the attributes that are named.   E.g.::

    from sqlalchemy.orm import declarative_base

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'

        id = Column(...)
        name = Column(...)
        fullname = Column(...)

An object of type ``User`` above will have a constructor which allows
``User`` objects to be created as::

    u1 = User(name='some name', fullname='some fullname')

The above constructor may be customized by passing a Python callable to
the :paramref:`_orm.registry.constructor` parameter which provides the
desired default ``__init__()`` behavior.

The constructor also applies to imperative mappings::

    from sqlalchemy.orm import registry

    mapper_registry = registry()

    user_table = Table(
        'user',
        mapper_registry.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(50))
    )

    class User:
        pass

    mapper_registry.map_imperatively(User, user_table)

The above class, mapped imperatively as described at :ref:`classical_mapping`,
will also feature the default constructor associated with the :class:`_orm.registry`.

.. versionadded:: 1.4  classical mappings now support a standard configuration-level
   constructor when they are mapped via the :meth:`_orm.registry.map_imperatively`
   method.

Runtime Introspection of Mapped classes and Mappers
---------------------------------------------------

A class that is mapped using :class:`_orm.registry` will also feature a few
attributes that are common to all mappings:

* The ``__mapper__`` attribute will refer to the :class:`_orm.Mapper` that
  is associated with the class::

    mapper = User.__mapper__

  This :class:`_orm.Mapper` is also what's returned when using the
  :func:`_sa.inspect` function against the mapped class::

    from sqlalchemy import inspect

    mapper = inspect(User)

  ..

* The ``__table__`` attribute will refer to the :class:`_schema.Table`, or
  more generically to the :class:`_schema.FromClause` object, to which the
  class is mapped::

    table = User.__table__

  This :class:`_schema.FromClause` is also what's returned when using the
  :attr:`_orm.Mapper.local_table` attribute of the :class:`_orm.Mapper`::

    table = inspect(User).local_table

  For a single-table inheritance mapping, where the class is a subclass that
  does not have a table of its own, the :attr:`_orm.Mapper.local_table` attribute as well
  as the ``.__table__`` attribute will be ``None``.   To retrieve the
  "selectable" that is actually selected from during a query for this class,
  this is available via the :attr:`_orm.Mapper.selectable` attribute::

    table = inspect(User).selectable

  ..

Mapper Inspection Features
--------------------------

As illustrated in the previous section, the :class:`_orm.Mapper` object is
available from any mapped class, regardless of method, using the
:ref:`core_inspection_toplevel` system.  Using the
:func:`_sa.inspect` function, one can acquire the :class:`_orm.Mapper` from a
mapped class::

    >>> from sqlalchemy import inspect
    >>> insp = inspect(User)

Detailed information is available including :attr:`_orm.Mapper.columns`::

    >>> insp.columns
    <sqlalchemy.util._collections.OrderedProperties object at 0x102f407f8>

This is a namespace that can be viewed in a list format or
via individual names::

    >>> list(insp.columns)
    [Column('id', Integer(), table=<user>, primary_key=True, nullable=False), Column('name', String(length=50), table=<user>), Column('fullname', String(length=50), table=<user>), Column('nickname', String(length=50), table=<user>)]
    >>> insp.columns.name
    Column('name', String(length=50), table=<user>)

Other namespaces include :attr:`_orm.Mapper.all_orm_descriptors`, which includes all mapped
attributes as well as hybrids, association proxies::

    >>> insp.all_orm_descriptors
    <sqlalchemy.util._collections.ImmutableProperties object at 0x1040e2c68>
    >>> insp.all_orm_descriptors.keys()
    ['fullname', 'nickname', 'name', 'id']

As well as :attr:`_orm.Mapper.column_attrs`::

    >>> list(insp.column_attrs)
    [<ColumnProperty at 0x10403fde0; id>, <ColumnProperty at 0x10403fce8; name>, <ColumnProperty at 0x1040e9050; fullname>, <ColumnProperty at 0x1040e9148; nickname>]
    >>> insp.column_attrs.name
    <ColumnProperty at 0x10403fce8; name>
    >>> insp.column_attrs.name.expression
    Column('name', String(length=50), table=<user>)

.. seealso::

    :ref:`core_inspection_toplevel`

    :class:`_orm.Mapper`

    :class:`.InstanceState`
