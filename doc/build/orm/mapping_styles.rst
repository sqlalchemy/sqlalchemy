.. _orm_mapping_classes_toplevel:

==========================
ORM Mapped Class Overview
==========================

Overview of ORM class mapping configuration.

For readers new to the SQLAlchemy ORM and/or new to Python in general,
it's recommended to browse through the
:ref:`orm_quickstart` and preferably to work through the
:ref:`unified_tutorial`, where ORM configuration is first introduced at
:ref:`tutorial_orm_table_metadata`.

.. _orm_mapping_styles:

ORM Mapping Styles
==================

SQLAlchemy features two distinct styles of mapper configuration, which then
feature further sub-options for how they are set up.   The variability in mapper
styles is present to suit a varied list of developer preferences, including
the degree of abstraction of a user-defined class from how it is to be
mapped to relational schema tables and columns, what kinds of class hierarchies
are in use, including whether or not custom metaclass schemes are present,
and finally if there are other class-instrumentation approaches present such
as if Python dataclasses_ are in use simultaneously.

In modern SQLAlchemy, the difference between these styles is mostly
superficial; when a particular SQLAlchemy configurational style is used to
express the intent to map a class, the internal process of mapping the class
proceeds in mostly the same way for each, where the end result is always a
user-defined class that has a :class:`_orm.Mapper` configured against a
selectable unit, typically represented by a :class:`_schema.Table` object, and
the class itself has been :term:`instrumented` to include behaviors linked to
relational operations both at the level of the class as well as on instances of
that class. As the process is basically the same in all cases, classes mapped
from different styles are always fully interoperable with each other.
The protocol :class:`_orm.MappedClassProtocol` can be used to indicate a mapped
class when using type checkers such as mypy.

The original mapping API is commonly referred to as "classical" style,
whereas the more automated style of mapping is known as "declarative" style.
SQLAlchemy now refers to these two mapping styles as **imperative mapping**
and **declarative mapping**.

Regardless of what style of mapping used, all ORM mappings as of SQLAlchemy 1.4
originate from a single object known as :class:`_orm.registry`, which is a
registry of mapped classes. Using this registry, a set of mapper configurations
can be finalized as a group, and classes within a particular registry may refer
to each other by name within the configurational process.

.. versionchanged:: 1.4  Declarative and classical mapping are now referred
   to as "declarative" and "imperative" mapping, and are unified internally,
   all originating from the :class:`_orm.registry` construct that represents
   a collection of related mappings.

.. _orm_declarative_mapping:

Declarative Mapping
-------------------

The **Declarative Mapping** is the typical way that mappings are constructed in
modern SQLAlchemy. The most common pattern is to first construct a base class
using the :class:`_orm.DeclarativeBase` superclass. The resulting base class,
when subclassed will apply the declarative mapping process to all subclasses
that derive from it, relative to a particular :class:`_orm.registry` that
is local to the new base by default. The example below illustrates
the use of a declarative base which is then used in a declarative table mapping::

    from sqlalchemy import Integer, String, ForeignKey
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column

    # declarative base class
    class Base(DeclarativeBase):
        pass


    # an example mapping using the base
    class User(Base):
        __tablename__ = "user"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        fullname: Mapped[str] = mapped_column(String(30))
        nickname: Mapped[Optional[str]]

Above, the :class:`_orm.DeclarativeBase` class is used to generate a new
base class (within SQLAlchemy's documentation it's typically referred towards
as ``Base``, however can have any desired name) from
which new classes to be mapped may inherit from, as above a new mapped
class ``User`` is constructed.

.. versionchanged:: 2.0 The :class:`_orm.DeclarativeBase` superclass supersedes
   the use of the :func:`_orm.declarative_base` function and
   :meth:`_orm.registry.generate_base` methods; the superclass approach
   integrates with :pep:`484` tools without the use of plugins.
   See :ref:`whatsnew_20_orm_declarative_typing` for migration notes.

The base class refers to a :class:`_orm.registry` object that maintains a
collection of related mapped classes. as well as to a :class:`_schema.MetaData`
object that retains a collection of :class:`_schema.Table` objects to which
the classes are mapped.

The major Declarative mapping styles are further detailed in the following
sections:

* :ref:`orm_declarative_generated_base_class` - declarative mapping using a
  base class.

* :ref:`orm_declarative_decorator` - declarative mapping using a decorator,
  rather than a base class.

Within the scope of a Declarative mapped class, there are also two varieties
of how the :class:`_schema.Table` metadata may be declared.  These include:

* :ref:`orm_declarative_table` - table columns are declared inline
  within the mapped class using the :func:`_orm.mapped_column` directive
  (or in legacy form, using the :class:`_schema.Column` object directly).
  The :func:`_orm.mapped_column` directive may also be optionally combined with
  type annotations using the :class:`_orm.Mapped` class which can provide
  some details about the mapped columns directly.  The column
  directives, in combination with the ``__tablename__`` and optional
  ``__table_args__`` class level directives will allow the
  Declarative mapping process to construct a :class:`_schema.Table` object to
  be mapped.

* :ref:`orm_imperative_table_configuration` - Instead of specifying table name
  and attributes separately, an explicitly constructed :class:`_schema.Table` object
  is associated with a class that is otherwise mapped declaratively.  This
  style of mapping is a hybrid of "declarative" and "imperative" mapping,
  and applies to techniques such as mapping classes to :term:`reflected`
  :class:`_schema.Table` objects, as well as mapping classes to existing
  Core constructs such as joins and subqueries.


Documentation for Declarative mapping continues at :ref:`declarative_config_toplevel`.

.. _classical_mapping:
.. _orm_imperative_mapping:

Imperative Mapping
-------------------

An **imperative** or **classical** mapping refers to the configuration of a
mapped class using the :meth:`_orm.registry.map_imperatively` method,
where the target class does not include any declarative class attributes.

.. tip:: The imperative mapping form is a lesser-used form of mapping that
   originates from the very first releases of SQLAlchemy in 2006.  It's
   essentially a means of bypassing the Declarative system to provide a
   more "barebones" system of mapping, and does not offer modern features
   such as :pep:`484` support.  As such, most documentation examples
   use Declarative forms, and it's recommended that new users start
   with :ref:`Declarative Table <orm_declarative_table_config_toplevel>`
   configuration.

.. versionchanged:: 2.0  The :meth:`_orm.registry.map_imperatively` method
   is now used to create classical mappings.  The ``sqlalchemy.orm.mapper()``
   standalone function is effectively removed.

In "classical" form, the table metadata is created separately with the
:class:`_schema.Table` construct, then associated with the ``User`` class via
the :meth:`_orm.registry.map_imperatively` method, after establishing
a :class:`_orm.registry` instance.  Normally, a single instance of
:class:`_orm.registry`
shared for all mapped classes that are related to each other::

    from sqlalchemy import Table, Column, Integer, String, ForeignKey
    from sqlalchemy.orm import registry

    mapper_registry = registry()

    user_table = Table(
        "user",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("fullname", String(50)),
        Column("nickname", String(12)),
    )


    class User:
        pass


    mapper_registry.map_imperatively(User, user_table)

Information about mapped attributes, such as relationships to other classes, are provided
via the ``properties`` dictionary.  The example below illustrates a second :class:`_schema.Table`
object, mapped to a class called ``Address``, then linked to ``User`` via :func:`_orm.relationship`::

    address = Table(
        "address",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("user_id", Integer, ForeignKey("user.id")),
        Column("email_address", String(50)),
    )

    mapper_registry.map_imperatively(
        User,
        user,
        properties={
            "addresses": relationship(Address, backref="user", order_by=address.c.id)
        },
    )

    mapper_registry.map_imperatively(Address, address)

Note that classes which are mapped with the Imperative approach are **fully
interchangeable** with those mapped with the Declarative approach. Both systems
ultimately create the same configuration, consisting of a
:class:`_schema.Table`, user-defined class, linked together with a
:class:`_orm.Mapper` object. When we talk about "the behavior of
:class:`_orm.Mapper`", this includes when using the Declarative system as well
- it's still used, just behind the scenes.


.. _orm_mapper_configuration_overview:

Mapped Class Essential Components
==================================

With all mapping forms, the mapping of the class can be configured in many ways
by passing construction arguments that ultimately become part of the :class:`_orm.Mapper`
object via its constructor.  The parameters that are delivered to
:class:`_orm.Mapper` originate from the given mapping form, including
parameters passed to :meth:`_orm.registry.map_imperatively` for an Imperative
mapping, or when using the Declarative system, from a combination
of the table columns, SQL expressions and
relationships being mapped along with that of attributes such as
:ref:`__mapper_args__ <orm_declarative_mapper_options>`.

There are four general classes of configuration information that the
:class:`_orm.Mapper` class looks for:

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
properties dictionary is passed directly as the
``properties`` parameter
to :meth:`_orm.registry.map_imperatively`, which will pass it along to the
:paramref:`_orm.Mapper.properties` parameter.

Other mapper configuration parameters
-------------------------------------

When mapping with the :ref:`declarative <orm_declarative_mapping>` mapping
style, additional mapper configuration arguments are configured via the
``__mapper_args__`` class attribute.   Examples of use are available
at :ref:`orm_declarative_mapper_options`.

When mapping with the :ref:`imperative <orm_imperative_mapping>` style,
keyword arguments are passed to the to :meth:`_orm.registry.map_imperatively`
method which passes them along to the :class:`_orm.Mapper` class.

The full range of parameters accepted are documented at  :class:`_orm.Mapper`.


.. _orm_mapped_class_behavior:


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

    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        fullname: Mapped[str]

An object of type ``User`` above will have a constructor which allows
``User`` objects to be created as::

    u1 = User(name="some name", fullname="some fullname")

.. tip::

    The :ref:`orm_declarative_native_dataclasses` feature provides an alternate
    means of generating a default ``__init__()`` method by using
    Python dataclasses, and allows for a highly configurable constructor
    form.

A class that includes an explicit ``__init__()`` method will maintain
that method, and no default constructor will be applied.

To change the default constructor used, a user-defined Python callable may be
provided to the :paramref:`_orm.registry.constructor` parameter which will be
used as the default constructor.

The constructor also applies to imperative mappings::

    from sqlalchemy.orm import registry

    mapper_registry = registry()

    user_table = Table(
        "user",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
    )


    class User:
        pass


    mapper_registry.map_imperatively(User, user_table)

The above class, mapped imperatively as described at :ref:`orm_imperative_mapping`,
will also feature the default constructor associated with the :class:`_orm.registry`.

.. versionadded:: 1.4  classical mappings now support a standard configuration-level
   constructor when they are mapped via the :meth:`_orm.registry.map_imperatively`
   method.

.. _orm_mapper_inspection:

Runtime Introspection of Mapped classes, Instances and Mappers
---------------------------------------------------------------

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
  more generically to the :class:`.FromClause` object, to which the
  class is mapped::

    table = User.__table__

  This :class:`.FromClause` is also what's returned when using the
  :attr:`_orm.Mapper.local_table` attribute of the :class:`_orm.Mapper`::

    table = inspect(User).local_table

  For a single-table inheritance mapping, where the class is a subclass that
  does not have a table of its own, the :attr:`_orm.Mapper.local_table` attribute as well
  as the ``.__table__`` attribute will be ``None``.   To retrieve the
  "selectable" that is actually selected from during a query for this class,
  this is available via the :attr:`_orm.Mapper.selectable` attribute::

    table = inspect(User).selectable

  ..

.. _orm_mapper_inspection_mapper:

Inspection of Mapper objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

    :class:`.Mapper`

.. _orm_mapper_inspection_instancestate:

Inspection of Mapped Instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`_sa.inspect` function also provides information about instances
of a mapped class.  When applied to an instance of a mapped class, rather
than the class itself, the object returned is known as :class:`.InstanceState`,
which will provide links to not only the :class:`.Mapper` in use by the
class, but also a detailed interface that provides information on the state
of individual attributes within the instance including their current value
and how this relates to what their database-loaded value is.

Given an instance of the ``User`` class loaded from the database::

  >>> u1 = session.scalars(select(User)).first()

The :func:`_sa.inspect` function will return to us an :class:`.InstanceState`
object::

  >>> insp = inspect(u1)
  >>> insp
  <sqlalchemy.orm.state.InstanceState object at 0x7f07e5fec2e0>

With this object we can see elements such as the :class:`.Mapper`::

  >>> insp.mapper
  <Mapper at 0x7f07e614ef50; User>

The :class:`_orm.Session` to which the object is :term:`attached`, if any::

  >>> insp.session
  <sqlalchemy.orm.session.Session object at 0x7f07e614f160>

Information about the current :ref:`persistence state <session_object_states>`
for the object::

  >>> insp.persistent
  True
  >>> insp.pending
  False

Attribute state information such as attributes that have not been loaded or
:term:`lazy loaded` (assume ``addresses`` refers to a :func:`_orm.relationship`
on the mapped class to a related class)::

  >>> insp.unloaded
  {'addresses'}

Information regarding the current in-Python status of attributes, such as
attributes that have not been modified since the last flush::

  >>> insp.unmodified
  {'nickname', 'name', 'fullname', 'id'}

as well as specific history on modifications to attributes since the last flush::

  >>> insp.attrs.nickname.value
  'nickname'
  >>> u1.nickname = "new nickname"
  >>> insp.attrs.nickname.history
  History(added=['new nickname'], unchanged=(), deleted=['nickname'])

.. seealso::

    :class:`.InstanceState`

    :attr:`.InstanceState.attrs`

    :class:`.AttributeState`


.. _dataclasses: https://docs.python.org/3/library/dataclasses.html

.. [1] When running under Python 2, a Python 2 "old style" class is the only
       kind of class that isn't compatible.    When running code on Python 2,
       all classes must extend from the Python ``object`` class.  Under
       Python 3 this is always the case.

.. [2] There is a legacy feature known as a "non primary mapper", where
       additional :class:`_orm.Mapper` objects may be associated with a class
       that's already mapped, however they don't apply instrumentation
       to the class.  This feature is deprecated as of SQLAlchemy 1.3.

