.. _orm_declarative_mapper_config_toplevel:

=============================================
Mapper Configuration with Declarative
=============================================

The section :ref:`orm_mapper_configuration_overview` discusses the general
configurational elements of a :class:`_orm.Mapper` construct, which is the
structure that defines how a particular user defined class is mapped to a
database table or other SQL construct.    The following sections describe
specific details about how the declarative system goes about constructing
the :class:`_orm.Mapper`.

.. _orm_declarative_properties:

Defining Mapped Properties with Declarative
--------------------------------------------

The examples given at :ref:`orm_declarative_table_config_toplevel`
illustrate mappings against table-bound columns, using the :func:`_orm.mapped_column`
construct.  There are several other varieties of ORM mapped constructs
that may be configured besides table-bound columns, the most common being the
:func:`_orm.relationship` construct.  Other kinds of properties include
SQL expressions that are defined using the :func:`_orm.column_property`
construct and multiple-column mappings using the :func:`_orm.composite`
construct.

While an :ref:`imperative mapping <orm_imperative_mapping>` makes use of
the :ref:`properties <orm_mapping_properties>` dictionary to establish
all the mapped class attributes, in the declarative
mapping, these properties are all specified inline with the class definition,
which in the case of a declarative table mapping are inline with the
:class:`_schema.Column` objects that will be used to generate a
:class:`_schema.Table` object.

Working with the example mapping of ``User`` and ``Address``, we may illustrate
a declarative table mapping that includes not just :func:`_orm.mapped_column`
objects but also relationships and SQL expressions::

    from typing import List
    from typing import Optional

    from sqlalchemy import Column
    from sqlalchemy import ForeignKey
    from sqlalchemy import String
    from sqlalchemy import Text
    from sqlalchemy.orm import column_property
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        firstname: Mapped[str] = mapped_column(String(50))
        lastname: Mapped[str] = mapped_column(String(50))
        fullname: Mapped[str] = column_property(firstname + " " + lastname)

        addresses: Mapped[List["Address"]] = relationship(back_populates="user")


    class Address(Base):
        __tablename__ = "address"

        id: Mapped[int] = mapped_column(primary_key=True)
        user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
        email_address: Mapped[str]
        address_statistics: Mapped[Optional[str]] = mapped_column(Text, deferred=True)

        user: Mapped["User"] = relationship(back_populates="addresses")

The above declarative table mapping features two tables, each with a
:func:`_orm.relationship` referring to the other, as well as a simple
SQL expression mapped by :func:`_orm.column_property`, and an additional
:func:`_orm.mapped_column` that indicates loading should be on a
"deferred" basis as defined
by the :paramref:`_orm.mapped_column.deferred` keyword.    More documentation
on these particular concepts may be found at :ref:`relationship_patterns`,
:ref:`mapper_column_property_sql_expressions`, and :ref:`orm_queryguide_column_deferral`.

Properties may be specified with a declarative mapping as above using
"hybrid table" style as well; the :class:`_schema.Column` objects that
are directly part of a table move into the :class:`_schema.Table` definition
but everything else, including composed SQL expressions, would still be
inline with the class definition.  Constructs that need to refer to a
:class:`_schema.Column` directly would reference it in terms of the
:class:`_schema.Table` object.  To illustrate the above mapping using
hybrid table style::

    # mapping attributes using declarative with imperative table
    # i.e. __table__

    from sqlalchemy import Column, ForeignKey, Integer, String, Table, Text
    from sqlalchemy.orm import column_property
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import deferred
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __table__ = Table(
            "user",
            Base.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("firstname", String(50)),
            Column("lastname", String(50)),
        )

        fullname = column_property(__table__.c.firstname + " " + __table__.c.lastname)

        addresses = relationship("Address", back_populates="user")


    class Address(Base):
        __table__ = Table(
            "address",
            Base.metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", ForeignKey("user.id")),
            Column("email_address", String),
            Column("address_statistics", Text),
        )

        address_statistics = deferred(__table__.c.address_statistics)

        user = relationship("User", back_populates="addresses")

Things to note above:

* The address :class:`_schema.Table` contains a column called ``address_statistics``,
  however we re-map this column under the same attribute name to be under
  the control of a :func:`_orm.deferred` construct.

* With both declararative table and hybrid table mappings, when we define a
  :class:`_schema.ForeignKey` construct, we always name the target table
  using the **table name**, and not the mapped class name.

* When we define :func:`_orm.relationship` constructs, as these constructs
  create a linkage between two mapped classes where one necessarily is defined
  before the other, we can refer to the remote class using its string name.
  This functionality also extends into the area of other arguments specified
  on the :func:`_orm.relationship` such as the "primary join" and "order by"
  arguments.   See the section :ref:`orm_declarative_relationship_eval` for
  details on this.


.. _orm_declarative_mapper_options:

Mapper Configuration Options with Declarative
----------------------------------------------

With all mapping forms, the mapping of the class is configured through
parameters that become part of the :class:`_orm.Mapper` object.
The function which ultimately receives these arguments is the
:class:`_orm.Mapper` function, and are delivered to it from one of
the front-facing mapping functions defined on the :class:`_orm.registry`
object.

For the declarative form of mapping, mapper arguments are specified
using the ``__mapper_args__`` declarative class variable, which is a dictionary
that is passed as keyword arguments to the :class:`_orm.Mapper` function.
Some examples:

**Map Specific Primary Key Columns**

The example below illustrates Declarative-level settings for the
:paramref:`_orm.Mapper.primary_key` parameter, which establishes
particular columns as part of what the ORM should consider to be a primary
key for the class, independently of schema-level primary key constraints::

    class GroupUsers(Base):
        __tablename__ = "group_users"

        user_id = mapped_column(String(40))
        group_id = mapped_column(String(40))

        __mapper_args__ = {"primary_key": [user_id, group_id]}

.. seealso::

    :ref:`mapper_primary_key` - further background on ORM mapping of explicit
    columns as primary key columns

**Version ID Column**

The example below illustrates Declarative-level settings for the
:paramref:`_orm.Mapper.version_id_col` and
:paramref:`_orm.Mapper.version_id_generator` parameters, which configure
an ORM-maintained version counter that is updated and checked within the
:term:`unit of work` flush process::

    from datetime import datetime


    class Widget(Base):
        __tablename__ = "widgets"

        id = mapped_column(Integer, primary_key=True)
        timestamp = mapped_column(DateTime, nullable=False)

        __mapper_args__ = {
            "version_id_col": timestamp,
            "version_id_generator": lambda v: datetime.now(),
        }

.. seealso::

    :ref:`mapper_version_counter` - background on the ORM version counter feature

**Single Table Inheritance**

The example below illustrates Declarative-level settings for the
:paramref:`_orm.Mapper.polymorphic_on` and
:paramref:`_orm.Mapper.polymorphic_identity` parameters, which are used when
configuring a single-table inheritance mapping::

    class Person(Base):
        __tablename__ = "person"

        person_id = mapped_column(Integer, primary_key=True)
        type = mapped_column(String, nullable=False)

        __mapper_args__ = dict(
            polymorphic_on=type,
            polymorphic_identity="person",
        )


    class Employee(Person):
        __mapper_args__ = dict(
            polymorphic_identity="employee",
        )

.. seealso::

    :ref:`single_inheritance` - background on the ORM single table inheritance
    mapping feature.

Constructing mapper arguments dynamically
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``__mapper_args__`` dictionary may be generated from a class-bound
descriptor method rather than from a fixed dictionary by making use of the
:func:`_orm.declared_attr` construct.    This is useful to create arguments
for mappers that are programmatically derived from the table configuration
or other aspects of the mapped class.    A dynamic ``__mapper_args__``
attribute will typically be useful when using a Declarative Mixin or
abstract base class.

For example, to omit from the mapping
any columns that have a special :attr:`.Column.info` value, a mixin
can use a ``__mapper_args__`` method that scans for these columns from the
``cls.__table__`` attribute and passes them to the :paramref:`_orm.Mapper.exclude_properties`
collection::

    from sqlalchemy import Column
    from sqlalchemy import Integer
    from sqlalchemy import select
    from sqlalchemy import String
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import declared_attr


    class ExcludeColsWFlag:
        @declared_attr
        def __mapper_args__(cls):
            return {
                "exclude_properties": [
                    column.key
                    for column in cls.__table__.c
                    if column.info.get("exclude", False)
                ]
            }


    class Base(DeclarativeBase):
        pass


    class SomeClass(ExcludeColsWFlag, Base):
        __tablename__ = "some_table"

        id = mapped_column(Integer, primary_key=True)
        data = mapped_column(String)
        not_needed = mapped_column(String, info={"exclude": True})

Above, the ``ExcludeColsWFlag`` mixin provides a per-class ``__mapper_args__``
hook that will scan for :class:`.Column` objects that include the key/value
``'exclude': True`` passed to the :paramref:`.Column.info` parameter, and then
add their string "key" name to the :paramref:`_orm.Mapper.exclude_properties`
collection which will prevent the resulting :class:`.Mapper` from considering
these columns for any SQL operations.

.. seealso::

    :ref:`orm_mixins_toplevel`


Other Declarative Mapping Directives
--------------------------------------

``__declare_last__()``
~~~~~~~~~~~~~~~~~~~~~~

The ``__declare_last__()`` hook allows definition of
a class level function that is automatically called by the
:meth:`.MapperEvents.after_configured` event, which occurs after mappings are
assumed to be completed and the 'configure' step has finished::

    class MyClass(Base):
        @classmethod
        def __declare_last__(cls):
            """ """
            # do something with mappings

``__declare_first__()``
~~~~~~~~~~~~~~~~~~~~~~~

Like ``__declare_last__()``, but is called at the beginning of mapper
configuration via the :meth:`.MapperEvents.before_configured` event::

    class MyClass(Base):
        @classmethod
        def __declare_first__(cls):
            """ """
            # do something before mappings are configured

.. versionadded:: 0.9.3


.. _declarative_metadata:

``metadata``
~~~~~~~~~~~~

The :class:`_schema.MetaData` collection normally used to assign a new
:class:`_schema.Table` is the :attr:`_orm.registry.metadata` attribute
associated with the :class:`_orm.registry` object in use. When using a
declarative base class such as that produced by the
:class:`_orm.DeclarativeBase` superclass, as well as legacy functions such as
:func:`_orm.declarative_base` and :meth:`_orm.registry.generate_base`, this
:class:`_schema.MetaData` is also normally present as an attribute named
``.metadata`` that's directly on the base class, and thus also on the mapped
class via inheritance. Declarative uses this attribute, when present, in order
to determine the target :class:`_schema.MetaData` collection, or if not
present, uses the :class:`_schema.MetaData` associated directly with the
:class:`_orm.registry`.

This attribute may also be assigned towards in order to affect the
:class:`_schema.MetaData` collection to be used on a per-mapped-hierarchy basis
for a single base and/or :class:`_orm.registry`. This takes effect whether a
declarative base class is used or if the :meth:`_orm.registry.mapped` decorator
is used directly, thus allowing patterns such as the metadata-per-abstract base
example in the next section, :ref:`declarative_abstract`. A similar pattern can
be illustrated using :meth:`_orm.registry.mapped` as follows::

    reg = registry()


    class BaseOne:
        metadata = MetaData()


    class BaseTwo:
        metadata = MetaData()


    @reg.mapped
    class ClassOne:
        __tablename__ = "t1"  # will use reg.metadata

        id = mapped_column(Integer, primary_key=True)


    @reg.mapped
    class ClassTwo(BaseOne):
        __tablename__ = "t1"  # will use BaseOne.metadata

        id = mapped_column(Integer, primary_key=True)


    @reg.mapped
    class ClassThree(BaseTwo):
        __tablename__ = "t1"  # will use BaseTwo.metadata

        id = mapped_column(Integer, primary_key=True)

.. seealso::

    :ref:`declarative_abstract`

.. _declarative_abstract:

``__abstract__``
~~~~~~~~~~~~~~~~

``__abstract__`` causes declarative to skip the production
of a table or mapper for the class entirely.  A class can be added within a
hierarchy in the same way as mixin (see :ref:`declarative_mixins`), allowing
subclasses to extend just from the special class::

    class SomeAbstractBase(Base):
        __abstract__ = True

        def some_helpful_method(self):
            """ """

        @declared_attr
        def __mapper_args__(cls):
            return {"helpful mapper arguments": True}


    class MyMappedClass(SomeAbstractBase):
        pass

One possible use of ``__abstract__`` is to use a distinct
:class:`_schema.MetaData` for different bases::

    class Base(DeclarativeBase):
        pass


    class DefaultBase(Base):
        __abstract__ = True
        metadata = MetaData()


    class OtherBase(Base):
        __abstract__ = True
        metadata = MetaData()

Above, classes which inherit from ``DefaultBase`` will use one
:class:`_schema.MetaData` as the registry of tables, and those which inherit from
``OtherBase`` will use a different one. The tables themselves can then be
created perhaps within distinct databases::

    DefaultBase.metadata.create_all(some_engine)
    OtherBase.metadata.create_all(some_other_engine)

.. seealso::

    :ref:`orm_inheritance_abstract_poly` - an alternative form of "abstract"
    mapped class that is appropriate for inheritance hierarchies.

.. _declarative_table_cls:

``__table_cls__``
~~~~~~~~~~~~~~~~~

Allows the callable / class used to generate a :class:`_schema.Table` to be customized.
This is a very open-ended hook that can allow special customizations
to a :class:`_schema.Table` that one generates here::

    class MyMixin:
        @classmethod
        def __table_cls__(cls, name, metadata_obj, *arg, **kw):
            return Table(f"my_{name}", metadata_obj, *arg, **kw)

The above mixin would cause all :class:`_schema.Table` objects generated to include
the prefix ``"my_"``, followed by the name normally specified using the
``__tablename__`` attribute.

``__table_cls__`` also supports the case of returning ``None``, which
causes the class to be considered as single-table inheritance vs. its subclass.
This may be useful in some customization schemes to determine that single-table
inheritance should take place based on the arguments for the table itself,
such as, define as single-inheritance if there is no primary key present::

    class AutoTable:
        @declared_attr
        def __tablename__(cls):
            return cls.__name__

        @classmethod
        def __table_cls__(cls, *arg, **kw):
            for obj in arg[1:]:
                if (isinstance(obj, Column) and obj.primary_key) or isinstance(
                    obj, PrimaryKeyConstraint
                ):
                    return Table(*arg, **kw)

            return None


    class Person(AutoTable, Base):
        id = mapped_column(Integer, primary_key=True)


    class Employee(Person):
        employee_name = mapped_column(String)

The above ``Employee`` class would be mapped as single-table inheritance
against ``Person``; the ``employee_name`` column would be added as a member
of the ``Person`` table.

