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
illustrate mappings against table-bound columns;
the mapping of an individual column to an ORM class attribute is represented
internally by the :class:`_orm.ColumnProperty` construct.   There are many
other varieties of mapper properties, the most common being the
:func:`_orm.relationship` construct.  Other kinds of properties include
synonyms to columns which are defined using the :func:`_orm.synonym`
construct, SQL expressions that are defined using the :func:`_orm.column_property`
construct, and deferred columns and SQL expressions which load only when
accessed, defined using the :func:`_orm.deferred` construct.

While an :ref:`imperative mapping <orm_imperative_mapping>` makes use of
the :ref:`properties <orm_mapping_properties>` dictionary to establish
all the mapped class attributes, in the declarative
mapping, these properties are all specified inline with the class definition,
which in the case of a declarative table mapping are inline with the
:class:`_schema.Column` objects that will be used to generate a
:class:`_schema.Table` object.

Working with the example mapping of ``User`` and ``Address``, we may illustrate
a declarative table mapping that includes not just :class:`_schema.Column`
objects but also relationships and SQL expressions::

    # mapping attributes using declarative with declarative table
    # i.e. __tablename__

    from sqlalchemy import Column, Integer, String, Text, ForeignKey
    from sqlalchemy.orm import column_property, relationship, deferred
    from sqlalchemy.orm import declarative_base

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        firstname = Column(String(50))
        lastname = Column(String(50))

        fullname = column_property(firstname + " " + lastname)

        addresses = relationship("Address", back_populates="user")

    class Address(Base):
        __tablename__ = 'address'

        id = Column(Integer, primary_key=True)
        user_id = Column(ForeignKey("user.id"))
        email_address = Column(String)
        address_statistics = deferred(Column(Text))

        user = relationship("User", back_populates="addresses")

The above declarative table mapping features two tables, each with a
:func:`_orm.relationship` referring to the other, as well as a simple
SQL expression mapped by :func:`_orm.column_property`, and an additional
:class:`_schema.Column` that will be loaded on a "deferred" basis as defined
by the :func:`_orm.deferred` construct.    More documentation
on these particular concepts may be found at :ref:`relationship_patterns`,
:ref:`mapper_column_property_sql_expressions`, and :ref:`deferred`.

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

    from sqlalchemy import Table
    from sqlalchemy import Column, Integer, String, Text, ForeignKey
    from sqlalchemy.orm import column_property, relationship, deferred
    from sqlalchemy.orm import declarative_base

    Base = declarative_base()

    class User(Base):
        __table__ = Table(
            "user",
            Base.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("firstname", String(50)),
            Column("lastname", String(50))
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
            Column("address_statistics", Text)
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
:func:`_orm.mapper` function, and are delivered to it from one of
the front-facing mapping functions defined on the :class:`_orm.registry`
object.

For the declarative form of mapping, mapper arguments are specified
using the ``__mapper_args__`` declarative class variable, which is a dictionary
that is passed as keyword arguments to the :func:`_orm.mapper` function.
Some examples:

**Version ID Column**

The :paramref:`_orm.mapper.version_id_col` and
:paramref:`_orm.mapper.version_id_generator` parameters::

    from datetime import datetime

    class Widget(Base):
        __tablename__ = 'widgets'

        id = Column(Integer, primary_key=True)
        timestamp = Column(DateTime, nullable=False)

        __mapper_args__ = {
            'version_id_col': timestamp,
            'version_id_generator': lambda v:datetime.now()
        }

**Single Table Inheritance**

The :paramref:`_orm.mapper.polymorphic_on` and
:paramref:`_orm.mapper.polymorphic_identity` parameters::

    class Person(Base):
        __tablename__ = 'person'

        person_id = Column(Integer, primary_key=True)
        type = Column(String, nullable=False)

        __mapper_args__ = dict(
            polymorphic_on=type,
            polymorphic_identity="person"
        )

    class Employee(Person):
        __mapper_args__ = dict(
            polymorphic_identity="employee"
        )

The ``__mapper_args__`` dictionary may be generated from a class-bound
descriptor method rather than from a fixed dictionary by making use of the
:func:`_orm.declared_attr` construct.   The section :ref:`orm_mixins_toplevel`
discusses this concept further.

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
            ""
            # do something with mappings

``__declare_first__()``
~~~~~~~~~~~~~~~~~~~~~~~

Like ``__declare_last__()``, but is called at the beginning of mapper
configuration via the :meth:`.MapperEvents.before_configured` event::

    class MyClass(Base):
        @classmethod
        def __declare_first__(cls):
            ""
            # do something before mappings are configured

.. versionadded:: 0.9.3

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
            ""

        @declared_attr
        def __mapper_args__(cls):
            return {"helpful mapper arguments":True}

    class MyMappedClass(SomeAbstractBase):
        ""

One possible use of ``__abstract__`` is to use a distinct
:class:`_schema.MetaData` for different bases::

    Base = declarative_base()

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


``__table_cls__``
~~~~~~~~~~~~~~~~~

Allows the callable / class used to generate a :class:`_schema.Table` to be customized.
This is a very open-ended hook that can allow special customizations
to a :class:`_schema.Table` that one generates here::

    class MyMixin(object):
        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return Table(
                "my_" + name,
                metadata, *arg, **kw
            )

The above mixin would cause all :class:`_schema.Table` objects generated to include
the prefix ``"my_"``, followed by the name normally specified using the
``__tablename__`` attribute.

``__table_cls__`` also supports the case of returning ``None``, which
causes the class to be considered as single-table inheritance vs. its subclass.
This may be useful in some customization schemes to determine that single-table
inheritance should take place based on the arguments for the table itself,
such as, define as single-inheritance if there is no primary key present::

    class AutoTable(object):
        @declared_attr
        def __tablename__(cls):
            return cls.__name__

        @classmethod
        def __table_cls__(cls, *arg, **kw):
            for obj in arg[1:]:
                if (isinstance(obj, Column) and obj.primary_key) or \
                        isinstance(obj, PrimaryKeyConstraint):
                    return Table(*arg, **kw)

            return None

    class Person(AutoTable, Base):
        id = Column(Integer, primary_key=True)

    class Employee(Person):
        employee_name = Column(String)

The above ``Employee`` class would be mapped as single-table inheritance
against ``Person``; the ``employee_name`` column would be added as a member
of the ``Person`` table.

