
.. _orm_declarative_table_config_toplevel:

=============================================
Table Configuration with Declarative
=============================================

As introduced at :ref:`orm_declarative_mapping`, the Declarative style
includes the ability to generate a mapped :class:`_schema.Table` object
at the same time, or to accommodate a :class:`_schema.Table` or other
:class:`_sql.FromClause` object directly.

The following examples assume a declarative base class as::

    from sqlalchemy.orm import declarative_base

    Base = declarative_base()

All of the examples that follow illustrate a class inheriting from the above
``Base``.  The decorator style introduced at :ref:`orm_declarative_decorator`
is fully supported with all the following examples as well.

.. _orm_declarative_table:

Declarative Table
-----------------

With the declarative base class, the typical form of mapping includes an
attribute ``__tablename__`` that indicates the name of a :class:`_schema.Table`
that should be generated along with the mapping::

    from sqlalchemy import Column, Integer, String, ForeignKey
    from sqlalchemy.orm import declarative_base

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)
        nickname = Column(String)

Above, :class:`_schema.Column` objects are placed inline with the class
definition.   The declarative mapping process will generate a new
:class:`_schema.Table` object against the :class:`_schema.MetaData` collection
associated with the declarative base, and each specified
:class:`_schema.Column` object will become part of the :attr:`.schema.Table.columns`
collection of this :class:`_schema.Table` object.   The :class:`_schema.Column`
objects can omit their "name" field, which is usually the first positional
argument to the :class:`_schema.Column` constructor; the declarative system
will assign the key associated with each :class:`_schema.Column` as the name,
to produce a :class:`_schema.Table` that is equivalent to::

    # equivalent Table object produced
    user_table = Table(
        "user",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("fullname", String),
        Column("nickname", String),
    )

.. _orm_declarative_metadata:

Accessing Table and Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A declaratively mapped class will always include an attribute called
``__table__``; when the above configuration using ``__tablename__`` is
complete, the declarative process makes the :class:`_schema.Table`
available via the ``__table__`` attribute::


    # access the Table
    user_table = User.__table__

The above table is ultimately the same one that corresponds to the
:attr:`_orm.Mapper.local_table` attribute, which we can see through the
:ref:`runtime inspection system <inspection_toplevel>`::

    from sqlalchemy import inspect

    user_table = inspect(User).local_table

The :class:`_schema.MetaData` collection associated with both the declarative
:class:`_orm.registry` as well as the base class is frequently necessary in
order to run DDL operations such as CREATE, as well as in use with migration
tools such as Alembic.   This object is available via the ``.metadata``
attribute of :class:`_orm.registry` as well as the declarative base class.
Below, for a small script we may wish to emit a CREATE for all tables against a
SQLite database::

    engine = create_engine("sqlite://")

    Base.metadata.create_all(engine)

.. _orm_declarative_table_configuration:

Declarative Table Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using Declarative Table configuration with the ``__tablename__``
declarative class attribute, additional arguments to be supplied to the
:class:`_schema.Table` constructor should be provided using the
``__table_args__`` declarative class attribute.

This attribute accommodates both positional as well as keyword
arguments that are normally sent to the
:class:`_schema.Table` constructor.
The attribute can be specified in one of two forms. One is as a
dictionary::

    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = {'mysql_engine':'InnoDB'}

The other, a tuple, where each argument is positional
(usually constraints)::

    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = (
                ForeignKeyConstraint(['id'], ['remote_table.id']),
                UniqueConstraint('foo'),
                )

Keyword arguments can be specified with the above form by
specifying the last argument as a dictionary::

    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = (
                ForeignKeyConstraint(['id'], ['remote_table.id']),
                UniqueConstraint('foo'),
                {'autoload':True}
                )

A class may also specify the ``__table_args__`` declarative attribute,
as well as the ``__tablename__`` attribute, in a dynamic style using the
:func:`_orm.declared_attr` method decorator.   See the section
:ref:`declarative_mixins` for examples on how this is often used.

.. _orm_declarative_table_schema_name:

Explicit Schema Name with Declarative Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The schema name for a :class:`_schema.Table` as documented at
:ref:`schema_table_schema_name` is applied to an individual :class:`_schema.Table`
using the :paramref:`_schema.Table.schema` argument.   When using Declarative
tables, this option is passed like any other to the ``__table_args__``
dictionary::


    class MyClass(Base):
        __tablename__ = 'sometable'
        __table_args__ = {'schema': 'some_schema'}


The schema name can also be applied to all :class:`_schema.Table` objects
globally by using the :paramref:`_schema.MetaData.schema` parameter documented
at :ref:`schema_metadata_schema_name`.   The :class:`_schema.MetaData` object
may be constructed separately and passed either to :func:`_orm.registry`
or :func:`_orm.declarative_base`::

    from sqlalchemy import metadata
    metadata = MetaData(schema="some_schema")

    Base = declarative_base(metadata = metadata)


    class MyClass(Base):
        # will use "some_schema" by default
        __tablename__ = 'sometable'


.. seealso::

    :ref:`schema_table_schema_name` - in the :ref:`metadata_toplevel` documentation.

.. _orm_declarative_table_adding_columns:

Adding New Columns
^^^^^^^^^^^^^^^^^^^

The declarative table configuration allows the addition of new
:class:`_schema.Column` objects under two scenarios.  The most basic
is that of simply assigning new :class:`_schema.Column` objects to the
class::

    MyClass.some_new_column = Column('data', Unicode)

The above operation performed against a declarative class that has been
mapped using the declarative base (note, not the decorator form of declarative)
will add the above :class:`_schema.Column` to the :class:`_schema.Table`
using the :meth:`_schema.Table.append_column` method and will also add the
column to the :class:`_orm.Mapper` to be fully mapped.

.. note:: assignment of new columns to an existing declaratively mapped class
   will only function correctly if the "declarative base" class is used, which
   also provides for a metaclass-driven ``__setattr__()`` method which will
   intercept these operations.   It will **not** work if the declarative
   decorator provided by
   :meth:`_orm.registry.mapped` is used, nor will it work for an imperatively
   mapped class mapped by :meth:`_orm.registry.map_imperatively`.


The other scenario where a :class:`_schema.Column` is added on the fly is
when an inheriting subclass that has no table of its own indicates
additional columns; these columns will be added to the superclass table.
The section :ref:`single_inheritance` discusses single table inheritance.


.. _orm_imperative_table_configuration:

Declarative with Imperative Table (a.k.a. Hybrid Declarative)
-------------------------------------------------------------

Declarative mappings may also be provided with a pre-existing
:class:`_schema.Table` object, or otherwise a :class:`_schema.Table` or other
arbitrary :class:`_sql.FromClause` construct (such as a :class:`_sql.Join`
or :class:`_sql.Subquery`) that is constructed separately.

This is referred to as a "hybrid declarative"
mapping, as the class is mapped using the declarative style for everything
involving the mapper configuration, however the mapped :class:`_schema.Table`
object is produced separately and passed to the declarative process
directly::


    from sqlalchemy.orm import declarative_base
    from sqlalchemy import Column, Integer, String, ForeignKey


    Base = declarative_base()

    # construct a Table directly.  The Base.metadata collection is
    # usually a good choice for MetaData but any MetaData
    # collection may be used.

    user_table = Table(
        "user",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("fullname", String),
        Column("nickname", String),
    )

    # construct the User class using this table.
    class User(Base):
        __table__ = user_table

Above, a :class:`_schema.Table` object is constructed using the approach
described at :ref:`metadata_describing`.   It can then be applied directly
to a class that is declaratively mapped.  The ``__tablename__`` and
``__table_args__`` declarative class attributes are not used in this form.
The above configuration is often more readable as an inline definition::

    class User(Base):
        __table__ = Table(
            "user",
            Base.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("fullname", String),
            Column("nickname", String),
        )

A natural effect of the above style is that the ``__table__`` attribute is
itself defined within the class definition block.   As such it may be
immediately referred towards within subsequent attributes, such as the example
below which illustrates referring to the ``type`` column in a polymorphic
mapper configuration::

    class Person(Base):
        __table__ = Table(
            'person',
            Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('type', String(50))
        )

        __mapper_args__ = {
            "polymorphic_on": __table__.c.type,
            "polymorhpic_identity": "person"
        }

The "imperative table" form is also used when a non-:class:`_schema.Table`
construct, such as a :class:`_sql.Join` or :class:`_sql.Subquery` object,
is to be mapped.  An example below::

    from sqlalchemy import select, func

    subq = select(
        func.count(orders.c.id).label('order_count'),
        func.max(orders.c.price).label('highest_order'),
        orders.c.customer_id
    ).group_by(orders.c.customer_id).subquery()

    customer_select = select(customers, subq).join_from(
        customers, subq, customers.c.id == subq.c.customer_id
    ).subquery()

    class Customer(Base):
        __table__ = customer_select

For background on mapping to non-:class:`_schema.Table` constructs see
the sections :ref:`orm_mapping_joins` and :ref:`orm_mapping_arbitrary_subqueries`.

The "imperative table" form is of particular use when the class itself
is using an alternative form of attribute declaration, such as Python
dataclasses.   See the section :ref:`orm_declarative_dataclasses` for detail.

.. seealso::

    :ref:`metadata_describing`

    :ref:`orm_declarative_dataclasses`

.. _orm_declarative_reflected:

Mapping Declaratively with Reflected Tables
--------------------------------------------

There are several patterns available which provide for producing mapped
classes against a series of :class:`_schema.Table` objects that were
introspected from the database, using the reflection process described at
:ref:`metadata_reflection`.

A very simple way to map a class to a table reflected from the database is to
use a declarative hybrid mapping, passing the
:paramref:`_schema.Table.autoload_with` parameter to the
:class:`_schema.Table`::

    engine = create_engine("postgresql://user:pass@hostname/my_existing_database")

    class MyClass(Base):
        __table__ = Table(
            'mytable',
            Base.metadata,
            autoload_with=engine
        )

A major downside of the above approach however is that it requires the database
connectivity source to be present while the application classes are being
declared; it's typical that classes are declared as the modules of an
application are being imported, but database connectivity isn't available
until the application starts running code so that it can consume configuration
information and create an engine.

Using DeferredReflection
^^^^^^^^^^^^^^^^^^^^^^^^^

To accommodate this case, a simple extension called the
:class:`.DeferredReflection` mixin is available, which alters the declarative
mapping process to be delayed until a special class-level
:meth:`.DeferredReflection.prepare` method is called, which will perform
the reflection process against a target database, and will integrate the
results with the declarative table mapping process, that is, classes which
use the ``__tablename__`` attribute::

    from sqlalchemy.orm import declarative_base
    from sqlalchemy.ext.declarative import DeferredReflection

    Base = declarative_base()

    class Reflected(DeferredReflection):
        __abstract__ = True

    class Foo(Reflected, Base):
        __tablename__ = 'foo'
        bars = relationship("Bar")

    class Bar(Reflected, Base):
        __tablename__ = 'bar'

        foo_id = Column(Integer, ForeignKey('foo.id'))

Above, we create a mixin class ``Reflected`` that will serve as a base
for classes in our declarative hierarchy that should become mapped when
the ``Reflected.prepare`` method is called.   The above mapping is not
complete until we do so, given an :class:`_engine.Engine`::


    engine = create_engine("postgresql://user:pass@hostname/my_existing_database")
    Reflected.prepare(engine)

The purpose of the ``Reflected`` class is to define the scope at which
classes should be reflectively mapped.   The plugin will search among the
subclass tree of the target against which ``.prepare()`` is called and reflect
all tables.

Using Automap
^^^^^^^^^^^^^^

A more automated solution to mapping against an existing database where
table reflection is to be used is to use the :ref:`automap_toplevel`
extension.  This extension will generate entire mapped classes from a
database schema, and allows several hooks for customization including the
ability to explicitly map some or all classes while still making use of
reflection to fill in the remaining columns.

.. seealso::

    :ref:`automap_toplevel`
