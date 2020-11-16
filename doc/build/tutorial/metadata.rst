.. |prev| replace:: :doc:`dbapi_transactions`
.. |next| replace:: :doc:`data`

.. include:: tutorial_nav_include.rst

.. _tutorial_working_with_metadata:

Working with Database Metadata
==============================

With engines and SQL execution down, we are ready to begin some Alchemy.
The central element of both SQLAlchemy Core and ORM is the SQL Expression
Language which allows for fluent, composable construction of SQL queries.
The foundation for these queries are Python objects that represent database
concepts like tables and columns.   These objects are known collectively
as :term:`database metadata`.

The most common foundational objects for database metadata in SQLAlchemy are
known as  :class:`_schema.MetaData`, :class:`_schema.Table`, and :class:`_schema.Column`.
The sections below will illustrate how these objects are used in both a
Core-oriented style as well as an ORM-oriented style.

.. container:: orm-header

    **ORM readers, stay with us!**

    As with other sections, Core users can skip the ORM sections, but ORM users
    would best be familiar with these objects from both perspectives.


.. rst-class:: core-header

.. _tutorial_core_metadata:

Setting up MetaData with Table objects
---------------------------------------

When we work with a relational database, the basic structure that we create and
query from is known as a **table**.   In SQLAlchemy, the "table" is represented
by a Python object similarly named :class:`_schema.Table`.

To start using the SQLAlchemy Expression Language,
we will want to have :class:`_schema.Table` objects constructed that represent
all of the database tables we are interested in working with.   Each
:class:`_schema.Table` may be **declared**, meaning we explicitly spell out
in source code what the table looks like, or may be **reflected**, which means
we generate the object based on what's already present in a particular database.
The two approaches can also be blended in many ways.

Whether we will declare or reflect our tables, we start out with a collection
that will be where we place our tables known as the :class:`_schema.MetaData`
object.  This object is essentially a :term:`facade` around a Python dictionary
that stores a series of :class:`_schema.Table` objects keyed to their string
name.   Constructing this object looks like::

    >>> from sqlalchemy import MetaData
    >>> metadata = MetaData()

Having a single :class:`_schema.MetaData` object for an entire application is
the most common case, represented as a module-level variable in a single place
in an application, often in a "models" or "dbschema" type of package.  There
can be multiple :class:`_schema.MetaData` collections as well,  however
it's typically most helpful if a series :class:`_schema.Table` objects that are
related to each other belong to a single :class:`_schema.MetaData` collection.


Once we have a :class:`_schema.MetaData` object, we can declare some
:class:`_schema.Table` objects.  This tutorial will start with the classic
SQLAlchemy tutorial model, that of the table ``user``, which would for
example represent the users of a website, and the table ``address``,
representing a list of email addresses associated with rows in the ``user``
table.   We normally assign each :class:`_schema.Table` object to a variable
that will be how we will refer to the table in application code::

    >>> from sqlalchemy import Table, Column, Integer, String
    >>> user_table = Table(
    ...     "user_account",
    ...     metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String(30)),
    ...     Column('fullname', String)
    ... )

We can observe that the above :class:`_schema.Table` construct looks a lot like
a SQL CREATE TABLE statement; starting with the table name, then listing out
each column, where each column has a name and a datatype.   The objects we
use above are:

* :class:`_schema.Table` - represents a database table and assigns itself
  to a :class:`_schema.MetaData` collection.

* :class:`_schema.Column` - represents a column in a database table, and
  assigns itself to a :class:`_schema.Table` object.   The :class:`_schema.Column`
  usually includes a string name and a type object.   The collection of
  :class:`_schema.Column` objects in terms of the parent :class:`_schema.Table`
  are typically accessed via an associative array located at :attr:`_schema.Table.c`::

    >>> user_table.c.name
    Column('name', String(length=30), table=<user_account>)

    >>> user_table.c.keys()
    ['id', 'name', 'fullname']

* :class:`_types.Integer`, :class:`_types.String` - these classes represent
  SQL datatypes and can be passed to a :class:`_schema.Column` with or without
  necessarily being instantiated.  Above, we want to give a length of "30" to
  the "name" column, so we instantiated ``String(30)``.  But for "id" and
  "fullname" we did not specify these, so we can send the class itself.

.. seealso::

    The reference and API documentation for :class:`_schema.MetaData`,
    :class:`_schema.Table` and :class:`_schema.Column` is at :ref:`metadata_toplevel`.
    The reference documentation for datatypes is at :ref:`types_toplevel`.

In an upcoming section, we will illustrate one of the fundamental
functions of :class:`_schema.Table` which
is to generate :term:`DDL` on a particular database connection.  But first
we will declare a second :class:`_schema.Table`.

.. rst-class:: core-header

Declaring Simple Constraints
-----------------------------

The first :class:`_schema.Column` in the above ``user_table`` includes the
:paramref:`_schema.Column.primary_key` parameter which is a shorthand technique
of indicating that this :class:`_schema.Column` should be part of the primary
key for this table.  The primary key itself is normally declared implicitly
and is represented by the :class:`_schema.PrimaryKeyConstraint` construct,
which we can see on the :attr:`_schema.Table.primary_key`
attribute on the :class:`_schema.Table` object::

    >>> user_table.primary_key
    PrimaryKeyConstraint(Column('id', Integer(), table=<user_account>, primary_key=True, nullable=False))

The constraint that is most typically declared explicitly is the
:class:`_schema.ForeignKeyConstraint` object that corresponds to a database
:term:`foreign key constraint`.  When we declare tables that are related to
each other, SQLAlchemy uses the presence of these foreign key constraint
declarations not only so that they are emitted within CREATE statements to
the database, but also to assist in constructing SQL expressions.

A :class:`_schema.ForeignKeyConstraint` that involves only a single column
on the target table is typically declared using a column-level shorthand notation
via the :class:`_schema.ForeignKey` object.  Below we declare a second table
``address`` that will have a foreign key constraint referring to the ``user``
table::

    >>> from sqlalchemy import ForeignKey
    >>> address_table = Table(
    ...     "address",
    ...     metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('user_id', ForeignKey('user_account.id'), nullable=False),
    ...     Column('email_address', String, nullable=False)
    ... )

The table above also features a third kind of constraint, which in SQL is the
"NOT NULL" constraint, indicated above using the :paramref:`_schema.Column.nullable`
parameter.

.. tip:: When using the :class:`_schema.ForeignKey` object within a
   :class:`_schema.Column` definition, we can omit the datatype for that
   :class:`_schema.Column`; it is automatically inferred from that of the
   related column, in the above example the :class:`_types.Integer` datatype
   of the ``user_account.id`` column.

In the next section we will emit the completed DDL for the ``user`` and
``address`` table to see the completed result.

.. rst-class:: core-header, orm-dependency


.. _tutorial_emitting_ddl:

Emitting DDL to the Database
----------------------------

We've constructed a fairly elaborate object hierarchy to represent
two database tables, starting at the root :class:`_schema.MetaData`
object, then into two :class:`_schema.Table` objects, each of which hold
onto a collection of :class:`_schema.Column` and :class:`_schema.Constraint`
objects.   This object structure will be at the center of most operations
we perform with both Core and ORM going forward.

The first useful thing we can do with this structure will be to emit CREATE
TABLE statements, or :term:`DDL`, to our SQLite database so that we can insert
and query data from them.   We have already all the tools needed to do so, by
invoking the
:meth:`_schema.MetaData.create_all` method on our :class:`_schema.MetaData`,
sending it the :class:`_future.Engine` that refers to the target database:

.. sourcecode:: pycon+sql

    >>> metadata.create_all(engine)
    {opensql}BEGIN (implicit)
    PRAGMA main.table_...info("user_account")
    ...
    PRAGMA main.table_...info("address")
    ...
    CREATE TABLE user_account (
        id INTEGER NOT NULL,
        name VARCHAR(30),
        fullname VARCHAR,
        PRIMARY KEY (id)
    )
    ...
    CREATE TABLE address (
        id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        email_address VARCHAR NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY(user_id) REFERENCES user_account (id)
    )
    ...
    COMMIT

The DDL create process by default includes some SQLite-specific PRAGMA statements
that test for the existence of each table before emitting a CREATE.   The full
series of steps are also included within a BEGIN/COMMIT pair to accommodate
for transactional DDL (SQLite does actually support transactional DDL, however
the ``sqlite3`` database driver historically runs DDL in "autocommit" mode).

The create process also takes care of emitting CREATE statements in the correct
order; above, the FOREIGN KEY constraint is dependent on the ``user`` table
existing, so the ``address`` table is created second.   In more complicated
dependency scenarios the FOREIGN KEY constraints may also be applied to tables
after the fact using ALTER.

The :class:`_schema.MetaData` object also features a
:meth:`_schema.MetaData.drop_all` method that will emit DROP statements in the
reverse order as it would emit CREATE in order to drop schema elements.

.. topic:: Migration tools are usually appropriate

    Overall, the CREATE / DROP feature of :class:`_schema.MetaData` is useful
    for test suites, small and/or new applications, and applications that use
    short-lived databases.  For management of an application database schema
    over the long term however, a schema management tool such as `Alembic
    <https://alembic.sqlalchemy.org>`_, which builds upon SQLAlchemy, is likely
    a better choice, as it can manage and orchestrate the process of
    incrementally altering a fixed database schema over time as the design of
    the application changes.


.. rst-class:: orm-header

.. _tutorial_orm_table_metadata:

Defining Table Metadata with the ORM
------------------------------------

This ORM-only section will provide an example of the declaring the
same database structure illustrated in the previous section, using a more
ORM-centric configuration paradigm.   When using
the ORM, the process by which we declare :class:`_schema.Table` metadata
is usually combined with the process of declaring :term:`mapped` classes.
The mapped class is any Python class we'd like to create, which will then
have attributes on it that will be linked to the columns in a database table.
While there are a few varieties of how this is achieved, the most common
style is known as
:ref:`declarative <orm_declarative_mapper_config_toplevel>`, and allows us
to declare our user-defined classes and :class:`_schema.Table` metadata
at once.

Setting up the Registry
^^^^^^^^^^^^^^^^^^^^^^^

When using the ORM, the :class:`_schema.MetaData` collection remains present,
however it itself is contained within an ORM-only object known as the
:class:`_orm.registry`.   We create a :class:`_orm.registry` by constructing
it::

    >>> from sqlalchemy.orm import registry
    >>> mapper_registry = registry()

The above :class:`_orm.registry`, when constructed, automatically includes
a :class:`_schema.MetaData` object that will store a collection of
:class:`_schema.Table` objects::

    >>> mapper_registry.metadata
    MetaData()

Instead of declaring :class:`_schema.Table` objects directly, we will now
declare them indirectly through directives applied to our mapped classes. In
the most common approach, each mapped class descends from a common base class
known as the **declarative base**.   We get a new declarative base from the
:class:`_orm.registry` using the :meth:`_orm.registry.generate_base` method::

    >>> Base = mapper_registry.generate_base()

.. tip::

    The steps of creating the :class:`_orm.registry` and "declarative base"
    classes can be combined into one step using the historically familiar
    :func:`_orm.declarative_base` function::

        from sqlalchemy.orm import declarative_base
        Base = declarative_base()

    ..

.. _tutorial_declaring_mapped_classes:

Declaring Mapped Classes
^^^^^^^^^^^^^^^^^^^^^^^^

The ``Base`` object above is a Python class which will serve as the base class
for the ORM mapped classes we declare.  We can now define ORM mapped classes
for the ``user`` and ``address`` table in terms of new classes ``User`` and
``Address``::

    >>> from sqlalchemy.orm import relationship
    >>> class User(Base):
    ...     __tablename__ = 'user_account'
    ...
    ...     id = Column(Integer, primary_key=True)
    ...     name = Column(String(30))
    ...     fullname = Column(String)
    ...
    ...     addresses = relationship("Address", back_populates="user")
    ...
    ...     def __repr__(self):
    ...        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

    >>> class Address(Base):
    ...     __tablename__ = 'address'
    ...
    ...     id = Column(Integer, primary_key=True)
    ...     email_address = Column(String, nullable=False)
    ...     user_id = Column(Integer, ForeignKey('user_account.id'))
    ...
    ...     user = relationship("User", back_populates="addresses")
    ...
    ...     def __repr__(self):
    ...         return f"Address(id={self.id!r}, email_address={self.email_address!r})"

The above two classes are now our mapped classes, and are available for use in
ORM persistence and query operations, which will be described later. But they
also include :class:`_schema.Table` objects that were generated as part of the
declarative mapping process, and are equivalent to the ones that we declared
directly in the previous Core section.   We can see these
:class:`_schema.Table` objects from a declarative mapped class using the
``.__table__`` attribute::

    >>> User.__table__
    Table('user_account', MetaData(),
        Column('id', Integer(), table=<user_account>, primary_key=True, nullable=False),
        Column('name', String(length=30), table=<user_account>),
        Column('fullname', String(), table=<user_account>), schema=None)

This :class:`_schema.Table` object was generated from the declarative process
based on the ``.__tablename__`` attribute defined on each of our classes,
as well as through the use of :class:`_schema.Column` objects assigned
to class-level attributes within the classes.   These :class:`_schema.Column`
objects can usually be declared without an explicit "name" field inside
the constructor, as the Declarative process will name them automatically
based on the attribute name that was used.

.. seealso::

    :ref:`orm_declarative_mapping` - overview of Declarative class mapping


Other Mapped Class Details
^^^^^^^^^^^^^^^^^^^^^^^^^^^

For a few quick explanations for the classes above, note the following
attributes:

* **the classes have an automatically generated __init__() method** - both classes by default
  receive an ``__init__()`` method that allows for parameterized construction
  of the objects.  We are free to provide our own ``__init__()`` method as well.
  The ``__init__()`` allows us to create instances of ``User`` and ``Address``
  passing attribute names, most of which above are linked directly to
  :class:`_schema.Column` objects, as parameter names::

    >>> sandy = User(name="sandy", fullname="Sandy Cheeks")

  More detail on this method is at :ref:`mapped_class_default_constructor`.

  ..

* **we provided a __repr__() method** - this is **fully optional**, and is
  strictly so that our custom classes have a descriptive string representation
  and is not otherwise required::

    >>> sandy
    User(id=None, name='sandy', fullname='Sandy Cheeks')

  ..

  An interesting thing to note above is that the ``id`` attribute automatically
  returns ``None`` when accessed, rather than raising ``AttributeError`` as
  would be the usual Python behavior for missing attributes.

* **we also included a bidirectional relationship** - this  is another **fully optional**
  construct, where we made use of an ORM construct called
  :func:`_orm.relationship` on both classes, which indicates to the ORM that
  these ``User`` and ``Address`` classes refer to each other in a :term:`one to
  many` / :term:`many to one` relationship.  The use of
  :func:`_orm.relationship` above is so that we may demonstrate its behavior
  later in this tutorial; it is  **not required** in order to define the
  :class:`_schema.Table` structure.


Emitting DDL to the database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This section is named the same as the section :ref:`tutorial_emitting_ddl`
discussed in terms of Core.   This is because emitting DDL with our
ORM mapped classes is not any different.  If we wanted to emit DDL
for the :class:`_schema.Table` objects we've created as part of
our declaratively mapped classes, we still can use
:meth:`_schema.MetaData.create_all` as before.

In our case, we have already generated the ``user`` and ``address`` tables
in our SQLite database.   If we had not done so already, we would be free to
make use of the :class:`_schema.MetaData` associated with our
:class:`_orm.registry` and ORM declarative base class in order to do so,
using :meth:`_schema.MetaData.create_all`::

    # emit CREATE statements given ORM registry
    mapper_registry.metadata.create_all(engine)

    # the identical MetaData object is also present on the
    # declarative base
    Base.metadata.create_all(engine)


Combining Core Table Declarations with ORM Declarative
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As an alternative approach to the mapping process shown previously
at :ref:`tutorial_declaring_mapped_classes`, we may also make
use of the :class:`_schema.Table` objects we created directly in the section
:ref:`tutorial_core_metadata` in conjunction with
declarative mapped classes from a :func:`_orm.declarative_base` generated base
class.

This form is called  :ref:`hybrid table <orm_imperative_table_configuration>`,
and it consists of assigning to the ``.__table__`` attribute directly, rather
than having the declarative process generate it::

    class User(Base):
        __table__ = user_table

         addresses = relationship("Address", back_populates="user")

         def __repr__(self):
            return f"User({self.name!r}, {self.fullname!r})"

    class Address(Base):
        __table__ = address_table

         user = relationship("User", back_populates="addresses")

         def __repr__(self):
             return f"Address({self.email_address!r})"

The above two classes are equivalent to those which we declared in the
previous mapping example.

The traditional "declarative base" approach using ``__tablename__`` to
automatically generate :class:`_schema.Table` objects remains the most popular
method to declare table metadata.  However, disregarding the ORM mapping
functionality it achieves, as far as table declaration it's merely a syntactical
convenience on top of the :class:`_schema.Table` constructor.

We will next refer to our ORM mapped classes above when we talk about data
manipulation in terms of the ORM, in the section :ref:`tutorial_inserting_orm`.


.. rst-class:: core-header

.. _tutorial_table_reflection:

Table Reflection
-------------------------------

To round out the section on working with table metadata, we will illustrate
another operation that was mentioned at the beginning of the section,
that of **table reflection**.   Table reflection refers to the process of
generating :class:`_schema.Table` and related objects by reading the current
state of a database.   Whereas in the previous sections we've been declaring
:class:`_schema.Table` objects in Python and then emitting DDL to the database,
the reflection process does it in reverse.

As an example of reflection, we will create a new :class:`_schema.Table`
object which represents the ``some_table`` object we created manually in
the earler sections of this document.  There are again some varieties of
how this is performed, however the most basic is to construct a
:class:`_schema.Table` object, given the name of the table and a
:class:`_schema.MetaData` collection to which it will belong, then
instead of indicating individual :class:`_schema.Column` and
:class:`_schema.Constraint` objects, pass it the target :class:`_future.Engine`
using the :paramref:`_schema.Table.autoload_with` parameter:

.. sourcecode:: pycon+sql

    >>> some_table = Table("some_table", metadata, autoload_with=engine)
    {opensql}BEGIN (implicit)
    PRAGMA main.table_...info("some_table")
    [raw sql] ()
    SELECT sql FROM  (SELECT * FROM sqlite_master UNION ALL   SELECT * FROM sqlite_temp_master) WHERE name = ? AND type = 'table'
    [raw sql] ('some_table',)
    PRAGMA main.foreign_key_list("some_table")
    ...
    PRAGMA main.index_list("some_table")
    ...
    ROLLBACK{stop}

At the end of the process, the ``some_table`` object now contains the
information about the :class:`_schema.Column` objects present in the table, and
the object is usable in exactly the same way as a :class:`_schema.Table` that
we declared explicitly.::

    >>> some_table
    Table('some_table', MetaData(),
        Column('x', INTEGER(), table=<some_table>),
        Column('y', INTEGER(), table=<some_table>),
        schema=None)

.. seealso::

    Read more about table and schema reflection at :ref:`metadata_reflection_toplevel`.

    For ORM-related variants of table reflection, the section
    :ref:`orm_declarative_reflected` includes an overview of the available
    options.
