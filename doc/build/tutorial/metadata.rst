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
    >>> metadata_obj = MetaData()

Having a single :class:`_schema.MetaData` object for an entire application is
the most common case, represented as a module-level variable in a single place
in an application, often in a "models" or "dbschema" type of package.  There
can be multiple :class:`_schema.MetaData` collections as well,  however
it's typically most helpful if a series of :class:`_schema.Table` objects that are
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
    ...     metadata_obj,
    ...     Column("id", Integer, primary_key=True),
    ...     Column("name", String(30)),
    ...     Column("fullname", String),
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
    ...     metadata_obj,
    ...     Column("id", Integer, primary_key=True),
    ...     Column("user_id", ForeignKey("user_account.id"), nullable=False),
    ...     Column("email_address", String, nullable=False),
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
sending it the :class:`_engine.Engine` that refers to the target database:

.. sourcecode:: pycon+sql

    >>> metadata_obj.create_all(engine)
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

This ORM-only section will provide an example declaring the
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

Establishing a Declarative Base
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using the ORM, the :class:`_schema.MetaData` collection remains present,
however it itself is associated with an ORM-only construct commonly referred
towards as the **Declarative Base**.   The most expedient way to acquire
a new Declarative Base is to create a new class that subclasses the
SQLAlchemy :class:`_orm.DeclarativeBase` class::

    >>> from sqlalchemy.orm import DeclarativeBase
    >>> class Base(DeclarativeBase):
    ...     pass

Above, the ``Base`` class is what we'll refer towards as the Declarative Base.
When we make new classes that are subclasses of ``Base``, combined with
appropriate class-level directives, they will each be established as part of an
object relational mapping against a particular database table (or tables, in
advanced usages).

The Declarative Base, when declared as a new class, refers to a
:class:`_schema.MetaData` collection that is
created for us automatically (options exist to use our own :class:`.MetaData`
object as well); this :class:`.MetaData` is accessible via the ``.metadata``
class-level attribute.  As we create new mapped classes, they each will reference a
:class:`.Table` within this :class:`.MetaData` collection::

    >>> Base.metadata
    MetaData()

The Declarative Base also refers to a collection called :class:`_orm.registry`, which
is the central "mapper configuration" unit in the SQLAlchemy ORM.  While
seldom accessed directly, this object is central to the mapper configuration
process, as a set of ORM mapped classes will coordinate with each other via
this registry.   As was the case with :class:`.MetaData`, our Declarative
Base also created a :class:`_orm.registry` for us (again with options to
pass our own :class:`_orm.registry`), which we can access
via the ``.registry`` class variable::

    >>> Base.registry
    <sqlalchemy.orm.decl_api.registry object at 0x...>

:class:`_orm.registry` also provides other mapper configurational patterns,
including different ways to acquire a Declarative Base object, as well as class
decorators and class-processing functions which allow user-defined classes to
be mapped without using any particular base class. Therefore, keep in mind that
all the ORM patterns here that use "declarative base" can just as easily
use other patterns based on class decorators or configurational functions.

.. _tutorial_declaring_mapped_classes:

Declaring Mapped Classes
^^^^^^^^^^^^^^^^^^^^^^^^

With the ``Base`` class established, we can now define ORM mapped classes
for the ``user_account`` and ``address`` tables in terms of new classes ``User`` and
``Address``.  We illustrate below the most modern form of Declarative, which
is driven from :pep:`484` type annotations using a special type
:class:`.Mapped`, which indicates attributes to be mapped as particular
types::

    >>> from typing import List
    >>> from typing import Optional
    >>> from sqlalchemy.orm import Mapped
    >>> from sqlalchemy.orm import mapped_column
    >>> from sqlalchemy.orm import relationship

    >>> class User(Base):
    ...     __tablename__ = "user_account"
    ...
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     name: Mapped[str] = mapped_column(String(30))
    ...     fullname: Mapped[Optional[str]]
    ...
    ...     addresses: Mapped[List["Address"]] = relationship(back_populates="user")
    ...
    ...     def __repr__(self) -> str:
    ...         return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

    >>> class Address(Base):
    ...     __tablename__ = "address"
    ...
    ...     id: Mapped[int] = mapped_column(primary_key=True)
    ...     email_address: Mapped[str]
    ...     user_id = mapped_column(ForeignKey("user_account.id"))
    ...
    ...     user: Mapped[User] = relationship(back_populates="addresses")
    ...
    ...     def __repr__(self) -> str:
    ...         return f"Address(id={self.id!r}, email_address={self.email_address!r})"

The two classes above, ``User`` and ``Address``, are now referred towards
as **ORM Mapped Classes**, and are available for use in
ORM persistence and query operations, which will be described later.  Details
about these classes include:

* Each class refers to a :class:`_schema.Table` object that was generated as
  part of the declarative mapping process, and are equivalent
  in structure to the :class:`_schema.Table` objects we constructed
  directly in the previous Core section.   This :class:`._schema.Table`
  is available from an attribute added to the class called ``__table__``.
* To indicate columns in the :class:`_schema.Table`, we use the
  :func:`_orm.mapped_column` construct, in combination with
  typing annotations based on the :class:`_orm.Mapped` type.
* For columns with simple datatypes and no other options, we can indicate a
  :class:`_orm.Mapped` type annotation alone, using simple Python types like
  ``int`` and ``str`` to mean :class:`.Integer` and :class:`.String`.
  Customization of how Python types are interpreted within the Declarative
  mapping process is very open ended; see the section
  :ref:`orm_declarative_mapped_column` for background.
* A column can be declared as "nullable" or "not null" based on the
  presence of the ``Optional[]`` type annotation; alternatively, the
  :paramref:`_orm.mapped_column.nullable` parameter may be used instead.
* Usage of explicit typing annotations is **completely
  optional**.  We can also use :func:`_orm.mapped_column` without annotations.
  When using this form, we would use more explicit type objects like
  :class:`.Integer` and :class:`.String` as well as ``nullable=False``
  as needed within each :func:`_orm.mapped_column` construct.
* Two additional attributes, ``User.addresses`` and ``Address.user``, define
  a different kind of attribute called :func:`_orm.relationship`, which
  features similar annotation-aware configuration styles as shown.  The
  :func:`_orm.relationship` construct is discussed more fully at
  :ref:`tutorial_orm_related_objects`.
* The classes are automatically given an ``__init__()`` method if we don't
  declare one of our own.  The default form of this method accepts all
  attribute names as optional keyword arguments::

    >>> sandy = User(name="sandy", fullname="Sandy Cheeks")

* The ``__repr__()`` methods are added so that we get a readable string output;
  there's no requirement for these methods to be here.

.. topic::  Where'd the old Declarative go?

    Users of SQLAlchemy 1.4 or previous will note that the above mapping
    uses a dramatically different form than before; not only does it use
    :func:`_orm.mapped_column` instead of :class:`.Column` in the Declarative
    mapping, it also uses Python type annotations to derive column information.

    To provide context for users of the "old" way, Declarative mappings can
    still be made using :class:`.Column` objects (as well as using the
    :func:`_orm.declarative_base` function to create the base class) as before,
    and these forms will continue to be supported with no plans to
    remove support.  The reason these two facilities
    are superseded by new constructs is first and foremost to integrate
    smoothly with :pep:`484` tools, including IDEs such as VSCode and type
    checkers such as Mypy and Pyright, without the need for plugins. Secondly,
    deriving the declarations from type annotations is part of SQLAlchemy's
    integration with Python dataclasses, which can now be
    :ref:`generated natively <orm_declarative_native_dataclasses>` from mappings.

    For users who like the "old" way, but still desire their IDEs to not
    mistakenly report typing errors for their declarative mappings, the
    :func:`_orm.mapped_column` construct is a drop-in replacement for
    :class:`.Column` in an ORM Declarative mapping (note that
    :func:`_orm.mapped_column` is for ORM Declarative mappings only; it can't
    be used within a :class:`.Table` construct), and the type annotations are
    optional. Our mapping above can be written without annotations as::

        class User(Base):
            __tablename__ = "user_account"

            id = mapped_column(Integer, primary_key=True)
            name = mapped_column(String(30), nullable=False)
            fullname = mapped_column(String)

            addresses = relationship("Address", back_populates="user")

            # ... definition continues

    The above class has an advantage over one that uses :class:`.Column`
    directly, in that the ``User`` class as well as instances of ``User``
    will indicate the correct typing information to typing tools, without
    the use of plugins.  :func:`_orm.mapped_column` also allows for additional
    ORM-specific parameters to configure behaviors such as deferred column loading,
    which previously needed a separate :func:`_orm.deferred` function to be
    used with :class:`_schema.Column`.

    There's also an example of converting an old-style Declarative class
    to the new style, which can be seen at :ref:`whatsnew_20_orm_declarative_typing`
    in the :ref:`whatsnew_20_toplevel` guide.

.. seealso::

    :ref:`orm_mapping_styles` - full background on different ORM configurational
    styles.

    :ref:`orm_declarative_mapping` - overview of Declarative class mapping

    :ref:`orm_declarative_table` - detail on how to use
    :func:`_orm.mapped_column` and :class:`_orm.Mapped` to define the columns
    within a :class:`_schema.Table` to be mapped when using Declarative.


Emitting DDL to the database from an ORM mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As our ORM mapped classes refer to :class:`_schema.Table` objects contained
within a :class:`_schema.MetaData` collection, emitting DDL given the
Declarative Base uses the same process as that described previously at
:ref:`tutorial_emitting_ddl`. In our case, we have already generated the
``user`` and ``address`` tables in our SQLite database. If we had not done so
already, we would be free to make use of the :class:`_schema.MetaData`
associated with our ORM Declarative Base class in order to do so, by accessing
the collection from the ``Base.metadata`` attribute and then using
:meth:`_schema.MetaData.create_all` as before::

    Base.metadata.create_all(engine)

Combining Core Table Declarations with ORM Declarative
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As an alternative approach to the mapping process shown previously
at :ref:`tutorial_declaring_mapped_classes`, we may also make
use of the :class:`_schema.Table` objects we created directly in the section
:ref:`tutorial_core_metadata` in conjunction with
declarative mapped classes from a Declarative Base
class.

This form is called
:ref:`Declarative with Imperative Table <orm_imperative_table_configuration>`,
and it consists of assigning a :class:`_schema.Table` object to the
``.__table__`` attribute directly, rather than having the declarative process
generate it from the ``.__tablename__`` attribute with separate
:class:`_orm.Mapped` and/or :func:`_orm.mapped_column` directives. This is
illustrated below by using our pre-existing ``user_table`` and
``address_table`` :class:`_schema.Table` objects to map them to new classes
(**note to readers running code**: these examples are for illustration only
and should not be run)::

    class Base(DeclarativeBase):
        pass


    class User(Base):
        __table__ = user_table

        addresses: Mapped[List["Address"]] = relationship(back_populates="user")

        def __repr__(self):
            return f"User({self.name!r}, {self.fullname!r})"


    class Address(Base):
        __table__ = address_table

        user: Mapped["User"] = relationship(back_populates="addresses")

        def __repr__(self):
            return f"Address({self.email_address!r})"

The above two classes, ``User`` and ``Address``, are equivalent to those which
we declared in the previous mapping example using ``__tablename__`` and
:func:`_orm.mapped_column`.

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
the earlier sections of this document.  There are again some varieties of
how this is performed, however the most basic is to construct a
:class:`_schema.Table` object, given the name of the table and a
:class:`_schema.MetaData` collection to which it will belong, then
instead of indicating individual :class:`_schema.Column` and
:class:`_schema.Constraint` objects, pass it the target :class:`_engine.Engine`
using the :paramref:`_schema.Table.autoload_with` parameter:

.. sourcecode:: pycon+sql

    >>> some_table = Table("some_table", metadata_obj, autoload_with=engine)
    {opensql}BEGIN (implicit)
    PRAGMA main.table_...info("some_table")
    [raw sql] ()
    SELECT sql FROM  (SELECT * FROM sqlite_master UNION ALL   SELECT * FROM sqlite_temp_master) WHERE name = ? AND type in ('table', 'view')
    [raw sql] ('some_table',)
    PRAGMA main.foreign_key_list("some_table")
    ...
    PRAGMA main.index_list("some_table")
    ...
    ROLLBACK{stop}

At the end of the process, the ``some_table`` object now contains the
information about the :class:`_schema.Column` objects present in the table, and
the object is usable in exactly the same way as a :class:`_schema.Table` that
we declared explicitly::

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
