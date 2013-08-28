.. _metadata_toplevel:

.. _metadata_describing_toplevel:

.. _metadata_describing:

==================================
Describing Databases with MetaData
==================================

.. module:: sqlalchemy.schema

This section discusses the fundamental :class:`.Table`, :class:`.Column`
and :class:`.MetaData` objects.

A collection of metadata entities is stored in an object aptly named
:class:`~sqlalchemy.schema.MetaData`::

    from sqlalchemy import *

    metadata = MetaData()

:class:`~sqlalchemy.schema.MetaData` is a container object that keeps together
many different features of a database (or multiple databases) being described.

To represent a table, use the :class:`~sqlalchemy.schema.Table` class. Its two
primary arguments are the table name, then the
:class:`~sqlalchemy.schema.MetaData` object which it will be associated with.
The remaining positional arguments are mostly
:class:`~sqlalchemy.schema.Column` objects describing each column::

    user = Table('user', metadata,
        Column('user_id', Integer, primary_key = True),
        Column('user_name', String(16), nullable = False),
        Column('email_address', String(60)),
        Column('password', String(20), nullable = False)
    )

Above, a table called ``user`` is described, which contains four columns. The
primary key of the table consists of the ``user_id`` column. Multiple columns
may be assigned the ``primary_key=True`` flag which denotes a multi-column
primary key, known as a *composite* primary key.

Note also that each column describes its datatype using objects corresponding
to genericized types, such as :class:`~sqlalchemy.types.Integer` and
:class:`~sqlalchemy.types.String`. SQLAlchemy features dozens of types of
varying levels of specificity as well as the ability to create custom types.
Documentation on the type system can be found at :ref:`types`.

Accessing Tables and Columns
----------------------------

The :class:`~sqlalchemy.schema.MetaData` object contains all of the schema
constructs we've associated with it. It supports a few methods of accessing
these table objects, such as the ``sorted_tables`` accessor which returns a
list of each :class:`~sqlalchemy.schema.Table` object in order of foreign key
dependency (that is, each table is preceded by all tables which it
references)::

    >>> for t in metadata.sorted_tables:
    ...    print t.name
    user
    user_preference
    invoice
    invoice_item

In most cases, individual :class:`~sqlalchemy.schema.Table` objects have been
explicitly declared, and these objects are typically accessed directly as
module-level variables in an application. Once a
:class:`~sqlalchemy.schema.Table` has been defined, it has a full set of
accessors which allow inspection of its properties. Given the following
:class:`~sqlalchemy.schema.Table` definition::

    employees = Table('employees', metadata,
        Column('employee_id', Integer, primary_key=True),
        Column('employee_name', String(60), nullable=False),
        Column('employee_dept', Integer, ForeignKey("departments.department_id"))
    )

Note the :class:`~sqlalchemy.schema.ForeignKey` object used in this table -
this construct defines a reference to a remote table, and is fully described
in :ref:`metadata_foreignkeys`. Methods of accessing information about this
table include::

    # access the column "EMPLOYEE_ID":
    employees.columns.employee_id

    # or just
    employees.c.employee_id

    # via string
    employees.c['employee_id']

    # iterate through all columns
    for c in employees.c:
        print c

    # get the table's primary key columns
    for primary_key in employees.primary_key:
        print primary_key

    # get the table's foreign key objects:
    for fkey in employees.foreign_keys:
        print fkey

    # access the table's MetaData:
    employees.metadata

    # access the table's bound Engine or Connection, if its MetaData is bound:
    employees.bind

    # access a column's name, type, nullable, primary key, foreign key
    employees.c.employee_id.name
    employees.c.employee_id.type
    employees.c.employee_id.nullable
    employees.c.employee_id.primary_key
    employees.c.employee_dept.foreign_keys

    # get the "key" of a column, which defaults to its name, but can
    # be any user-defined string:
    employees.c.employee_name.key

    # access a column's table:
    employees.c.employee_id.table is employees

    # get the table related by a foreign key
    list(employees.c.employee_dept.foreign_keys)[0].column.table

Creating and Dropping Database Tables
-------------------------------------

Once you've defined some :class:`~sqlalchemy.schema.Table` objects, assuming
you're working with a brand new database one thing you might want to do is
issue CREATE statements for those tables and their related constructs (as an
aside, it's also quite possible that you *don't* want to do this, if you
already have some preferred methodology such as tools included with your
database or an existing scripting system - if that's the case, feel free to
skip this section - SQLAlchemy has no requirement that it be used to create
your tables).

The usual way to issue CREATE is to use
:func:`~sqlalchemy.schema.MetaData.create_all` on the
:class:`~sqlalchemy.schema.MetaData` object. This method will issue queries
that first check for the existence of each individual table, and if not found
will issue the CREATE statements:

    .. sourcecode:: python+sql

        engine = create_engine('sqlite:///:memory:')

        metadata = MetaData()

        user = Table('user', metadata,
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(16), nullable = False),
            Column('email_address', String(60), key='email'),
            Column('password', String(20), nullable = False)
        )

        user_prefs = Table('user_prefs', metadata,
            Column('pref_id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey("user.user_id"), nullable=False),
            Column('pref_name', String(40), nullable=False),
            Column('pref_value', String(100))
        )

        {sql}metadata.create_all(engine)
        PRAGMA table_info(user){}
        CREATE TABLE user(
                user_id INTEGER NOT NULL PRIMARY KEY,
                user_name VARCHAR(16) NOT NULL,
                email_address VARCHAR(60),
                password VARCHAR(20) NOT NULL
        )
        PRAGMA table_info(user_prefs){}
        CREATE TABLE user_prefs(
                pref_id INTEGER NOT NULL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES user(user_id),
                pref_name VARCHAR(40) NOT NULL,
                pref_value VARCHAR(100)
        )

:func:`~sqlalchemy.schema.MetaData.create_all` creates foreign key constraints
between tables usually inline with the table definition itself, and for this
reason it also generates the tables in order of their dependency. There are
options to change this behavior such that ``ALTER TABLE`` is used instead.

Dropping all tables is similarly achieved using the
:func:`~sqlalchemy.schema.MetaData.drop_all` method. This method does the
exact opposite of :func:`~sqlalchemy.schema.MetaData.create_all` - the
presence of each table is checked first, and tables are dropped in reverse
order of dependency.

Creating and dropping individual tables can be done via the ``create()`` and
``drop()`` methods of :class:`~sqlalchemy.schema.Table`. These methods by
default issue the CREATE or DROP regardless of the table being present:

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///:memory:')

    meta = MetaData()

    employees = Table('employees', meta,
        Column('employee_id', Integer, primary_key=True),
        Column('employee_name', String(60), nullable=False, key='name'),
        Column('employee_dept', Integer, ForeignKey("departments.department_id"))
    )
    {sql}employees.create(engine)
    CREATE TABLE employees(
    employee_id SERIAL NOT NULL PRIMARY KEY,
    employee_name VARCHAR(60) NOT NULL,
    employee_dept INTEGER REFERENCES departments(department_id)
    )
    {}

``drop()`` method:

.. sourcecode:: python+sql

    {sql}employees.drop(engine)
    DROP TABLE employees
    {}

To enable the "check first for the table existing" logic, add the
``checkfirst=True`` argument to ``create()`` or ``drop()``::

    employees.create(engine, checkfirst=True)
    employees.drop(engine, checkfirst=False)

.. _schema_migrations:

Altering Schemas through Migrations
-----------------------------------

While SQLAlchemy directly supports emitting CREATE and DROP statements for schema
constructs, the ability to alter those constructs, usually via the ALTER statement
as well as other database-specific constructs, is outside of the scope of SQLAlchemy
itself.  While it's easy enough to emit ALTER statements and similar by hand,
such as by passing a string to :meth:`.Connection.execute` or by using the
:class:`.DDL` construct, it's a common practice to automate the maintenance of
database schemas in relation to application code using schema migration tools.

There are two major migration tools available for SQLAlchemy:

* `Alembic <http://alembic.readthedocs.org>`_ - Written by the author of SQLAlchemy,
  Alembic features a highly customizable environment and a minimalistic usage pattern,
  supporting such features as transactional DDL, automatic generation of "candidate"
  migrations, an "offline" mode which generates SQL scripts, and support for branch
  resolution.
* `SQLAlchemy-Migrate <http://code.google.com/p/sqlalchemy-migrate/>`_ - The original
  migration tool for SQLAlchemy, SQLAlchemy-Migrate is widely used and continues
  under active development.   SQLAlchemy-Migrate includes features such as
  SQL script generation, ORM class generation, ORM model comparison, and extensive
  support for SQLite migrations.


Specifying the Schema Name
---------------------------

Some databases support the concept of multiple schemas. A
:class:`~sqlalchemy.schema.Table` can reference this by specifying the
``schema`` keyword argument::

    financial_info = Table('financial_info', meta,
        Column('id', Integer, primary_key=True),
        Column('value', String(100), nullable=False),
        schema='remote_banks'
    )

Within the :class:`~sqlalchemy.schema.MetaData` collection, this table will be
identified by the combination of ``financial_info`` and ``remote_banks``. If
another table called ``financial_info`` is referenced without the
``remote_banks`` schema, it will refer to a different
:class:`~sqlalchemy.schema.Table`. :class:`~sqlalchemy.schema.ForeignKey`
objects can specify references to columns in this table using the form
``remote_banks.financial_info.id``.

The ``schema`` argument should be used for any name qualifiers required,
including Oracle's "owner" attribute and similar. It also can accommodate a
dotted name for longer schemes::

    schema="dbo.scott"

Backend-Specific Options
------------------------

:class:`~sqlalchemy.schema.Table` supports database-specific options. For
example, MySQL has different table backend types, including "MyISAM" and
"InnoDB". This can be expressed with :class:`~sqlalchemy.schema.Table` using
``mysql_engine``::

    addresses = Table('engine_email_addresses', meta,
        Column('address_id', Integer, primary_key = True),
        Column('remote_user_id', Integer, ForeignKey(users.c.user_id)),
        Column('email_address', String(20)),
        mysql_engine='InnoDB'
    )

Other backends may support table-level options as well - these would be
described in the individual documentation sections for each dialect.

Column, Table, MetaData API
---------------------------

.. autoclass:: Column
    :members:
    :inherited-members:
    :undoc-members:


.. autoclass:: MetaData
    :members:
    :undoc-members:


.. autoclass:: SchemaItem
    :members:

.. autoclass:: Table
    :members:
    :inherited-members:
    :undoc-members:


.. autoclass:: ThreadLocalMetaData
    :members:
    :undoc-members:


