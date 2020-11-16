.. _metadata_toplevel:

.. _metadata_describing_toplevel:

.. _metadata_describing:

==================================
Describing Databases with MetaData
==================================

.. module:: sqlalchemy.schema

This section discusses the fundamental :class:`_schema.Table`, :class:`_schema.Column`
and :class:`_schema.MetaData` objects.

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
        Column('user_id', Integer, primary_key=True),
        Column('user_name', String(16), nullable=False),
        Column('email_address', String(60)),
        Column('nickname', String(50), nullable=False)
    )

Above, a table called ``user`` is described, which contains four columns. The
primary key of the table consists of the ``user_id`` column. Multiple columns
may be assigned the ``primary_key=True`` flag which denotes a multi-column
primary key, known as a *composite* primary key.

Note also that each column describes its datatype using objects corresponding
to genericized types, such as :class:`~sqlalchemy.types.Integer` and
:class:`~sqlalchemy.types.String`. SQLAlchemy features dozens of types of
varying levels of specificity as well as the ability to create custom types.
Documentation on the type system can be found at :ref:`types_toplevel`.

Accessing Tables and Columns
----------------------------

The :class:`~sqlalchemy.schema.MetaData` object contains all of the schema
constructs we've associated with it. It supports a few methods of accessing
these table objects, such as the ``sorted_tables`` accessor which returns a
list of each :class:`~sqlalchemy.schema.Table` object in order of foreign key
dependency (that is, each table is preceded by all tables which it
references)::

    >>> for t in metadata.sorted_tables:
    ...    print(t.name)
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
        print(c)

    # get the table's primary key columns
    for primary_key in employees.primary_key:
        print(primary_key)

    # get the table's foreign key objects:
    for fkey in employees.foreign_keys:
        print(fkey)

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
            Column('user_id', Integer, primary_key=True),
            Column('user_name', String(16), nullable=False),
            Column('email_address', String(60), key='email'),
            Column('nickname', String(50), nullable=False)
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
                nickname VARCHAR(50) NOT NULL
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

Altering Database Objects through Migrations
---------------------------------------------

While SQLAlchemy directly supports emitting CREATE and DROP statements for
schema constructs, the ability to alter those constructs, usually via the ALTER
statement as well as other database-specific constructs, is outside of the
scope of SQLAlchemy itself.  While it's easy enough to emit ALTER statements
and similar by hand, such as by passing a :func:`_expression.text` construct to
:meth:`_engine.Connection.execute` or by using the :class:`.DDL` construct, it's a
common practice to automate the maintenance of database schemas in relation to
application code using schema migration tools.

The SQLAlchemy project offers the  `Alembic <https://alembic.sqlalchemy.org>`_
migration tool for this purpose.   Alembic features a highly customizable
environment and a minimalistic usage pattern, supporting such features as
transactional DDL, automatic generation of "candidate" migrations, an "offline"
mode which generates SQL scripts, and support for branch resolution.

Alembic supersedes the `SQLAlchemy-Migrate
<https://github.com/openstack/sqlalchemy-migrate>`_   project, which is the
original migration tool for SQLAlchemy and is now  considered legacy.

.. _schema_table_schema_name:

Specifying the Schema Name
--------------------------

Most databases support the concept of multiple "schemas" - namespaces that
refer to alternate sets of tables and other constructs.  The server-side
geometry of a "schema" takes many forms, including names of "schemas" under the
scope of a particular database (e.g. PostgreSQL schemas), named sibling
databases (e.g. MySQL / MariaDB access to other databases on the same server),
as well as other concepts like tables owned by other usernames (Oracle, SQL
Server) or even names that refer to alternate database files (SQLite ATTACH) or
remote servers (Oracle DBLINK with synonyms).

What all of the above approaches have (mostly) in common is that there's a way
of referring to this alternate set of tables using a string name.  SQLAlchemy
refers to this name as the **schema name**.  Within SQLAlchemy, this is nothing more than
a string name which is associated with a :class:`_schema.Table` object, and
is then rendered into SQL statements in a manner appropriate to the target
database such that the table is referred towards in its remote "schema", whatever
mechanism that is on the target database.

The "schema" name may be associated directly with a :class:`_schema.Table`
using the :paramref:`_schema.Table.schema` argument; when using the ORM
with :ref:`declarative table <orm_declarative_table_config_toplevel>` configuration,
the parameter is passed using the ``__table_args__`` parameter dictionary.

The "schema" name may also be associated with the :class:`_schema.MetaData`
object where it will take effect automatically for all :class:`_schema.Table`
objects associated with that :class:`_schema.MetaData` that don't otherwise
specify their own name.   Finally, SQLAlchemy also supports a "dynamic" schema name
system that is often used for multi-tenant applications such that a single set
of :class:`_schema.Table` metadata may refer to a dynamically configured set of
schema names on a per-connection or per-statement basis.

.. seealso::

    :ref:`orm_declarative_table_schema_name` - schema name specification when using the ORM
    :ref:`declarative table <orm_declarative_table_config_toplevel>` configuration


The most basic example is that of the :paramref:`_schema.Table.schema` argument
using a Core :class:`_schema.Table` object as follows::

    metadata = MetaData()

    financial_info = Table(
        'financial_info',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('value', String(100), nullable=False),
        schema='remote_banks'
    )

SQL that is rendered using this :class:`_schema.Table`, such as the SELECT
statement below, will explicitly qualify the table name ``financial_info`` with
the ``remote_banks`` schema name::

    >>> print(select(financial_info))
    SELECT remote_banks.financial_info.id, remote_banks.financial_info.value
    FROM remote_banks.financial_info

When a :class:`_schema.Table` object is declared with an explicit schema
name, it is stored in the internal :class:`_schema.MetaData` namespace
using the combination of the schema and table name.  We can view this
in the :attr:`_schema.MetaData.tables` collection by searching for the
key ``'remote_banks.financial_info'``::

    >>> metadata.tables['remote_banks.financial_info']
    Table('financial_info', MetaData(),
    Column('id', Integer(), table=<financial_info>, primary_key=True, nullable=False),
    Column('value', String(length=100), table=<financial_info>, nullable=False),
    schema='remote_banks')

This dotted name is also what must be used when referring to the table
for use with the :class:`_schema.ForeignKey` or :class:`_schema.ForeignKeyConstraint`
objects, even if the referring table is also in that same schema::

    customer = Table(
        "customer",
        metadata,
        Column('id', Integer, primary_key=True),
        Column('financial_info_id', ForeignKey("remote_banks.financial_info.id")),
        schema='remote_banks'
    )

The :paramref:`_schema.Table.schema` argument may also be used with certain
dialects to indicate
a multiple-token (e.g. dotted) path to a particular table.  This is particularly
important on a database such as Microsoft SQL Server where there are often
dotted "database/owner" tokens.  The tokens may be placed directly in the name
at once, such as::

    schema="dbo.scott"

.. seealso::

    :ref:`multipart_schema_names` - describes use of dotted schema names
    with the SQL Server dialect.


.. _schema_metadata_schema_name:

Specifying a Default Schema Name with MetaData
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`_schema.MetaData` object may also set up an explicit default
option for all :paramref:`_schema.Table.schema` parameters by passing the
:paramref:`_schema.MetaData.schema` argument to the top level :class:`_schema.MetaData`
construct::

    metadata = MetaData(schema="remote_banks")

    financial_info = Table(
        'financial_info',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('value', String(100), nullable=False),
    )

Above, for any :class:`_schema.Table` object (or :class:`_schema.Sequence` object
directly associated with the :class:`_schema.MetaData`) which leaves the
:paramref:`_schema.Table.schema` parameter at its default of ``None`` will instead
act as though the parameter were set to the value ``"remote_banks"``.  This
includes that the :class:`_schema.Table` is cataloged in the :class:`_schema.MetaData`
using the schema-qualified name, that is::

    metadata.tables['remote_banks.financial_info']

When using the :class:`_schema.ForeignKey` or :class:`_schema.ForeignKeyConstraint`
objects to refer to this table, either the schema-qualified name or the
non-schema-qualified name may be used to refer to the ``remote_banks.financial_info``
table::

    # either will work:

    refers_to_financial_info = Table(
        'refers_to_financial_info',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('fiid', ForeignKey('financial_info.id')),
    )


    # or

    refers_to_financial_info = Table(
        'refers_to_financial_info',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('fiid', ForeignKey('remote_banks.financial_info.id')),
    )

When using a :class:`_schema.MetaData` object that sets
:paramref:`_schema.MetaData.schema`, a :class:`_schema.Table` that wishes
to specify that it should not be schema qualified may use the special symbol
:data:`_schema.BLANK_SCHEMA`::

    from sqlalchemy import BLANK_SCHEMA

    metadata = MetaData(schema="remote_banks")

    financial_info = Table(
        'financial_info',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('value', String(100), nullable=False),
        schema=BLANK_SCHEMA  # will not use "remote_banks"
    )


.. seealso::

    :paramref:`_schema.MetaData.schema`

.. _schema_dynamic_naming_convention:

Applying Dynamic Schema Naming Conventions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The names used by the :paramref:`_schema.Table.schema` parameter may also be
applied against a lookup that is dynamic on a per-connection or per-execution
basis, so that for example in multi-tenant situations, each transaction
or statement may be targeted at a specific set of schema names that change.
The section :ref:`schema_translating` describes how this feature is used.


.. seealso::

    :ref:`schema_translating`

.. _schema_set_default_connections:

Setting a Default Schema for New Connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The above approaches all refer to methods of including an explicit schema-name
within SQL statements.  Database connections in fact feature the concept
of a "default" schema, which is the name of the "schema" (or database, owner,
etc.) that takes place if a table name is not explicitly schema-qualified.
These names are usually configured at the login level, such as when connecting
to a PostgreSQL database, the default "schema" is called "public".

There are often cases where the default "schema" cannot be set via the login
itself and instead would usefully be configured each time a connection
is made, using a statement such as "SET SEARCH_PATH" on PostgreSQL or
"ALTER SESSION" on Oracle.  These approaches may be achieved by using
the :meth:`_pool.PoolEvents.connect` event, which allows access to the
DBAPI connection when it is first created.    For example, to set the
Oracle CURRENT_SCHEMA variable to an alternate name::

    from sqlalchemy import event
    from sqlalchemy import create_engine

    engine = create_engine("oracle+cx_oracle://scott:tiger@tsn_name")

    @event.listens_for(engine, "connect", insert=True)
    def set_current_schema(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("ALTER SESSION SET CURRENT_SCHEMA=%s" % schema_name)
        cursor.close()

Above, the ``set_current_schema()`` event handler will take place immediately
when the above :class:`_engine.Engine` first connects; as the event is
"inserted" into the beginning of the handler list, it will also take place
before the dialect's own event handlers are run, in particular including the
one that will determine the "default schema" for the connection.

For other databases, consult the database and/or dialect documentation
for specific information regarding how default schemas are configured.

.. versionchanged:: 1.4.0b2  The above recipe now works without the need to
   establish additional event handlers.

.. seealso::

    :ref:`postgresql_alternate_search_path` - in the :ref:`postgresql_toplevel` dialect documentation.

Backend-Specific Options
------------------------

:class:`~sqlalchemy.schema.Table` supports database-specific options. For
example, MySQL has different table backend types, including "MyISAM" and
"InnoDB". This can be expressed with :class:`~sqlalchemy.schema.Table` using
``mysql_engine``::

    addresses = Table('engine_email_addresses', meta,
        Column('address_id', Integer, primary_key=True),
        Column('remote_user_id', Integer, ForeignKey(users.c.user_id)),
        Column('email_address', String(20)),
        mysql_engine='InnoDB'
    )

Other backends may support table-level options as well - these would be
described in the individual documentation sections for each dialect.

Column, Table, MetaData API
---------------------------

.. attribute:: sqlalchemy.schema.BLANK_SCHEMA

    Symbol indicating that a :class:`_schema.Table` or :class:`.Sequence`
    should have 'None' for its schema, even if the parent
    :class:`_schema.MetaData` has specified a schema.

    .. seealso::

        :paramref:`_schema.MetaData.schema`

        :paramref:`_schema.Table.schema`

        :paramref:`.Sequence.schema`

    .. versionadded:: 1.0.14


.. autoclass:: Column
    :members:
    :inherited-members:


.. autoclass:: MetaData
    :members:


.. autoclass:: SchemaItem
    :members:

.. autoclass:: Table
    :members:
    :inherited-members:


.. autoclass:: ThreadLocalMetaData
    :members:


