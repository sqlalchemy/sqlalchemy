.. _metadata_toplevel:

.. _metadata_describing_toplevel:

.. _metadata_describing:

==================================
Describing Databases with MetaData
==================================

.. module:: sqlalchemy.schema

This section discusses the fundamental :class:`_schema.Table`, :class:`_schema.Column`
and :class:`_schema.MetaData` objects.

.. seealso::

    :ref:`tutorial_working_with_metadata` - tutorial introduction to
    SQLAlchemy's database metadata concept in the :ref:`unified_tutorial`

A collection of metadata entities is stored in an object aptly named
:class:`~sqlalchemy.schema.MetaData`::

    from sqlalchemy import MetaData

    metadata_obj = MetaData()

:class:`~sqlalchemy.schema.MetaData` is a container object that keeps together
many different features of a database (or multiple databases) being described.

To represent a table, use the :class:`~sqlalchemy.schema.Table` class. Its two
primary arguments are the table name, then the
:class:`~sqlalchemy.schema.MetaData` object which it will be associated with.
The remaining positional arguments are mostly
:class:`~sqlalchemy.schema.Column` objects describing each column::

    from sqlalchemy import Table, Column, Integer, String

    user = Table(
        "user",
        metadata_obj,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
        Column("email_address", String(60)),
        Column("nickname", String(50), nullable=False),
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

.. _metadata_tables_and_columns:

Accessing Tables and Columns
----------------------------

The :class:`~sqlalchemy.schema.MetaData` object contains all of the schema
constructs we've associated with it. It supports a few methods of accessing
these table objects, such as the ``sorted_tables`` accessor which returns a
list of each :class:`~sqlalchemy.schema.Table` object in order of foreign key
dependency (that is, each table is preceded by all tables which it
references)::

    >>> for t in metadata_obj.sorted_tables:
    ...     print(t.name)
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

    employees = Table(
        "employees",
        metadata_obj,
        Column("employee_id", Integer, primary_key=True),
        Column("employee_name", String(60), nullable=False),
        Column("employee_dept", Integer, ForeignKey("departments.department_id")),
    )

Note the :class:`~sqlalchemy.schema.ForeignKey` object used in this table -
this construct defines a reference to a remote table, and is fully described
in :ref:`metadata_foreignkeys`. Methods of accessing information about this
table include::

    # access the column "employee_id":
    employees.columns.employee_id

    # or just
    employees.c.employee_id

    # via string
    employees.c["employee_id"]

    # a tuple of columns may be returned using multiple strings
    # (new in 2.0)
    emp_id, name, type = employees.c["employee_id", "name", "type"]

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

.. tip::

  The :attr:`_sql.FromClause.c` collection, synonymous with the
  :attr:`_sql.FromClause.columns` collection, is an instance of
  :class:`_sql.ColumnCollection`, which provides a **dictionary-like interface**
  to the collection of columns.   Names are ordinarily accessed like
  attribute names, e.g. ``employees.c.employee_name``.  However for special names
  with spaces or those that match the names of dictionary methods such as
  :meth:`_sql.ColumnCollection.keys` or :meth:`_sql.ColumnCollection.values`,
  indexed access must be used, such as ``employees.c['values']`` or
  ``employees.c["some column"]``.  See :class:`_sql.ColumnCollection` for
  further information.


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

    engine = create_engine("sqlite:///:memory:")

    metadata_obj = MetaData()

    user = Table(
        "user",
        metadata_obj,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
        Column("email_address", String(60), key="email"),
        Column("nickname", String(50), nullable=False),
    )

    user_prefs = Table(
        "user_prefs",
        metadata_obj,
        Column("pref_id", Integer, primary_key=True),
        Column("user_id", Integer, ForeignKey("user.user_id"), nullable=False),
        Column("pref_name", String(40), nullable=False),
        Column("pref_value", String(100)),
    )

    metadata_obj.create_all(engine)
    {execsql}PRAGMA table_info(user){}
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

    engine = create_engine("sqlite:///:memory:")

    metadata_obj = MetaData()

    employees = Table(
        "employees",
        metadata_obj,
        Column("employee_id", Integer, primary_key=True),
        Column("employee_name", String(60), nullable=False, key="name"),
        Column("employee_dept", Integer, ForeignKey("departments.department_id")),
    )
    employees.create(engine)
    {execsql}CREATE TABLE employees(
        employee_id SERIAL NOT NULL PRIMARY KEY,
        employee_name VARCHAR(60) NOT NULL,
        employee_dept INTEGER REFERENCES departments(department_id)
    )
    {}

``drop()`` method:

.. sourcecode:: python+sql

    employees.drop(engine)
    {execsql}DROP TABLE employees
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
refers to this name as the **schema name**.  Within SQLAlchemy, this is nothing
more than a string name which is associated with a :class:`_schema.Table`
object, and is then rendered into SQL statements in a manner appropriate to the
target database such that the table is referred towards in its remote "schema",
whatever mechanism that is on the target database.

The "schema" name may be associated directly with a :class:`_schema.Table`
using the :paramref:`_schema.Table.schema` argument; when using the ORM
with :ref:`declarative table <orm_declarative_table_config_toplevel>` configuration,
the parameter is passed using the ``__table_args__`` parameter dictionary.

The "schema" name may also be associated with the :class:`_schema.MetaData`
object where it will take effect automatically for all :class:`_schema.Table`
objects associated with that :class:`_schema.MetaData` that don't otherwise
specify their own name.  Finally, SQLAlchemy also supports a "dynamic" schema name
system that is often used for multi-tenant applications such that a single set
of :class:`_schema.Table` metadata may refer to a dynamically configured set of
schema names on a per-connection or per-statement basis.

.. topic::  What's "schema" ?

    SQLAlchemy's support for database "schema" was designed with first party
    support for PostgreSQL-style schemas.  In this style, there is first a
    "database" that typically has a single "owner".  Within this database there
    can be any number of "schemas" which then contain the actual table objects.

    A table within a specific schema is referred towards explicitly using the
    syntax "<schemaname>.<tablename>".  Contrast this to an architecture such
    as that of MySQL, where there are only "databases", however SQL statements
    can refer to multiple databases at once, using the same syntax except it
    is "<database>.<tablename>".  On Oracle, this syntax refers to yet another
    concept, the "owner" of a table.  Regardless of which kind of database is
    in use, SQLAlchemy uses the phrase "schema" to refer to the qualifying
    identifier within the general syntax of "<qualifier>.<tablename>".

.. seealso::

    :ref:`orm_declarative_table_schema_name` - schema name specification when using the ORM
    :ref:`declarative table <orm_declarative_table_config_toplevel>` configuration


The most basic example is that of the :paramref:`_schema.Table.schema` argument
using a Core :class:`_schema.Table` object as follows::

    metadata_obj = MetaData()

    financial_info = Table(
        "financial_info",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("value", String(100), nullable=False),
        schema="remote_banks",
    )

SQL that is rendered using this :class:`_schema.Table`, such as the SELECT
statement below, will explicitly qualify the table name ``financial_info`` with
the ``remote_banks`` schema name:

.. sourcecode:: pycon+sql

    >>> print(select(financial_info))
    {printsql}SELECT remote_banks.financial_info.id, remote_banks.financial_info.value
    FROM remote_banks.financial_info

When a :class:`_schema.Table` object is declared with an explicit schema
name, it is stored in the internal :class:`_schema.MetaData` namespace
using the combination of the schema and table name.  We can view this
in the :attr:`_schema.MetaData.tables` collection by searching for the
key ``'remote_banks.financial_info'``::

    >>> metadata_obj.tables["remote_banks.financial_info"]
    Table('financial_info', MetaData(),
    Column('id', Integer(), table=<financial_info>, primary_key=True, nullable=False),
    Column('value', String(length=100), table=<financial_info>, nullable=False),
    schema='remote_banks')

This dotted name is also what must be used when referring to the table
for use with the :class:`_schema.ForeignKey` or :class:`_schema.ForeignKeyConstraint`
objects, even if the referring table is also in that same schema::

    customer = Table(
        "customer",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("financial_info_id", ForeignKey("remote_banks.financial_info.id")),
        schema="remote_banks",
    )

The :paramref:`_schema.Table.schema` argument may also be used with certain
dialects to indicate
a multiple-token (e.g. dotted) path to a particular table.  This is particularly
important on a database such as Microsoft SQL Server where there are often
dotted "database/owner" tokens.  The tokens may be placed directly in the name
at once, such as::

    schema = "dbo.scott"

.. seealso::

    :ref:`multipart_schema_names` - describes use of dotted schema names
    with the SQL Server dialect.

    :ref:`metadata_reflection_schemas`


.. _schema_metadata_schema_name:

Specifying a Default Schema Name with MetaData
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`_schema.MetaData` object may also set up an explicit default
option for all :paramref:`_schema.Table.schema` parameters by passing the
:paramref:`_schema.MetaData.schema` argument to the top level :class:`_schema.MetaData`
construct::

    metadata_obj = MetaData(schema="remote_banks")

    financial_info = Table(
        "financial_info",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("value", String(100), nullable=False),
    )

Above, for any :class:`_schema.Table` object (or :class:`_schema.Sequence` object
directly associated with the :class:`_schema.MetaData`) which leaves the
:paramref:`_schema.Table.schema` parameter at its default of ``None`` will instead
act as though the parameter were set to the value ``"remote_banks"``.  This
includes that the :class:`_schema.Table` is cataloged in the :class:`_schema.MetaData`
using the schema-qualified name, that is::

    metadata_obj.tables["remote_banks.financial_info"]

When using the :class:`_schema.ForeignKey` or :class:`_schema.ForeignKeyConstraint`
objects to refer to this table, either the schema-qualified name or the
non-schema-qualified name may be used to refer to the ``remote_banks.financial_info``
table::

    # either will work:

    refers_to_financial_info = Table(
        "refers_to_financial_info",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("fiid", ForeignKey("financial_info.id")),
    )


    # or

    refers_to_financial_info = Table(
        "refers_to_financial_info",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("fiid", ForeignKey("remote_banks.financial_info.id")),
    )

When using a :class:`_schema.MetaData` object that sets
:paramref:`_schema.MetaData.schema`, a :class:`_schema.Table` that wishes
to specify that it should not be schema qualified may use the special symbol
:data:`_schema.BLANK_SCHEMA`::

    from sqlalchemy import BLANK_SCHEMA

    metadata_obj = MetaData(schema="remote_banks")

    financial_info = Table(
        "financial_info",
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("value", String(100), nullable=False),
        schema=BLANK_SCHEMA,  # will not use "remote_banks"
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
        cursor_obj = dbapi_connection.cursor()
        cursor_obj.execute("ALTER SESSION SET CURRENT_SCHEMA=%s" % schema_name)
        cursor_obj.close()

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




Schemas and Reflection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The schema feature of SQLAlchemy interacts with the table reflection
feature introduced at :ref:`metadata_reflection_toplevel`.  See the section
:ref:`metadata_reflection_schemas` for additional details on how this works.


Backend-Specific Options
------------------------

:class:`~sqlalchemy.schema.Table` supports database-specific options. For
example, MySQL has different table backend types, including "MyISAM" and
"InnoDB". This can be expressed with :class:`~sqlalchemy.schema.Table` using
``mysql_engine``::

    addresses = Table(
        "engine_email_addresses",
        metadata_obj,
        Column("address_id", Integer, primary_key=True),
        Column("remote_user_id", Integer, ForeignKey(users.c.user_id)),
        Column("email_address", String(20)),
        mysql_engine="InnoDB",
    )

Other backends may support table-level options as well - these would be
described in the individual documentation sections for each dialect.

Column, Table, MetaData API
---------------------------

.. attribute:: sqlalchemy.schema.BLANK_SCHEMA
    :noindex:

    Refers to :attr:`.SchemaConst.BLANK_SCHEMA`.

.. attribute:: sqlalchemy.schema.RETAIN_SCHEMA
    :noindex:

    Refers to :attr:`.SchemaConst.RETAIN_SCHEMA`


.. autoclass:: Column
    :members:
    :inherited-members:


.. autoclass:: MetaData
    :members:

.. autoclass:: SchemaConst
    :members:

.. autoclass:: SchemaItem
    :members:

.. autofunction:: insert_sentinel

.. autoclass:: Table
    :members:
    :inherited-members:
