.. _metadata_ddl_toplevel:
.. _metadata_ddl:
.. module:: sqlalchemy.schema

Customizing DDL
===============

In the preceding sections we've discussed a variety of schema constructs
including :class:`~sqlalchemy.schema.Table`,
:class:`~sqlalchemy.schema.ForeignKeyConstraint`,
:class:`~sqlalchemy.schema.CheckConstraint`, and
:class:`~sqlalchemy.schema.Sequence`. Throughout, we've relied upon the
``create()`` and :func:`~sqlalchemy.schema.MetaData.create_all` methods of
:class:`~sqlalchemy.schema.Table` and :class:`~sqlalchemy.schema.MetaData` in
order to issue data definition language (DDL) for all constructs. When issued,
a pre-determined order of operations is invoked, and DDL to create each table
is created unconditionally including all constraints and other objects
associated with it. For more complex scenarios where database-specific DDL is
required, SQLAlchemy offers two techniques which can be used to add any DDL
based on any condition, either accompanying the standard generation of tables
or by itself.

Custom DDL
----------

Custom DDL phrases are most easily achieved using the
:class:`~sqlalchemy.schema.DDL` construct. This construct works like all the
other DDL elements except it accepts a string which is the text to be emitted:

.. sourcecode:: python+sql

    event.listen(
        metadata,
        "after_create",
        DDL("ALTER TABLE users ADD CONSTRAINT "
            "cst_user_name_length "
            " CHECK (length(user_name) >= 8)")
    )

A more comprehensive method of creating libraries of DDL constructs is to use
custom compilation - see :ref:`sqlalchemy.ext.compiler_toplevel` for
details.


.. _schema_ddl_sequences:

Controlling DDL Sequences
-------------------------

The :class:`~.schema.DDL` construct introduced previously also has the
ability to be invoked conditionally based on inspection of the
database.  This feature is available using the :meth:`.DDLElement.execute_if`
method.  For example, if we wanted to create a trigger but only on
the PostgreSQL backend, we could invoke this as::

    mytable = Table(
        'mytable', metadata,
        Column('id', Integer, primary_key=True),
        Column('data', String(50))
    )

    trigger = DDL(
        "CREATE TRIGGER dt_ins BEFORE INSERT ON mytable "
        "FOR EACH ROW BEGIN SET NEW.data='ins'; END"
    )

    event.listen(
        mytable,
        'after_create',
        trigger.execute_if(dialect='postgresql')
    )

The :paramref:`.DDLElement.execute_if.dialect` keyword also accepts a tuple
of string dialect names::

    event.listen(
        mytable,
        "after_create",
        trigger.execute_if(dialect=('postgresql', 'mysql'))
    )
    event.listen(
        mytable,
        "before_drop",
        trigger.execute_if(dialect=('postgresql', 'mysql'))
    )

The :meth:`.DDLElement.execute_if` method can also work against a callable
function that will receive the database connection in use.  In the
example below, we use this to conditionally create a CHECK constraint,
first looking within the PostgreSQL catalogs to see if it exists:

.. sourcecode:: python+sql

    def should_create(ddl, target, connection, **kw):
        row = connection.execute(
            "select conname from pg_constraint where conname='%s'" %
            ddl.element.name).scalar()
        return not bool(row)

    def should_drop(ddl, target, connection, **kw):
        return not should_create(ddl, target, connection, **kw)

    event.listen(
        users,
        "after_create",
        DDL(
            "ALTER TABLE users ADD CONSTRAINT "
            "cst_user_name_length CHECK (length(user_name) >= 8)"
        ).execute_if(callable_=should_create)
    )
    event.listen(
        users,
        "before_drop",
        DDL(
            "ALTER TABLE users DROP CONSTRAINT cst_user_name_length"
        ).execute_if(callable_=should_drop)
    )

    {sql}users.create(engine)
    CREATE TABLE users (
        user_id SERIAL NOT NULL,
        user_name VARCHAR(40) NOT NULL,
        PRIMARY KEY (user_id)
    )

    select conname from pg_constraint where conname='cst_user_name_length'
    ALTER TABLE users ADD CONSTRAINT cst_user_name_length  CHECK (length(user_name) >= 8){stop}

    {sql}users.drop(engine)
    select conname from pg_constraint where conname='cst_user_name_length'
    ALTER TABLE users DROP CONSTRAINT cst_user_name_length
    DROP TABLE users{stop}

Using the built-in DDLElement Classes
--------------------------------------

The ``sqlalchemy.schema`` package contains SQL expression constructs that
provide DDL expressions. For example, to produce a ``CREATE TABLE`` statement:

.. sourcecode:: python+sql

    from sqlalchemy.schema import CreateTable
    {sql}engine.execute(CreateTable(mytable))
    CREATE TABLE mytable (
        col1 INTEGER,
        col2 INTEGER,
        col3 INTEGER,
        col4 INTEGER,
        col5 INTEGER,
        col6 INTEGER
    ){stop}

Above, the :class:`~sqlalchemy.schema.CreateTable` construct works like any
other expression construct (such as ``select()``, ``table.insert()``, etc.).
All of SQLAlchemy's DDL oriented constructs are subclasses of
the :class:`.DDLElement` base class; this is the base of all the
objects corresponding to CREATE and DROP as well as ALTER,
not only in SQLAlchemy but in Alembic Migrations as well.
A full reference of available constructs is in :ref:`schema_api_ddl`.

User-defined DDL constructs may also be created as subclasses of
:class:`.DDLElement` itself.   The documentation in
:ref:`sqlalchemy.ext.compiler_toplevel` has several examples of this.

The event-driven DDL system described in the previous section
:ref:`schema_ddl_sequences` is available with other :class:`.DDLElement`
objects as well.  However, when dealing with the built-in constructs
such as :class:`.CreateIndex`, :class:`.CreateSequence`, etc, the event
system is of **limited** use, as methods like :meth:`.Table.create` and
:meth:`.MetaData.create_all` will invoke these constructs unconditionally.
In a future SQLAlchemy release, the DDL event system including conditional
execution will taken into account for built-in constructs that currently
invoke in all cases.

We can illustrate an event-driven
example with the :class:`.AddConstraint` and :class:`.DropConstraint`
constructs, as the event-driven system will work for CHECK and UNIQUE
constraints, using these as we did in our previous example of
:meth:`.DDLElement.execute_if`:

.. sourcecode:: python+sql

    def should_create(ddl, target, connection, **kw):
        row = connection.execute(
            "select conname from pg_constraint where conname='%s'" %
            ddl.element.name).scalar()
        return not bool(row)

    def should_drop(ddl, target, connection, **kw):
        return not should_create(ddl, target, connection, **kw)

    event.listen(
        users,
        "after_create",
        AddConstraint(constraint).execute_if(callable_=should_create)
    )
    event.listen(
        users,
        "before_drop",
        DropConstraint(constraint).execute_if(callable_=should_drop)
    )

    {sql}users.create(engine)
    CREATE TABLE users (
        user_id SERIAL NOT NULL,
        user_name VARCHAR(40) NOT NULL,
        PRIMARY KEY (user_id)
    )

    select conname from pg_constraint where conname='cst_user_name_length'
    ALTER TABLE users ADD CONSTRAINT cst_user_name_length  CHECK (length(user_name) >= 8){stop}

    {sql}users.drop(engine)
    select conname from pg_constraint where conname='cst_user_name_length'
    ALTER TABLE users DROP CONSTRAINT cst_user_name_length
    DROP TABLE users{stop}

While the above example is against the built-in :class:`.AddConstraint`
and :class:`.DropConstraint` objects, the main usefulness of DDL events
for now remains focused on the use of the :class:`.DDL` construct itself,
as well as with user-defined subclasses of :class:`.DDLElement` that aren't
already part of the :meth:`.MetaData.create_all`, :meth:`.Table.create`,
and corresponding "drop" processes.

.. _schema_api_ddl:

DDL Expression Constructs API
-----------------------------

.. autofunction:: sort_tables

.. autofunction:: sort_tables_and_constraints

.. autoclass:: DDLElement
    :members:
    :undoc-members:


.. autoclass:: DDL
    :members:
    :undoc-members:

.. autoclass:: _CreateDropBase

.. autoclass:: CreateTable
    :members:
    :undoc-members:


.. autoclass:: DropTable
    :members:
    :undoc-members:


.. autoclass:: CreateColumn
    :members:
    :undoc-members:


.. autoclass:: CreateSequence
    :members:
    :undoc-members:


.. autoclass:: DropSequence
    :members:
    :undoc-members:


.. autoclass:: CreateIndex
    :members:
    :undoc-members:


.. autoclass:: DropIndex
    :members:
    :undoc-members:


.. autoclass:: AddConstraint
    :members:
    :undoc-members:


.. autoclass:: DropConstraint
    :members:
    :undoc-members:


.. autoclass:: CreateSchema
    :members:
    :undoc-members:


.. autoclass:: DropSchema
    :members:
    :undoc-members:


