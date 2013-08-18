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

.. _schema_ddl_sequences:

Controlling DDL Sequences
-------------------------

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
other expression construct (such as ``select()``, ``table.insert()``, etc.). A
full reference of available constructs is in :ref:`schema_api_ddl`.

The DDL constructs all extend a common base class which provides the
capability to be associated with an individual
:class:`~sqlalchemy.schema.Table` or :class:`~sqlalchemy.schema.MetaData`
object, to be invoked upon create/drop events. Consider the example of a table
which contains a CHECK constraint:

.. sourcecode:: python+sql

    users = Table('users', metadata,
                   Column('user_id', Integer, primary_key=True),
                   Column('user_name', String(40), nullable=False),
                   CheckConstraint('length(user_name) >= 8',name="cst_user_name_length")
                   )

    {sql}users.create(engine)
    CREATE TABLE users (
        user_id SERIAL NOT NULL,
        user_name VARCHAR(40) NOT NULL,
        PRIMARY KEY (user_id),
        CONSTRAINT cst_user_name_length  CHECK (length(user_name) >= 8)
    ){stop}

The above table contains a column "user_name" which is subject to a CHECK
constraint that validates that the length of the string is at least eight
characters. When a ``create()`` is issued for this table, DDL for the
:class:`~sqlalchemy.schema.CheckConstraint` will also be issued inline within
the table definition.

The :class:`~sqlalchemy.schema.CheckConstraint` construct can also be
constructed externally and associated with the
:class:`~sqlalchemy.schema.Table` afterwards::

    constraint = CheckConstraint('length(user_name) >= 8',name="cst_user_name_length")
    users.append_constraint(constraint)

So far, the effect is the same. However, if we create DDL elements
corresponding to the creation and removal of this constraint, and associate
them with the :class:`.Table` as events, these new events
will take over the job of issuing DDL for the constraint. Additionally, the
constraint will be added via ALTER:

.. sourcecode:: python+sql

    from sqlalchemy import event

    event.listen(
        users,
        "after_create",
        AddConstraint(constraint)
    )
    event.listen(
        users,
        "before_drop",
        DropConstraint(constraint)
    )

    {sql}users.create(engine)
    CREATE TABLE users (
        user_id SERIAL NOT NULL,
        user_name VARCHAR(40) NOT NULL,
        PRIMARY KEY (user_id)
    )

    ALTER TABLE users ADD CONSTRAINT cst_user_name_length  CHECK (length(user_name) >= 8){stop}

    {sql}users.drop(engine)
    ALTER TABLE users DROP CONSTRAINT cst_user_name_length
    DROP TABLE users{stop}

The real usefulness of the above becomes clearer once we illustrate the
:meth:`.DDLElement.execute_if` method.  This method returns a modified form of
the DDL callable which will filter on criteria before responding to a
received event.   It accepts a parameter ``dialect``, which is the string
name of a dialect or a tuple of such, which will limit the execution of the
item to just those dialects.  It also accepts a ``callable_`` parameter which
may reference a Python callable which will be invoked upon event reception,
returning ``True`` or ``False`` indicating if the event should proceed.

If our :class:`~sqlalchemy.schema.CheckConstraint` was only supported by
Postgresql and not other databases, we could limit its usage to just that dialect::

    event.listen(
        users,
        'after_create',
        AddConstraint(constraint).execute_if(dialect='postgresql')
    )
    event.listen(
        users,
        'before_drop',
        DropConstraint(constraint).execute_if(dialect='postgresql')
    )

Or to any set of dialects::

    event.listen(
        users,
        "after_create",
        AddConstraint(constraint).execute_if(dialect=('postgresql', 'mysql'))
    )
    event.listen(
        users,
        "before_drop",
        DropConstraint(constraint).execute_if(dialect=('postgresql', 'mysql'))
    )

When using a callable, the callable is passed the ddl element, the
:class:`.Table` or :class:`.MetaData`
object whose "create" or "drop" event is in progress, and the
:class:`.Connection` object being used for the
operation, as well as additional information as keyword arguments. The
callable can perform checks, such as whether or not a given item already
exists. Below we define ``should_create()`` and ``should_drop()`` callables
that check for the presence of our named constraint:

.. sourcecode:: python+sql

    def should_create(ddl, target, connection, **kw):
        row = connection.execute("select conname from pg_constraint where conname='%s'" % ddl.element.name).scalar()
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

.. _schema_api_ddl:

DDL Expression Constructs API
-----------------------------

.. autoclass:: DDLElement
    :members:
    :undoc-members:
     

.. autoclass:: DDL
    :members:
    :undoc-members:
     

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
     

