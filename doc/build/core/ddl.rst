.. _metadata_ddl_toplevel:
.. _metadata_ddl:
.. currentmodule:: sqlalchemy.schema

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
        DDL(
            "ALTER TABLE users ADD CONSTRAINT "
            "cst_user_name_length "
            " CHECK (length(user_name) >= 8)"
        ),
    )

A more comprehensive method of creating libraries of DDL constructs is to use
custom compilation - see :ref:`sqlalchemy.ext.compiler_toplevel` for
details.


.. _schema_ddl_sequences:

Controlling DDL Sequences
-------------------------

The :class:`_schema.DDL` construct introduced previously also has the
ability to be invoked conditionally based on inspection of the
database.  This feature is available using the :meth:`.ExecutableDDLElement.execute_if`
method.  For example, if we wanted to create a trigger but only on
the PostgreSQL backend, we could invoke this as::

    mytable = Table(
        "mytable",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", String(50)),
    )

    func = DDL(
        "CREATE FUNCTION my_func() "
        "RETURNS TRIGGER AS $$ "
        "BEGIN "
        "NEW.data := 'ins'; "
        "RETURN NEW; "
        "END; $$ LANGUAGE PLPGSQL"
    )

    trigger = DDL(
        "CREATE TRIGGER dt_ins BEFORE INSERT ON mytable "
        "FOR EACH ROW EXECUTE PROCEDURE my_func();"
    )

    event.listen(mytable, "after_create", func.execute_if(dialect="postgresql"))

    event.listen(mytable, "after_create", trigger.execute_if(dialect="postgresql"))

The :paramref:`.ExecutableDDLElement.execute_if.dialect` keyword also accepts a tuple
of string dialect names::

    event.listen(
        mytable, "after_create", trigger.execute_if(dialect=("postgresql", "mysql"))
    )
    event.listen(
        mytable, "before_drop", trigger.execute_if(dialect=("postgresql", "mysql"))
    )

The :meth:`.ExecutableDDLElement.execute_if` method can also work against a callable
function that will receive the database connection in use.  In the
example below, we use this to conditionally create a CHECK constraint,
first looking within the PostgreSQL catalogs to see if it exists:

.. sourcecode:: python+sql

    def should_create(ddl, target, connection, **kw):
        row = connection.execute(
            "select conname from pg_constraint where conname='%s'" % ddl.element.name
        ).scalar()
        return not bool(row)


    def should_drop(ddl, target, connection, **kw):
        return not should_create(ddl, target, connection, **kw)


    event.listen(
        users,
        "after_create",
        DDL(
            "ALTER TABLE users ADD CONSTRAINT "
            "cst_user_name_length CHECK (length(user_name) >= 8)"
        ).execute_if(callable_=should_create),
    )
    event.listen(
        users,
        "before_drop",
        DDL("ALTER TABLE users DROP CONSTRAINT cst_user_name_length").execute_if(
            callable_=should_drop
        ),
    )

    users.create(engine)
    {execsql}CREATE TABLE users (
        user_id SERIAL NOT NULL,
        user_name VARCHAR(40) NOT NULL,
        PRIMARY KEY (user_id)
    )

    SELECT conname FROM pg_constraint WHERE conname='cst_user_name_length'
    ALTER TABLE users ADD CONSTRAINT cst_user_name_length  CHECK (length(user_name) >= 8)
    {stop}

    users.drop(engine)
    {execsql}SELECT conname FROM pg_constraint WHERE conname='cst_user_name_length'
    ALTER TABLE users DROP CONSTRAINT cst_user_name_length
    DROP TABLE users{stop}

Using the built-in DDLElement Classes
-------------------------------------

The ``sqlalchemy.schema`` package contains SQL expression constructs that
provide DDL expressions, all of which extend from the common base
:class:`.ExecutableDDLElement`. For example, to produce a ``CREATE TABLE`` statement,
one can use the :class:`.CreateTable` construct:

.. sourcecode:: python+sql

    from sqlalchemy.schema import CreateTable

    with engine.connect() as conn:
        conn.execute(CreateTable(mytable))
    {execsql}CREATE TABLE mytable (
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
the :class:`.ExecutableDDLElement` base class; this is the base of all the
objects corresponding to CREATE and DROP as well as ALTER,
not only in SQLAlchemy but in Alembic Migrations as well.
A full reference of available constructs is in :ref:`schema_api_ddl`.

User-defined DDL constructs may also be created as subclasses of
:class:`.ExecutableDDLElement` itself.   The documentation in
:ref:`sqlalchemy.ext.compiler_toplevel` has several examples of this.

.. _schema_ddl_ddl_if:

Controlling DDL Generation of Constraints and Indexes
-----------------------------------------------------

.. versionadded:: 2.0

While the previously mentioned :meth:`.ExecutableDDLElement.execute_if` method is
useful for custom :class:`.DDL` classes which need to invoke conditionally,
there is also a common need for elements that are typically related to a
particular :class:`.Table`, namely constraints and indexes, to also be
subject to "conditional" rules, such as an index that includes features
that are specific to a particular backend such as PostgreSQL or SQL Server.
For this use case, the :meth:`.Constraint.ddl_if` and :meth:`.Index.ddl_if`
methods may be used against constructs such as :class:`.CheckConstraint`,
:class:`.UniqueConstraint` and :class:`.Index`, accepting the same
arguments as the :meth:`.ExecutableDDLElement.execute_if` method in order to control
whether or not their DDL will be emitted in terms of their parent
:class:`.Table` object.  These methods may be used inline when
creating the definition for a :class:`.Table`
(or similarly, when using the ``__table_args__`` collection in an ORM
declarative mapping), such as::

    from sqlalchemy import CheckConstraint, Index
    from sqlalchemy import MetaData, Table, Column
    from sqlalchemy import Integer, String

    meta = MetaData()

    my_table = Table(
        "my_table",
        meta,
        Column("id", Integer, primary_key=True),
        Column("num", Integer),
        Column("data", String),
        Index("my_pg_index", "data").ddl_if(dialect="postgresql"),
        CheckConstraint("num > 5").ddl_if(dialect="postgresql"),
    )

In the above example, the :class:`.Table` construct refers to both an
:class:`.Index` and a :class:`.CheckConstraint` construct, both which
indicate ``.ddl_if(dialect="postgresql")``, which indicates that these
elements will be included in the CREATE TABLE sequence only against the
PostgreSQL dialect.  If we run ``meta.create_all()`` against the SQLite
dialect, for example, neither construct will be included:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import create_engine
    >>> sqlite_engine = create_engine("sqlite+pysqlite://", echo=True)
    >>> meta.create_all(sqlite_engine)
    {execsql}BEGIN (implicit)
    PRAGMA main.table_info("my_table")
    [raw sql] ()
    PRAGMA temp.table_info("my_table")
    [raw sql] ()

    CREATE TABLE my_table (
        id INTEGER NOT NULL,
        num INTEGER,
        data VARCHAR,
        PRIMARY KEY (id)
    )

However, if we run the same commands against a PostgreSQL database, we will
see inline DDL for the CHECK constraint as well as a separate CREATE
statement emitted for the index:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import create_engine
    >>> postgresql_engine = create_engine(
    ...     "postgresql+psycopg2://scott:tiger@localhost/test", echo=True
    ... )
    >>> meta.create_all(postgresql_engine)
    {execsql}BEGIN (implicit)
    select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where pg_catalog.pg_table_is_visible(c.oid) and relname=%(name)s
    [generated in 0.00009s] {'name': 'my_table'}

    CREATE TABLE my_table (
        id SERIAL NOT NULL,
        num INTEGER,
        data VARCHAR,
        PRIMARY KEY (id),
        CHECK (num > 5)
    )
    [no key 0.00007s] {}
    CREATE INDEX my_pg_index ON my_table (data)
    [no key 0.00013s] {}
    COMMIT

The :meth:`.Constraint.ddl_if` and :meth:`.Index.ddl_if` methods create
an event hook that may be consulted not just at DDL execution time, as is the
behavior with :meth:`.ExecutableDDLElement.execute_if`, but also within the SQL compilation
phase of the :class:`.CreateTable` object, which is responsible for rendering
the ``CHECK (num > 5)`` DDL inline within the CREATE TABLE statement.
As such, the event hook that is received by the :meth:`.Constraint.ddl_if.callable_`
parameter has a richer argument set present, including that there is
a ``dialect`` keyword argument passed, as well as an instance of :class:`.DDLCompiler`
via the ``compiler`` keyword argument for the "inline rendering" portion of the
sequence.  The ``bind`` argument is **not** present when the event is triggered
within the :class:`.DDLCompiler` sequence, so a modern event hook that wishes
to inspect the database versioning information would best use the given
:class:`.Dialect` object, such as to test PostgreSQL versioning:

.. sourcecode:: python+sql

    def only_pg_14(ddl_element, target, bind, dialect, **kw):
        return dialect.name == "postgresql" and dialect.server_version_info >= (14,)


    my_table = Table(
        "my_table",
        meta,
        Column("id", Integer, primary_key=True),
        Column("num", Integer),
        Column("data", String),
        Index("my_pg_index", "data").ddl_if(callable_=only_pg_14),
    )

.. seealso::

    :meth:`.Constraint.ddl_if`

    :meth:`.Index.ddl_if`



.. _schema_api_ddl:

DDL Expression Constructs API
-----------------------------

.. autofunction:: sort_tables

.. autofunction:: sort_tables_and_constraints

.. autoclass:: BaseDDLElement
    :members:

.. autoclass:: ExecutableDDLElement
    :members:

.. autoclass:: DDL
    :members:

.. autoclass:: _CreateDropBase

.. autoclass:: CreateTable
    :members:


.. autoclass:: DropTable
    :members:


.. autoclass:: CreateColumn
    :members:


.. autoclass:: CreateSequence
    :members:


.. autoclass:: DropSequence
    :members:


.. autoclass:: CreateIndex
    :members:


.. autoclass:: DropIndex
    :members:


.. autoclass:: AddConstraint
    :members:


.. autoclass:: DropConstraint
    :members:


.. autoclass:: CreateSchema
    :members:


.. autoclass:: DropSchema
    :members:
