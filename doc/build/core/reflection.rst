.. currentmodule:: sqlalchemy.schema

.. _metadata_reflection_toplevel:
.. _metadata_reflection:


Reflecting Database Objects
===========================

A :class:`~sqlalchemy.schema.Table` object can be instructed to load
information about itself from the corresponding database schema object already
existing within the database. This process is called *reflection*. In the
most simple case you need only specify the table name, a :class:`~sqlalchemy.schema.MetaData`
object, and the ``autoload_with`` argument::

    >>> messages = Table("messages", metadata_obj, autoload_with=engine)
    >>> [c.name for c in messages.columns]
    ['message_id', 'message_name', 'date']

The above operation will use the given engine to query the database for
information about the ``messages`` table, and will then generate
:class:`~sqlalchemy.schema.Column`, :class:`~sqlalchemy.schema.ForeignKey`,
and other objects corresponding to this information as though the
:class:`~sqlalchemy.schema.Table` object were hand-constructed in Python.

When tables are reflected, if a given table references another one via foreign
key, a second :class:`~sqlalchemy.schema.Table` object is created within the
:class:`~sqlalchemy.schema.MetaData` object representing the connection.
Below, assume the table ``shopping_cart_items`` references a table named
``shopping_carts``. Reflecting the ``shopping_cart_items`` table has the
effect such that the ``shopping_carts`` table will also be loaded::

    >>> shopping_cart_items = Table("shopping_cart_items", metadata_obj, autoload_with=engine)
    >>> "shopping_carts" in metadata_obj.tables
    True

The :class:`~sqlalchemy.schema.MetaData` has an interesting "singleton-like"
behavior such that if you requested both tables individually,
:class:`~sqlalchemy.schema.MetaData` will ensure that exactly one
:class:`~sqlalchemy.schema.Table` object is created for each distinct table
name. The :class:`~sqlalchemy.schema.Table` constructor actually returns to
you the already-existing :class:`~sqlalchemy.schema.Table` object if one
already exists with the given name. Such as below, we can access the already
generated ``shopping_carts`` table just by naming it::

    shopping_carts = Table("shopping_carts", metadata_obj)

Of course, it's a good idea to use ``autoload_with=engine`` with the above table
regardless. This is so that the table's attributes will be loaded if they have
not been already. The autoload operation only occurs for the table if it
hasn't already been loaded; once loaded, new calls to
:class:`~sqlalchemy.schema.Table` with the same name will not re-issue any
reflection queries.

.. _reflection_overriding_columns:

Overriding Reflected Columns
----------------------------

Individual columns can be overridden with explicit values when reflecting
tables; this is handy for specifying custom datatypes, constraints such as
primary keys that may not be configured within the database, etc.::

    >>> mytable = Table(
    ...     "mytable",
    ...     metadata_obj,
    ...     Column(
    ...         "id", Integer, primary_key=True
    ...     ),  # override reflected 'id' to have primary key
    ...     Column("mydata", Unicode(50)),  # override reflected 'mydata' to be Unicode
    ...     # additional Column objects which require no change are reflected normally
    ...     autoload_with=some_engine,
    ... )

.. seealso::

    :ref:`custom_and_decorated_types_reflection` - illustrates how the above
    column override technique applies to the use of custom datatypes with
    table reflection.

Reflecting Views
----------------

The reflection system can also reflect views. Basic usage is the same as that
of a table::

    my_view = Table("some_view", metadata, autoload_with=engine)

Above, ``my_view`` is a :class:`~sqlalchemy.schema.Table` object with
:class:`~sqlalchemy.schema.Column` objects representing the names and types of
each column within the view "some_view".

Usually, it's desired to have at least a primary key constraint when
reflecting a view, if not foreign keys as well. View reflection doesn't
extrapolate these constraints.

Use the "override" technique for this, specifying explicitly those columns
which are part of the primary key or have foreign key constraints::

    my_view = Table(
        "some_view",
        metadata,
        Column("view_id", Integer, primary_key=True),
        Column("related_thing", Integer, ForeignKey("othertable.thing_id")),
        autoload_with=engine,
    )

Reflecting All Tables at Once
-----------------------------

The :class:`~sqlalchemy.schema.MetaData` object can also get a listing of
tables and reflect the full set. This is achieved by using the
:func:`~sqlalchemy.schema.MetaData.reflect` method. After calling it, all
located tables are present within the :class:`~sqlalchemy.schema.MetaData`
object's dictionary of tables::

    metadata_obj = MetaData()
    metadata_obj.reflect(bind=someengine)
    users_table = metadata_obj.tables["users"]
    addresses_table = metadata_obj.tables["addresses"]

``metadata.reflect()`` also provides a handy way to clear or delete all the rows in a database::

    metadata_obj = MetaData()
    metadata_obj.reflect(bind=someengine)
    with someengine.begin() as conn:
        for table in reversed(metadata_obj.sorted_tables):
            conn.execute(table.delete())

.. _metadata_reflection_schemas:

Reflecting Tables from Other Schemas
------------------------------------

The section :ref:`schema_table_schema_name` introduces the concept of table
schemas, which are namespaces within a database that contain tables and other
objects, and which can be specified explicitly. The "schema" for a
:class:`_schema.Table` object, as well as for other objects like views, indexes and
sequences, can be set up using the :paramref:`_schema.Table.schema` parameter,
and also as the default schema for a :class:`_schema.MetaData` object using the
:paramref:`_schema.MetaData.schema` parameter.

The use of this schema parameter directly affects where the table reflection
feature will look when it is asked to reflect objects.  For example, given
a :class:`_schema.MetaData` object configured with a default schema name
"project" via its :paramref:`_schema.MetaData.schema` parameter::

    >>> metadata_obj = MetaData(schema="project")

The :meth:`.MetaData.reflect` will then utilize that configured ``.schema``
for reflection::

    >>> # uses `schema` configured in metadata_obj
    >>> metadata_obj.reflect(someengine)

The end result is that :class:`_schema.Table` objects from the "project"
schema will be reflected, and they will be populated as schema-qualified
with that name::

    >>> metadata_obj.tables["project.messages"]
    Table('messages', MetaData(), Column('message_id', INTEGER(), table=<messages>), schema='project')

Similarly, an individual :class:`_schema.Table` object that includes the
:paramref:`_schema.Table.schema` parameter will also be reflected from that
database schema, overriding any default schema that may have been configured on the
owning :class:`_schema.MetaData` collection::

    >>> messages = Table("messages", metadata_obj, schema="project", autoload_with=someengine)
    >>> messages
    Table('messages', MetaData(), Column('message_id', INTEGER(), table=<messages>), schema='project')

Finally, the :meth:`_schema.MetaData.reflect` method itself also allows a
:paramref:`_schema.MetaData.reflect.schema` parameter to be passed, so we
could also load tables from the "project" schema for a default configured
:class:`_schema.MetaData` object::

    >>> metadata_obj = MetaData()
    >>> metadata_obj.reflect(someengine, schema="project")

We can call :meth:`_schema.MetaData.reflect` any number of times with different
:paramref:`_schema.MetaData.schema` arguments (or none at all) to continue
populating the :class:`_schema.MetaData` object with more objects::

    >>> # add tables from the "customer" schema
    >>> metadata_obj.reflect(someengine, schema="customer")
    >>> # add tables from the default schema
    >>> metadata_obj.reflect(someengine)

.. _reflection_schema_qualified_interaction:

Interaction of Schema-qualified Reflection with the Default Schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. admonition:: Section Best Practices Summarized

   In this section, we discuss SQLAlchemy's reflection behavior regarding
   tables that are visible in the "default schema" of a database session,
   and how these interact with SQLAlchemy directives that include the schema
   explicitly.  As a best practice, ensure the "default" schema for a database
   is just a single name, and not a list of names; for tables that are
   part of this "default" schema and can be named without schema qualification
   in DDL and SQL, leave corresponding :paramref:`_schema.Table.schema` and
   similar schema parameters set to their default of ``None``.

As described at :ref:`schema_metadata_schema_name`, databases that have
the concept of schemas usually also include the concept of a "default" schema.
The reason for this is naturally that when one refers to table objects without
a schema as is common, a schema-capable database will still consider that
table to be in a "schema" somewhere.   Some databases such as PostgreSQL
take this concept further into the notion of a
`schema search path
<https://www.postgresql.org/docs/current/static/ddl-schemas.html#DDL-SCHEMAS-PATH>`_
where *multiple* schema names can be considered in a particular database
session to be "implicit"; referring to a table name that it's any of those
schemas will not require that the schema name be present (while at the same time
it's also perfectly fine if the schema name *is* present).

Since most relational databases therefore have the concept of a particular
table object which can be referenced both in a schema-qualified way, as
well as an "implicit" way where no schema is present, this presents a
complexity for SQLAlchemy's reflection
feature.  Reflecting a table in
a schema-qualified manner will always populate its :attr:`_schema.Table.schema`
attribute and additionally affect how this :class:`_schema.Table` is organized
into the :attr:`_schema.MetaData.tables` collection, that is, in a schema
qualified manner.  Conversely, reflecting the **same** table in a non-schema
qualified manner will organize it into the :attr:`_schema.MetaData.tables`
collection **without** being schema qualified.  The end result is that there
would be two separate :class:`_schema.Table` objects in the single
:class:`_schema.MetaData` collection representing the same table in the
actual database.

To illustrate the ramifications of this issue, consider tables from the
"project" schema in the previous example, and suppose also that the "project"
schema is the default schema of our database connection, or if using a database
such as PostgreSQL suppose the "project" schema is set up in the PostgreSQL
``search_path``.  This would mean that the database accepts the following
two SQL statements as equivalent:

.. sourcecode:: sql

    -- schema qualified
    SELECT message_id FROM project.messages

    -- non-schema qualified
    SELECT message_id FROM messages

This is not a problem as the table can be found in both ways.  However
in SQLAlchemy, it's the **identity** of the :class:`_schema.Table` object
that determines its semantic role within a SQL statement.  Based on the current
decisions within SQLAlchemy, this means that if we reflect the same "messages" table in
both a schema-qualified as well as a non-schema qualified manner, we get
**two** :class:`_schema.Table` objects that will **not** be treated as
semantically equivalent::

    >>> # reflect in non-schema qualified fashion
    >>> messages_table_1 = Table("messages", metadata_obj, autoload_with=someengine)
    >>> # reflect in schema qualified fashion
    >>> messages_table_2 = Table(
    ...     "messages", metadata_obj, schema="project", autoload_with=someengine
    ... )
    >>> # two different objects
    >>> messages_table_1 is messages_table_2
    False
    >>> # stored in two different ways
    >>> metadata.tables["messages"] is messages_table_1
    True
    >>> metadata.tables["project.messages"] is messages_table_2
    True

The above issue becomes more complicated when the tables being reflected contain
foreign key references to other tables.  Suppose "messages" has a "project_id"
column which refers to rows in another schema-local table "projects", meaning
there is a :class:`_schema.ForeignKeyConstraint` object that is part of the
definition of the "messages" table.

We can find ourselves in a situation where one :class:`_schema.MetaData`
collection may contain as many as four :class:`_schema.Table` objects
representing these two database tables, where one or two of the additional
tables were generated by the reflection process; this is because when
the reflection process encounters a foreign key constraint on a table
being reflected, it branches out to reflect that referenced table as well.
The decision making it uses to assign the schema to this referenced
table is that SQLAlchemy will **omit a default schema** from the reflected
:class:`_schema.ForeignKeyConstraint` object if the owning
:class:`_schema.Table` also omits its schema name and also that these two objects
are in the same schema, but will **include** it if
it were not omitted.

The common scenario is when the reflection of a table in a schema qualified
fashion then loads a related table that will also be performed in a schema
qualified fashion::

    >>> # reflect "messages" in a schema qualified fashion
    >>> messages_table_1 = Table(
    ...     "messages", metadata_obj, schema="project", autoload_with=someengine
    ... )

The above ``messages_table_1`` will refer to ``projects`` also in a schema
qualified fashion.  This "projects" table will be reflected automatically by
the fact that "messages" refers to it::

    >>> messages_table_1.c.project_id
    Column('project_id', INTEGER(), ForeignKey('project.projects.project_id'), table=<messages>)

if some other part of the code reflects "projects" in a non-schema qualified
fashion, there are now two projects tables that are not the same:

    >>> # reflect "projects" in a non-schema qualified fashion
    >>> projects_table_1 = Table("projects", metadata_obj, autoload_with=someengine)

    >>> # messages does not refer to projects_table_1 above
    >>> messages_table_1.c.project_id.references(projects_table_1.c.project_id)
    False

    >>> # it refers to this one
    >>> projects_table_2 = metadata_obj.tables["project.projects"]
    >>> messages_table_1.c.project_id.references(projects_table_2.c.project_id)
    True

    >>> # they're different, as one non-schema qualified and the other one is
    >>> projects_table_1 is projects_table_2
    False

The above confusion can cause problems within applications that use table
reflection to load up application-level :class:`_schema.Table` objects, as
well as within migration scenarios, in particular such as when using Alembic
Migrations to detect new tables and foreign key constraints.

The above behavior can be remedied by sticking to one simple practice:

* Don't include the :paramref:`_schema.Table.schema` parameter for any
  :class:`_schema.Table` that expects to be located in the **default** schema
  of the database.

For PostgreSQL and other databases that support a "search" path for schemas,
add the following additional practice:

* Keep the "search path" narrowed down to **one schema only, which is the
  default schema**.


.. seealso::

    :ref:`postgresql_schema_reflection` - additional details of this behavior
    as regards the PostgreSQL database.


.. _metadata_reflection_inspector:

Fine Grained Reflection with Inspector
--------------------------------------

A low level interface which provides a backend-agnostic system of loading
lists of schema, table, column, and constraint descriptions from a given
database is also available. This is known as the "Inspector"::

    from sqlalchemy import create_engine
    from sqlalchemy import inspect

    engine = create_engine("...")
    insp = inspect(engine)
    print(insp.get_table_names())

.. autoclass:: sqlalchemy.engine.reflection.Inspector
    :members:
    :undoc-members:

.. autoclass:: sqlalchemy.engine.interfaces.ReflectedColumn
    :members:
    :inherited-members: dict

.. autoclass:: sqlalchemy.engine.interfaces.ReflectedComputed
    :members:
    :inherited-members: dict

.. autoclass:: sqlalchemy.engine.interfaces.ReflectedCheckConstraint
    :members:
    :inherited-members: dict

.. autoclass:: sqlalchemy.engine.interfaces.ReflectedForeignKeyConstraint
    :members:
    :inherited-members: dict

.. autoclass:: sqlalchemy.engine.interfaces.ReflectedIdentity
    :members:
    :inherited-members: dict

.. autoclass:: sqlalchemy.engine.interfaces.ReflectedIndex
    :members:
    :inherited-members: dict

.. autoclass:: sqlalchemy.engine.interfaces.ReflectedPrimaryKeyConstraint
    :members:
    :inherited-members: dict

.. autoclass:: sqlalchemy.engine.interfaces.ReflectedUniqueConstraint
    :members:
    :inherited-members: dict

.. autoclass:: sqlalchemy.engine.interfaces.ReflectedTableComment
    :members:
    :inherited-members: dict


.. _metadata_reflection_dbagnostic_types:

Reflecting with Database-Agnostic Types
---------------------------------------

When the columns of a table are reflected, using either the
:paramref:`_schema.Table.autoload_with` parameter of :class:`_schema.Table` or
the :meth:`_reflection.Inspector.get_columns` method of
:class:`_reflection.Inspector`, the datatypes will be as specific as possible
to the target database.   This means that if an "integer" datatype is reflected
from a MySQL database, the type will be represented by the
:class:`sqlalchemy.dialects.mysql.INTEGER` class, which includes MySQL-specific
attributes such as "display_width".   Or on PostgreSQL, a PostgreSQL-specific
datatype such as :class:`sqlalchemy.dialects.postgresql.INTERVAL` or
:class:`sqlalchemy.dialects.postgresql.ENUM` may be returned.

There is a use case for reflection which is that a given :class:`_schema.Table`
is to be transferred to a different vendor database.   To suit this use case,
there is a technique by which these vendor-specific datatypes can be converted
on the fly to be instance of SQLAlchemy backend-agnostic datatypes, for
the examples above types such as :class:`_types.Integer`, :class:`_types.Interval`
and :class:`_types.Enum`.   This may be achieved by intercepting the
column reflection using the :meth:`_events.DDLEvents.column_reflect` event
in conjunction with the :meth:`_types.TypeEngine.as_generic` method.

Given a table in MySQL (chosen because MySQL has a lot of vendor-specific
datatypes and options):

.. sourcecode:: sql

    CREATE TABLE IF NOT EXISTS my_table (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        data1 VARCHAR(50) CHARACTER SET latin1,
        data2 MEDIUMINT(4),
        data3 TINYINT(2)
    )

The above table includes MySQL-only integer types ``MEDIUMINT`` and
``TINYINT`` as well as a ``VARCHAR`` that includes the MySQL-only ``CHARACTER
SET`` option.   If we reflect this table normally, it produces a
:class:`_schema.Table` object that will contain those MySQL-specific datatypes
and options:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import MetaData, Table, create_engine
    >>> mysql_engine = create_engine("mysql+mysqldb://scott:tiger@localhost/test")
    >>> metadata_obj = MetaData()
    >>> my_mysql_table = Table("my_table", metadata_obj, autoload_with=mysql_engine)

The above example reflects the above table schema into a new :class:`_schema.Table`
object.  We can then, for demonstration purposes, print out the MySQL-specific
"CREATE TABLE" statement using the :class:`_schema.CreateTable` construct:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.schema import CreateTable
    >>> print(CreateTable(my_mysql_table).compile(mysql_engine))
    {printsql}CREATE TABLE my_table (
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    data1 VARCHAR(50) CHARACTER SET latin1,
    data2 MEDIUMINT(4),
    data3 TINYINT(2),
    PRIMARY KEY (id)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4


Above, the MySQL-specific datatypes and options were maintained.   If we wanted
a :class:`_schema.Table` that we could instead transfer cleanly to another
database vendor, replacing the special datatypes
:class:`sqlalchemy.dialects.mysql.MEDIUMINT` and
:class:`sqlalchemy.dialects.mysql.TINYINT` with :class:`_types.Integer`, we can
choose instead to "genericize" the datatypes on this table, or otherwise change
them in any way we'd like, by establishing a handler using the
:meth:`_events.DDLEvents.column_reflect` event.  The custom handler will make use
of the :meth:`_types.TypeEngine.as_generic` method to convert the above
MySQL-specific type objects into generic ones, by replacing the ``"type"``
entry within the column dictionary entry that is passed to the event handler.
The format of this dictionary is described at :meth:`_reflection.Inspector.get_columns`:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import event
    >>> metadata_obj = MetaData()

    >>> @event.listens_for(metadata_obj, "column_reflect")
    ... def genericize_datatypes(inspector, tablename, column_dict):
    ...     column_dict["type"] = column_dict["type"].as_generic()

    >>> my_generic_table = Table("my_table", metadata_obj, autoload_with=mysql_engine)

We now get a new :class:`_schema.Table` that is generic and uses
:class:`_types.Integer` for those datatypes.  We can now emit a
"CREATE TABLE" statement for example on a PostgreSQL database:

.. sourcecode:: pycon+sql

    >>> pg_engine = create_engine("postgresql+psycopg2://scott:tiger@localhost/test", echo=True)
    >>> my_generic_table.create(pg_engine)
    {execsql}CREATE TABLE my_table (
        id SERIAL NOT NULL,
        data1 VARCHAR(50),
        data2 INTEGER,
        data3 INTEGER,
        PRIMARY KEY (id)
    )

Noting above also that SQLAlchemy will usually make a decent guess for other
behaviors, such as that the MySQL ``AUTO_INCREMENT`` directive is represented
in PostgreSQL most closely using the ``SERIAL`` auto-incrementing datatype.

.. versionadded:: 1.4 Added the :meth:`_types.TypeEngine.as_generic` method
   and additionally improved the use of the :meth:`_events.DDLEvents.column_reflect`
   event such that it may be applied to a :class:`_schema.MetaData` object
   for convenience.


Limitations of Reflection
-------------------------

It's important to note that the reflection process recreates :class:`_schema.Table`
metadata using only information which is represented in the relational database.
This process by definition cannot restore aspects of a schema that aren't
actually stored in the database.   State which is not available from reflection
includes but is not limited to:

* Client side defaults, either Python functions or SQL expressions defined using
  the ``default`` keyword of :class:`_schema.Column` (note this is separate from ``server_default``,
  which specifically is what's available via reflection).

* Column information, e.g. data that might have been placed into the
  :attr:`_schema.Column.info` dictionary

* The value of the ``.quote`` setting for :class:`_schema.Column` or :class:`_schema.Table`

* The association of a particular :class:`.Sequence` with a given :class:`_schema.Column`

The relational database also in many cases reports on table metadata in a
different format than what was specified in SQLAlchemy.   The :class:`_schema.Table`
objects returned from reflection cannot be always relied upon to produce the identical
DDL as the original Python-defined :class:`_schema.Table` objects.   Areas where
this occurs includes server defaults, column-associated sequences and various
idiosyncrasies regarding constraints and datatypes.   Server side defaults may
be returned with cast directives (typically PostgreSQL will include a ``::<type>``
cast) or different quoting patterns than originally specified.

Another category of limitation includes schema structures for which reflection
is only partially or not yet defined.  Recent improvements to reflection allow
things like views, indexes and foreign key options to be reflected.  As of this
writing, structures like CHECK constraints, table comments, and triggers are
not reflected.

