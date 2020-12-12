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

    >>> messages = Table('messages', meta, autoload_with=engine)
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

    >>> shopping_cart_items = Table('shopping_cart_items', meta, autoload_with=engine)
    >>> 'shopping_carts' in meta.tables:
    True

The :class:`~sqlalchemy.schema.MetaData` has an interesting "singleton-like"
behavior such that if you requested both tables individually,
:class:`~sqlalchemy.schema.MetaData` will ensure that exactly one
:class:`~sqlalchemy.schema.Table` object is created for each distinct table
name. The :class:`~sqlalchemy.schema.Table` constructor actually returns to
you the already-existing :class:`~sqlalchemy.schema.Table` object if one
already exists with the given name. Such as below, we can access the already
generated ``shopping_carts`` table just by naming it::

    shopping_carts = Table('shopping_carts', meta)

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

    >>> mytable = Table('mytable', meta,
    ... Column('id', Integer, primary_key=True),   # override reflected 'id' to have primary key
    ... Column('mydata', Unicode(50)),    # override reflected 'mydata' to be Unicode
    ... # additional Column objects which require no change are reflected normally
    ... autoload_with=some_engine)

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

    my_view = Table("some_view", metadata,
                    Column("view_id", Integer, primary_key=True),
                    Column("related_thing", Integer, ForeignKey("othertable.thing_id")),
                    autoload_with=engine
    )

Reflecting All Tables at Once
-----------------------------

The :class:`~sqlalchemy.schema.MetaData` object can also get a listing of
tables and reflect the full set. This is achieved by using the
:func:`~sqlalchemy.schema.MetaData.reflect` method. After calling it, all
located tables are present within the :class:`~sqlalchemy.schema.MetaData`
object's dictionary of tables::

    meta = MetaData()
    meta.reflect(bind=someengine)
    users_table = meta.tables['users']
    addresses_table = meta.tables['addresses']

``metadata.reflect()`` also provides a handy way to clear or delete all the rows in a database::

    meta = MetaData()
    meta.reflect(bind=someengine)
    for table in reversed(meta.sorted_tables):
        someengine.execute(table.delete())

.. _metadata_reflection_inspector:

Fine Grained Reflection with Inspector
--------------------------------------

A low level interface which provides a backend-agnostic system of loading
lists of schema, table, column, and constraint descriptions from a given
database is also available. This is known as the "Inspector"::

    from sqlalchemy import create_engine
    from sqlalchemy import inspect
    engine = create_engine('...')
    insp = inspect(engine)
    print(insp.get_table_names())

.. autoclass:: sqlalchemy.engine.reflection.Inspector
    :members:
    :undoc-members:

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
datatypes and options)::

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
    >>> mysql_engine = create_engine("mysql://scott:tiger@localhost/test")
    >>> metadata = MetaData()
    >>> my_mysql_table = Table("my_table", metadata, autoload_with=mysql_engine)

The above example reflects the above table schema into a new :class:`_schema.Table`
object.  We can then, for demonstration purposes, print out the MySQL-specific
"CREATE TABLE" statement using the :class:`_schema.CreateTable` construct:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.schema import CreateTable
    >>> print(CreateTable(my_mysql_table).compile(mysql_engine))
    {opensql}CREATE TABLE my_table (
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
    >>> metadata = MetaData()

    >>> @event.listens_for(metadata, "column_reflect")
    >>> def genericize_datatypes(inspector, tablename, column_dict):
    ...     column_dict["type"] = column_dict["type"].as_generic()

    >>> my_generic_table = Table("my_table", metadata, autoload_with=mysql_engine)

We now get a new :class:`_schema.Table` that is generic and uses
:class:`_types.Integer` for those datatypes.  We can now emit a
"CREATE TABLE" statement for example on a PostgreSQL database:

.. sourcecode:: pycon+sql

    >>> pg_engine = create_engine("postgresql://scott:tiger@localhost/test", echo=True)
    >>> my_generic_table.create(pg_engine)
    {opensql}CREATE TABLE my_table (
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

