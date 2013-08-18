.. _metadata_reflection_toplevel:
.. _metadata_reflection:

.. module:: sqlalchemy.schema


Reflecting Database Objects
===========================

A :class:`~sqlalchemy.schema.Table` object can be instructed to load
information about itself from the corresponding database schema object already
existing within the database. This process is called *reflection*. In the
most simple case you need only specify the table name, a :class:`~sqlalchemy.schema.MetaData`
object, and the ``autoload=True`` flag. If the
:class:`~sqlalchemy.schema.MetaData` is not persistently bound, also add the
``autoload_with`` argument::

    >>> messages = Table('messages', meta, autoload=True, autoload_with=engine)
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

    >>> shopping_cart_items = Table('shopping_cart_items', meta, autoload=True, autoload_with=engine)
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

Of course, it's a good idea to use ``autoload=True`` with the above table
regardless. This is so that the table's attributes will be loaded if they have
not been already. The autoload operation only occurs for the table if it
hasn't already been loaded; once loaded, new calls to
:class:`~sqlalchemy.schema.Table` with the same name will not re-issue any
reflection queries.

Overriding Reflected Columns
-----------------------------

Individual columns can be overridden with explicit values when reflecting
tables; this is handy for specifying custom datatypes, constraints such as
primary keys that may not be configured within the database, etc.::

    >>> mytable = Table('mytable', meta,
    ... Column('id', Integer, primary_key=True),   # override reflected 'id' to have primary key
    ... Column('mydata', Unicode(50)),    # override reflected 'mydata' to be Unicode
    ... autoload=True)

Reflecting Views
-----------------

The reflection system can also reflect views. Basic usage is the same as that
of a table::

    my_view = Table("some_view", metadata, autoload=True)

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
                    autoload=True
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

Fine Grained Reflection with Inspector
--------------------------------------

A low level interface which provides a backend-agnostic system of loading
lists of schema, table, column, and constraint descriptions from a given
database is also available. This is known as the "Inspector"::

    from sqlalchemy import create_engine
    from sqlalchemy.engine import reflection
    engine = create_engine('...')
    insp = reflection.Inspector.from_engine(engine)
    print insp.get_table_names()

.. autoclass:: sqlalchemy.engine.reflection.Inspector
    :members:
    :undoc-members:
     

