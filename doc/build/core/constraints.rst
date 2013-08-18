.. _metadata_constraints_toplevel:
.. _metadata_constraints:

.. module:: sqlalchemy.schema

=================================
Defining Constraints and Indexes
=================================

.. _metadata_foreignkeys:

This section will discuss SQL :term:`constraints` and indexes.  In SQLAlchemy
the key classes include :class:`.ForeignKeyConstraint` and :class:`.Index`.

Defining Foreign Keys
---------------------

A *foreign key* in SQL is a table-level construct that constrains one or more
columns in that table to only allow values that are present in a different set
of columns, typically but not always located on a different table. We call the
columns which are constrained the *foreign key* columns and the columns which
they are constrained towards the *referenced* columns. The referenced columns
almost always define the primary key for their owning table, though there are
exceptions to this. The foreign key is the "joint" that connects together
pairs of rows which have a relationship with each other, and SQLAlchemy
assigns very deep importance to this concept in virtually every area of its
operation.

In SQLAlchemy as well as in DDL, foreign key constraints can be defined as
additional attributes within the table clause, or for single-column foreign
keys they may optionally be specified within the definition of a single
column. The single column foreign key is more common, and at the column level
is specified by constructing a :class:`~sqlalchemy.schema.ForeignKey` object
as an argument to a :class:`~sqlalchemy.schema.Column` object::

    user_preference = Table('user_preference', metadata,
        Column('pref_id', Integer, primary_key=True),
        Column('user_id', Integer, ForeignKey("user.user_id"), nullable=False),
        Column('pref_name', String(40), nullable=False),
        Column('pref_value', String(100))
    )

Above, we define a new table ``user_preference`` for which each row must
contain a value in the ``user_id`` column that also exists in the ``user``
table's ``user_id`` column.

The argument to :class:`~sqlalchemy.schema.ForeignKey` is most commonly a
string of the form *<tablename>.<columnname>*, or for a table in a remote
schema or "owner" of the form *<schemaname>.<tablename>.<columnname>*. It may
also be an actual :class:`~sqlalchemy.schema.Column` object, which as we'll
see later is accessed from an existing :class:`~sqlalchemy.schema.Table`
object via its ``c`` collection::

    ForeignKey(user.c.user_id)

The advantage to using a string is that the in-python linkage between ``user``
and ``user_preference`` is resolved only when first needed, so that table
objects can be easily spread across multiple modules and defined in any order.

Foreign keys may also be defined at the table level, using the
:class:`~sqlalchemy.schema.ForeignKeyConstraint` object. This object can
describe a single- or multi-column foreign key. A multi-column foreign key is
known as a *composite* foreign key, and almost always references a table that
has a composite primary key. Below we define a table ``invoice`` which has a
composite primary key::

    invoice = Table('invoice', metadata,
        Column('invoice_id', Integer, primary_key=True),
        Column('ref_num', Integer, primary_key=True),
        Column('description', String(60), nullable=False)
    )

And then a table ``invoice_item`` with a composite foreign key referencing
``invoice``::

    invoice_item = Table('invoice_item', metadata,
        Column('item_id', Integer, primary_key=True),
        Column('item_name', String(60), nullable=False),
        Column('invoice_id', Integer, nullable=False),
        Column('ref_num', Integer, nullable=False),
        ForeignKeyConstraint(['invoice_id', 'ref_num'], ['invoice.invoice_id', 'invoice.ref_num'])
    )

It's important to note that the
:class:`~sqlalchemy.schema.ForeignKeyConstraint` is the only way to define a
composite foreign key. While we could also have placed individual
:class:`~sqlalchemy.schema.ForeignKey` objects on both the
``invoice_item.invoice_id`` and ``invoice_item.ref_num`` columns, SQLAlchemy
would not be aware that these two values should be paired together - it would
be two individual foreign key constraints instead of a single composite
foreign key referencing two columns.

.. _use_alter:

Creating/Dropping Foreign Key Constraints via ALTER
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In all the above examples, the :class:`~sqlalchemy.schema.ForeignKey` object
causes the "REFERENCES" keyword to be added inline to a column definition
within a "CREATE TABLE" statement when
:func:`~sqlalchemy.schema.MetaData.create_all` is issued, and
:class:`~sqlalchemy.schema.ForeignKeyConstraint` invokes the "CONSTRAINT"
keyword inline with "CREATE TABLE". There are some cases where this is
undesireable, particularly when two tables reference each other mutually, each
with a foreign key referencing the other. In such a situation at least one of
the foreign key constraints must be generated after both tables have been
built. To support such a scheme, :class:`~sqlalchemy.schema.ForeignKey` and
:class:`~sqlalchemy.schema.ForeignKeyConstraint` offer the flag
``use_alter=True``. When using this flag, the constraint will be generated
using a definition similar to "ALTER TABLE <tablename> ADD CONSTRAINT <name>
...". Since a name is required, the ``name`` attribute must also be specified.
For example::

    node = Table('node', meta,
        Column('node_id', Integer, primary_key=True),
        Column('primary_element', Integer,
            ForeignKey('element.element_id', use_alter=True, name='fk_node_element_id')
        )
    )

    element = Table('element', meta,
        Column('element_id', Integer, primary_key=True),
        Column('parent_node_id', Integer),
        ForeignKeyConstraint(
            ['parent_node_id'],
            ['node.node_id'],
            use_alter=True,
            name='fk_element_parent_node_id'
        )
    )

ON UPDATE and ON DELETE
~~~~~~~~~~~~~~~~~~~~~~~

Most databases support *cascading* of foreign key values, that is the when a
parent row is updated the new value is placed in child rows, or when the
parent row is deleted all corresponding child rows are set to null or deleted.
In data definition language these are specified using phrases like "ON UPDATE
CASCADE", "ON DELETE CASCADE", and "ON DELETE SET NULL", corresponding to
foreign key constraints. The phrase after "ON UPDATE" or "ON DELETE" may also
other allow other phrases that are specific to the database in use. The
:class:`~sqlalchemy.schema.ForeignKey` and
:class:`~sqlalchemy.schema.ForeignKeyConstraint` objects support the
generation of this clause via the ``onupdate`` and ``ondelete`` keyword
arguments. The value is any string which will be output after the appropriate
"ON UPDATE" or "ON DELETE" phrase::

    child = Table('child', meta,
        Column('id', Integer,
                ForeignKey('parent.id', onupdate="CASCADE", ondelete="CASCADE"),
                primary_key=True
        )
    )

    composite = Table('composite', meta,
        Column('id', Integer, primary_key=True),
        Column('rev_id', Integer),
        Column('note_id', Integer),
        ForeignKeyConstraint(
                    ['rev_id', 'note_id'],
                    ['revisions.id', 'revisions.note_id'],
                    onupdate="CASCADE", ondelete="SET NULL"
        )
    )

Note that these clauses are not supported on SQLite, and require ``InnoDB``
tables when used with MySQL. They may also not be supported on other
databases.


UNIQUE Constraint
-----------------

Unique constraints can be created anonymously on a single column using the
``unique`` keyword on :class:`~sqlalchemy.schema.Column`. Explicitly named
unique constraints and/or those with multiple columns are created via the
:class:`~sqlalchemy.schema.UniqueConstraint` table-level construct.

.. sourcecode:: python+sql

    meta = MetaData()
    mytable = Table('mytable', meta,

        # per-column anonymous unique constraint
        Column('col1', Integer, unique=True),

        Column('col2', Integer),
        Column('col3', Integer),

        # explicit/composite unique constraint.  'name' is optional.
        UniqueConstraint('col2', 'col3', name='uix_1')
        )

CHECK Constraint
----------------

Check constraints can be named or unnamed and can be created at the Column or
Table level, using the :class:`~sqlalchemy.schema.CheckConstraint` construct.
The text of the check constraint is passed directly through to the database,
so there is limited "database independent" behavior. Column level check
constraints generally should only refer to the column to which they are
placed, while table level constraints can refer to any columns in the table.

Note that some databases do not actively support check constraints such as
MySQL.

.. sourcecode:: python+sql

    meta = MetaData()
    mytable = Table('mytable', meta,

        # per-column CHECK constraint
        Column('col1', Integer, CheckConstraint('col1>5')),

        Column('col2', Integer),
        Column('col3', Integer),

        # table level CHECK constraint.  'name' is optional.
        CheckConstraint('col2 > col3 + 5', name='check1')
        )

    {sql}mytable.create(engine)
    CREATE TABLE mytable (
        col1 INTEGER  CHECK (col1>5),
        col2 INTEGER,
        col3 INTEGER,
        CONSTRAINT check1  CHECK (col2 > col3 + 5)
    ){stop}

Setting up Constraints when using the Declarative ORM Extension
----------------------------------------------------------------

The :class:`.Table` is the SQLAlchemy Core construct that allows one to define
table metadata, which among other things can be used by the SQLAlchemy ORM
as a target to map a class.  The :ref:`Declarative <declarative_toplevel>`
extension allows the :class:`.Table` object to be created automatically, given
the contents of the table primarily as a mapping of :class:`.Column` objects.

To apply table-level constraint objects such as :class:`.ForeignKeyConstraint`
to a table defined using Declarative, use the ``__table_args__`` attribute,
described at :ref:`declarative_table_args`.

Constraints API
---------------
.. autoclass:: Constraint


.. autoclass:: CheckConstraint


.. autoclass:: ColumnCollectionConstraint


.. autoclass:: ForeignKey
    :members:


.. autoclass:: ForeignKeyConstraint
    :members:


.. autoclass:: PrimaryKeyConstraint


.. autoclass:: UniqueConstraint


.. _schema_indexes:

Indexes
-------

Indexes can be created anonymously (using an auto-generated name ``ix_<column
label>``) for a single column using the inline ``index`` keyword on
:class:`~sqlalchemy.schema.Column`, which also modifies the usage of
``unique`` to apply the uniqueness to the index itself, instead of adding a
separate UNIQUE constraint. For indexes with specific names or which encompass
more than one column, use the :class:`~sqlalchemy.schema.Index` construct,
which requires a name.

Below we illustrate a :class:`~sqlalchemy.schema.Table` with several
:class:`~sqlalchemy.schema.Index` objects associated. The DDL for "CREATE
INDEX" is issued right after the create statements for the table:

.. sourcecode:: python+sql

    meta = MetaData()
    mytable = Table('mytable', meta,
        # an indexed column, with index "ix_mytable_col1"
        Column('col1', Integer, index=True),

        # a uniquely indexed column with index "ix_mytable_col2"
        Column('col2', Integer, index=True, unique=True),

        Column('col3', Integer),
        Column('col4', Integer),

        Column('col5', Integer),
        Column('col6', Integer),
        )

    # place an index on col3, col4
    Index('idx_col34', mytable.c.col3, mytable.c.col4)

    # place a unique index on col5, col6
    Index('myindex', mytable.c.col5, mytable.c.col6, unique=True)

    {sql}mytable.create(engine)
    CREATE TABLE mytable (
        col1 INTEGER,
        col2 INTEGER,
        col3 INTEGER,
        col4 INTEGER,
        col5 INTEGER,
        col6 INTEGER
    )
    CREATE INDEX ix_mytable_col1 ON mytable (col1)
    CREATE UNIQUE INDEX ix_mytable_col2 ON mytable (col2)
    CREATE UNIQUE INDEX myindex ON mytable (col5, col6)
    CREATE INDEX idx_col34 ON mytable (col3, col4){stop}

Note in the example above, the :class:`.Index` construct is created
externally to the table which it corresponds, using :class:`.Column`
objects directly.  :class:`.Index` also supports
"inline" definition inside the :class:`.Table`, using string names to
identify columns::

    meta = MetaData()
    mytable = Table('mytable', meta,
        Column('col1', Integer),

        Column('col2', Integer),

        Column('col3', Integer),
        Column('col4', Integer),

        # place an index on col1, col2
        Index('idx_col12', 'col1', 'col2'),

        # place a unique index on col3, col4
        Index('idx_col34', 'col3', 'col4', unique=True)
    )

.. versionadded:: 0.7
    Support of "inline" definition inside the :class:`.Table`
    for :class:`.Index`\ .

The :class:`~sqlalchemy.schema.Index` object also supports its own ``create()`` method:

.. sourcecode:: python+sql

    i = Index('someindex', mytable.c.col5)
    {sql}i.create(engine)
    CREATE INDEX someindex ON mytable (col5){stop}

.. _schema_indexes_functional:

Functional Indexes
~~~~~~~~~~~~~~~~~~~

:class:`.Index` supports SQL and function expressions, as supported by the
target backend.  To create an index against a column using a descending
value, the :meth:`.ColumnElement.desc` modifier may be used::

    from sqlalchemy import Index

    Index('someindex', mytable.c.somecol.desc())

Or with a backend that supports functional indexes such as Postgresql,
a "case insensitive" index can be created using the ``lower()`` function::

    from sqlalchemy import func, Index

    Index('someindex', func.lower(mytable.c.somecol))

.. versionadded:: 0.8 :class:`.Index` supports SQL expressions and functions
   as well as plain columns.

Index API
---------

.. autoclass:: Index
    :members:
