.. _metadata_constraints_toplevel:
.. _metadata_constraints:

.. currentmodule:: sqlalchemy.schema

================================
Defining Constraints and Indexes
================================

This section will discuss SQL :term:`constraints` and indexes.  In SQLAlchemy
the key classes include :class:`_schema.ForeignKeyConstraint` and :class:`.Index`.

.. _metadata_foreignkeys:

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

The behavior we've seen in tutorials and elsewhere involving
foreign keys with DDL illustrates that the constraints are typically
rendered "inline" within the CREATE TABLE statement, such as:

.. sourcecode:: sql

    CREATE TABLE addresses (
        id INTEGER NOT NULL,
        user_id INTEGER,
        email_address VARCHAR NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT user_id_fk FOREIGN KEY(user_id) REFERENCES users (id)
    )

The ``CONSTRAINT .. FOREIGN KEY`` directive is used to create the constraint
in an "inline" fashion within the CREATE TABLE definition.   The
:meth:`_schema.MetaData.create_all` and :meth:`_schema.MetaData.drop_all` methods do
this by default, using a topological sort of all the :class:`_schema.Table` objects
involved such that tables are created and dropped in order of their foreign
key dependency (this sort is also available via the
:attr:`_schema.MetaData.sorted_tables` accessor).

This approach can't work when two or more foreign key constraints are
involved in a "dependency cycle", where a set of tables
are mutually dependent on each other, assuming the backend enforces foreign
keys (always the case except on SQLite, MySQL/MyISAM).   The methods will
therefore break out constraints in such a cycle into separate ALTER
statements, on all backends other than SQLite which does not support
most forms of ALTER.  Given a schema like::

    node = Table(
        'node', metadata,
        Column('node_id', Integer, primary_key=True),
        Column(
            'primary_element', Integer,
            ForeignKey('element.element_id')
        )
    )

    element = Table(
        'element', metadata,
        Column('element_id', Integer, primary_key=True),
        Column('parent_node_id', Integer),
        ForeignKeyConstraint(
            ['parent_node_id'], ['node.node_id'],
            name='fk_element_parent_node_id'
        )
    )

When we call upon :meth:`_schema.MetaData.create_all` on a backend such as the
PostgreSQL backend, the cycle between these two tables is resolved and the
constraints are created separately:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...    metadata.create_all(conn, checkfirst=False)
    {opensql}CREATE TABLE element (
        element_id SERIAL NOT NULL,
        parent_node_id INTEGER,
        PRIMARY KEY (element_id)
    )

    CREATE TABLE node (
        node_id SERIAL NOT NULL,
        primary_element INTEGER,
        PRIMARY KEY (node_id)
    )

    ALTER TABLE element ADD CONSTRAINT fk_element_parent_node_id
        FOREIGN KEY(parent_node_id) REFERENCES node (node_id)
    ALTER TABLE node ADD FOREIGN KEY(primary_element)
        REFERENCES element (element_id)
    {stop}

In order to emit DROP for these tables, the same logic applies, however
note here that in SQL, to emit DROP CONSTRAINT requires that the constraint
has a name.  In the case of the ``'node'`` table above, we haven't named
this constraint; the system will therefore attempt to emit DROP for only
those constraints that are named:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...    metadata.drop_all(conn, checkfirst=False)
    {opensql}ALTER TABLE element DROP CONSTRAINT fk_element_parent_node_id
    DROP TABLE node
    DROP TABLE element
    {stop}


In the case where the cycle cannot be resolved, such as if we hadn't applied
a name to either constraint here, we will receive the following error::

    sqlalchemy.exc.CircularDependencyError: Can't sort tables for DROP;
    an unresolvable foreign key dependency exists between tables:
    element, node.  Please ensure that the ForeignKey and ForeignKeyConstraint
    objects involved in the cycle have names so that they can be dropped
    using DROP CONSTRAINT.

This error only applies to the DROP case as we can emit "ADD CONSTRAINT"
in the CREATE case without a name; the database typically assigns one
automatically.

The :paramref:`_schema.ForeignKeyConstraint.use_alter` and
:paramref:`_schema.ForeignKey.use_alter` keyword arguments can be used
to manually resolve dependency cycles.  We can add this flag only to
the ``'element'`` table as follows::

    element = Table(
        'element', metadata,
        Column('element_id', Integer, primary_key=True),
        Column('parent_node_id', Integer),
        ForeignKeyConstraint(
            ['parent_node_id'], ['node.node_id'],
            use_alter=True, name='fk_element_parent_node_id'
        )
    )

in our CREATE DDL we will see the ALTER statement only for this constraint,
and not the other one:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...    metadata.create_all(conn, checkfirst=False)
    {opensql}CREATE TABLE element (
        element_id SERIAL NOT NULL,
        parent_node_id INTEGER,
        PRIMARY KEY (element_id)
    )

    CREATE TABLE node (
        node_id SERIAL NOT NULL,
        primary_element INTEGER,
        PRIMARY KEY (node_id),
        FOREIGN KEY(primary_element) REFERENCES element (element_id)
    )

    ALTER TABLE element ADD CONSTRAINT fk_element_parent_node_id
    FOREIGN KEY(parent_node_id) REFERENCES node (node_id)
    {stop}

:paramref:`_schema.ForeignKeyConstraint.use_alter` and
:paramref:`_schema.ForeignKey.use_alter`, when used in conjunction with a drop
operation, will require that the constraint is named, else an error
like the following is generated::

    sqlalchemy.exc.CompileError: Can't emit DROP CONSTRAINT for constraint
    ForeignKeyConstraint(...); it has no name

.. versionchanged:: 1.0.0 - The DDL system invoked by
   :meth:`_schema.MetaData.create_all`
   and :meth:`_schema.MetaData.drop_all` will now automatically resolve mutually
   dependent foreign keys between tables declared by
   :class:`_schema.ForeignKeyConstraint` and :class:`_schema.ForeignKey` objects, without
   the need to explicitly set the :paramref:`_schema.ForeignKeyConstraint.use_alter`
   flag.

.. versionchanged:: 1.0.0 - The :paramref:`_schema.ForeignKeyConstraint.use_alter`
   flag can be used with an un-named constraint; only the DROP operation
   will emit a specific error when actually called upon.

.. seealso::

    :ref:`constraint_naming_conventions`

    :func:`.sort_tables_and_constraints`

.. _on_update_on_delete:

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

Note that these clauses require ``InnoDB`` tables when used with MySQL.
They may also not be supported on other databases.

.. seealso::

    For background on integration of ``ON DELETE CASCADE`` with
    ORM :func:`_orm.relationship` constructs, see the following sections:

    :ref:`passive_deletes`

    :ref:`passive_deletes_many_to_many`

.. _schema_unique_constraint:

UNIQUE Constraint
-----------------

Unique constraints can be created anonymously on a single column using the
``unique`` keyword on :class:`~sqlalchemy.schema.Column`. Explicitly named
unique constraints and/or those with multiple columns are created via the
:class:`~sqlalchemy.schema.UniqueConstraint` table-level construct.

.. sourcecode:: python+sql

    from sqlalchemy import UniqueConstraint

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

    from sqlalchemy import CheckConstraint

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

PRIMARY KEY Constraint
----------------------

The primary key constraint of any :class:`_schema.Table` object is implicitly
present, based on the :class:`_schema.Column` objects that are marked with the
:paramref:`_schema.Column.primary_key` flag.   The :class:`.PrimaryKeyConstraint`
object provides explicit access to this constraint, which includes the
option of being configured directly::

    from sqlalchemy import PrimaryKeyConstraint

    my_table = Table('mytable', metadata,
                Column('id', Integer),
                Column('version_id', Integer),
                Column('data', String(50)),
                PrimaryKeyConstraint('id', 'version_id', name='mytable_pk')
            )

.. seealso::

    :class:`.PrimaryKeyConstraint` - detailed API documentation.

Setting up Constraints when using the Declarative ORM Extension
---------------------------------------------------------------

The :class:`_schema.Table` is the SQLAlchemy Core construct that allows one to define
table metadata, which among other things can be used by the SQLAlchemy ORM
as a target to map a class.  The :ref:`Declarative <declarative_toplevel>`
extension allows the :class:`_schema.Table` object to be created automatically, given
the contents of the table primarily as a mapping of :class:`_schema.Column` objects.

To apply table-level constraint objects such as :class:`_schema.ForeignKeyConstraint`
to a table defined using Declarative, use the ``__table_args__`` attribute,
described at :ref:`declarative_table_args`.

.. _constraint_naming_conventions:

Configuring Constraint Naming Conventions
-----------------------------------------

Relational databases typically assign explicit names to all constraints and
indexes.  In the common case that a table is created using ``CREATE TABLE``
where constraints such as CHECK, UNIQUE, and PRIMARY KEY constraints are
produced inline with the table definition, the database usually has a system
in place in which names are automatically assigned to these constraints, if
a name is not otherwise specified.  When an existing database table is altered
in a database using a command such as ``ALTER TABLE``, this command typically
needs to specify explicit names for new constraints as well as be able to
specify the name of an existing constraint that is to be dropped or modified.

Constraints can be named explicitly using the :paramref:`.Constraint.name` parameter,
and for indexes the :paramref:`.Index.name` parameter.  However, in the
case of constraints this parameter is optional.  There are also the use
cases of using the :paramref:`_schema.Column.unique` and :paramref:`_schema.Column.index`
parameters which create :class:`.UniqueConstraint` and :class:`.Index` objects
without an explicit name being specified.

The use case of alteration of existing tables and constraints can be handled
by schema migration tools such as `Alembic <https://alembic.sqlalchemy.org/>`_.
However, neither Alembic nor SQLAlchemy currently create names for constraint
objects where the name is otherwise unspecified, leading to the case where
being able to alter existing constraints means that one must reverse-engineer
the naming system used by the relational database to auto-assign names,
or that care must be taken to ensure that all constraints are named.

In contrast to having to assign explicit names to all :class:`.Constraint`
and :class:`.Index` objects, automated naming schemes can be constructed
using events.  This approach has the advantage that constraints will get
a consistent naming scheme without the need for explicit name parameters
throughout the code, and also that the convention takes place just as well
for those constraints and indexes produced by the :paramref:`_schema.Column.unique`
and :paramref:`_schema.Column.index` parameters.  As of SQLAlchemy 0.9.2 this
event-based approach is included, and can be configured using the argument
:paramref:`_schema.MetaData.naming_convention`.

Configuring a Naming Convention for a MetaData Collection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:paramref:`_schema.MetaData.naming_convention` refers to a dictionary which accepts
the :class:`.Index` class or individual :class:`.Constraint` classes as keys,
and Python string templates as values.   It also accepts a series of
string-codes as alternative keys, ``"fk"``, ``"pk"``,
``"ix"``, ``"ck"``, ``"uq"`` for foreign key, primary key, index,
check, and unique constraint, respectively.  The string templates in this
dictionary are used whenever a constraint or index is associated with this
:class:`_schema.MetaData` object that does not have an existing name given (including
one exception case where an existing name can be further embellished).

An example naming convention that suits basic cases is as follows::

    convention = {
      "ix": 'ix_%(column_0_label)s',
      "uq": "uq_%(table_name)s_%(column_0_name)s",
      "ck": "ck_%(table_name)s_%(constraint_name)s",
      "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
      "pk": "pk_%(table_name)s"
    }

    metadata = MetaData(naming_convention=convention)

The above convention will establish names for all constraints within
the target :class:`_schema.MetaData` collection.
For example, we can observe the name produced when we create an unnamed
:class:`.UniqueConstraint`::

    >>> user_table = Table('user', metadata,
    ...                 Column('id', Integer, primary_key=True),
    ...                 Column('name', String(30), nullable=False),
    ...                 UniqueConstraint('name')
    ... )
    >>> list(user_table.constraints)[1].name
    'uq_user_name'

This same feature takes effect even if we just use the :paramref:`_schema.Column.unique`
flag::

    >>> user_table = Table('user', metadata,
    ...                  Column('id', Integer, primary_key=True),
    ...                  Column('name', String(30), nullable=False, unique=True)
    ...     )
    >>> list(user_table.constraints)[1].name
    'uq_user_name'

A key advantage to the naming convention approach is that the names are established
at Python construction time, rather than at DDL emit time.  The effect this has
when using Alembic's ``--autogenerate`` feature is that the naming convention
will be explicit when a new migration script is generated::

    def upgrade():
        op.create_unique_constraint("uq_user_name", "user", ["name"])

The above ``"uq_user_name"`` string was copied from the :class:`.UniqueConstraint`
object that ``--autogenerate`` located in our metadata.

The tokens available include ``%(table_name)s``, ``%(referred_table_name)s``,
``%(column_0_name)s``, ``%(column_0_label)s``, ``%(column_0_key)s``,
``%(referred_column_0_name)s``, and  ``%(constraint_name)s``, as well as
multiple-column versions of each including ``%(column_0N_name)s``,
``%(column_0_N_name)s``,  ``%(referred_column_0_N_name)s`` which render all
column names separated with or without an underscore.  The documentation for
:paramref:`_schema.MetaData.naming_convention` has further detail on each  of these
conventions.

.. _constraint_default_naming_convention:

The Default Naming Convention
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default value for :paramref:`_schema.MetaData.naming_convention` handles
the long-standing SQLAlchemy behavior of assigning a name to a :class:`.Index`
object that is created using the :paramref:`_schema.Column.index` parameter::

    >>> from sqlalchemy.sql.schema import DEFAULT_NAMING_CONVENTION
    >>> DEFAULT_NAMING_CONVENTION
    immutabledict({'ix': 'ix_%(column_0_label)s'})

Truncation of Long Names
~~~~~~~~~~~~~~~~~~~~~~~~~

When a generated name, particularly those that use the multiple-column tokens,
is too long for the identifier length limit of the target database
(for example, PostgreSQL has a limit of 63 characters), the name will be
deterministically truncated using a 4-character suffix based on the md5
hash of the long name.  For example, the naming convention below will
generate very long names given the column names in use::

    metadata = MetaData(naming_convention={
        "uq": "uq_%(table_name)s_%(column_0_N_name)s"
    })

    long_names = Table(
        'long_names', metadata,
        Column('information_channel_code', Integer, key='a'),
        Column('billing_convention_name', Integer, key='b'),
        Column('product_identifier', Integer, key='c'),
        UniqueConstraint('a', 'b', 'c')
    )

On the PostgreSQL dialect, names longer than 63 characters will be truncated
as in the following example::

    CREATE TABLE long_names (
        information_channel_code INTEGER,
        billing_convention_name INTEGER,
        product_identifier INTEGER,
        CONSTRAINT uq_long_names_information_channel_code_billing_conventi_a79e
        UNIQUE (information_channel_code, billing_convention_name, product_identifier)
    )

The above suffix ``a79e`` is based on the md5 hash of the long name and will
generate the same value every time to produce consistent names for a given
schema.

Creating Custom Tokens for Naming Conventions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New tokens can also be added, by specifying an additional token
and a callable within the naming_convention dictionary.  For example, if we
wanted to name our foreign key constraints using a GUID scheme, we could do
that as follows::

    import uuid

    def fk_guid(constraint, table):
        str_tokens = [
            table.name,
        ] + [
            element.parent.name for element in constraint.elements
        ] + [
            element.target_fullname for element in constraint.elements
        ]
        guid = uuid.uuid5(uuid.NAMESPACE_OID, "_".join(str_tokens).encode('ascii'))
        return str(guid)

    convention = {
        "fk_guid": fk_guid,
        "ix": 'ix_%(column_0_label)s',
        "fk": "fk_%(fk_guid)s",
    }

Above, when we create a new :class:`_schema.ForeignKeyConstraint`, we will get a
name as follows::

    >>> metadata = MetaData(naming_convention=convention)

    >>> user_table = Table('user', metadata,
    ...         Column('id', Integer, primary_key=True),
    ...         Column('version', Integer, primary_key=True),
    ...         Column('data', String(30))
    ...     )
    >>> address_table = Table('address', metadata,
    ...        Column('id', Integer, primary_key=True),
    ...        Column('user_id', Integer),
    ...        Column('user_version_id', Integer)
    ...    )
    >>> fk = ForeignKeyConstraint(['user_id', 'user_version_id'],
    ...                ['user.id', 'user.version'])
    >>> address_table.append_constraint(fk)
    >>> fk.name
    fk_0cd51ab5-8d70-56e8-a83c-86661737766d

.. seealso::

    :paramref:`_schema.MetaData.naming_convention` - for additional usage details
    as well as a listing of all available naming components.

    `The Importance of Naming Constraints <https://alembic.sqlalchemy.org/en/latest/naming.html>`_ - in the Alembic documentation.


.. versionadded:: 1.3.0 added multi-column naming tokens such as ``%(column_0_N_name)s``.
   Generated names that go beyond the character limit for the target database will be
   deterministically truncated.

.. _naming_check_constraints:

Naming CHECK Constraints
~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`.CheckConstraint` object is configured against an arbitrary
SQL expression, which can have any number of columns present, and additionally
is often configured using a raw SQL string.  Therefore a common convention
to use with :class:`.CheckConstraint` is one where we expect the object
to have a name already, and we then enhance it with other convention elements.
A typical convention is ``"ck_%(table_name)s_%(constraint_name)s"``::

    metadata = MetaData(
        naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
    )

    Table('foo', metadata,
        Column('value', Integer),
        CheckConstraint('value > 5', name='value_gt_5')
    )

The above table will produce the name ``ck_foo_value_gt_5``::

    CREATE TABLE foo (
        value INTEGER,
        CONSTRAINT ck_foo_value_gt_5 CHECK (value > 5)
    )

:class:`.CheckConstraint` also supports the ``%(columns_0_name)s``
token; we can make use of this by ensuring we use a :class:`_schema.Column` or
:func:`_expression.column` element within the constraint's expression,
either by declaring the constraint separate from the table::

    metadata = MetaData(
        naming_convention={"ck": "ck_%(table_name)s_%(column_0_name)s"}
    )

    foo = Table('foo', metadata,
        Column('value', Integer)
    )

    CheckConstraint(foo.c.value > 5)

or by using a :func:`_expression.column` inline::

    from sqlalchemy import column

    metadata = MetaData(
        naming_convention={"ck": "ck_%(table_name)s_%(column_0_name)s"}
    )

    foo = Table('foo', metadata,
        Column('value', Integer),
        CheckConstraint(column('value') > 5)
    )

Both will produce the name ``ck_foo_value``::

    CREATE TABLE foo (
        value INTEGER,
        CONSTRAINT ck_foo_value CHECK (value > 5)
    )

The determination of the name of "column zero" is performed by scanning
the given expression for column objects.  If the expression has more than
one column present, the scan does use a deterministic search, however the
structure of the expression will determine which column is noted as
"column zero".

.. versionadded:: 1.0.0 The :class:`.CheckConstraint` object now supports
   the ``column_0_name`` naming convention token.

.. _naming_schematypes:

Configuring Naming for Boolean, Enum, and other schema types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`.SchemaType` class refers to type objects such as :class:`.Boolean`
and :class:`.Enum` which generate a CHECK constraint accompanying the type.
The name for the constraint here is most directly set up by sending
the "name" parameter, e.g. :paramref:`.Boolean.name`::

    Table('foo', metadata,
        Column('flag', Boolean(name='ck_foo_flag'))
    )

The naming convention feature may be combined with these types as well,
normally by using a convention which includes ``%(constraint_name)s``
and then applying a name to the type::

    metadata = MetaData(
        naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
    )

    Table('foo', metadata,
        Column('flag', Boolean(name='flag_bool'))
    )

The above table will produce the constraint name ``ck_foo_flag_bool``::

    CREATE TABLE foo (
        flag BOOL,
        CONSTRAINT ck_foo_flag_bool CHECK (flag IN (0, 1))
    )

The :class:`.SchemaType` classes use special internal symbols so that
the naming convention is only determined at DDL compile time.  On PostgreSQL,
there's a native BOOLEAN type, so the CHECK constraint of :class:`.Boolean`
is not needed; we are safe to set up a :class:`.Boolean` type without a
name, even though a naming convention is in place for check constraints.
This convention will only be consulted for the CHECK constraint if we
run against a database without a native BOOLEAN type like SQLite or
MySQL.

The CHECK constraint may also make use of the ``column_0_name`` token,
which works nicely with :class:`.SchemaType` since these constraints have
only one column::

    metadata = MetaData(
        naming_convention={"ck": "ck_%(table_name)s_%(column_0_name)s"}
    )

    Table('foo', metadata,
        Column('flag', Boolean())
    )

The above schema will produce::

    CREATE TABLE foo (
        flag BOOL,
        CONSTRAINT ck_foo_flag CHECK (flag IN (0, 1))
    )

.. versionchanged:: 1.0 Constraint naming conventions that don't include
   ``%(constraint_name)s`` again work with :class:`.SchemaType` constraints.

Constraints API
---------------
.. autoclass:: Constraint
    :members:

.. autoclass:: ColumnCollectionMixin
    :members:

.. autoclass:: ColumnCollectionConstraint
    :members:
    :inherited-members:

.. autoclass:: CheckConstraint
    :members:
    :inherited-members:

.. autoclass:: ForeignKey
    :members:
    :inherited-members:

.. autoclass:: ForeignKeyConstraint
    :members:
    :inherited-members:

.. autoclass:: PrimaryKeyConstraint
    :members:
    :inherited-members:


.. autoclass:: UniqueConstraint
    :members:
    :inherited-members:


.. autofunction:: sqlalchemy.schema.conv

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
externally to the table which it corresponds, using :class:`_schema.Column`
objects directly.  :class:`.Index` also supports
"inline" definition inside the :class:`_schema.Table`, using string names to
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

The :class:`~sqlalchemy.schema.Index` object also supports its own ``create()`` method:

.. sourcecode:: python+sql

    i = Index('someindex', mytable.c.col5)
    {sql}i.create(engine)
    CREATE INDEX someindex ON mytable (col5){stop}

.. _schema_indexes_functional:

Functional Indexes
~~~~~~~~~~~~~~~~~~

:class:`.Index` supports SQL and function expressions, as supported by the
target backend.  To create an index against a column using a descending
value, the :meth:`_expression.ColumnElement.desc` modifier may be used::

    from sqlalchemy import Index

    Index('someindex', mytable.c.somecol.desc())

Or with a backend that supports functional indexes such as PostgreSQL,
a "case insensitive" index can be created using the ``lower()`` function::

    from sqlalchemy import func, Index

    Index('someindex', func.lower(mytable.c.somecol))

Index API
---------

.. autoclass:: Index
    :members:
    :inherited-members:
