.. currentmodule:: sqlalchemy.schema

.. _metadata_defaults_toplevel:

.. _metadata_defaults:

Column INSERT/UPDATE Defaults
=============================

Column INSERT and UPDATE defaults refer to functions that create a **default
value** for a particular column in a row as an INSERT or UPDATE statement is
proceeding against that row, in the case where **no value was provided to the
INSERT or UPDATE statement for that column**.  That is, if a table has a column
called "timestamp", and an INSERT statement proceeds which does not include a
value for this column, an INSERT default would create a new value, such as
the current time, that is used as the value to be INSERTed into the "timestamp"
column.  If the statement *does* include a value  for this column, then the
default does *not* take place.

Column defaults can be server-side functions or constant values which are
defined in the database along with the schema in :term:`DDL`, or as SQL
expressions which are rendered directly within an INSERT or UPDATE statement
emitted by SQLAlchemy; they may also be client-side Python functions or
constant values which are invoked by SQLAlchemy before data is passed to the
database.

.. note::

    A column default handler should not be confused with a construct that
    intercepts and modifies incoming values for INSERT and UPDATE statements
    which *are* provided to the statement as it is invoked.  This is known
    as :term:`data marshalling`, where a column value is modified in some way
    by the application before being sent to the database.  SQLAlchemy provides
    a few means of achieving this which include using :ref:`custom datatypes
    <types_typedecorator>`, :ref:`SQL execution events <core_sql_events>` and
    in the ORM :ref:`custom  validators <simple_validators>` as well as
    :ref:`attribute events <orm_attribute_events>`.    Column defaults are only
    invoked when there is **no value present** for a column in a SQL
    :term:`DML` statement.


SQLAlchemy provides an array of features regarding default generation
functions which take place for non-present values during INSERT and UPDATE
statements. Options include:

* Scalar values used as defaults during INSERT and UPDATE operations
* Python functions which execute upon INSERT and UPDATE operations
* SQL expressions which are embedded in INSERT statements (or in some cases execute beforehand)
* SQL expressions which are embedded in UPDATE statements
* Server side default values used during INSERT
* Markers for server-side triggers used during UPDATE

The general rule for all insert/update defaults is that they only take effect
if no value for a particular column is passed as an ``execute()`` parameter;
otherwise, the given value is used.

Scalar Defaults
---------------

The simplest kind of default is a scalar value used as the default value of a column::

    Table("mytable", meta,
        Column("somecolumn", Integer, default=12)
    )

Above, the value "12" will be bound as the column value during an INSERT if no
other value is supplied.

A scalar value may also be associated with an UPDATE statement, though this is
not very common (as UPDATE statements are usually looking for dynamic
defaults)::

    Table("mytable", meta,
        Column("somecolumn", Integer, onupdate=25)
    )


Python-Executed Functions
-------------------------

The :paramref:`_schema.Column.default` and :paramref:`_schema.Column.onupdate` keyword arguments also accept Python
functions. These functions are invoked at the time of insert or update if no
other value for that column is supplied, and the value returned is used for
the column's value. Below illustrates a crude "sequence" that assigns an
incrementing counter to a primary key column::

    # a function which counts upwards
    i = 0
    def mydefault():
        global i
        i += 1
        return i

    t = Table("mytable", meta,
        Column('id', Integer, primary_key=True, default=mydefault),
    )

It should be noted that for real "incrementing sequence" behavior, the
built-in capabilities of the database should normally be used, which may
include sequence objects or other autoincrementing capabilities. For primary
key columns, SQLAlchemy will in most cases use these capabilities
automatically. See the API documentation for
:class:`~sqlalchemy.schema.Column` including the :paramref:`_schema.Column.autoincrement` flag, as
well as the section on :class:`~sqlalchemy.schema.Sequence` later in this
chapter for background on standard primary key generation techniques.

To illustrate onupdate, we assign the Python ``datetime`` function ``now`` to
the :paramref:`_schema.Column.onupdate` attribute::

    import datetime

    t = Table("mytable", meta,
        Column('id', Integer, primary_key=True),

        # define 'last_updated' to be populated with datetime.now()
        Column('last_updated', DateTime, onupdate=datetime.datetime.now),
    )

When an update statement executes and no value is passed for ``last_updated``,
the ``datetime.datetime.now()`` Python function is executed and its return
value used as the value for ``last_updated``. Notice that we provide ``now``
as the function itself without calling it (i.e. there are no parenthesis
following) - SQLAlchemy will execute the function at the time the statement
executes.

.. _context_default_functions:

Context-Sensitive Default Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Python functions used by :paramref:`_schema.Column.default` and
:paramref:`_schema.Column.onupdate` may also make use of the current statement's
context in order to determine a value. The `context` of a statement is an
internal SQLAlchemy object which contains all information about the statement
being executed, including its source expression, the parameters associated with
it and the cursor. The typical use case for this context with regards to
default generation is to have access to the other values being inserted or
updated on the row. To access the context, provide a function that accepts a
single ``context`` argument::

    def mydefault(context):
        return context.get_current_parameters()['counter'] + 12

    t = Table('mytable', meta,
        Column('counter', Integer),
        Column('counter_plus_twelve', Integer, default=mydefault, onupdate=mydefault)
    )

The above default generation function is applied so that it will execute for
all INSERT and UPDATE statements where a value for ``counter_plus_twelve`` was
otherwise not provided, and the value will be that of whatever value is present
in the execution for the ``counter`` column, plus the number 12.

For a single statement that is being executed using "executemany" style, e.g.
with multiple parameter sets passed to :meth:`_engine.Connection.execute`, the
user-defined function is called once for each set of parameters. For the use case of
a multi-valued :class:`_expression.Insert` construct (e.g. with more than one VALUES
clause set up via the :meth:`_expression.Insert.values` method), the user-defined function
is also called once for each set of parameters.

When the function is invoked, the special method
:meth:`.DefaultExecutionContext.get_current_parameters` is available from
the context object (an subclass of :class:`.DefaultExecutionContext`).  This
method returns a dictionary of column-key to values that represents the
full set of values for the INSERT or UPDATE statement.   In the case of a
multi-valued INSERT construct, the subset of parameters that corresponds to
the individual VALUES clause is isolated from the full parameter dictionary
and returned alone.

.. versionadded:: 1.2

    Added :meth:`.DefaultExecutionContext.get_current_parameters` method,
    which improves upon the still-present
    :attr:`.DefaultExecutionContext.current_parameters` attribute
    by offering the service of organizing multiple VALUES clauses
    into individual parameter dictionaries.

Client-Invoked SQL Expressions
------------------------------

The :paramref:`_schema.Column.default` and :paramref:`_schema.Column.onupdate` keywords may
also be passed SQL expressions, which are in most cases rendered inline within the
INSERT or UPDATE statement::

    t = Table("mytable", meta,
        Column('id', Integer, primary_key=True),

        # define 'create_date' to default to now()
        Column('create_date', DateTime, default=func.now()),

        # define 'key' to pull its default from the 'keyvalues' table
        Column('key', String(20), default=select(keyvalues.c.key).where(keyvalues.c.type='type1')),

        # define 'last_modified' to use the current_timestamp SQL function on update
        Column('last_modified', DateTime, onupdate=func.utc_timestamp())
        )

Above, the ``create_date`` column will be populated with the result of the
``now()`` SQL function (which, depending on backend, compiles into ``NOW()``
or ``CURRENT_TIMESTAMP`` in most cases) during an INSERT statement, and the
``key`` column with the result of a SELECT subquery from another table. The
``last_modified`` column will be populated with the value of
the SQL ``UTC_TIMESTAMP()`` MySQL function when an UPDATE statement is
emitted for this table.

.. note::

    When using SQL functions with the :attr:`.func` construct, we "call" the
    named function, e.g. with parenthesis as in ``func.now()``.   This differs
    from when we specify a Python callable as a default such as
    ``datetime.datetime``, where we pass the function itself, but we don't
    invoke it ourselves.   In the case of a SQL function, invoking
    ``func.now()`` returns the SQL expression object that will render the
    "NOW" function into the SQL being emitted.

Default and update SQL expressions specified by :paramref:`_schema.Column.default` and
:paramref:`_schema.Column.onupdate` are invoked explicitly by SQLAlchemy when an
INSERT or UPDATE statement occurs, typically rendered inline within the DML
statement except in certain cases listed below.   This is different than a
"server side" default, which is part of the table's DDL definition, e.g. as
part of the "CREATE TABLE" statement, which are likely more common.   For
server side defaults, see the next section :ref:`server_defaults`.

When a SQL expression indicated by :paramref:`_schema.Column.default` is used with
primary key columns, there are some cases where SQLAlchemy must "pre-execute"
the default generation SQL function, meaning it is invoked in a separate SELECT
statement, and the resulting value is passed as a parameter to the INSERT.
This only occurs for primary key columns for an INSERT statement that is being
asked to return this primary key value, where RETURNING or ``cursor.lastrowid``
may not be used.   An :class:`_expression.Insert` construct that specifies the
:paramref:`~.expression.insert.inline` flag will always render default expressions
inline.

When the statement is executed with a single set of parameters (that is, it is
not an "executemany" style execution), the returned
:class:`~sqlalchemy.engine.CursorResult` will contain a collection accessible
via :meth:`_engine.CursorResult.postfetch_cols` which contains a list of all
:class:`~sqlalchemy.schema.Column` objects which had an inline-executed
default. Similarly, all parameters which were bound to the statement, including
all Python and SQL expressions which were pre-executed, are present in the
:meth:`_engine.CursorResult.last_inserted_params` or
:meth:`_engine.CursorResult.last_updated_params` collections on
:class:`~sqlalchemy.engine.CursorResult`. The
:attr:`_engine.CursorResult.inserted_primary_key` collection contains a list of primary
key values for the row inserted (a list so that single-column and
composite-column primary keys are represented in the same format).

.. _server_defaults:

Server-invoked DDL-Explicit Default Expressions
-----------------------------------------------

A variant on the SQL expression default is the :paramref:`_schema.Column.server_default`, which gets
placed in the CREATE TABLE statement during a :meth:`_schema.Table.create` operation:

.. sourcecode:: python+sql

    t = Table('test', meta,
        Column('abc', String(20), server_default='abc'),
        Column('created_at', DateTime, server_default=func.sysdate()),
        Column('index_value', Integer, server_default=text("0"))
    )

A create call for the above table will produce::

    CREATE TABLE test (
        abc varchar(20) default 'abc',
        created_at datetime default sysdate,
        index_value integer default 0
    )

The above example illustrates the two typical use cases for :paramref:`_schema.Column.server_default`,
that of the SQL function (SYSDATE in the above example) as well as a server-side constant
value (the integer "0" in the above example).  It is advisable to use the
:func:`_expression.text` construct for any literal SQL values as opposed to passing the
raw value, as SQLAlchemy does not typically perform any quoting or escaping on
these values.

Like client-generated expressions, :paramref:`_schema.Column.server_default` can accommodate
SQL expressions in general, however it is expected that these will usually be simple
functions and expressions, and not the more complex cases like an embedded SELECT.


.. _triggered_columns:

Marking Implicitly Generated Values, timestamps, and Triggered Columns
----------------------------------------------------------------------

Columns which generate a new value on INSERT or UPDATE based on other
server-side database mechanisms, such as database-specific auto-generating
behaviors such as seen with TIMESTAMP columns on some platforms, as well as
custom triggers that invoke upon INSERT or UPDATE to generate a new value,
may be called out using :class:`.FetchedValue` as a marker::

    from sqlalchemy.schema import FetchedValue

    t = Table('test', meta,
        Column('id', Integer, primary_key=True),
        Column('abc', TIMESTAMP, server_default=FetchedValue()),
        Column('def', String(20), server_onupdate=FetchedValue())
    )

The :class:`.FetchedValue` indicator does not affect the rendered DDL for the
CREATE TABLE.  Instead, it marks the column as one that will have a new value
populated by the database during the process of an INSERT or UPDATE statement,
and for supporting  databases may be used to indicate that the column should be
part of a RETURNING or OUTPUT clause for the statement.    Tools such as the
SQLAlchemy ORM then make use of this marker in order to know how to get at the
value of the column after such an operation.   In particular, the
:meth:`.ValuesBase.return_defaults` method can be used with an :class:`_expression.Insert`
or :class:`_expression.Update` construct to indicate that these values should be
returned.

For details on using :class:`.FetchedValue` with the ORM, see
:ref:`orm_server_defaults`.

.. warning:: The :paramref:`_schema.Column.server_onupdate` directive
    **does not** currently produce MySQL's
    "ON UPDATE CURRENT_TIMESTAMP()" clause.  See
    :ref:`mysql_timestamp_onupdate` for background on how to produce
    this clause.


.. seealso::

    :ref:`orm_server_defaults`

.. _defaults_sequences:

Defining Sequences
------------------

SQLAlchemy represents database sequences using the
:class:`~sqlalchemy.schema.Sequence` object, which is considered to be a
special case of "column default". It only has an effect on databases which have
explicit support for sequences, which currently includes PostgreSQL, Oracle,
MariaDB 10.3 or greater, and Firebird. The :class:`~sqlalchemy.schema.Sequence`
object is otherwise ignored.

The :class:`~sqlalchemy.schema.Sequence` may be placed on any column as a
"default" generator to be used during INSERT operations, and can also be
configured to fire off during UPDATE operations if desired. It is most
commonly used in conjunction with a single integer primary key column::

    table = Table("cartitems", meta,
        Column(
            "cart_id",
            Integer,
            Sequence('cart_id_seq', metadata=meta), primary_key=True),
        Column("description", String(40)),
        Column("createdate", DateTime())
    )

Where above, the table "cartitems" is associated with a sequence named
"cart_id_seq". When INSERT statements take place for "cartitems", and no value
is passed for the "cart_id" column, the "cart_id_seq" sequence will be used to
generate a value.   Typically, the sequence function is embedded in the
INSERT statement, which is combined with RETURNING so that the newly generated
value can be returned to the Python code::

    INSERT INTO cartitems (cart_id, description, createdate)
    VALUES (next_val(cart_id_seq), 'some description', '2015-10-15 12:00:15')
    RETURNING cart_id

When the :class:`~sqlalchemy.schema.Sequence` is associated with a
:class:`_schema.Column` as its **Python-side** default generator, the
:class:`.Sequence` will also be subject to "CREATE SEQUENCE" and "DROP
SEQUENCE" DDL when similar DDL is emitted for the owning :class:`_schema.Table`.
This is a limited scope convenience feature that does not accommodate for
inheritance of other aspects of the :class:`_schema.MetaData`, such as the default
schema.  Therefore, it is best practice that for a :class:`.Sequence` which
is local to a certain :class:`_schema.Column` / :class:`_schema.Table`, that it be
explicitly associated with the :class:`_schema.MetaData` using the
:paramref:`.Sequence.metadata` parameter.  See the section
:ref:`sequence_metadata` for more background on this.

Associating a Sequence on a SERIAL column
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PostgreSQL's SERIAL datatype is an auto-incrementing type that implies
the implicit creation of a PostgreSQL sequence when CREATE TABLE is emitted.
If a :class:`_schema.Column` specifies an explicit :class:`.Sequence` object
which also specifies a ``True`` value for the :paramref:`.Sequence.optional`
boolean flag, the :class:`.Sequence` will not take effect under PostgreSQL,
and the SERIAL datatype will proceed normally.   Instead, the :class:`.Sequence`
will only take effect when used against other sequence-supporting
databases, currently Oracle and Firebird.

Executing a Sequence Standalone
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A SEQUENCE is a first class schema object in SQL and can be used to generate
values independently in the database.   If you have a :class:`.Sequence`
object, it can be invoked with its "next value" instruction by
passing it directly to a SQL execution method::

    with my_engine.connect() as conn:
        seq = Sequence('some_sequence')
        nextid = conn.execute(seq)

In order to embed the "next value" function of a :class:`.Sequence`
inside of a SQL statement like a SELECT or INSERT, use the :meth:`.Sequence.next_value`
method, which will render at statement compilation time a SQL function that is
appropriate for the target backend::

    >>> my_seq = Sequence('some_sequence')
    >>> stmt = select(my_seq.next_value())
    >>> print(stmt.compile(dialect=postgresql.dialect()))
    SELECT nextval('some_sequence') AS next_value_1

.. _sequence_metadata:

Associating a Sequence with the MetaData
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For many years, the SQLAlchemy documentation referred to the
example of associating a :class:`.Sequence` with a table as follows::

    table = Table("cartitems", meta,
        Column("cart_id", Integer, Sequence('cart_id_seq'),
               primary_key=True),
        Column("description", String(40)),
        Column("createdate", DateTime())
    )

While the above is a prominent idiomatic pattern, it is recommended that
the :class:`.Sequence` in most cases be explicitly associated with the
:class:`_schema.MetaData`, using the :paramref:`.Sequence.metadata` parameter::

    table = Table("cartitems", meta,
        Column(
            "cart_id",
            Integer,
            Sequence('cart_id_seq', metadata=meta), primary_key=True),
        Column("description", String(40)),
        Column("createdate", DateTime())
    )

The :class:`.Sequence` object is a first class
schema construct that can exist independently of any table in a database, and
can also be shared among tables.   Therefore SQLAlchemy does not implicitly
modify the :class:`.Sequence` when it is associated with a :class:`_schema.Column`
object as either the Python-side or server-side default  generator.  While the
CREATE SEQUENCE / DROP SEQUENCE DDL is emitted for a  :class:`.Sequence`
defined as a Python side generator at the same time the table itself is subject
to CREATE or DROP, this is a convenience feature that does not imply that the
:class:`.Sequence` is fully associated with the :class:`_schema.MetaData` object.

Explicitly associating the :class:`.Sequence` with :class:`_schema.MetaData`
allows for the following behaviors:

* The :class:`.Sequence` will inherit the :paramref:`_schema.MetaData.schema`
  parameter specified to the target :class:`_schema.MetaData`, which
  affects the production of CREATE / DROP DDL, if any.

* The :meth:`.Sequence.create` and :meth:`.Sequence.drop` methods
  automatically use the engine bound to the :class:`_schema.MetaData`
  object, if any.

* The :meth:`_schema.MetaData.create_all` and :meth:`_schema.MetaData.drop_all`
  methods will emit CREATE / DROP for this :class:`.Sequence`,
  even if the :class:`.Sequence` is not associated with any
  :class:`_schema.Table` / :class:`_schema.Column` that's a member of this
  :class:`_schema.MetaData`.

Since the vast majority of cases that deal with :class:`.Sequence` expect
that :class:`.Sequence` to be fully "owned" by the associated :class:`_schema.Table`
and that options like default schema are propagated, setting the
:paramref:`.Sequence.metadata` parameter should be considered a best practice.

Associating a Sequence as the Server Side Default
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note:: The following technique is known to work only with the PostgreSQL
   database.  It does not work with Oracle.

The preceding sections illustrate how to associate a :class:`.Sequence` with a
:class:`_schema.Column` as the **Python side default generator**::

    Column(
        "cart_id", Integer, Sequence('cart_id_seq', metadata=meta),
        primary_key=True)

In the above case, the :class:`.Sequence` will automatically be subject
to CREATE SEQUENCE / DROP SEQUENCE DDL when the related :class:`_schema.Table`
is subject to CREATE / DROP.  However, the sequence will **not** be present
as the server-side default for the column when CREATE TABLE is emitted.

If we want the sequence to be used as a server-side default,
meaning it takes place even if we emit INSERT commands to the table from
the SQL command line, we can use the :paramref:`_schema.Column.server_default`
parameter in conjunction with the value-generation function of the
sequence, available from the :meth:`.Sequence.next_value` method.  Below
we illustrate the same :class:`.Sequence` being associated with the
:class:`_schema.Column` both as the Python-side default generator as well as
the server-side default generator::

    cart_id_seq = Sequence('cart_id_seq', metadata=meta)
    table = Table("cartitems", meta,
        Column(
            "cart_id", Integer, cart_id_seq,
            server_default=cart_id_seq.next_value(), primary_key=True),
        Column("description", String(40)),
        Column("createdate", DateTime())
    )

or with the ORM::

    class CartItem(Base):
        __tablename__ = 'cartitems'

        cart_id_seq = Sequence('cart_id_seq', metadata=Base.metadata)
        cart_id = Column(
            Integer, cart_id_seq,
            server_default=cart_id_seq.next_value(), primary_key=True)
        description = Column(String(40))
        createdate = Column(DateTime)

When the "CREATE TABLE" statement is emitted, on PostgreSQL it would be
emitted as::

    CREATE TABLE cartitems (
        cart_id INTEGER DEFAULT nextval('cart_id_seq') NOT NULL,
        description VARCHAR(40),
        createdate TIMESTAMP WITHOUT TIME ZONE,
        PRIMARY KEY (cart_id)
    )

Placement of the :class:`.Sequence` in both the Python-side and server-side
default generation contexts ensures that the "primary key fetch" logic
works in all cases.  Typically, sequence-enabled databases also support
RETURNING for INSERT statements, which is used automatically by SQLAlchemy
when emitting this statement.  However if RETURNING is not used for a particular
insert, then SQLAlchemy would prefer to "pre-execute" the sequence outside
of the INSERT statement itself, which only works if the sequence is
included as the Python-side default generator function.

The example also associates the :class:`.Sequence` with the enclosing
:class:`_schema.MetaData` directly, which again ensures that the :class:`.Sequence`
is fully associated with the parameters of the :class:`_schema.MetaData` collection
including the default schema, if any.

.. seealso::

    :ref:`postgresql_sequences` - in the PostgreSQL dialect documentation

    :ref:`oracle_returning` - in the Oracle dialect documentation

.. _computed_ddl:

Computed Columns (GENERATED ALWAYS AS)
--------------------------------------

.. versionadded:: 1.3.11

The :class:`.Computed` construct allows a :class:`_schema.Column` to be declared in
DDL as a "GENERATED ALWAYS AS" column, that is, one which has a value that is
computed by the database server.    The construct accepts a SQL expression
typically declared textually using a string or the :func:`_expression.text` construct, in
a similar manner as that of :class:`.CheckConstraint`.   The SQL expression is
then interpreted by the database server in order to determine the value for the
column within a row.

Example::

    from sqlalchemy import Table, Column, MetaData, Integer, Computed

    metadata = MetaData()

    square = Table(
        "square",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("side", Integer),
        Column("area", Integer, Computed("side * side")),
        Column("perimeter", Integer, Computed("4 * side")),
    )

The DDL for the ``square`` table when run on a PostgreSQL 12 backend will look
like::

    CREATE TABLE square (
        id SERIAL NOT NULL,
        side INTEGER,
        area INTEGER GENERATED ALWAYS AS (side * side) STORED,
        perimeter INTEGER GENERATED ALWAYS AS (4 * side) STORED,
        PRIMARY KEY (id)
    )

Whether the value is persisted upon INSERT and UPDATE, or if it is calculated
on fetch, is an implementation detail of the database; the former is known as
"stored" and the latter is known as "virtual".  Some database implementations
support both, but some only support one or the other.  The optional
:paramref:`.Computed.persisted` flag may be specified as ``True`` or ``False``
to indicate if the "STORED" or "VIRTUAL" keyword should be rendered in DDL,
however this will raise an error if the keyword is not supported by the target
backend; leaving it unset will use  a working default for the target backend.

The :class:`.Computed` construct is a subclass of the :class:`.FetchedValue`
object, and will set itself up as both the "server default" and "server
onupdate" generator for the target :class:`_schema.Column`, meaning it will be treated
as a default generating column when INSERT and UPDATE statements are generated,
as well as that it will be fetched as a generating column when using the ORM.
This includes that it will be part of the RETURNING clause of the database
for databases which support RETURNING and the generated values are to be
eagerly fetched.

.. note:: A :class:`_schema.Column` that is defined with the :class:`.Computed`
   construct may not store any value outside of that which the server applies
   to it;  SQLAlchemy's behavior when a value is passed for such a column
   to be written in INSERT or UPDATE is currently that the value will be
   ignored.

"GENERATED ALWAYS AS" is currently known to be supported by:

* MySQL version 5.7 and onwards

* MariaDB 10.x series and onwards

* PostgreSQL as of version 12

* Oracle - with the caveat that RETURNING does not work correctly with UPDATE
  (a warning will be emitted to this effect when the UPDATE..RETURNING that
  includes a computed column is rendered)

* Microsoft SQL Server

* SQLite as of version 3.31

* Firebird

When :class:`.Computed` is used with an unsupported backend, if the target
dialect does not support it, a :class:`.CompileError` is raised when attempting
to render the construct.  Otherwise, if the dialect supports it but the
particular database server version in use does not, then a subclass of
:class:`.DBAPIError`, usually :class:`.OperationalError`, is raised when the
DDL is emitted to the database.

.. seealso::

    :class:`.Computed`

.. _identity_ddl:

Identity Columns (GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY)
-----------------------------------------------------------------

.. versionadded:: 1.4

The :class:`.Identity` construct allows a :class:`_schema.Column` to be declared
as an identity column and rendered in DDL as "GENERATED { ALWAYS | BY DEFAULT }
AS IDENTITY".  An identity column has its value automatically generated by the
database server using an incrementing (or decrementing) sequence. The construct
shares most of its option to control the database behaviour with
:class:`.Sequence`.

Example::

    from sqlalchemy import Table, Column, MetaData, Integer, Computed

    metadata = MetaData()

    data = Table(
        "data",
        metadata,
        Column('id', Integer, Identity(start=42, cycle=True), primary_key=True),
        Column('data', String)
    )

The DDL for the ``data`` table when run on a PostgreSQL 12 backend will look
like::

    CREATE TABLE data (
        id INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 42 CYCLE) NOT NULL,
        data VARCHAR,
        PRIMARY KEY (id)
    )

The database will generate a value for the ``id`` column upon insert,
starting from ``42``, if the statement did not already contain a value for
the ``id`` column.
An identity column can also require that the database generates the value
of the column, ignoring the value passed with the statement or raising an
error, depending on the backend. To activate this mode, set the parameter
:paramref:`_schema.Identity.always` to ``True`` in the
:class:`.Identity` construct. Updating the previous
example to include this parameter will generate the following DDL::

    CREATE TABLE data (
        id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 42 CYCLE) NOT NULL,
        data VARCHAR,
        PRIMARY KEY (id)
    )

The :class:`.Identity` construct is a subclass of the :class:`.FetchedValue`
object, and will set itself up as the "server default" generator for the
target :class:`_schema.Column`, meaning it will be treated
as a default generating column when INSERT statements are generated,
as well as that it will be fetched as a generating column when using the ORM.
This includes that it will be part of the RETURNING clause of the database
for databases which support RETURNING and the generated values are to be
eagerly fetched.

The :class:`.Identity` construct is currently known to be supported by:

* PostgreSQL as of version 10.

* Oracle as of version 12. It also supports passing ``always=None`` to
  enable the default generated mode and the parameter ``on_null=True`` to
  specify "ON NULL" in conjunction with a "BY DEFAULT" identity column.

* Microsoft SQL Server. MSSQL uses a custom syntax that only supports the
  ``start`` and ``increment`` parameters, and ignores all other.

When :class:`.Identity` is used with an unsupported backend, it is ignored,
and the default SQLAlchemy logic for autoincrementing columns is used.

An error is raised when a :class:`_schema.Column` specifies both an
:class:`.Identity` and also sets :paramref:`_schema.Column.autoincrement`
to ``False``.

.. seealso::

    :class:`.Identity`


Default Objects API
-------------------

.. autoclass:: Computed
    :members:


.. autoclass:: ColumnDefault


.. autoclass:: DefaultClause


.. autoclass:: DefaultGenerator


.. autoclass:: FetchedValue


.. autoclass:: Sequence
    :members:


.. autoclass:: Identity
    :members:
