.. module:: sqlalchemy.schema

.. _metadata_defaults_toplevel:

.. _metadata_defaults:

Column Insert/Update Defaults
==============================

SQLAlchemy provides a very rich featureset regarding column level events which
take place during INSERT and UPDATE statements. Options include:

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

The :paramref:`.Column.default` and :paramref:`.Column.onupdate` keyword arguments also accept Python
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
:class:`~sqlalchemy.schema.Column` including the :paramref:`.Column.autoincrement` flag, as
well as the section on :class:`~sqlalchemy.schema.Sequence` later in this
chapter for background on standard primary key generation techniques.

To illustrate onupdate, we assign the Python ``datetime`` function ``now`` to
the :paramref:`.Column.onupdate` attribute::

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

Context-Sensitive Default Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Python functions used by :paramref:`.Column.default` and :paramref:`.Column.onupdate` may also make use of
the current statement's context in order to determine a value. The `context`
of a statement is an internal SQLAlchemy object which contains all information
about the statement being executed, including its source expression, the
parameters associated with it and the cursor. The typical use case for this
context with regards to default generation is to have access to the other
values being inserted or updated on the row. To access the context, provide a
function that accepts a single ``context`` argument::

    def mydefault(context):
        return context.current_parameters['counter'] + 12

    t = Table('mytable', meta,
        Column('counter', Integer),
        Column('counter_plus_twelve', Integer, default=mydefault, onupdate=mydefault)
    )

Above we illustrate a default function which will execute for all INSERT and
UPDATE statements where a value for ``counter_plus_twelve`` was otherwise not
provided, and the value will be that of whatever value is present in the
execution for the ``counter`` column, plus the number 12.

While the context object passed to the default function has many attributes,
the ``current_parameters`` member is a special member provided only during the
execution of a default function for the purposes of deriving defaults from its
existing values. For a single statement that is executing many sets of bind
parameters, the user-defined function is called for each set of parameters,
and ``current_parameters`` will be provided with each individual parameter set
for each execution.

SQL Expressions
---------------

The "default" and "onupdate" keywords may also be passed SQL expressions,
including select statements or direct function calls::

    t = Table("mytable", meta,
        Column('id', Integer, primary_key=True),

        # define 'create_date' to default to now()
        Column('create_date', DateTime, default=func.now()),

        # define 'key' to pull its default from the 'keyvalues' table
        Column('key', String(20), default=keyvalues.select(keyvalues.c.type='type1', limit=1)),

        # define 'last_modified' to use the current_timestamp SQL function on update
        Column('last_modified', DateTime, onupdate=func.utc_timestamp())
        )

Above, the ``create_date`` column will be populated with the result of the
``now()`` SQL function (which, depending on backend, compiles into ``NOW()``
or ``CURRENT_TIMESTAMP`` in most cases) during an INSERT statement, and the
``key`` column with the result of a SELECT subquery from another table. The
``last_modified`` column will be populated with the value of
``UTC_TIMESTAMP()``, a function specific to MySQL, when an UPDATE statement is
emitted for this table.

Note that when using ``func`` functions, unlike when using Python `datetime`
functions we *do* call the function, i.e. with parenthesis "()" - this is
because what we want in this case is the return value of the function, which
is the SQL expression construct that will be rendered into the INSERT or
UPDATE statement.

The above SQL functions are usually executed "inline" with the INSERT or
UPDATE statement being executed, meaning, a single statement is executed which
embeds the given expressions or subqueries within the VALUES or SET clause of
the statement. Although in some cases, the function is "pre-executed" in a
SELECT statement of its own beforehand. This happens when all of the following
is true:

* the column is a primary key column
* the database dialect does not support a usable ``cursor.lastrowid`` accessor
  (or equivalent); this currently includes PostgreSQL, Oracle, and Firebird, as
  well as some MySQL dialects.
* the dialect does not support the "RETURNING" clause or similar, or the
  ``implicit_returning`` flag is set to ``False`` for the dialect. Dialects
  which support RETURNING currently include Postgresql, Oracle, Firebird, and
  MS-SQL.
* the statement is a single execution, i.e. only supplies one set of
  parameters and doesn't use "executemany" behavior
* the ``inline=True`` flag is not set on the
  :class:`~sqlalchemy.sql.expression.Insert()` or
  :class:`~sqlalchemy.sql.expression.Update()` construct, and the statement has
  not defined an explicit `returning()` clause.

Whether or not the default generation clause "pre-executes" is not something
that normally needs to be considered, unless it is being addressed for
performance reasons.

When the statement is executed with a single set of parameters (that is, it is
not an "executemany" style execution), the returned
:class:`~sqlalchemy.engine.ResultProxy` will contain a collection
accessible via :meth:`.ResultProxy.postfetch_cols` which contains a list of all
:class:`~sqlalchemy.schema.Column` objects which had an inline-executed
default. Similarly, all parameters which were bound to the statement,
including all Python and SQL expressions which were pre-executed, are present
in the :meth:`.ResultProxy.last_inserted_params` or :meth:`.ResultProxy.last_updated_params` collections on
:class:`~sqlalchemy.engine.ResultProxy`. The :attr:`.ResultProxy.inserted_primary_key`
collection contains a list of primary key values for the row inserted (a list
so that single-column and composite-column primary keys are represented in the
same format).

.. _server_defaults:

Server Side Defaults
--------------------

A variant on the SQL expression default is the :paramref:`.Column.server_default`, which gets
placed in the CREATE TABLE statement during a :meth:`.Table.create` operation:

.. sourcecode:: python+sql

    t = Table('test', meta,
        Column('abc', String(20), server_default='abc'),
        Column('created_at', DateTime, server_default=text("sysdate"))
    )

A create call for the above table will produce::

    CREATE TABLE test (
        abc varchar(20) default 'abc',
        created_at datetime default sysdate
    )

The behavior of :paramref:`.Column.server_default` is similar to that of a regular SQL
default; if it's placed on a primary key column for a database which doesn't
have a way to "postfetch" the ID, and the statement is not "inlined", the SQL
expression is pre-executed; otherwise, SQLAlchemy lets the default fire off on
the database side normally.


.. _triggered_columns:

Triggered Columns
------------------

Columns with values set by a database trigger or other external process may be
called out using :class:`.FetchedValue` as a marker::

    t = Table('test', meta,
        Column('abc', String(20), server_default=FetchedValue()),
        Column('def', String(20), server_onupdate=FetchedValue())
    )

.. versionchanged:: 0.8.0b2,0.7.10
    The ``for_update`` argument on :class:`.FetchedValue` is set automatically
    when specified as the ``server_onupdate`` argument.  If using an older version,
    specify the onupdate above as ``server_onupdate=FetchedValue(for_update=True)``.

These markers do not emit a "default" clause when the table is created,
however they do set the same internal flags as a static ``server_default``
clause, providing hints to higher-level tools that a "post-fetch" of these
rows should be performed after an insert or update.

.. note::

    It's generally not appropriate to use :class:`.FetchedValue` in
    conjunction with a primary key column, particularly when using the
    ORM or any other scenario where the :attr:`.ResultProxy.inserted_primary_key`
    attribute is required.  This is becaue the "post-fetch" operation requires
    that the primary key value already be available, so that the
    row can be selected on its primary key.

    For a server-generated primary key value, all databases provide special
    accessors or other techniques in order to acquire the "last inserted
    primary key" column of a table.  These mechanisms aren't affected by the presence
    of :class:`.FetchedValue`.  For special situations where triggers are
    used to generate primary key values, and the database in use does not
    support the ``RETURNING`` clause, it may be necessary to forego the usage
    of the trigger and instead apply the SQL expression or function as a
    "pre execute" expression::

        t = Table('test', meta,
                Column('abc', MyType, default=func.generate_new_value(), primary_key=True)
        )

    Where above, when :meth:`.Table.insert` is used,
    the ``func.generate_new_value()`` expression will be pre-executed
    in the context of a scalar ``SELECT`` statement, and the new value will
    be applied to the subsequent ``INSERT``, while at the same time being
    made available to the :attr:`.ResultProxy.inserted_primary_key`
    attribute.


Defining Sequences
-------------------

SQLAlchemy represents database sequences using the
:class:`~sqlalchemy.schema.Sequence` object, which is considered to be a
special case of "column default". It only has an effect on databases which
have explicit support for sequences, which currently includes Postgresql,
Oracle, and Firebird. The :class:`~sqlalchemy.schema.Sequence` object is
otherwise ignored.

The :class:`~sqlalchemy.schema.Sequence` may be placed on any column as a
"default" generator to be used during INSERT operations, and can also be
configured to fire off during UPDATE operations if desired. It is most
commonly used in conjunction with a single integer primary key column::

    table = Table("cartitems", meta,
        Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
        Column("description", String(40)),
        Column("createdate", DateTime())
    )

Where above, the table "cartitems" is associated with a sequence named
"cart_id_seq". When INSERT statements take place for "cartitems", and no value
is passed for the "cart_id" column, the "cart_id_seq" sequence will be used to
generate a value.

When the :class:`~sqlalchemy.schema.Sequence` is associated with a table,
CREATE and DROP statements issued for that table will also issue CREATE/DROP
for the sequence object as well, thus "bundling" the sequence object with its
parent table.

The :class:`~sqlalchemy.schema.Sequence` object also implements special
functionality to accommodate Postgresql's SERIAL datatype. The SERIAL type in
PG automatically generates a sequence that is used implicitly during inserts.
This means that if a :class:`~sqlalchemy.schema.Table` object defines a
:class:`~sqlalchemy.schema.Sequence` on its primary key column so that it
works with Oracle and Firebird, the :class:`~sqlalchemy.schema.Sequence` would
get in the way of the "implicit" sequence that PG would normally use. For this
use case, add the flag ``optional=True`` to the
:class:`~sqlalchemy.schema.Sequence` object - this indicates that the
:class:`~sqlalchemy.schema.Sequence` should only be used if the database
provides no other option for generating primary key identifiers.

The :class:`~sqlalchemy.schema.Sequence` object also has the ability to be
executed standalone like a SQL expression, which has the effect of calling its
"next value" function::

    seq = Sequence('some_sequence')
    nextid = connection.execute(seq)

Associating a Sequence as the Server Side Default
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When we associate a :class:`.Sequence` with a :class:`.Column` as above,
this association is an **in-Python only** association.    The CREATE TABLE
that would be generated for our :class:`.Table` would not refer to this
sequence.  If we want the sequence to be used as a server-side default,
meaning it takes place even if we emit INSERT commands to the table from
the SQL commandline, we can use the :paramref:`.Column.server_default`
parameter in conjunction with the value-generation function of the
sequence, available from the :meth:`.Sequence.next_value` method::

    cart_id_seq = Sequence('cart_id_seq')
    table = Table("cartitems", meta,
        Column(
            "cart_id", Integer, cart_id_seq,
            server_default=cart_id_seq.next_value(), primary_key=True),
        Column("description", String(40)),
        Column("createdate", DateTime())
    )

The above metadata will generate a CREATE TABLE statement on Postgresql as::

    CREATE TABLE cartitems (
        cart_id INTEGER DEFAULT nextval('cart_id_seq') NOT NULL,
        description VARCHAR(40),
        createdate TIMESTAMP WITHOUT TIME ZONE,
        PRIMARY KEY (cart_id)
    )

We place the :class:`.Sequence` also as a Python-side default above, that
is, it is mentioned twice in the :class:`.Column` definition.   Depending
on the backend in use, this may not be strictly necessary, for example
on the Postgresql backend the Core will use ``RETURNING`` to access the
newly generated primary key value in any case.   However, for the best
compatibility, :class:`.Sequence` was originally intended to be a Python-side
directive first and foremost so it's probably a good idea to specify it
in this way as well.


Default Objects API
-------------------

.. autoclass:: ColumnDefault


.. autoclass:: DefaultClause


.. autoclass:: DefaultGenerator


.. autoclass:: FetchedValue


.. autoclass:: PassiveDefault


.. autoclass:: Sequence
    :members:
