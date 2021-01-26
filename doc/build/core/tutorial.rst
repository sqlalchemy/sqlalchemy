.. _sqlexpression_toplevel:

==========================================
SQL Expression Language Tutorial (1.x API)
==========================================

.. admonition:: About this document

    This tutorial covers the well known SQLAlchemy Core API
    that has been in use for many years.  As of SQLAlchemy 1.4, there are two
    distinct styles of Core use known as :term:`1.x style` and :term:`2.0
    style`, the latter of which makes some adjustments mostly in the area
    of how transactions are controlled as well as narrows down the patterns
    for how SQL statement constructs are executed.

    The plan is that in SQLAlchemy 2.0, those elements of 1.x style
    Core use will be removed, after a deprecation phase that continues
    throughout the 1.4 series.   For ORM use, some elements of 1.x style
    will still be available; see the :ref:`migration_20_toplevel` document
    for a complete overview.

    The tutorial here is applicable to users who want to learn how SQLAlchemy
    Core has been used for many years, particularly those users working with
    existing applications or related learning material that is in 1.x style.

    For an introduction to SQLAlchemy Core from the new 1.4/2.0 perspective,
    see :ref:`unified_tutorial`.

    .. seealso::

        :ref:`migration_20_toplevel`

        :ref:`unified_tutorial`


The SQLAlchemy Expression Language presents a system of representing
relational database structures and expressions using Python constructs. These
constructs are modeled to resemble those of the underlying database as closely
as possible, while providing a modicum of abstraction of the various
implementation differences between database backends. While the constructs
attempt to represent equivalent concepts between backends with consistent
structures, they do not conceal useful concepts that are unique to particular
subsets of backends. The Expression Language therefore presents a method of
writing backend-neutral SQL expressions, but does not attempt to enforce that
expressions are backend-neutral.

The Expression Language is in contrast to the Object Relational Mapper, which
is a distinct API that builds on top of the Expression Language. Whereas the
ORM, introduced in :ref:`ormtutorial_toplevel`, presents a high level and
abstracted pattern of usage, which itself is an example of applied usage of
the Expression Language, the Expression Language presents a system of
representing the primitive constructs of the relational database directly
without opinion.

While there is overlap among the usage patterns of the ORM and the Expression
Language, the similarities are more superficial than they may at first appear.
One approaches the structure and content of data from the perspective of a
user-defined `domain model
<http://en.wikipedia.org/wiki/Domain_model>`_ which is transparently
persisted and refreshed from its underlying storage model. The other
approaches it from the perspective of literal schema and SQL expression
representations which are explicitly composed into messages consumed
individually by the database.

A successful application may be constructed using the Expression Language
exclusively, though the application will need to define its own system of
translating application concepts into individual database messages and from
individual database result sets. Alternatively, an application constructed
with the ORM may, in advanced scenarios, make occasional usage of the
Expression Language directly in certain areas where specific database
interactions are required.

The following tutorial is in doctest format, meaning each ``>>>`` line
represents something you can type at a Python command prompt, and the
following text represents the expected return value. The tutorial has no
prerequisites.

Version Check
=============


A quick check to verify that we are on at least **version 1.4** of SQLAlchemy:

.. sourcecode:: pycon+sql

    >>> import sqlalchemy
    >>> sqlalchemy.__version__  # doctest: +SKIP
    1.4.0

Connecting
==========

For this tutorial we will use an in-memory-only SQLite database. This is an
easy way to test things without needing to have an actual database defined
anywhere. To connect we use :func:`~sqlalchemy.create_engine`:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('sqlite:///:memory:', echo=True)

The ``echo`` flag is a shortcut to setting up SQLAlchemy logging, which is
accomplished via Python's standard ``logging`` module. With it enabled, we'll
see all the generated SQL produced. If you are working through this tutorial
and want less output generated, set it to ``False``. This tutorial will format
the SQL behind a popup window so it doesn't get in our way; just click the
"SQL" links to see what's being generated.

The return value of :func:`_sa.create_engine` is an instance of
:class:`_engine.Engine`, and it represents the core interface to the
database, adapted through a :term:`dialect` that handles the details
of the database and :term:`DBAPI` in use.  In this case the SQLite
dialect will interpret instructions to the Python built-in ``sqlite3``
module.

.. sidebar:: Lazy Connecting

    The :class:`_engine.Engine`, when first returned by :func:`_sa.create_engine`,
    has not actually tried to connect to the database yet; that happens
    only the first time it is asked to perform a task against the database.

The first time a method like :meth:`_engine.Engine.execute` or :meth:`_engine.Engine.connect`
is called, the :class:`_engine.Engine` establishes a real :term:`DBAPI` connection to the
database, which is then used to emit the SQL.

.. seealso::

    :ref:`database_urls` - includes examples of :func:`_sa.create_engine`
    connecting to several kinds of databases with links to more information.

Define and Create Tables
========================

The SQL Expression Language constructs its expressions in most cases against
table columns. In SQLAlchemy, a column is most often represented by an object
called :class:`~sqlalchemy.schema.Column`, and in all cases a
:class:`~sqlalchemy.schema.Column` is associated with a
:class:`~sqlalchemy.schema.Table`. A collection of
:class:`~sqlalchemy.schema.Table` objects and their associated child objects
is referred to as **database metadata**. In this tutorial we will explicitly
lay out several :class:`~sqlalchemy.schema.Table` objects, but note that SA
can also "import" whole sets of :class:`~sqlalchemy.schema.Table` objects
automatically from an existing database (this process is called **table
reflection**).

We define our tables all within a catalog called
:class:`~sqlalchemy.schema.MetaData`, using the
:class:`~sqlalchemy.schema.Table` construct, which resembles regular SQL
CREATE TABLE statements. We'll make two tables, one of which represents
"users" in an application, and another which represents zero or more "email
addresses" for each row in the "users" table:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
    >>> metadata = MetaData()
    >>> users = Table('users', metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String),
    ...     Column('fullname', String),
    ... )

    >>> addresses = Table('addresses', metadata,
    ...   Column('id', Integer, primary_key=True),
    ...   Column('user_id', None, ForeignKey('users.id')),
    ...   Column('email_address', String, nullable=False)
    ...  )

All about how to define :class:`~sqlalchemy.schema.Table` objects, as well as
how to create them from an existing database automatically, is described in
:ref:`metadata_toplevel`.

Next, to tell the :class:`~sqlalchemy.schema.MetaData` we'd actually like to
create our selection of tables for real inside the SQLite database, we use
:func:`~sqlalchemy.schema.MetaData.create_all`, passing it the ``engine``
instance which points to our database. This will check for the presence of
each table first before creating, so it's safe to call multiple times:

.. sourcecode:: pycon+sql

    {sql}>>> metadata.create_all(engine)
    BEGIN...
    CREATE TABLE users (
        id INTEGER NOT NULL,
        name VARCHAR,
        fullname VARCHAR,
        PRIMARY KEY (id)
    )
    [...] ()
    CREATE TABLE addresses (
        id INTEGER NOT NULL,
        user_id INTEGER,
        email_address VARCHAR NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY(user_id) REFERENCES users (id)
    )
    [...] ()
    COMMIT

.. note::

    Users familiar with the syntax of CREATE TABLE may notice that the
    VARCHAR columns were generated without a length; on SQLite and PostgreSQL,
    this is a valid datatype, but on others, it's not allowed. So if running
    this tutorial on one of those databases, and you wish to use SQLAlchemy to
    issue CREATE TABLE, a "length" may be provided to the :class:`~sqlalchemy.types.String` type as
    below::

        Column('name', String(50))

    The length field on :class:`~sqlalchemy.types.String`, as well as similar precision/scale fields
    available on :class:`~sqlalchemy.types.Integer`, :class:`~sqlalchemy.types.Numeric`, etc. are not referenced by
    SQLAlchemy other than when creating tables.

    Additionally, Firebird and Oracle require sequences to generate new
    primary key identifiers, and SQLAlchemy doesn't generate or assume these
    without being instructed. For that, you use the :class:`~sqlalchemy.schema.Sequence` construct::

        from sqlalchemy import Sequence
        Column('id', Integer, Sequence('user_id_seq'), primary_key=True)

    A full, foolproof :class:`~sqlalchemy.schema.Table` is therefore::

        users = Table('users', metadata,
           Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
           Column('name', String(50)),
           Column('fullname', String(50)),
           Column('nickname', String(50))
        )

    We include this more verbose :class:`_schema.Table` construct separately
    to highlight the difference between a minimal construct geared primarily
    towards in-Python usage only, versus one that will be used to emit CREATE
    TABLE statements on a particular set of backends with more stringent
    requirements.

.. _coretutorial_insert_expressions:

Insert Expressions
==================

The first SQL expression we'll create is the
:class:`~sqlalchemy.sql.expression.Insert` construct, which represents an
INSERT statement. This is typically created relative to its target table::

    >>> ins = users.insert()

To see a sample of the SQL this construct produces, use the ``str()``
function::

    >>> str(ins)
    'INSERT INTO users (id, name, fullname) VALUES (:id, :name, :fullname)'

Notice above that the INSERT statement names every column in the ``users``
table. This can be limited by using the ``values()`` method, which establishes
the VALUES clause of the INSERT explicitly::

    >>> ins = users.insert().values(name='jack', fullname='Jack Jones')
    >>> str(ins)
    'INSERT INTO users (name, fullname) VALUES (:name, :fullname)'

Above, while the ``values`` method limited the VALUES clause to just two
columns, the actual data we placed in ``values`` didn't get rendered into the
string; instead we got named bind parameters. As it turns out, our data *is*
stored within our :class:`~sqlalchemy.sql.expression.Insert` construct, but it
typically only comes out when the statement is actually executed; since the
data consists of literal values, SQLAlchemy automatically generates bind
parameters for them. We can peek at this data for now by looking at the
compiled form of the statement::

    >>> ins.compile().params  # doctest: +SKIP
    {'fullname': 'Jack Jones', 'name': 'jack'}

Executing
=========

The interesting part of an :class:`~sqlalchemy.sql.expression.Insert` is
executing it.  This is performed using a database connection, which  is
represented by the :class:`_engine.Connection` object.  To acquire a
connection, we will use the :meth:`_engine.Engine.connect` method::

    >>> conn = engine.connect()
    >>> conn
    <sqlalchemy.engine.base.Connection object at 0x...>

The :class:`~sqlalchemy.engine.Connection` object represents an actively
checked out DBAPI connection resource. Lets feed it our
:class:`~sqlalchemy.sql.expression.Insert` object and see what happens:

.. sourcecode:: pycon+sql

    >>> result = conn.execute(ins)
    {opensql}INSERT INTO users (name, fullname) VALUES (?, ?)
    [...] ('jack', 'Jack Jones')
    COMMIT

So the INSERT statement was now issued to the database. Although we got
positional "qmark" bind parameters instead of "named" bind parameters in the
output. How come ? Because when executed, the
:class:`~sqlalchemy.engine.Connection` used the SQLite **dialect** to
help generate the statement; when we use the ``str()`` function, the statement
isn't aware of this dialect, and falls back onto a default which uses named
parameters. We can view this manually as follows:

.. sourcecode:: pycon+sql

    >>> ins.bind = engine
    >>> str(ins)
    'INSERT INTO users (name, fullname) VALUES (?, ?)'

What about the ``result`` variable we got when we called ``execute()`` ? As
the SQLAlchemy :class:`~sqlalchemy.engine.Connection` object references a
DBAPI connection, the result, known as a
:class:`~sqlalchemy.engine.CursorResult` object, is analogous to the DBAPI
cursor object. In the case of an INSERT, we can get important information from
it, such as the primary key values which were generated from our statement
using :attr:`_engine.CursorResult.inserted_primary_key`:

.. sourcecode:: pycon+sql

    >>> result.inserted_primary_key
    (1,)

The value of ``1`` was automatically generated by SQLite, but only because we
did not specify the ``id`` column in our
:class:`~sqlalchemy.sql.expression.Insert` statement; otherwise, our explicit
value would have been used. In either case, SQLAlchemy always knows how to get
at a newly generated primary key value, even though the method of generating
them is different across different databases; each database's
:class:`~sqlalchemy.engine.interfaces.Dialect` knows the specific steps needed to
determine the correct value (or values; note that
:attr:`_engine.CursorResult.inserted_primary_key`
returns a list so that it supports composite primary keys).    Methods here
range from using ``cursor.lastrowid``, to selecting from a database-specific
function, to using ``INSERT..RETURNING`` syntax; this all occurs transparently.

.. _execute_multiple:

Executing Multiple Statements
=============================

Our insert example above was intentionally a little drawn out to show some
various behaviors of expression language constructs. In the usual case, an
:class:`~sqlalchemy.sql.expression.Insert` statement is usually compiled
against the parameters sent to the ``execute()`` method on
:class:`~sqlalchemy.engine.Connection`, so that there's no need to use
the ``values`` keyword with :class:`~sqlalchemy.sql.expression.Insert`. Lets
create a generic :class:`~sqlalchemy.sql.expression.Insert` statement again
and use it in the "normal" way:

.. sourcecode:: pycon+sql

    >>> ins = users.insert()
    >>> conn.execute(ins, {"id": 2, "name":"wendy", "fullname": "Wendy Williams"})
    {opensql}INSERT INTO users (id, name, fullname) VALUES (?, ?, ?)
    [...] (2, 'wendy', 'Wendy Williams')
    COMMIT
    {stop}<sqlalchemy.engine.cursor.LegacyCursorResult object at 0x...>

Above, because we specified all three columns in the ``execute()`` method,
the compiled :class:`_expression.Insert` included all three
columns. The :class:`_expression.Insert` statement is compiled
at execution time based on the parameters we specified; if we specified fewer
parameters, the :class:`_expression.Insert` would have fewer
entries in its VALUES clause.

To issue many inserts using DBAPI's ``executemany()`` method, we can send in a
list of dictionaries each containing a distinct set of parameters to be
inserted, as we do here to add some email addresses:

.. sourcecode:: pycon+sql

    >>> conn.execute(addresses.insert(), [
    ...    {'user_id': 1, 'email_address' : 'jack@yahoo.com'},
    ...    {'user_id': 1, 'email_address' : 'jack@msn.com'},
    ...    {'user_id': 2, 'email_address' : 'www@www.org'},
    ...    {'user_id': 2, 'email_address' : 'wendy@aol.com'},
    ... ])
    {opensql}INSERT INTO addresses (user_id, email_address) VALUES (?, ?)
    [...] ((1, 'jack@yahoo.com'), (1, 'jack@msn.com'), (2, 'www@www.org'), (2, 'wendy@aol.com'))
    COMMIT
    {stop}<sqlalchemy.engine.cursor.LegacyCursorResult object at 0x...>

Above, we again relied upon SQLite's automatic generation of primary key
identifiers for each ``addresses`` row.

When executing multiple sets of parameters, each dictionary must have the
**same** set of keys; i.e. you cant have fewer keys in some dictionaries than
others. This is because the :class:`~sqlalchemy.sql.expression.Insert`
statement is compiled against the **first** dictionary in the list, and it's
assumed that all subsequent argument dictionaries are compatible with that
statement.

The "executemany" style of invocation is available for each of the
:func:`_expression.insert`, :func:`_expression.update` and :func:`_expression.delete` constructs.


.. _coretutorial_selecting:

Selecting
=========

We began with inserts just so that our test database had some data in it. The
more interesting part of the data is selecting it! We'll cover UPDATE and
DELETE statements later. The primary construct used to generate SELECT
statements is the :func:`_expression.select` function:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import select
    >>> s = select(users)
    >>> result = conn.execute(s)
    {opensql}SELECT users.id, users.name, users.fullname
    FROM users
    [...] ()

Above, we issued a basic :func:`_expression.select` call, placing the ``users`` table
within the COLUMNS clause of the select, and then executing. SQLAlchemy
expanded the ``users`` table into the set of each of its columns, and also
generated a FROM clause for us.

.. versionchanged:: 1.4  The :func:`_expression.select` construct now accepts
   column arguments positionally, as ``select(*args)``.  The previous style
   of ``select()`` accepting a list of column elements is now deprecated.
   See :ref:`change_5284`.

The result returned is again a
:class:`~sqlalchemy.engine.CursorResult` object, which acts much like a
DBAPI cursor, including methods such as
:func:`~sqlalchemy.engine.CursorResult.fetchone` and
:func:`~sqlalchemy.engine.CursorResult.fetchall`.    These methods return
row objects, which are provided via the :class:`.Row` class.  The
result object can be iterated directly in order to provide an iterator
of :class:`.Row` objects:

.. sourcecode:: pycon+sql

    >>> for row in result:
    ...     print(row)
    (1, u'jack', u'Jack Jones')
    (2, u'wendy', u'Wendy Williams')

Above, we see that printing each :class:`.Row` produces a simple
tuple-like result.  The most canonical way in Python to access the values
of these tuples as rows are fetched is through tuple assignment:

.. sourcecode:: pycon+sql

    {sql}>>> result = conn.execute(s)
    SELECT users.id, users.name, users.fullname
    FROM users
    [...] ()

    {stop}>>> for id, name, fullname in result:
    ...     print("name:", name, "; fullname: ", fullname)
    name: jack ; fullname:  Jack Jones
    name: wendy ; fullname:  Wendy Williams

The :class:`.Row` object actually behaves like a Python named tuple, so
we may also access these attributes from the row itself using attribute
access:

.. sourcecode:: pycon+sql

    {sql}>>> result = conn.execute(s)
    SELECT users.id, users.name, users.fullname
    FROM users
    [...] ()

    {stop}>>> for row in result:
    ...     print("name:", row.name, "; fullname: ", row.fullname)
    name: jack ; fullname:  Jack Jones
    name: wendy ; fullname:  Wendy Williams

To access columns via name using strings, either when the column name is
programmatically generated, or contains non-ascii characters, the
:attr:`.Row._mapping` view may be used that provides dictionary-like access:

.. sourcecode:: pycon+sql

    {sql}>>> result = conn.execute(s)
    SELECT users.id, users.name, users.fullname
    FROM users
    [...] ()

    {stop}>>> row = result.fetchone()
    >>> print("name:", row._mapping['name'], "; fullname:", row._mapping['fullname'])
    name: jack ; fullname: Jack Jones

.. deprecated:: 1.4

    In versions of SQLAlchemy prior to 1.4, the above access using
    :attr:`.Row._mapping` would proceed against the row object itself, that
    is::

        row = result.fetchone()
        name, fullname = row["name"], row["fullname"]

    This pattern is now deprecated and will be removed in SQLAlchemy 2.0, so
    that the :class:`.Row` object may now behave fully like a Python named
    tuple.

.. versionchanged:: 1.4  Added :attr:`.Row._mapping` which provides for
   dictionary-like access to a :class:`.Row`, superseding the use of string/
   column keys against the :class:`.Row` object directly.

As the :class:`.Row` is a tuple, sequence (i.e. integer or slice) access
may be used as well:

.. sourcecode:: pycon+sql

    >>> row = result.fetchone()
    >>> print("name:", row[1], "; fullname:", row[2])
    name: wendy ; fullname: Wendy Williams

A more specialized method of column access is to use the SQL construct that
directly corresponds to a particular column as the mapping key; in this
example, it means we would use the  :class:`_schema.Column` objects selected in our
SELECT directly as keys in conjunction with the :attr:`.Row._mapping`
collection:

.. sourcecode:: pycon+sql

    {sql}>>> for row in conn.execute(s):
    ...     print("name:", row._mapping[users.c.name], "; fullname:", row._mapping[users.c.fullname])
    SELECT users.id, users.name, users.fullname
    FROM users
    [...] ()
    {stop}name: jack ; fullname: Jack Jones
    name: wendy ; fullname: Wendy Williams

.. sidebar:: Results and Rows are changing

    The :class:`.Row` class was known as ``RowProxy`` and the
    :class:`_engine.CursorResult` class was known as ``ResultProxy``,  for all
    SQLAlchemy versions through 1.3.  In 1.4, the objects returned by
    :class:`_engine.CursorResult` are actually a subclass of :class:`.Row` known as
    :class:`.LegacyRow`.   See :ref:`change_4710_core` for background on this
    change.

The :class:`_engine.CursorResult` object features "auto-close" behavior that closes the
underlying DBAPI ``cursor`` object when all pending result rows have been
fetched.   If a :class:`_engine.CursorResult` is to be discarded before such an
autoclose has occurred, it can be explicitly closed using the
:meth:`_engine.CursorResult.close` method:

.. sourcecode:: pycon+sql

    >>> result.close()

Selecting Specific Columns
===========================

If we'd like to more carefully control the columns which are placed in the
COLUMNS clause of the select, we reference individual
:class:`~sqlalchemy.schema.Column` objects from our
:class:`~sqlalchemy.schema.Table`. These are available as named attributes off
the ``c`` attribute of the :class:`~sqlalchemy.schema.Table` object:

.. sourcecode:: pycon+sql

    >>> s = select(users.c.name, users.c.fullname)
    {sql}>>> result = conn.execute(s)
    SELECT users.name, users.fullname
    FROM users
    [...] ()
    {stop}>>> for row in result:
    ...     print(row)
    (u'jack', u'Jack Jones')
    (u'wendy', u'Wendy Williams')

Lets observe something interesting about the FROM clause. Whereas the
generated statement contains two distinct sections, a "SELECT columns" part
and a "FROM table" part, our :func:`_expression.select` construct only has a list
containing columns. How does this work ? Let's try putting *two* tables into
our :func:`_expression.select` statement:

.. sourcecode:: pycon+sql

    {sql}>>> for row in conn.execute(select(users, addresses)):
    ...     print(row)
    SELECT users.id, users.name, users.fullname, addresses.id AS id_1, addresses.user_id, addresses.email_address
    FROM users, addresses
    [...] ()
    {stop}(1, u'jack', u'Jack Jones', 1, 1, u'jack@yahoo.com')
    (1, u'jack', u'Jack Jones', 2, 1, u'jack@msn.com')
    (1, u'jack', u'Jack Jones', 3, 2, u'www@www.org')
    (1, u'jack', u'Jack Jones', 4, 2, u'wendy@aol.com')
    (2, u'wendy', u'Wendy Williams', 1, 1, u'jack@yahoo.com')
    (2, u'wendy', u'Wendy Williams', 2, 1, u'jack@msn.com')
    (2, u'wendy', u'Wendy Williams', 3, 2, u'www@www.org')
    (2, u'wendy', u'Wendy Williams', 4, 2, u'wendy@aol.com')

It placed **both** tables into the FROM clause. But also, it made a real mess.
Those who are familiar with SQL joins know that this is a **Cartesian
product**; each row from the ``users`` table is produced against each row from
the ``addresses`` table. So to put some sanity into this statement, we need a
WHERE clause.  We do that using :meth:`_expression.Select.where`:

.. sourcecode:: pycon+sql

    >>> s = select(users, addresses).where(users.c.id == addresses.c.user_id)
    {sql}>>> for row in conn.execute(s):
    ...     print(row)
    SELECT users.id, users.name, users.fullname, addresses.id AS id_1,
       addresses.user_id, addresses.email_address
    FROM users, addresses
    WHERE users.id = addresses.user_id
    [...] ()
    {stop}(1, u'jack', u'Jack Jones', 1, 1, u'jack@yahoo.com')
    (1, u'jack', u'Jack Jones', 2, 1, u'jack@msn.com')
    (2, u'wendy', u'Wendy Williams', 3, 2, u'www@www.org')
    (2, u'wendy', u'Wendy Williams', 4, 2, u'wendy@aol.com')

So that looks a lot better, we added an expression to our :func:`_expression.select`
which had the effect of adding ``WHERE users.id = addresses.user_id`` to our
statement, and our results were managed down so that the join of ``users`` and
``addresses`` rows made sense. But let's look at that expression? It's using
just a Python equality operator between two different
:class:`~sqlalchemy.schema.Column` objects. It should be clear that something
is up. Saying ``1 == 1`` produces ``True``, and ``1 == 2`` produces ``False``, not
a WHERE clause. So lets see exactly what that expression is doing:

.. sourcecode:: pycon+sql

    >>> users.c.id == addresses.c.user_id
    <sqlalchemy.sql.elements.BinaryExpression object at 0x...>

Wow, surprise ! This is neither a ``True`` nor a ``False``. Well what is it ?

.. sourcecode:: pycon+sql

    >>> str(users.c.id == addresses.c.user_id)
    'users.id = addresses.user_id'

As you can see, the ``==`` operator is producing an object that is very much
like the :class:`_expression.Insert` and :func:`_expression.select`
objects we've made so far, thanks to Python's ``__eq__()`` builtin; you call
``str()`` on it and it produces SQL. By now, one can see that everything we
are working with is ultimately the same type of object. SQLAlchemy terms the
base class of all of these expressions as :class:`_expression.ColumnElement`.

Operators
=========

Since we've stumbled upon SQLAlchemy's operator paradigm, let's go through
some of its capabilities. We've seen how to equate two columns to each other:

.. sourcecode:: pycon+sql

    >>> print(users.c.id == addresses.c.user_id)
    users.id = addresses.user_id

If we use a literal value (a literal meaning, not a SQLAlchemy clause object),
we get a bind parameter:

.. sourcecode:: pycon+sql

    >>> print(users.c.id == 7)
    users.id = :id_1

The ``7`` literal is embedded the resulting
:class:`_expression.ColumnElement`; we can use the same trick
we did with the :class:`~sqlalchemy.sql.expression.Insert` object to see it:

.. sourcecode:: pycon+sql

    >>> (users.c.id == 7).compile().params
    {u'id_1': 7}

Most Python operators, as it turns out, produce a SQL expression here, like
equals, not equals, etc.:

.. sourcecode:: pycon+sql

    >>> print(users.c.id != 7)
    users.id != :id_1

    >>> # None converts to IS NULL
    >>> print(users.c.name == None)
    users.name IS NULL

    >>> # reverse works too
    >>> print('fred' > users.c.name)
    users.name < :name_1

If we add two integer columns together, we get an addition expression:

.. sourcecode:: pycon+sql

    >>> print(users.c.id + addresses.c.id)
    users.id + addresses.id

Interestingly, the type of the :class:`~sqlalchemy.schema.Column` is important!
If we use ``+`` with two string based columns (recall we put types like
:class:`~sqlalchemy.types.Integer` and :class:`~sqlalchemy.types.String` on
our :class:`~sqlalchemy.schema.Column` objects at the beginning), we get
something different:

.. sourcecode:: pycon+sql

    >>> print(users.c.name + users.c.fullname)
    users.name || users.fullname

Where ``||`` is the string concatenation operator used on most databases. But
not all of them. MySQL users, fear not:

.. sourcecode:: pycon+sql

    >>> print((users.c.name + users.c.fullname).
    ...      compile(bind=create_engine('mysql://'))) # doctest: +SKIP
    concat(users.name, users.fullname)

The above illustrates the SQL that's generated for an
:class:`~sqlalchemy.engine.Engine` that's connected to a MySQL database;
the ``||`` operator now compiles as MySQL's ``concat()`` function.

If you have come across an operator which really isn't available, you can
always use the :meth:`.Operators.op` method; this generates whatever operator you need:

.. sourcecode:: pycon+sql

    >>> print(users.c.name.op('tiddlywinks')('foo'))
    users.name tiddlywinks :name_1

This function can also be used to make bitwise operators explicit. For example::

    somecolumn.op('&')(0xff)

is a bitwise AND of the value in ``somecolumn``.

When using :meth:`.Operators.op`, the return type of the expression may be important,
especially when the operator is used in an expression that will be sent as a result
column.   For this case, be sure to make the type explicit, if not what's
normally expected, using :func:`.type_coerce`::

    from sqlalchemy import type_coerce
    expr = type_coerce(somecolumn.op('-%>')('foo'), MySpecialType())
    stmt = select(expr)


For boolean operators, use the :meth:`.Operators.bool_op` method, which
will ensure that the return type of the expression is handled as boolean::

    somecolumn.bool_op('-->')('some value')


Commonly Used Operators
-------------------------


Here's a rundown of some of the most common operators used in both the
Core expression language as well as in the ORM.  Here we see expressions
that are most commonly present when using the :meth:`_sql.Select.where` method,
but can be used in other scenarios as well.

A listing of all the column-level operations common to all column-like
objects is at :class:`.ColumnOperators`.


* :meth:`equals <.ColumnOperators.__eq__>`::

    statement.where(users.c.name == 'ed')

* :meth:`not equals <.ColumnOperators.__ne__>`::

    statement.where(users.c.name != 'ed')

* :meth:`LIKE <.ColumnOperators.like>`::

    statement.where(users.c.name.like('%ed%'))

 .. note:: :meth:`.ColumnOperators.like` renders the LIKE operator, which
    is case insensitive on some backends, and case sensitive
    on others.  For guaranteed case-insensitive comparisons, use
    :meth:`.ColumnOperators.ilike`.

* :meth:`ILIKE <.ColumnOperators.ilike>` (case-insensitive LIKE)::

    statement.where(users.c.name.ilike('%ed%'))

 .. note:: most backends don't support ILIKE directly.  For those,
    the :meth:`.ColumnOperators.ilike` operator renders an expression
    combining LIKE with the LOWER SQL function applied to each operand.

* :meth:`IN <.ColumnOperators.in_>`::

    statement.where(users.c..name.in_(['ed', 'wendy', 'jack']))

    # works with Select objects too:
    statement.where.filter(users.c.name.in_(
        select(users.c.name).where(users.c.name.like('%ed%'))
    ))

    # use tuple_() for composite (multi-column) queries
    from sqlalchemy import tuple_
    statement.where(
        tuple_(users.c.name, users.c.nickname).\
        in_([('ed', 'edsnickname'), ('wendy', 'windy')])
    )

* :meth:`NOT IN <.ColumnOperators.not_in>`::

    statement.where(~users.c.name.in_(['ed', 'wendy', 'jack']))

* :meth:`IS NULL <.ColumnOperators.is_>`::

    statement.where(users.c. == None)

    # alternatively, if pep8/linters are a concern
    statement.where(users.c.name.is_(None))

* :meth:`IS NOT NULL <.ColumnOperators.is_not>`::

    statement.where(users.c.name != None)

    # alternatively, if pep8/linters are a concern
    statement.where(users.c.name.is_not(None))

* :func:`AND <.sql.expression.and_>`::

    # use and_()
    from sqlalchemy import and_
    statement.where(and_(users.c.name == 'ed', users.c.fullname == 'Ed Jones'))

    # or send multiple expressions to .where()
    statement.where(users.c.name == 'ed', users.c.fullname == 'Ed Jones')

    # or chain multiple where() calls
    statement.where(users.c.name == 'ed').where(users.c.fullname == 'Ed Jones')

 .. note::  Make sure you use :func:`.and_` and **not** the
    Python ``and`` operator!

* :func:`OR <.sql.expression.or_>`::

    from sqlalchemy import or_
    statement.where(or_(users.c.name == 'ed', users.c.name == 'wendy'))

 .. note::  Make sure you use :func:`.or_` and **not** the
    Python ``or`` operator!

* :meth:`MATCH <.ColumnOperators.match>`::

    statement.where(users.c.name.match('wendy'))

 .. note::

    :meth:`~.ColumnOperators.match` uses a database-specific ``MATCH``
    or ``CONTAINS`` function; its behavior will vary by backend and is not
    available on some backends such as SQLite.


Operator Customization
----------------------

While :meth:`.Operators.op` is handy to get at a custom operator in a hurry,
the Core supports fundamental customization and extension of the operator system at
the type level.   The behavior of existing operators can be modified on a per-type
basis, and new operations can be defined which become available for all column
expressions that are part of that particular type.  See the section :ref:`types_operators`
for a description.



Conjunctions
============


We'd like to show off some of our operators inside of :func:`_expression.select`
constructs. But we need to lump them together a little more, so let's first
introduce some conjunctions. Conjunctions are those little words like AND and
OR that put things together. We'll also hit upon NOT. :func:`.and_`, :func:`.or_`,
and :func:`.not_` can work
from the corresponding functions SQLAlchemy provides (notice we also throw in
a :meth:`~.ColumnOperators.like`):

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import and_, or_, not_
    >>> print(and_(
    ...         users.c.name.like('j%'),
    ...         users.c.id == addresses.c.user_id,
    ...         or_(
    ...              addresses.c.email_address == 'wendy@aol.com',
    ...              addresses.c.email_address == 'jack@yahoo.com'
    ...         ),
    ...         not_(users.c.id > 5)
    ...       )
    ...  )
    users.name LIKE :name_1 AND users.id = addresses.user_id AND
    (addresses.email_address = :email_address_1
       OR addresses.email_address = :email_address_2)
    AND users.id <= :id_1

And you can also use the re-jiggered bitwise AND, OR and NOT operators,
although because of Python operator precedence you have to watch your
parenthesis:

.. sourcecode:: pycon+sql

    >>> print(users.c.name.like('j%') & (users.c.id == addresses.c.user_id) &
    ...     (
    ...       (addresses.c.email_address == 'wendy@aol.com') | \
    ...       (addresses.c.email_address == 'jack@yahoo.com')
    ...     ) \
    ...     & ~(users.c.id>5)
    ... )
    users.name LIKE :name_1 AND users.id = addresses.user_id AND
    (addresses.email_address = :email_address_1
        OR addresses.email_address = :email_address_2)
    AND users.id <= :id_1

So with all of this vocabulary, let's select all users who have an email
address at AOL or MSN, whose name starts with a letter between "m" and "z",
and we'll also generate a column containing their full name combined with
their email address. We will add two new constructs to this statement,
:meth:`~.ColumnOperators.between` and :meth:`_expression.ColumnElement.label`.
:meth:`~.ColumnOperators.between` produces a BETWEEN clause, and
:meth:`_expression.ColumnElement.label` is used in a column expression to produce labels using the ``AS``
keyword; it's recommended when selecting from expressions that otherwise would
not have a name:

.. sourcecode:: pycon+sql

    >>> s = select((users.c.fullname +
    ...               ", " + addresses.c.email_address).
    ...                label('title')).\
    ...        where(
    ...           and_(
    ...               users.c.id == addresses.c.user_id,
    ...               users.c.name.between('m', 'z'),
    ...               or_(
    ...                  addresses.c.email_address.like('%@aol.com'),
    ...                  addresses.c.email_address.like('%@msn.com')
    ...               )
    ...           )
    ...        )
    >>> conn.execute(s).fetchall()
    {opensql}SELECT users.fullname || ? || addresses.email_address AS title
    FROM users, addresses
    WHERE users.id = addresses.user_id AND users.name BETWEEN ? AND ? AND
    (addresses.email_address LIKE ? OR addresses.email_address LIKE ?)
    [...] (', ', 'm', 'z', '%@aol.com', '%@msn.com')
    {stop}[(u'Wendy Williams, wendy@aol.com',)]

Once again, SQLAlchemy figured out the FROM clause for our statement. In fact
it will determine the FROM clause based on all of its other bits; the columns
clause, the where clause, and also some other elements which we haven't
covered yet, which include ORDER BY, GROUP BY, and HAVING.

A shortcut to using :func:`.and_` is to chain together multiple
:meth:`_expression.Select.where` clauses.   The above can also be written as:

.. sourcecode:: pycon+sql

    >>> s = select((users.c.fullname +
    ...               ", " + addresses.c.email_address).
    ...                label('title')).\
    ...        where(users.c.id == addresses.c.user_id).\
    ...        where(users.c.name.between('m', 'z')).\
    ...        where(
    ...               or_(
    ...                  addresses.c.email_address.like('%@aol.com'),
    ...                  addresses.c.email_address.like('%@msn.com')
    ...               )
    ...        )
    >>> conn.execute(s).fetchall()
    {opensql}SELECT users.fullname || ? || addresses.email_address AS title
    FROM users, addresses
    WHERE users.id = addresses.user_id AND users.name BETWEEN ? AND ? AND
    (addresses.email_address LIKE ? OR addresses.email_address LIKE ?)
    [...] (', ', 'm', 'z', '%@aol.com', '%@msn.com')
    {stop}[(u'Wendy Williams, wendy@aol.com',)]

The way that we can build up a :func:`_expression.select` construct through successive
method calls is called :term:`method chaining`.

.. _sqlexpression_text:

Using Textual SQL
=================

Our last example really became a handful to type. Going from what one
understands to be a textual SQL expression into a Python construct which
groups components together in a programmatic style can be hard. That's why
SQLAlchemy lets you just use strings, for those cases when the SQL
is already known and there isn't a strong need for the statement to support
dynamic features.  The :func:`_expression.text` construct is used
to compose a textual statement that is passed to the database mostly
unchanged.  Below, we create a :func:`_expression.text` object and execute it:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import text
    >>> s = text(
    ...     "SELECT users.fullname || ', ' || addresses.email_address AS title "
    ...         "FROM users, addresses "
    ...         "WHERE users.id = addresses.user_id "
    ...         "AND users.name BETWEEN :x AND :y "
    ...         "AND (addresses.email_address LIKE :e1 "
    ...             "OR addresses.email_address LIKE :e2)")
    >>> conn.execute(s, {"x":"m", "y":"z", "e1":"%@aol.com", "e2":"%@msn.com"}).fetchall()
    {opensql}SELECT users.fullname || ', ' || addresses.email_address AS title
    FROM users, addresses
    WHERE users.id = addresses.user_id AND users.name BETWEEN ? AND ? AND
    (addresses.email_address LIKE ? OR addresses.email_address LIKE ?)
    [...] ('m', 'z', '%@aol.com', '%@msn.com')
    {stop}[(u'Wendy Williams, wendy@aol.com',)]

Above, we can see that bound parameters are specified in
:func:`_expression.text` using the named colon format; this format is
consistent regardless of database backend.  To send values in for the
parameters, we passed them into the :meth:`_engine.Connection.execute` method
as additional arguments.

Specifying Bound Parameter Behaviors
------------------------------------

The :func:`_expression.text` construct supports pre-established bound values
using the :meth:`_expression.TextClause.bindparams` method::

    stmt = text("SELECT * FROM users WHERE users.name BETWEEN :x AND :y")
    stmt = stmt.bindparams(x="m", y="z")

The parameters can also be explicitly typed::

    stmt = stmt.bindparams(bindparam("x", type_=String), bindparam("y", type_=String))
    result = conn.execute(stmt, {"x": "m", "y": "z"})

Typing for bound parameters is necessary when the type requires Python-side
or special SQL-side processing provided by the datatype.

.. seealso::

    :meth:`_expression.TextClause.bindparams` - full method description

.. _sqlexpression_text_columns:

Specifying Result-Column Behaviors
----------------------------------

We may also specify information about the result columns using the
:meth:`_expression.TextClause.columns` method; this method can be used to specify
the return types, based on name::

    stmt = stmt.columns(id=Integer, name=String)

or it can be passed full column expressions positionally, either typed
or untyped.  In this case it's a good idea to list out the columns
explicitly within our textual SQL, since the correlation of our column
expressions to the SQL will be done positionally::

    stmt = text("SELECT id, name FROM users")
    stmt = stmt.columns(users.c.id, users.c.name)

When we call the :meth:`_expression.TextClause.columns` method, we get back a
:class:`.TextAsFrom` object that supports the full suite of
:attr:`.TextAsFrom.c` and other "selectable" operations::

    j = stmt.join(addresses, stmt.c.id == addresses.c.user_id)

    new_stmt = select(stmt.c.id, addresses.c.id).\
        select_from(j).where(stmt.c.name == 'x')

The positional form of :meth:`_expression.TextClause.columns` is particularly useful
when relating textual SQL to existing Core or ORM models, because we can use
column expressions directly without worrying about name conflicts or other issues with the
result column names in the textual SQL:

.. sourcecode:: pycon+sql

    >>> stmt = text("SELECT users.id, addresses.id, users.id, "
    ...     "users.name, addresses.email_address AS email "
    ...     "FROM users JOIN addresses ON users.id=addresses.user_id "
    ...     "WHERE users.id = 1").columns(
    ...        users.c.id,
    ...        addresses.c.id,
    ...        addresses.c.user_id,
    ...        users.c.name,
    ...        addresses.c.email_address
    ...     )
    >>> result = conn.execute(stmt)
    {opensql}SELECT users.id, addresses.id, users.id, users.name,
        addresses.email_address AS email
    FROM users JOIN addresses ON users.id=addresses.user_id WHERE users.id = 1
    [...] ()
    {stop}

Above, there's three columns in the result that are named "id", but since
we've associated these with column expressions positionally, the names aren't an issue
when the result-columns are fetched using the actual column object as a key.
Fetching the ``email_address`` column would be::

    >>> row = result.fetchone()
    >>> row._mapping[addresses.c.email_address]
    'jack@yahoo.com'

If on the other hand we used a string column key, the usual rules of
name-based matching still apply, and we'd get an ambiguous column error for
the ``id`` value::

    >>> row._mapping["id"]
    Traceback (most recent call last):
    ...
    InvalidRequestError: Ambiguous column name 'id' in result set column descriptions

It's important to note that while accessing columns from a result set using
:class:`_schema.Column` objects may seem unusual, it is in fact the only system
used by the ORM, which occurs transparently beneath the facade of the
:class:`~.orm.query.Query` object; in this way, the :meth:`_expression.TextClause.columns` method
is typically very applicable to textual statements to be used in an ORM
context.   The example at :ref:`orm_tutorial_literal_sql` illustrates
a simple usage.

.. versionadded:: 1.1

    The :meth:`_expression.TextClause.columns` method now accepts column expressions
    which will be matched positionally to a plain text SQL result set,
    eliminating the need for column names to match or even be unique in the
    SQL statement when matching table metadata or ORM models to textual SQL.

.. seealso::

    :meth:`_expression.TextClause.columns` - full method description

    :ref:`orm_tutorial_literal_sql` - integrating ORM-level queries with
    :func:`_expression.text`


Using text() fragments inside bigger statements
-----------------------------------------------

:func:`_expression.text` can also be used to produce fragments of SQL
that can be freely within a
:func:`_expression.select` object, which accepts :func:`_expression.text`
objects as an argument for most of its builder functions.
Below, we combine the usage of :func:`_expression.text` within a
:func:`_expression.select` object.  The :func:`_expression.select` construct provides the "geometry"
of the statement, and the :func:`_expression.text` construct provides the
textual content within this form.  We can build a statement without the
need to refer to any pre-established :class:`_schema.Table` metadata:

.. sourcecode:: pycon+sql

    >>> s = select(
    ...        text("users.fullname || ', ' || addresses.email_address AS title")
    ...     ).\
    ...         where(
    ...             and_(
    ...                 text("users.id = addresses.user_id"),
    ...                 text("users.name BETWEEN 'm' AND 'z'"),
    ...                 text(
    ...                     "(addresses.email_address LIKE :x "
    ...                     "OR addresses.email_address LIKE :y)")
    ...             )
    ...         ).select_from(text('users, addresses'))
    >>> conn.execute(s, {"x": "%@aol.com", "y": "%@msn.com"}).fetchall()
    {opensql}SELECT users.fullname || ', ' || addresses.email_address AS title
    FROM users, addresses
    WHERE users.id = addresses.user_id AND users.name BETWEEN 'm' AND 'z'
    AND (addresses.email_address LIKE ? OR addresses.email_address LIKE ?)
    [...] ('%@aol.com', '%@msn.com')
    {stop}[(u'Wendy Williams, wendy@aol.com',)]

.. versionchanged:: 1.0.0
   The :func:`_expression.select` construct emits warnings when string SQL
   fragments are coerced to :func:`_expression.text`, and :func:`_expression.text` should
   be used explicitly.  See :ref:`migration_2992` for background.



.. _sqlexpression_literal_column:

Using More Specific Text with :func:`.table`, :func:`_expression.literal_column`, and :func:`_expression.column`
-----------------------------------------------------------------------------------------------------------------
We can move our level of structure back in the other direction too,
by using :func:`_expression.column`, :func:`_expression.literal_column`,
and :func:`_expression.table` for some of the
key elements of our statement.   Using these constructs, we can get
some more expression capabilities than if we used :func:`_expression.text`
directly, as they provide to the Core more information about how the strings
they store are to be used, but still without the need to get into full
:class:`_schema.Table` based metadata.  Below, we also specify the :class:`.String`
datatype for two of the key :func:`_expression.literal_column` objects,
so that the string-specific concatenation operator becomes available.
We also use :func:`_expression.literal_column` in order to use table-qualified
expressions, e.g. ``users.fullname``, that will be rendered as is;
using :func:`_expression.column` implies an individual column name that may
be quoted:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import select, and_, text, String
    >>> from sqlalchemy.sql import table, literal_column
    >>> s = select(
    ...    literal_column("users.fullname", String) +
    ...    ', ' +
    ...    literal_column("addresses.email_address").label("title")
    ... ).\
    ...    where(
    ...        and_(
    ...            literal_column("users.id") == literal_column("addresses.user_id"),
    ...            text("users.name BETWEEN 'm' AND 'z'"),
    ...            text(
    ...                "(addresses.email_address LIKE :x OR "
    ...                "addresses.email_address LIKE :y)")
    ...        )
    ...    ).select_from(table('users')).select_from(table('addresses'))

    >>> conn.execute(s, {"x":"%@aol.com", "y":"%@msn.com"}).fetchall()
    {opensql}SELECT users.fullname || ? || addresses.email_address AS anon_1
    FROM users, addresses
    WHERE users.id = addresses.user_id
    AND users.name BETWEEN 'm' AND 'z'
    AND (addresses.email_address LIKE ? OR addresses.email_address LIKE ?)
    [...] (', ', '%@aol.com', '%@msn.com')
    {stop}[(u'Wendy Williams, wendy@aol.com',)]

Ordering or Grouping by a Label
-------------------------------

One place where we sometimes want to use a string as a shortcut is when
our statement has some labeled column element that we want to refer to in
a place such as the "ORDER BY" or "GROUP BY" clause; other candidates include
fields within an "OVER" or "DISTINCT" clause.  If we have such a label
in our :func:`_expression.select` construct, we can refer to it directly by passing the
string straight into :meth:`_expression.select.order_by` or :meth:`_expression.select.group_by`,
among others.  This will refer to the named label and also prevent the
expression from being rendered twice.  Label names that resolve to columns
are rendered fully:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import func
    >>> stmt = select(
    ...         addresses.c.user_id,
    ...         func.count(addresses.c.id).label('num_addresses')).\
    ...         group_by("user_id").order_by("user_id", "num_addresses")

    {sql}>>> conn.execute(stmt).fetchall()
    SELECT addresses.user_id, count(addresses.id) AS num_addresses
    FROM addresses GROUP BY addresses.user_id ORDER BY addresses.user_id, num_addresses
    [...] ()
    {stop}[(1, 2), (2, 2)]

We can use modifiers like :func:`.asc` or :func:`.desc` by passing the string
name:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import func, desc
    >>> stmt = select(
    ...         addresses.c.user_id,
    ...         func.count(addresses.c.id).label('num_addresses')).\
    ...         group_by("user_id").order_by("user_id", desc("num_addresses"))

    {sql}>>> conn.execute(stmt).fetchall()
    SELECT addresses.user_id, count(addresses.id) AS num_addresses
    FROM addresses GROUP BY addresses.user_id ORDER BY addresses.user_id, num_addresses DESC
    [...] ()
    {stop}[(1, 2), (2, 2)]

Note that the string feature here is very much tailored to when we have
already used the :meth:`_expression.ColumnElement.label` method to create a
specifically-named label.  In other cases, we always want to refer to the
:class:`_expression.ColumnElement` object directly so that the expression system can
make the most effective choices for rendering.  Below, we illustrate how using
the :class:`_expression.ColumnElement` eliminates ambiguity when we want to order
by a column name that appears more than once:

.. sourcecode:: pycon+sql

    >>> u1a, u1b = users.alias(), users.alias()
    >>> stmt = select(u1a, u1b).\
    ...             where(u1a.c.name > u1b.c.name).\
    ...             order_by(u1a.c.name)  # using "name" here would be ambiguous

    {sql}>>> conn.execute(stmt).fetchall()
    SELECT users_1.id, users_1.name, users_1.fullname, users_2.id AS id_1,
    users_2.name AS name_1, users_2.fullname AS fullname_1
    FROM users AS users_1, users AS users_2
    WHERE users_1.name > users_2.name ORDER BY users_1.name
    [...] ()
    {stop}[(2, u'wendy', u'Wendy Williams', 1, u'jack', u'Jack Jones')]



.. _core_tutorial_aliases:

Using Aliases and Subqueries
============================

The alias in SQL corresponds to a "renamed" version of a table or SELECT
statement, which occurs anytime you say "SELECT .. FROM sometable AS
someothername". The ``AS`` creates a new name for the table. Aliases are a key
construct as they allow any table or subquery to be referenced by a unique
name. In the case of a table, this allows the same table to be named in the
FROM clause multiple times. In the case of a SELECT statement, it provides a
parent name for the columns represented by the statement, allowing them to be
referenced relative to this name.

In SQLAlchemy, any :class:`_schema.Table` or other :class:`_expression.FromClause` based
selectable can be turned into an alias using :meth:`_expression.FromClause.alias` method,
which produces an :class:`_expression.Alias` construct.   :class:`_expression.Alias` is a
:class:`_expression.FromClause` object that refers to a mapping of :class:`_schema.Column`
objects via its :attr:`_expression.FromClause.c` collection, and can be used within the
FROM clause of any subsequent SELECT statement, by referring to its column
elements in the columns or WHERE clause of the statement,  or through explicit
placement in the FROM clause, either directly or within a join.

As an example, suppose we know that our user ``jack`` has two particular email
addresses. How can we locate jack based on the combination of those two
addresses?   To accomplish this, we'd use a join to the ``addresses`` table,
once for each address.   We create two :class:`_expression.Alias` constructs against
``addresses``, and then use them both within a :func:`_expression.select` construct:

.. sourcecode:: pycon+sql

    >>> a1 = addresses.alias()
    >>> a2 = addresses.alias()
    >>> s = select(users).\
    ...        where(and_(
    ...            users.c.id == a1.c.user_id,
    ...            users.c.id == a2.c.user_id,
    ...            a1.c.email_address == 'jack@msn.com',
    ...            a2.c.email_address == 'jack@yahoo.com'
    ...        ))
    >>> conn.execute(s).fetchall()
    {opensql}SELECT users.id, users.name, users.fullname
    FROM users, addresses AS addresses_1, addresses AS addresses_2
    WHERE users.id = addresses_1.user_id
        AND users.id = addresses_2.user_id
        AND addresses_1.email_address = ?
        AND addresses_2.email_address = ?
    [...] ('jack@msn.com', 'jack@yahoo.com')
    {stop}[(1, u'jack', u'Jack Jones')]

Note that the :class:`_expression.Alias` construct generated the names ``addresses_1`` and
``addresses_2`` in the final SQL result.  The generation of these names is determined
by the position of the construct within the statement.   If we created a query using
only the second ``a2`` alias, the name would come out as ``addresses_1``.  The
generation of the names is also *deterministic*, meaning the same SQLAlchemy
statement construct will produce the identical SQL string each time it is
rendered for a particular dialect.

Since on the outside, we refer to the alias using the :class:`_expression.Alias` construct
itself, we don't need to be concerned about the generated name.  However, for
the purposes of debugging, it can be specified by passing a string name
to the :meth:`_expression.FromClause.alias` method::

    >>> a1 = addresses.alias('a1')

SELECT-oriented constructs which extend from :class:`_expression.SelectBase` may be turned
into aliased subqueries using the :meth:`_expression.SelectBase.subquery` method, which
produces a :class:`.Subquery` construct; for ease of use, there is also a
:meth:`_expression.SelectBase.alias` method that is synonymous with
:meth:`_expression.SelectBase.subquery`.   Like  :class:`_expression.Alias`, :class:`.Subquery` is
also a :class:`_expression.FromClause` object that may be part of any enclosing SELECT
using the same techniques one would use for a :class:`_expression.Alias`.

We can self-join the ``users`` table back to the :func:`_expression.select` we've created
by making :class:`.Subquery` of the entire statement:

.. sourcecode:: pycon+sql

    >>> address_subq = s.subquery()
    >>> s = select(users.c.name).where(users.c.id == address_subq.c.id)
    >>> conn.execute(s).fetchall()
    {opensql}SELECT users.name
    FROM users,
        (SELECT users.id AS id, users.name AS name, users.fullname AS fullname
            FROM users, addresses AS addresses_1, addresses AS addresses_2
            WHERE users.id = addresses_1.user_id AND users.id = addresses_2.user_id
            AND addresses_1.email_address = ?
            AND addresses_2.email_address = ?) AS anon_1
    WHERE users.id = anon_1.id
    [...] ('jack@msn.com', 'jack@yahoo.com')
    {stop}[(u'jack',)]

.. versionchanged:: 1.4 Added the :class:`.Subquery` object and created more of a
   separation between an "alias" of a FROM clause and a named subquery of a
   SELECT.   See :ref:`change_4617`.

Using Joins
===========

We're halfway along to being able to construct any SELECT expression. The next
cornerstone of the SELECT is the JOIN expression. We've already been doing
joins in our examples, by just placing two tables in either the columns clause
or the where clause of the :func:`_expression.select` construct. But if we want to make a
real "JOIN" or "OUTERJOIN" construct, we use the :meth:`_expression.FromClause.join` and
:meth:`_expression.FromClause.outerjoin` methods, most commonly accessed from the left table in the
join:

.. sourcecode:: pycon+sql

    >>> print(users.join(addresses))
    users JOIN addresses ON users.id = addresses.user_id

The alert reader will see more surprises; SQLAlchemy figured out how to JOIN
the two tables ! The ON condition of the join, as it's called, was
automatically generated based on the :class:`~sqlalchemy.schema.ForeignKey`
object which we placed on the ``addresses`` table way at the beginning of this
tutorial. Already the ``join()`` construct is looking like a much better way
to join tables.

Of course you can join on whatever expression you want, such as if we want to
join on all users who use the same name in their email address as their
username:

.. sourcecode:: pycon+sql

    >>> print(users.join(addresses,
    ...                 addresses.c.email_address.like(users.c.name + '%')
    ...             )
    ...  )
    users JOIN addresses ON addresses.email_address LIKE users.name || :name_1

When we create a :func:`_expression.select` construct, SQLAlchemy looks around at the
tables we've mentioned and then places them in the FROM clause of the
statement. When we use JOINs however, we know what FROM clause we want, so
here we make use of the :meth:`_expression.Select.select_from` method:

.. sourcecode:: pycon+sql

    >>> s = select(users.c.fullname).select_from(
    ...    users.join(addresses,
    ...             addresses.c.email_address.like(users.c.name + '%'))
    ...    )
    {sql}>>> conn.execute(s).fetchall()
    SELECT users.fullname
    FROM users JOIN addresses ON addresses.email_address LIKE users.name || ?
    [...] ('%',)
    {stop}[(u'Jack Jones',), (u'Jack Jones',), (u'Wendy Williams',)]

The :meth:`_expression.FromClause.outerjoin` method creates ``LEFT OUTER JOIN`` constructs,
and is used in the same way as :meth:`_expression.FromClause.join`:

.. sourcecode:: pycon+sql

    >>> s = select(users.c.fullname).select_from(users.outerjoin(addresses))
    >>> print(s)
    SELECT users.fullname
        FROM users
        LEFT OUTER JOIN addresses ON users.id = addresses.user_id

That's the output ``outerjoin()`` produces, unless, of course, you're stuck in
a gig using Oracle prior to version 9, and you've set up your engine (which
would be using ``OracleDialect``) to use Oracle-specific SQL:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.dialects.oracle import dialect as OracleDialect
    >>> print(s.compile(dialect=OracleDialect(use_ansi=False)))
    SELECT users.fullname
    FROM users, addresses
    WHERE users.id = addresses.user_id(+)

If you don't know what that SQL means, don't worry ! The secret tribe of
Oracle DBAs don't want their black magic being found out ;).

.. seealso::

    :func:`_expression.join`

    :func:`_expression.outerjoin`

    :class:`_expression.Join`

Common Table Expressions (CTE)
==============================

Common table expressions are now supported by every major database, including
modern MySQL, MariaDB, SQLite, PostgreSQL, Oracle and MS SQL Server.   SQLAlchemy
supports this construct via the :class:`_expression.CTE` object, which one
typically acquires using the :meth:`_expression.Select.cte` method on a
:class:`_expression.Select` construct:


.. sourcecode:: pycon+sql

    >>> users_cte = select(users.c.id, users.c.name).where(users.c.name == 'wendy').cte()
    >>> stmt = select(addresses).where(addresses.c.user_id == users_cte.c.id).order_by(addresses.c.id)
    >>> conn.execute(stmt).fetchall()
    {opensql}WITH anon_1 AS
    (SELECT users.id AS id, users.name AS name
    FROM users
    WHERE users.name = ?)
     SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses, anon_1
    WHERE addresses.user_id = anon_1.id ORDER BY addresses.id
    [...] ('wendy',)
    {stop}[(3, 2, 'www@www.org'), (4, 2, 'wendy@aol.com')]

The CTE construct is a great way to provide a source of rows that is
semantically similar to using a subquery, but with a much simpler format
where the source of rows is neatly tucked away at the top of the query
where it can be referenced anywhere in the main statement like a regular
table.

When we construct a :class:`_expression.CTE` object, we make use of it like
any other table in the statement.  However instead of being added to the
FROM clause as a subquery, it comes out on top, which has the additional
benefit of not causing surprise cartesian products.

The RECURSIVE format of CTE is available when one uses the
:paramref:`_expression.Select.cte.recursive` parameter.   A recursive
CTE typically requires that we are linking to ourselves as an alias.
The general form of this kind of operation involves a UNION of the
original CTE against itself.   Noting that our example tables are not
well suited to producing an actually useful query with this feature,
this form looks like:


.. sourcecode:: pycon+sql

    >>> users_cte = select(users.c.id, users.c.name).cte(recursive=True)
    >>> users_recursive = users_cte.alias()
    >>> users_cte = users_cte.union(select(users.c.id, users.c.name).where(users.c.id > users_recursive.c.id))
    >>> stmt = select(addresses).where(addresses.c.user_id == users_cte.c.id).order_by(addresses.c.id)
    >>> conn.execute(stmt).fetchall()
    {opensql}WITH RECURSIVE anon_1(id, name) AS
    (SELECT users.id AS id, users.name AS name
    FROM users UNION SELECT users.id AS id, users.name AS name
    FROM users, anon_1 AS anon_2
    WHERE users.id > anon_2.id)
     SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses, anon_1
    WHERE addresses.user_id = anon_1.id ORDER BY addresses.id
    [...] ()
    {stop}[(1, 1, 'jack@yahoo.com'), (2, 1, 'jack@msn.com'), (3, 2, 'www@www.org'), (4, 2, 'wendy@aol.com')]


Everything Else
===============

The concepts of creating SQL expressions have been introduced. What's left are
more variants of the same themes. So now we'll catalog the rest of the
important things we'll need to know.

.. _coretutorial_bind_param:

Bind Parameter Objects
----------------------

Throughout all these examples, SQLAlchemy is busy creating bind parameters
wherever literal expressions occur. You can also specify your own bind
parameters with your own names, and use the same statement repeatedly.
The :func:`.bindparam` construct is used to produce a bound parameter
with a given name.  While SQLAlchemy always refers to bound parameters by
name on the API side, the
database dialect converts to the appropriate named or positional style
at execution time, as here where it converts to positional for SQLite:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import bindparam
    >>> s = users.select().where(users.c.name == bindparam('username'))
    {sql}>>> conn.execute(s, {"username": "wendy"}).fetchall()
    SELECT users.id, users.name, users.fullname
    FROM users
    WHERE users.name = ?
    [...] ('wendy',)
    {stop}[(2, u'wendy', u'Wendy Williams')]

Another important aspect of :func:`.bindparam` is that it may be assigned a
type. The type of the bind parameter will determine its behavior within
expressions and also how the data bound to it is processed before being sent
off to the database:

.. sourcecode:: pycon+sql

    >>> s = users.select().where(users.c.name.like(bindparam('username', type_=String) + text("'%'")))
    {sql}>>> conn.execute(s, {"username": "wendy"}).fetchall()
    SELECT users.id, users.name, users.fullname
    FROM users
    WHERE users.name LIKE ? || '%'
    [...] ('wendy',)
    {stop}[(2, u'wendy', u'Wendy Williams')]


:func:`.bindparam` constructs of the same name can also be used multiple times, where only a
single named value is needed in the execute parameters:

.. sourcecode:: pycon+sql

    >>> s = select(users, addresses).\
    ...     where(
    ...        or_(
    ...          users.c.name.like(
    ...                 bindparam('name', type_=String) + text("'%'")),
    ...          addresses.c.email_address.like(
    ...                 bindparam('name', type_=String) + text("'@%'"))
    ...        )
    ...     ).\
    ...     select_from(users.outerjoin(addresses)).\
    ...     order_by(addresses.c.id)
    {sql}>>> conn.execute(s, {"name": "jack"}).fetchall()
    SELECT users.id, users.name, users.fullname, addresses.id AS id_1,
        addresses.user_id, addresses.email_address
    FROM users LEFT OUTER JOIN addresses ON users.id = addresses.user_id
    WHERE users.name LIKE ? || '%' OR addresses.email_address LIKE ? || '@%'
    ORDER BY addresses.id
    [...] ('jack', 'jack')
    {stop}[(1, u'jack', u'Jack Jones', 1, 1, u'jack@yahoo.com'), (1, u'jack', u'Jack Jones', 2, 1, u'jack@msn.com')]

.. seealso::

    :func:`.bindparam`

.. _coretutorial_functions:

Functions
---------

SQL functions are created using the :data:`~.expression.func` keyword, which
generates functions using attribute access:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import func
    >>> print(func.now())
    now()

    >>> print(func.concat('x', 'y'))
    concat(:concat_1, :concat_2)

By "generates", we mean that **any** SQL function is created based on the word
you choose::

    >>> print(func.xyz_my_goofy_function())
    xyz_my_goofy_function()

Certain function names are known by SQLAlchemy, allowing special behavioral
rules to be applied. Some for example are "ANSI" functions, which mean they
don't get the parenthesis added after them, such as CURRENT_TIMESTAMP:

.. sourcecode:: pycon+sql

    >>> print(func.current_timestamp())
    CURRENT_TIMESTAMP

A function, like any other column expression, has a type, which indicates the
type of expression as well as how SQLAlchemy will interpret result columns
that are returned from this expression.   The default type used for an
arbitrary function name derived from :attr:`.func` is simply a "null" datatype.
However, in order for the column expression generated by the function to
have type-specific operator behavior as well as result-set behaviors, such
as date and numeric coercions, the type may need to be specified explicitly::

    stmt = select(func.date(some_table.c.date_string, type_=Date))


Functions are most typically used in the columns clause of a select statement,
and can also be labeled as well as given a type. Labeling a function is
recommended so that the result can be targeted in a result row based on a
string name, and assigning it a type is required when you need result-set
processing to occur, such as for Unicode conversion and date conversions.
Below, we use the result function ``scalar()`` to just read the first column
of the first row and then close the result; the label, even though present, is
not important in this case:

.. sourcecode:: pycon+sql

    >>> conn.execute(
    ...     select(
    ...            func.max(addresses.c.email_address, type_=String).
    ...                label('maxemail')
    ...           )
    ...     ).scalar()
    {opensql}SELECT max(addresses.email_address) AS maxemail
    FROM addresses
    [...] ()
    {stop}u'www@www.org'

Databases such as PostgreSQL and Oracle which support functions that return
whole result sets can be assembled into selectable units, which can be used in
statements. Such as, a database function ``calculate()`` which takes the
parameters ``x`` and ``y``, and returns three columns which we'd like to name
``q``, ``z`` and ``r``, we can construct using "lexical" column objects as
well as bind parameters:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import column
    >>> calculate = select(column('q'), column('z'), column('r')).\
    ...        select_from(
    ...             func.calculate(
    ...                    bindparam('x'),
    ...                    bindparam('y')
    ...                )
    ...             )
    >>> calc = calculate.alias()
    >>> print(select(users).where(users.c.id > calc.c.z))
    SELECT users.id, users.name, users.fullname
    FROM users, (SELECT q, z, r
    FROM calculate(:x, :y)) AS anon_1
    WHERE users.id > anon_1.z

If we wanted to use our ``calculate`` statement twice with different bind
parameters, the :func:`~sqlalchemy.sql.expression.ClauseElement.unique_params`
function will create copies for us, and mark the bind parameters as "unique"
so that conflicting names are isolated. Note we also make two separate aliases
of our selectable:

.. sourcecode:: pycon+sql

    >>> calc1 = calculate.alias('c1').unique_params(x=17, y=45)
    >>> calc2 = calculate.alias('c2').unique_params(x=5, y=12)
    >>> s = select(users).\
    ...         where(users.c.id.between(calc1.c.z, calc2.c.z))
    >>> print(s)
    SELECT users.id, users.name, users.fullname
    FROM users,
        (SELECT q, z, r FROM calculate(:x_1, :y_1)) AS c1,
        (SELECT q, z, r FROM calculate(:x_2, :y_2)) AS c2
    WHERE users.id BETWEEN c1.z AND c2.z

    >>> s.compile().params # doctest: +SKIP
    {u'x_2': 5, u'y_2': 12, u'y_1': 45, u'x_1': 17}

.. seealso::

    :data:`.func`

.. _window_functions:

Window Functions
----------------

Any :class:`.FunctionElement`, including functions generated by
:data:`~.expression.func`, can be turned into a "window function", that is an
OVER clause, using the :meth:`.FunctionElement.over` method::

    >>> s = select(
    ...         users.c.id,
    ...         func.row_number().over(order_by=users.c.name)
    ...     )
    >>> print(s)
    SELECT users.id, row_number() OVER (ORDER BY users.name) AS anon_1
    FROM users

:meth:`.FunctionElement.over` also supports range specification using
either the :paramref:`.expression.over.rows` or
:paramref:`.expression.over.range` parameters::

    >>> s = select(
    ...         users.c.id,
    ...         func.row_number().over(
    ...                 order_by=users.c.name,
    ...                 rows=(-2, None))
    ...     )
    >>> print(s)
    SELECT users.id, row_number() OVER
    (ORDER BY users.name ROWS BETWEEN :param_1 PRECEDING AND UNBOUNDED FOLLOWING) AS anon_1
    FROM users

:paramref:`.expression.over.rows` and :paramref:`.expression.over.range` each
accept a two-tuple which contains a combination of negative and positive
integers for ranges, zero to indicate "CURRENT ROW" and ``None`` to
indicate "UNBOUNDED".  See the examples at :func:`.over` for more detail.

.. versionadded:: 1.1 support for "rows" and "range" specification for
   window functions

.. seealso::

    :func:`.over`

    :meth:`.FunctionElement.over`

.. _coretutorial_casts:

Data Casts and Type Coercion
-----------------------------

In SQL, we often need to indicate the datatype of an element explicitly, or
we need to convert between one datatype and another within a SQL statement.
The CAST SQL function performs this.  In SQLAlchemy, the :func:`.cast` function
renders the SQL CAST keyword.  It accepts a column expression and a data type
object as arguments:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import cast
    >>> s = select(cast(users.c.id, String))
    >>> conn.execute(s).fetchall()
    {opensql}SELECT CAST(users.id AS VARCHAR) AS id
    FROM users
    [...] ()
    {stop}[('1',), ('2',)]

The :func:`.cast` function is used not just when converting between datatypes,
but also in cases where the database needs to
know that some particular value should be considered to be of a particular
datatype within an expression.

The :func:`.cast` function also tells SQLAlchemy itself that an expression
should be treated as a particular type as well.   The datatype of an expression
directly impacts the behavior of Python operators upon that object, such as how
the ``+`` operator may indicate integer addition or string concatenation, and
it also impacts how a literal Python value is transformed or handled before
being passed to the database as well as how result values of that expression
should be transformed or handled.

Sometimes there is the need to have SQLAlchemy know the datatype of an
expression, for all the reasons mentioned above, but to not render the CAST
expression itself on the SQL side, where it may interfere with a SQL operation
that already works without it.  For this fairly common use case there is
another function :func:`.type_coerce` which is closely related to
:func:`.cast`, in that it sets up a Python expression as having a specific SQL
database type, but does not render the ``CAST`` keyword or datatype on the
database side.    :func:`.type_coerce` is particularly important when dealing
with the :class:`_types.JSON` datatype, which typically has an intricate
relationship with string-oriented datatypes on different platforms and
may not even be an explicit datatype, such as on SQLite and MariaDB.
Below, we use :func:`.type_coerce` to deliver a Python structure as a JSON
string into one of MySQL's JSON functions:

.. sourcecode:: pycon+sql

    >>> import json
    >>> from sqlalchemy import JSON
    >>> from sqlalchemy import type_coerce
    >>> from sqlalchemy.dialects import mysql
    >>> s = select(
    ... type_coerce(
    ...        {'some_key': {'foo': 'bar'}}, JSON
    ...    )['some_key']
    ... )
    >>> print(s.compile(dialect=mysql.dialect()))
    SELECT JSON_EXTRACT(%s, %s) AS anon_1

Above, MySQL's ``JSON_EXTRACT`` SQL function was invoked
because we used :func:`.type_coerce` to indicate that our Python dictionary
should be treated as :class:`_types.JSON`.  The Python ``__getitem__``
operator, ``['some_key']`` in this case, became available as a result and
allowed a ``JSON_EXTRACT`` path expression (not shown, however in this
case it would ultimately be ``'$."some_key"'``) to be rendered.

Unions and Other Set Operations
-------------------------------

Unions come in two flavors, UNION and UNION ALL, which are available via
module level functions :func:`_expression.union` and
:func:`_expression.union_all`:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import union
    >>> u = union(
    ...     addresses.select().
    ...             where(addresses.c.email_address == 'foo@bar.com'),
    ...    addresses.select().
    ...             where(addresses.c.email_address.like('%@yahoo.com')),
    ... ).order_by(addresses.c.email_address)

    {sql}>>> conn.execute(u).fetchall()
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address = ?
    UNION
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address LIKE ? ORDER BY email_address
    [...] ('foo@bar.com', '%@yahoo.com')
    {stop}[(1, 1, u'jack@yahoo.com')]

Also available, though not supported on all databases, are
:func:`_expression.intersect`,
:func:`_expression.intersect_all`,
:func:`_expression.except_`, and :func:`_expression.except_all`:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import except_
    >>> u = except_(
    ...    addresses.select().
    ...             where(addresses.c.email_address.like('%@%.com')),
    ...    addresses.select().
    ...             where(addresses.c.email_address.like('%@msn.com'))
    ... )

    {sql}>>> conn.execute(u).fetchall()
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address LIKE ?
    EXCEPT
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address LIKE ?
    [...] ('%@%.com', '%@msn.com')
    {stop}[(1, 1, u'jack@yahoo.com'), (4, 2, u'wendy@aol.com')]

A common issue with so-called "compound" selectables arises due to the fact
that they nest with parenthesis. SQLite in particular doesn't like a statement
that starts with parenthesis. So when nesting a "compound" inside a
"compound", it's often necessary to apply ``.subquery().select()`` to the first
element of the outermost compound, if that element is also a compound. For
example, to nest a "union" and a "select" inside of "except\_", SQLite will
want the "union" to be stated as a subquery:

.. sourcecode:: pycon+sql

    >>> u = except_(
    ...    union(
    ...         addresses.select().
    ...             where(addresses.c.email_address.like('%@yahoo.com')),
    ...         addresses.select().
    ...             where(addresses.c.email_address.like('%@msn.com'))
    ...     ).subquery().select(),   # apply subquery here
    ...    addresses.select().where(addresses.c.email_address.like('%@msn.com'))
    ... )
    {sql}>>> conn.execute(u).fetchall()
    SELECT anon_1.id, anon_1.user_id, anon_1.email_address
    FROM (SELECT addresses.id AS id, addresses.user_id AS user_id,
        addresses.email_address AS email_address
        FROM addresses
        WHERE addresses.email_address LIKE ?
        UNION
        SELECT addresses.id AS id,
            addresses.user_id AS user_id,
            addresses.email_address AS email_address
        FROM addresses
        WHERE addresses.email_address LIKE ?) AS anon_1
    EXCEPT
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address LIKE ?
    [...] ('%@yahoo.com', '%@msn.com', '%@msn.com')
    {stop}[(1, 1, u'jack@yahoo.com')]

.. seealso::

    :func:`_expression.union`

    :func:`_expression.union_all`

    :func:`_expression.intersect`

    :func:`_expression.intersect_all`

    :func:`.except_`

    :func:`_expression.except_all`

Ordering Unions
^^^^^^^^^^^^^^^

UNION and other set constructs have a special case when it comes to ordering
the results.  As the UNION consists of several SELECT statements, to ORDER the
whole result usually requires that an ORDER BY clause refer to column names but
not specific tables.  As in the previous examples, we used
``.order_by(addresses.c.email_address)`` but SQLAlchemy rendered the ORDER BY
without using the table name.    A generalized way to apply ORDER BY to a union
is also to refer to the :attr:`_selectable.CompoundSelect.selected_columns` collection in
order to access the column expressions which are synonymous with the columns
selected from the first SELECT; the SQLAlchemy compiler will ensure these will
be rendered without table names::

    >>> u = union(
    ...     addresses.select().
    ...             where(addresses.c.email_address == 'foo@bar.com'),
    ...    addresses.select().
    ...             where(addresses.c.email_address.like('%@yahoo.com')),
    ... )
    >>> u = u.order_by(u.selected_columns.email_address)
    >>> print(u)
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address = :email_address_1
    UNION SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address LIKE :email_address_2 ORDER BY email_address


.. _scalar_selects:

Scalar Selects
--------------

A scalar select is a SELECT that returns exactly one row and one
column.  It can then be used as a column expression.  A scalar select
is often a :term:`correlated subquery`, which relies upon the enclosing
SELECT statement in order to acquire at least one of its FROM clauses.

The :func:`_expression.select` construct can be modified to act as a
column expression by calling either the :meth:`_expression.SelectBase.scalar_subquery`
or :meth:`_expression.SelectBase.label` method:

.. sourcecode:: pycon+sql

    >>> subq = select(func.count(addresses.c.id)).\
    ...             where(users.c.id == addresses.c.user_id).\
    ...             scalar_subquery()

The above construct is now a :class:`_expression.ScalarSelect` object,
which is an adapter around the original :class:`.~expression.Select`
object; it participates within the :class:`_expression.ColumnElement`
family of expression constructs.  We can place this construct the same as any
other column within another :func:`_expression.select`:

.. sourcecode:: pycon+sql

    >>> conn.execute(select(users.c.name, subq)).fetchall()
    {opensql}SELECT users.name, (SELECT count(addresses.id) AS count_1
    FROM addresses
    WHERE users.id = addresses.user_id) AS anon_1
    FROM users
    [...] ()
    {stop}[(u'jack', 2), (u'wendy', 2)]

To apply a non-anonymous column name to our scalar select, we create
it using :meth:`_expression.SelectBase.label` instead:

.. sourcecode:: pycon+sql

    >>> subq = select(func.count(addresses.c.id)).\
    ...             where(users.c.id == addresses.c.user_id).\
    ...             label("address_count")
    >>> conn.execute(select(users.c.name, subq)).fetchall()
    {opensql}SELECT users.name, (SELECT count(addresses.id) AS count_1
    FROM addresses
    WHERE users.id = addresses.user_id) AS address_count
    FROM users
    [...] ()
    {stop}[(u'jack', 2), (u'wendy', 2)]

.. seealso::

    :meth:`_expression.Select.scalar_subquery`

    :meth:`_expression.Select.label`

.. _correlated_subqueries:

Correlated Subqueries
---------------------

In the examples on :ref:`scalar_selects`, the FROM clause of each embedded
select did not contain the ``users`` table in its FROM clause. This is because
SQLAlchemy automatically :term:`correlates` embedded FROM objects to that
of an enclosing query, if present, and if the inner SELECT statement would
still have at least one FROM clause of its own.  For example:

.. sourcecode:: pycon+sql

    >>> stmt = select(addresses.c.user_id).\
    ...             where(addresses.c.user_id == users.c.id).\
    ...             where(addresses.c.email_address == 'jack@yahoo.com')
    >>> enclosing_stmt = select(users.c.name).\
    ...             where(users.c.id == stmt.scalar_subquery())
    >>> conn.execute(enclosing_stmt).fetchall()
    {opensql}SELECT users.name
    FROM users
    WHERE users.id = (SELECT addresses.user_id
        FROM addresses
        WHERE addresses.user_id = users.id
        AND addresses.email_address = ?)
    [...] ('jack@yahoo.com',)
    {stop}[(u'jack',)]

Auto-correlation will usually do what's expected, however it can also be controlled.
For example, if we wanted a statement to correlate only to the ``addresses`` table
but not the ``users`` table, even if both were present in the enclosing SELECT,
we use the :meth:`_expression.Select.correlate` method to specify those FROM clauses that
may be correlated:

.. sourcecode:: pycon+sql

    >>> stmt = select(users.c.id).\
    ...             where(users.c.id == addresses.c.user_id).\
    ...             where(users.c.name == 'jack').\
    ...             correlate(addresses)
    >>> enclosing_stmt = select(
    ...         users.c.name, addresses.c.email_address).\
    ...     select_from(users.join(addresses)).\
    ...     where(users.c.id == stmt.scalar_subquery())
    >>> conn.execute(enclosing_stmt).fetchall()
    {opensql}SELECT users.name, addresses.email_address
     FROM users JOIN addresses ON users.id = addresses.user_id
     WHERE users.id = (SELECT users.id
     FROM users
     WHERE users.id = addresses.user_id AND users.name = ?)
     [...] ('jack',)
     {stop}[(u'jack', u'jack@yahoo.com'), (u'jack', u'jack@msn.com')]

To entirely disable a statement from correlating, we can pass ``None``
as the argument:

.. sourcecode:: pycon+sql

    >>> stmt = select(users.c.id).\
    ...             where(users.c.name == 'wendy').\
    ...             correlate(None)
    >>> enclosing_stmt = select(users.c.name).\
    ...     where(users.c.id == stmt.scalar_subquery())
    >>> conn.execute(enclosing_stmt).fetchall()
    {opensql}SELECT users.name
     FROM users
     WHERE users.id = (SELECT users.id
      FROM users
      WHERE users.name = ?)
    [...] ('wendy',)
    {stop}[(u'wendy',)]

We can also control correlation via exclusion, using the :meth:`_expression.Select.correlate_except`
method.   Such as, we can write our SELECT for the ``users`` table
by telling it to correlate all FROM clauses except for ``users``:

.. sourcecode:: pycon+sql

    >>> stmt = select(users.c.id).\
    ...             where(users.c.id == addresses.c.user_id).\
    ...             where(users.c.name == 'jack').\
    ...             correlate_except(users)
    >>> enclosing_stmt = select(
    ...         users.c.name, addresses.c.email_address).\
    ...     select_from(users.join(addresses)).\
    ...     where(users.c.id == stmt.scalar_subquery())
    >>> conn.execute(enclosing_stmt).fetchall()
    {opensql}SELECT users.name, addresses.email_address
     FROM users JOIN addresses ON users.id = addresses.user_id
     WHERE users.id = (SELECT users.id
     FROM users
     WHERE users.id = addresses.user_id AND users.name = ?)
     [...] ('jack',)
     {stop}[(u'jack', u'jack@yahoo.com'), (u'jack', u'jack@msn.com')]

.. _lateral_selects:

LATERAL correlation
^^^^^^^^^^^^^^^^^^^

LATERAL correlation is a special sub-category of SQL correlation which
allows a selectable unit to refer to another selectable unit within a
single FROM clause.  This is an extremely special use case which, while
part of the SQL standard, is only known to be supported by recent
versions of PostgreSQL.

Normally, if a SELECT statement refers to
``table1 JOIN (some SELECT) AS subquery`` in its FROM clause, the subquery
on the right side may not refer to the "table1" expression from the left side;
correlation may only refer to a table that is part of another SELECT that
entirely encloses this SELECT.  The LATERAL keyword allows us to turn this
behavior around, allowing an expression such as:

.. sourcecode:: sql

    SELECT people.people_id, people.age, people.name
    FROM people JOIN LATERAL (SELECT books.book_id AS book_id
    FROM books WHERE books.owner_id = people.people_id)
    AS book_subq ON true

Where above, the right side of the JOIN contains a subquery that refers not
just to the "books" table but also the "people" table, correlating
to the left side of the JOIN.   SQLAlchemy Core supports a statement
like the above using the :meth:`_expression.Select.lateral` method as follows::

    >>> from sqlalchemy import table, column, select, true
    >>> people = table('people', column('people_id'), column('age'), column('name'))
    >>> books = table('books', column('book_id'), column('owner_id'))
    >>> subq = select(books.c.book_id).\
    ...      where(books.c.owner_id == people.c.people_id).lateral("book_subq")
    >>> print(select(people).select_from(people.join(subq, true())))
    SELECT people.people_id, people.age, people.name
    FROM people JOIN LATERAL (SELECT books.book_id AS book_id
    FROM books WHERE books.owner_id = people.people_id)
    AS book_subq ON true

Above, we can see that the :meth:`_expression.Select.lateral` method acts a lot like
the :meth:`_expression.Select.alias` method, including that we can specify an optional
name.  However the construct is the :class:`_expression.Lateral` construct instead of
an :class:`_expression.Alias` which provides for the LATERAL keyword as well as special
instructions to allow correlation from inside the FROM clause of the
enclosing statement.

The :meth:`_expression.Select.lateral` method interacts normally with the
:meth:`_expression.Select.correlate` and :meth:`_expression.Select.correlate_except` methods, except
that the correlation rules also apply to any other tables present in the
enclosing statement's FROM clause.   Correlation is "automatic" to these
tables by default, is explicit if the table is specified to
:meth:`_expression.Select.correlate`, and is explicit to all tables except those
specified to :meth:`_expression.Select.correlate_except`.


.. versionadded:: 1.1

    Support for the LATERAL keyword and lateral correlation.

.. seealso::

    :class:`_expression.Lateral`

    :meth:`_expression.Select.lateral`


.. _core_tutorial_ordering:

Ordering, Grouping, Limiting, Offset...ing...
---------------------------------------------

Ordering is done by passing column expressions to the
:meth:`_expression.SelectBase.order_by` method:

.. sourcecode:: pycon+sql

    >>> stmt = select(users.c.name).order_by(users.c.name)
    >>> conn.execute(stmt).fetchall()
    {opensql}SELECT users.name
    FROM users ORDER BY users.name
    [...] ()
    {stop}[(u'jack',), (u'wendy',)]

Ascending or descending can be controlled using the :meth:`_expression.ColumnElement.asc`
and :meth:`_expression.ColumnElement.desc` modifiers:

.. sourcecode:: pycon+sql

    >>> stmt = select(users.c.name).order_by(users.c.name.desc())
    >>> conn.execute(stmt).fetchall()
    {opensql}SELECT users.name
    FROM users ORDER BY users.name DESC
    [...] ()
    {stop}[(u'wendy',), (u'jack',)]

Grouping refers to the GROUP BY clause, and is usually used in conjunction
with aggregate functions to establish groups of rows to be aggregated.
This is provided via the :meth:`_expression.SelectBase.group_by` method:

.. sourcecode:: pycon+sql

    >>> stmt = select(users.c.name, func.count(addresses.c.id)).\
    ...             select_from(users.join(addresses)).\
    ...             group_by(users.c.name)
    >>> conn.execute(stmt).fetchall()
    {opensql}SELECT users.name, count(addresses.id) AS count_1
    FROM users JOIN addresses
        ON users.id = addresses.user_id
    GROUP BY users.name
    [...] ()
    {stop}[(u'jack', 2), (u'wendy', 2)]

HAVING can be used to filter results on an aggregate value, after GROUP BY has
been applied.  It's available here via the :meth:`_expression.Select.having`
method:

.. sourcecode:: pycon+sql

    >>> stmt = select(users.c.name, func.count(addresses.c.id)).\
    ...             select_from(users.join(addresses)).\
    ...             group_by(users.c.name).\
    ...             having(func.length(users.c.name) > 4)
    >>> conn.execute(stmt).fetchall()
    {opensql}SELECT users.name, count(addresses.id) AS count_1
    FROM users JOIN addresses
        ON users.id = addresses.user_id
    GROUP BY users.name
    HAVING length(users.name) > ?
    [...] (4,)
    {stop}[(u'wendy', 2)]

A common system of dealing with duplicates in composed SELECT statements
is the DISTINCT modifier.  A simple DISTINCT clause can be added using the
:meth:`_expression.Select.distinct` method:

.. sourcecode:: pycon+sql

    >>> stmt = select(users.c.name).\
    ...             where(addresses.c.email_address.
    ...                    contains(users.c.name)).\
    ...             distinct()
    >>> conn.execute(stmt).fetchall()
    {opensql}SELECT DISTINCT users.name
    FROM users, addresses
    WHERE (addresses.email_address LIKE '%' || users.name || '%')
    [...] ()
    {stop}[(u'jack',), (u'wendy',)]

Most database backends support a system of limiting how many rows
are returned, and the majority also feature a means of starting to return
rows after a given "offset".   While common backends like PostgreSQL,
MySQL and SQLite support LIMIT and OFFSET keywords, other backends
need to refer to more esoteric features such as "window functions"
and row ids to achieve the same effect.  The :meth:`_expression.Select.limit`
and :meth:`_expression.Select.offset` methods provide an easy abstraction
into the current backend's methodology:

.. sourcecode:: pycon+sql

    >>> stmt = select(users.c.name, addresses.c.email_address).\
    ...             select_from(users.join(addresses)).\
    ...             limit(1).offset(1)
    >>> conn.execute(stmt).fetchall()
    {opensql}SELECT users.name, addresses.email_address
    FROM users JOIN addresses ON users.id = addresses.user_id
     LIMIT ? OFFSET ?
    [...] (1, 1)
    {stop}[(u'jack', u'jack@msn.com')]


.. _inserts_and_updates:

Inserts, Updates and Deletes
============================

We've seen :meth:`_expression.TableClause.insert` demonstrated
earlier in this tutorial.   Where :meth:`_expression.TableClause.insert`
produces INSERT, the :meth:`_expression.TableClause.update`
method produces UPDATE.  Both of these constructs feature
a method called :meth:`~.ValuesBase.values` which specifies
the VALUES or SET clause of the statement.

The :meth:`~.ValuesBase.values` method accommodates any column expression
as a value:

.. sourcecode:: pycon+sql

    >>> stmt = users.update().\
    ...             values(fullname="Fullname: " + users.c.name)
    >>> conn.execute(stmt)
    {opensql}UPDATE users SET fullname=(? || users.name)
    [...] ('Fullname: ',)
    COMMIT
    {stop}<sqlalchemy.engine.cursor.LegacyCursorResult object at 0x...>

When using :meth:`_expression.TableClause.insert` or :meth:`_expression.TableClause.update`
in an "execute many" context, we may also want to specify named
bound parameters which we can refer to in the argument list.
The two constructs will automatically generate bound placeholders
for any column names passed in the dictionaries sent to
:meth:`_engine.Connection.execute` at execution time.  However, if we
wish to use explicitly targeted named parameters with composed expressions,
we need to use the :func:`_expression.bindparam` construct.
When using :func:`_expression.bindparam` with
:meth:`_expression.TableClause.insert` or :meth:`_expression.TableClause.update`,
the names of the table's columns themselves are reserved for the
"automatic" generation of bind names.  We can combine the usage
of implicitly available bind names and explicitly named parameters
as in the example below:

.. sourcecode:: pycon+sql

    >>> stmt = users.insert().\
    ...         values(name=bindparam('_name') + " .. name")
    >>> conn.execute(stmt, [
    ...        {'id':4, '_name':'name1'},
    ...        {'id':5, '_name':'name2'},
    ...        {'id':6, '_name':'name3'},
    ...     ])
    {opensql}INSERT INTO users (id, name) VALUES (?, (? || ?))
    [...] ((4, 'name1', ' .. name'), (5, 'name2', ' .. name'), (6, 'name3', ' .. name'))
    COMMIT
    <sqlalchemy.engine.cursor.LegacyCursorResult object at 0x...>

An UPDATE statement is emitted using the :meth:`_expression.TableClause.update` construct.  This
works much like an INSERT, except there is an additional WHERE clause
that can be specified:

.. sourcecode:: pycon+sql

    >>> stmt = users.update().\
    ...             where(users.c.name == 'jack').\
    ...             values(name='ed')

    >>> conn.execute(stmt)
    {opensql}UPDATE users SET name=? WHERE users.name = ?
    [...] ('ed', 'jack')
    COMMIT
    {stop}<sqlalchemy.engine.cursor.LegacyCursorResult object at 0x...>

When using :meth:`_expression.TableClause.update` in an "executemany" context,
we may wish to also use explicitly named bound parameters in the
WHERE clause.  Again, :func:`_expression.bindparam` is the construct
used to achieve this:

.. sourcecode:: pycon+sql

    >>> stmt = users.update().\
    ...             where(users.c.name == bindparam('oldname')).\
    ...             values(name=bindparam('newname'))
    >>> conn.execute(stmt, [
    ...     {'oldname':'jack', 'newname':'ed'},
    ...     {'oldname':'wendy', 'newname':'mary'},
    ...     {'oldname':'jim', 'newname':'jake'},
    ...     ])
    {opensql}UPDATE users SET name=? WHERE users.name = ?
    [...] (('ed', 'jack'), ('mary', 'wendy'), ('jake', 'jim'))
    COMMIT
    {stop}<sqlalchemy.engine.cursor.LegacyCursorResult object at 0x...>

.. _tutorial_1x_correlated_updates:

Correlated Updates
------------------

A correlated update lets you update a table using selection from another
table, or the same table; the SELECT statement is passed as a scalar
subquery using :meth:`_expression.Select.scalar_subquery`:

.. sourcecode:: pycon+sql

    >>> stmt = select(addresses.c.email_address).\
    ...             where(addresses.c.user_id == users.c.id).\
    ...             limit(1)
    >>> conn.execute(users.update().values(fullname=stmt.scalar_subquery()))
    {opensql}UPDATE users SET fullname=(SELECT addresses.email_address
        FROM addresses
        WHERE addresses.user_id = users.id
        LIMIT ? OFFSET ?)
    [...] (1, 0)
    COMMIT
    {stop}<sqlalchemy.engine.cursor.LegacyCursorResult object at 0x...>

.. _multi_table_updates:

Multiple Table Updates
----------------------

The PostgreSQL, Microsoft SQL Server, and MySQL backends all support UPDATE statements
that refer to multiple tables.   For PG and MSSQL, this is the "UPDATE FROM" syntax,
which updates one table at a time, but can reference additional tables in an additional
"FROM" clause that can then be referenced in the WHERE clause directly.   On MySQL,
multiple tables can be embedded into a single UPDATE statement separated by a comma.
The SQLAlchemy :func:`_expression.update` construct supports both of these modes
implicitly, by specifying multiple tables in the WHERE clause::

    stmt = users.update().\
            values(name='ed wood').\
            where(users.c.id == addresses.c.id).\
            where(addresses.c.email_address.startswith('ed%'))
    conn.execute(stmt)

The resulting SQL from the above statement would render as::

    UPDATE users SET name=:name FROM addresses
    WHERE users.id = addresses.id AND
    addresses.email_address LIKE :email_address_1 || '%'

When using MySQL, columns from each table can be assigned to in the
SET clause directly, using the dictionary form passed to :meth:`_expression.Update.values`::

    stmt = users.update().\
            values({
                users.c.name:'ed wood',
                addresses.c.email_address:'ed.wood@foo.com'
            }).\
            where(users.c.id == addresses.c.id).\
            where(addresses.c.email_address.startswith('ed%'))

The tables are referenced explicitly in the SET clause::

    UPDATE users, addresses SET addresses.email_address=%s,
            users.name=%s WHERE users.id = addresses.id
            AND addresses.email_address LIKE concat(%s, '%')

When the construct is used on a non-supporting database, the compiler
will raise ``NotImplementedError``.   For convenience, when a statement
is printed as a string without specification of a dialect, the "string SQL"
compiler will be invoked which provides a non-working SQL representation of the
construct.

.. _updates_order_parameters:

Parameter-Ordered Updates
-------------------------

The default behavior of the :func:`_expression.update` construct when rendering the SET
clauses is to render them using the column ordering given in the
originating :class:`_schema.Table` object.
This is an important behavior, since it means that the rendering of a
particular UPDATE statement with particular columns
will be rendered the same each time, which has an impact on query caching systems
that rely on the form of the statement, either client side or server side.
Since the parameters themselves are passed to the :meth:`_expression.Update.values`
method as Python dictionary keys, there is no other fixed ordering
available.

However in some cases, the order of parameters rendered in the SET clause of an
UPDATE statement may need to be explicitly stated.  The main example of this is
when using MySQL and providing updates to column values based on that of other
column values.  The end result of the following statement::

    UPDATE some_table SET x = y + 10, y = 20

Will have a different result than::

    UPDATE some_table SET y = 20, x = y + 10

This because on MySQL, the individual SET clauses are fully evaluated on
a per-value basis, as opposed to on a per-row basis, and as each SET clause
is evaluated, the values embedded in the row are changing.

To suit this specific use case, the
:meth:`_expression.update.ordered_values` method may be used.  When using this method,
we supply a **series of 2-tuples**
as the argument to the method::

    stmt = some_table.update().\
        ordered_values((some_table.c.y, 20), (some_table.c.x, some_table.c.y + 10))

The series of 2-tuples is essentially the same structure as a Python
dictionary, except that it explicitly suggests a specific ordering. Using the
above form, we are assured that the "y" column's SET clause will render first,
then the "x" column's SET clause.

.. versionchanged:: 1.4  Added the :meth:`_expression.Update.ordered_values` method which
   supersedes the :paramref:`_expression.update.preserve_parameter_order` flag that will
   be removed in SQLAlchemy 2.0.

.. seealso::

    :ref:`mysql_insert_on_duplicate_key_update` - background on the MySQL
    ``ON DUPLICATE KEY UPDATE`` clause and how to support parameter ordering.

.. _deletes:

Deletes
-------

Finally, a delete.  This is accomplished easily enough using the
:meth:`_expression.TableClause.delete` construct:

.. sourcecode:: pycon+sql

    >>> conn.execute(addresses.delete())
    {opensql}DELETE FROM addresses
    [...] ()
    COMMIT
    {stop}<sqlalchemy.engine.cursor.LegacyCursorResult object at 0x...>

    >>> conn.execute(users.delete().where(users.c.name > 'm'))
    {opensql}DELETE FROM users WHERE users.name > ?
    [...] ('m',)
    COMMIT
    {stop}<sqlalchemy.engine.cursor.LegacyCursorResult object at 0x...>

.. _multi_table_deletes:

Multiple Table Deletes
----------------------

.. versionadded:: 1.2

The PostgreSQL, Microsoft SQL Server, and MySQL backends all support DELETE
statements that refer to multiple tables within the WHERE criteria.   For PG
and MySQL, this is the "DELETE USING" syntax, and for SQL Server, it's a
"DELETE FROM" that refers to more than one table.  The SQLAlchemy
:func:`_expression.delete` construct supports both of these modes
implicitly, by specifying multiple tables in the WHERE clause::

    stmt = users.delete().\
            where(users.c.id == addresses.c.id).\
            where(addresses.c.email_address.startswith('ed%'))
    conn.execute(stmt)

On a PostgreSQL backend, the resulting SQL from the above statement would render as::

    DELETE FROM users USING addresses
    WHERE users.id = addresses.id
    AND (addresses.email_address LIKE %(email_address_1)s || '%%')

When the construct is used on a non-supporting database, the compiler
will raise ``NotImplementedError``.   For convenience, when a statement
is printed as a string without specification of a dialect, the "string SQL"
compiler will be invoked which provides a non-working SQL representation of the
construct.

Matched Row Counts
------------------

Both of :meth:`_expression.TableClause.update` and
:meth:`_expression.TableClause.delete` are associated with *matched row counts*.  This is a
number indicating the number of rows that were matched by the WHERE clause.
Note that by "matched", this includes rows where no UPDATE actually took place.
The value is available as :attr:`_engine.CursorResult.rowcount`:

.. sourcecode:: pycon+sql

    >>> result = conn.execute(users.delete())
    {opensql}DELETE FROM users
    [...] ()
    COMMIT
    {stop}>>> result.rowcount
    1

Further Reference
=================

Expression Language Reference: :ref:`expression_api_toplevel`

Database Metadata Reference: :ref:`metadata_toplevel`

Engine Reference: :doc:`/core/engines`

Connection Reference: :ref:`connections_toplevel`

Types Reference: :ref:`types_toplevel`



..  Setup code, not for display

    >>> conn.close()
