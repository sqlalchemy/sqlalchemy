.. _sqlexpression_toplevel:

================================
SQL Expression Language Tutorial
================================

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


A quick check to verify that we are on at least **version 0.7** of SQLAlchemy:

.. sourcecode:: pycon+sql

    >>> import sqlalchemy
    >>> sqlalchemy.__version__ # doctest:+SKIP
    0.7.0

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

Define and Create Tables
=========================

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
addreses" for each row in the "users" table:

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

    {sql}>>> metadata.create_all(engine) #doctest: +NORMALIZE_WHITESPACE
    PRAGMA table_info("users")
    ()
    PRAGMA table_info("addresses")
    ()
    CREATE TABLE users (
        id INTEGER NOT NULL,
        name VARCHAR,
        fullname VARCHAR,
        PRIMARY KEY (id)
    )
    ()
    COMMIT
    CREATE TABLE addresses (
        id INTEGER NOT NULL,
        user_id INTEGER,
        email_address VARCHAR NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY(user_id) REFERENCES users (id)
    )
    ()
    COMMIT

.. note:: 

    Users familiar with the syntax of CREATE TABLE may notice that the
    VARCHAR columns were generated without a length; on SQLite and Postgresql,
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
           Column('password', String(12))
        )

    We include this more verbose :class:`~.schema.Table` construct separately
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

    >>> ins.compile().params #doctest: +NORMALIZE_WHITESPACE
    {'fullname': 'Jack Jones', 'name': 'jack'}

Executing
==========

The interesting part of an :class:`~sqlalchemy.sql.expression.Insert` is
executing it. In this tutorial, we will generally focus on the most explicit
method of executing a SQL construct, and later touch upon some "shortcut" ways
to do it. The ``engine`` object we created is a repository for database
connections capable of issuing SQL to the database. To acquire a connection,
we use the ``connect()`` method::

    >>> conn = engine.connect()
    >>> conn #doctest: +ELLIPSIS
    <sqlalchemy.engine.base.Connection object at 0x...>

The :class:`~sqlalchemy.engine.base.Connection` object represents an actively
checked out DBAPI connection resource. Lets feed it our
:class:`~sqlalchemy.sql.expression.Insert` object and see what happens:

.. sourcecode:: pycon+sql

    >>> result = conn.execute(ins)
    {opensql}INSERT INTO users (name, fullname) VALUES (?, ?)
    ('jack', 'Jack Jones')
    COMMIT

So the INSERT statement was now issued to the database. Although we got
positional "qmark" bind parameters instead of "named" bind parameters in the
output. How come ? Because when executed, the
:class:`~sqlalchemy.engine.base.Connection` used the SQLite **dialect** to
help generate the statement; when we use the ``str()`` function, the statement
isn't aware of this dialect, and falls back onto a default which uses named
parameters. We can view this manually as follows:

.. sourcecode:: pycon+sql

    >>> ins.bind = engine
    >>> str(ins)
    'INSERT INTO users (name, fullname) VALUES (?, ?)'

What about the ``result`` variable we got when we called ``execute()`` ? As
the SQLAlchemy :class:`~sqlalchemy.engine.base.Connection` object references a
DBAPI connection, the result, known as a
:class:`~sqlalchemy.engine.base.ResultProxy` object, is analogous to the DBAPI
cursor object. In the case of an INSERT, we can get important information from
it, such as the primary key values which were generated from our statement:

.. sourcecode:: pycon+sql

    >>> result.inserted_primary_key
    [1]

The value of ``1`` was automatically generated by SQLite, but only because we
did not specify the ``id`` column in our
:class:`~sqlalchemy.sql.expression.Insert` statement; otherwise, our explicit
value would have been used. In either case, SQLAlchemy always knows how to get
at a newly generated primary key value, even though the method of generating
them is different across different databases; each database's
:class:`~sqlalchemy.engine.base.Dialect` knows the specific steps needed to
determine the correct value (or values; note that ``inserted_primary_key``
returns a list so that it supports composite primary keys).

Executing Multiple Statements
==============================

Our insert example above was intentionally a little drawn out to show some
various behaviors of expression language constructs. In the usual case, an
:class:`~sqlalchemy.sql.expression.Insert` statement is usually compiled
against the parameters sent to the ``execute()`` method on
:class:`~sqlalchemy.engine.base.Connection`, so that there's no need to use
the ``values`` keyword with :class:`~sqlalchemy.sql.expression.Insert`. Lets
create a generic :class:`~sqlalchemy.sql.expression.Insert` statement again
and use it in the "normal" way:

.. sourcecode:: pycon+sql

    >>> ins = users.insert()
    >>> conn.execute(ins, id=2, name='wendy', fullname='Wendy Williams') # doctest: +ELLIPSIS
    {opensql}INSERT INTO users (id, name, fullname) VALUES (?, ?, ?)
    (2, 'wendy', 'Wendy Williams')
    COMMIT
    {stop}<sqlalchemy.engine.base.ResultProxy object at 0x...>

Above, because we specified all three columns in the the ``execute()`` method,
the compiled :class:`~sqlalchemy.sql.expression.Insert` included all three
columns. The :class:`~sqlalchemy.sql.expression.Insert` statement is compiled
at execution time based on the parameters we specified; if we specified fewer
parameters, the :class:`~sqlalchemy.sql.expression.Insert` would have fewer
entries in its VALUES clause.

To issue many inserts using DBAPI's ``executemany()`` method, we can send in a
list of dictionaries each containing a distinct set of parameters to be
inserted, as we do here to add some email addresses:

.. sourcecode:: pycon+sql

    >>> conn.execute(addresses.insert(), [ # doctest: +ELLIPSIS
    ...    {'user_id': 1, 'email_address' : 'jack@yahoo.com'},
    ...    {'user_id': 1, 'email_address' : 'jack@msn.com'},
    ...    {'user_id': 2, 'email_address' : 'www@www.org'},
    ...    {'user_id': 2, 'email_address' : 'wendy@aol.com'},
    ... ])
    {opensql}INSERT INTO addresses (user_id, email_address) VALUES (?, ?)
    ((1, 'jack@yahoo.com'), (1, 'jack@msn.com'), (2, 'www@www.org'), (2, 'wendy@aol.com'))
    COMMIT
    {stop}<sqlalchemy.engine.base.ResultProxy object at 0x...>

Above, we again relied upon SQLite's automatic generation of primary key
identifiers for each ``addresses`` row.

When executing multiple sets of parameters, each dictionary must have the
**same** set of keys; i.e. you cant have fewer keys in some dictionaries than
others. This is because the :class:`~sqlalchemy.sql.expression.Insert`
statement is compiled against the **first** dictionary in the list, and it's
assumed that all subsequent argument dictionaries are compatible with that
statement.

Connectionless / Implicit Execution
====================================

We're executing our :class:`~sqlalchemy.sql.expression.Insert` using a
:class:`~sqlalchemy.engine.base.Connection`. There's two options that allow
you to not have to deal with the connection part. You can execute in the
**connectionless** style, using the engine, which checks out from the
connection pool a connection for you, performs the execute operation with that
connection, and then checks the connection back into the pool upon completion
of the operation:

.. sourcecode:: pycon+sql

    {sql}>>> result = engine.execute(users.insert(), name='fred', fullname="Fred Flintstone")
    INSERT INTO users (name, fullname) VALUES (?, ?)
    ('fred', 'Fred Flintstone')
    COMMIT

and you can save even more steps than that, if you connect the
:class:`~sqlalchemy.engine.base.Engine` to the
:class:`~sqlalchemy.schema.MetaData` object we created earlier. When this is
done, all SQL expressions which involve tables within the
:class:`~sqlalchemy.schema.MetaData` object will be automatically **bound** to
the :class:`~sqlalchemy.engine.base.Engine`. In this case, we call it
**implicit execution**:

.. sourcecode:: pycon+sql

    >>> metadata.bind = engine
    {sql}>>> result = users.insert().execute(name="mary", fullname="Mary Contrary")
    INSERT INTO users (name, fullname) VALUES (?, ?)
    ('mary', 'Mary Contrary')
    COMMIT

When the :class:`~sqlalchemy.schema.MetaData` is bound, statements will also
compile against the engine's dialect. Since a lot of the examples here assume
the default dialect, we'll detach the engine from the metadata which we just
attached:

.. sourcecode:: pycon+sql

    >>> metadata.bind = None

Detailed examples of connectionless and implicit execution are available in
the "Engines" chapter: :ref:`dbengine_implicit`.

.. _coretutorial_selecting:

Selecting
==========

We began with inserts just so that our test database had some data in it. The
more interesting part of the data is selecting it ! We'll cover UPDATE and
DELETE statements later. The primary construct used to generate SELECT
statements is the :func:`.select` function:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import select
    >>> s = select([users])
    >>> result = conn.execute(s)  # doctest: +NORMALIZE_WHITESPACE
    {opensql}SELECT users.id, users.name, users.fullname
    FROM users
    ()

Above, we issued a basic :func:`.select` call, placing the ``users`` table
within the COLUMNS clause of the select, and then executing. SQLAlchemy
expanded the ``users`` table into the set of each of its columns, and also
generated a FROM clause for us. The result returned is again a
:class:`~sqlalchemy.engine.base.ResultProxy` object, which acts much like a
DBAPI cursor, including methods such as
:func:`~sqlalchemy.engine.base.ResultProxy.fetchone` and
:func:`~sqlalchemy.engine.base.ResultProxy.fetchall`. The easiest way to get
rows from it is to just iterate:

.. sourcecode:: pycon+sql

    >>> for row in result:
    ...     print row
    (1, u'jack', u'Jack Jones')
    (2, u'wendy', u'Wendy Williams')
    (3, u'fred', u'Fred Flintstone')
    (4, u'mary', u'Mary Contrary')

Above, we see that printing each row produces a simple tuple-like result. We
have more options at accessing the data in each row. One very common way is
through dictionary access, using the string names of columns:

.. sourcecode:: pycon+sql

    {sql}>>> result = conn.execute(s)  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname
    FROM users
    ()

    {stop}>>> row = result.fetchone()
    >>> print "name:", row['name'], "; fullname:", row['fullname']
    name: jack ; fullname: Jack Jones

Integer indexes work as well:

.. sourcecode:: pycon+sql

    >>> row = result.fetchone()
    >>> print "name:", row[1], "; fullname:", row[2]
    name: wendy ; fullname: Wendy Williams

But another way, whose usefulness will become apparent later on, is to use the
:class:`~sqlalchemy.schema.Column` objects directly as keys:

.. sourcecode:: pycon+sql

    {sql}>>> for row in conn.execute(s):  # doctest: +NORMALIZE_WHITESPACE
    ...     print "name:", row[users.c.name], "; fullname:", row[users.c.fullname]
    SELECT users.id, users.name, users.fullname
    FROM users
    ()
    {stop}name: jack ; fullname: Jack Jones
    name: wendy ; fullname: Wendy Williams
    name: fred ; fullname: Fred Flintstone
    name: mary ; fullname: Mary Contrary

Result sets which have pending rows remaining should be explicitly closed
before discarding. While the cursor and connection resources referenced by the
:class:`~sqlalchemy.engine.base.ResultProxy` will be respectively closed and
returned to the connection pool when the object is garbage collected, it's
better to make it explicit as some database APIs are very picky about such
things:

.. sourcecode:: pycon+sql

    >>> result.close()

If we'd like to more carefully control the columns which are placed in the
COLUMNS clause of the select, we reference individual
:class:`~sqlalchemy.schema.Column` objects from our
:class:`~sqlalchemy.schema.Table`. These are available as named attributes off
the ``c`` attribute of the :class:`~sqlalchemy.schema.Table` object:

.. sourcecode:: pycon+sql

    >>> s = select([users.c.name, users.c.fullname])
    {sql}>>> result = conn.execute(s)  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.name, users.fullname
    FROM users
    ()
    {stop}>>> for row in result:  #doctest: +NORMALIZE_WHITESPACE
    ...     print row
    (u'jack', u'Jack Jones')
    (u'wendy', u'Wendy Williams')
    (u'fred', u'Fred Flintstone')
    (u'mary', u'Mary Contrary')

Lets observe something interesting about the FROM clause. Whereas the
generated statement contains two distinct sections, a "SELECT columns" part
and a "FROM table" part, our :func:`.select` construct only has a list
containing columns. How does this work ? Let's try putting *two* tables into
our :func:`.select` statement:

.. sourcecode:: pycon+sql

    {sql}>>> for row in conn.execute(select([users, addresses])):
    ...     print row  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname, addresses.id, addresses.user_id, addresses.email_address
    FROM users, addresses
    ()
    {stop}(1, u'jack', u'Jack Jones', 1, 1, u'jack@yahoo.com')
    (1, u'jack', u'Jack Jones', 2, 1, u'jack@msn.com')
    (1, u'jack', u'Jack Jones', 3, 2, u'www@www.org')
    (1, u'jack', u'Jack Jones', 4, 2, u'wendy@aol.com')
    (2, u'wendy', u'Wendy Williams', 1, 1, u'jack@yahoo.com')
    (2, u'wendy', u'Wendy Williams', 2, 1, u'jack@msn.com')
    (2, u'wendy', u'Wendy Williams', 3, 2, u'www@www.org')
    (2, u'wendy', u'Wendy Williams', 4, 2, u'wendy@aol.com')
    (3, u'fred', u'Fred Flintstone', 1, 1, u'jack@yahoo.com')
    (3, u'fred', u'Fred Flintstone', 2, 1, u'jack@msn.com')
    (3, u'fred', u'Fred Flintstone', 3, 2, u'www@www.org')
    (3, u'fred', u'Fred Flintstone', 4, 2, u'wendy@aol.com')
    (4, u'mary', u'Mary Contrary', 1, 1, u'jack@yahoo.com')
    (4, u'mary', u'Mary Contrary', 2, 1, u'jack@msn.com')
    (4, u'mary', u'Mary Contrary', 3, 2, u'www@www.org')
    (4, u'mary', u'Mary Contrary', 4, 2, u'wendy@aol.com')

It placed **both** tables into the FROM clause. But also, it made a real mess.
Those who are familiar with SQL joins know that this is a **Cartesian
product**; each row from the ``users`` table is produced against each row from
the ``addresses`` table. So to put some sanity into this statement, we need a
WHERE clause. Which brings us to the second argument of :func:`.select`:

.. sourcecode:: pycon+sql

    >>> s = select([users, addresses], users.c.id==addresses.c.user_id)
    {sql}>>> for row in conn.execute(s):
    ...     print row  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname, addresses.id, addresses.user_id, addresses.email_address
    FROM users, addresses
    WHERE users.id = addresses.user_id
    ()
    {stop}(1, u'jack', u'Jack Jones', 1, 1, u'jack@yahoo.com')
    (1, u'jack', u'Jack Jones', 2, 1, u'jack@msn.com')
    (2, u'wendy', u'Wendy Williams', 3, 2, u'www@www.org')
    (2, u'wendy', u'Wendy Williams', 4, 2, u'wendy@aol.com')

So that looks a lot better, we added an expression to our :func:`.select`
which had the effect of adding ``WHERE users.id = addresses.user_id`` to our
statement, and our results were managed down so that the join of ``users`` and
``addresses`` rows made sense. But let's look at that expression? It's using
just a Python equality operator between two different
:class:`~sqlalchemy.schema.Column` objects. It should be clear that something
is up. Saying ``1==1`` produces ``True``, and ``1==2`` produces ``False``, not
a WHERE clause. So lets see exactly what that expression is doing:

.. sourcecode:: pycon+sql

    >>> users.c.id==addresses.c.user_id #doctest: +ELLIPSIS
    <sqlalchemy.sql.expression._BinaryExpression object at 0x...>

Wow, surprise ! This is neither a ``True`` nor a ``False``. Well what is it ?

.. sourcecode:: pycon+sql

    >>> str(users.c.id==addresses.c.user_id)
    'users.id = addresses.user_id'

As you can see, the ``==`` operator is producing an object that is very much
like the :class:`~sqlalchemy.sql.expression.Insert` and :func:`.select`
objects we've made so far, thanks to Python's ``__eq__()`` builtin; you call
``str()`` on it and it produces SQL. By now, one can see that everything we
are working with is ultimately the same type of object. SQLAlchemy terms the
base class of all of these expressions as ``sqlalchemy.sql.ClauseElement``.

Operators
==========

Since we've stumbled upon SQLAlchemy's operator paradigm, let's go through
some of its capabilities. We've seen how to equate two columns to each other:

.. sourcecode:: pycon+sql

    >>> print users.c.id==addresses.c.user_id
    users.id = addresses.user_id

If we use a literal value (a literal meaning, not a SQLAlchemy clause object),
we get a bind parameter:

.. sourcecode:: pycon+sql

    >>> print users.c.id==7
    users.id = :id_1

The ``7`` literal is embedded in
:class:`~sqlalchemy.sql.expression.ClauseElement`; we can use the same trick
we did with the :class:`~sqlalchemy.sql.expression.Insert` object to see it:

.. sourcecode:: pycon+sql

    >>> (users.c.id==7).compile().params
    {u'id_1': 7}

Most Python operators, as it turns out, produce a SQL expression here, like
equals, not equals, etc.:

.. sourcecode:: pycon+sql

    >>> print users.c.id != 7
    users.id != :id_1

    >>> # None converts to IS NULL
    >>> print users.c.name == None
    users.name IS NULL

    >>> # reverse works too
    >>> print 'fred' > users.c.name
    users.name < :name_1

If we add two integer columns together, we get an addition expression:

.. sourcecode:: pycon+sql

    >>> print users.c.id + addresses.c.id
    users.id + addresses.id

Interestingly, the type of the :class:`~sqlalchemy.schema.Column` is important
! If we use ``+`` with two string based columns (recall we put types like
:class:`~sqlalchemy.types.Integer` and :class:`~sqlalchemy.types.String` on
our :class:`~sqlalchemy.schema.Column` objects at the beginning), we get
something different:

.. sourcecode:: pycon+sql

    >>> print users.c.name + users.c.fullname
    users.name || users.fullname

Where ``||`` is the string concatenation operator used on most databases. But
not all of them. MySQL users, fear not:

.. sourcecode:: pycon+sql

    >>> print (users.c.name + users.c.fullname).compile(bind=create_engine('mysql://'))
    concat(users.name, users.fullname)

The above illustrates the SQL that's generated for an
:class:`~sqlalchemy.engine.base.Engine` that's connected to a MySQL database;
the ``||`` operator now compiles as MySQL's ``concat()`` function.

If you have come across an operator which really isn't available, you can
always use the ``op()`` method; this generates whatever operator you need:

.. sourcecode:: pycon+sql

    >>> print users.c.name.op('tiddlywinks')('foo')
    users.name tiddlywinks :name_1

This function can also be used to make bitwise operators explicit. For example::

    somecolumn.op('&')(0xff)

is a bitwise AND of the value in `somecolumn`.

Conjunctions
=============


We'd like to show off some of our operators inside of :func:`.select`
constructs. But we need to lump them together a little more, so let's first
introduce some conjunctions. Conjunctions are those little words like AND and
OR that put things together. We'll also hit upon NOT. AND, OR and NOT can work
from the corresponding functions SQLAlchemy provides (notice we also throw in
a LIKE):

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import and_, or_, not_
    >>> print and_(users.c.name.like('j%'), users.c.id==addresses.c.user_id, #doctest: +NORMALIZE_WHITESPACE
    ...     or_(addresses.c.email_address=='wendy@aol.com', addresses.c.email_address=='jack@yahoo.com'),
    ...     not_(users.c.id>5))
    users.name LIKE :name_1 AND users.id = addresses.user_id AND
    (addresses.email_address = :email_address_1 OR addresses.email_address = :email_address_2)
    AND users.id <= :id_1

And you can also use the re-jiggered bitwise AND, OR and NOT operators,
although because of Python operator precedence you have to watch your
parenthesis:

.. sourcecode:: pycon+sql

    >>> print users.c.name.like('j%') & (users.c.id==addresses.c.user_id) &  \
    ...     ((addresses.c.email_address=='wendy@aol.com') | (addresses.c.email_address=='jack@yahoo.com')) \
    ...     & ~(users.c.id>5) # doctest: +NORMALIZE_WHITESPACE
    users.name LIKE :name_1 AND users.id = addresses.user_id AND
    (addresses.email_address = :email_address_1 OR addresses.email_address = :email_address_2)
    AND users.id <= :id_1

So with all of this vocabulary, let's select all users who have an email
address at AOL or MSN, whose name starts with a letter between "m" and "z",
and we'll also generate a column containing their full name combined with
their email address. We will add two new constructs to this statement,
``between()`` and ``label()``. ``between()`` produces a BETWEEN clause, and
``label()`` is used in a column expression to produce labels using the ``AS``
keyword; it's recommended when selecting from expressions that otherwise would
not have a name:

.. sourcecode:: pycon+sql

    >>> s = select([(users.c.fullname + ", " + addresses.c.email_address).label('title')],
    ...        and_(
    ...            users.c.id==addresses.c.user_id,
    ...            users.c.name.between('m', 'z'),
    ...           or_(
    ...              addresses.c.email_address.like('%@aol.com'),
    ...              addresses.c.email_address.like('%@msn.com')
    ...           )
    ...        )
    ...    )
    >>> print conn.execute(s).fetchall() #doctest: +NORMALIZE_WHITESPACE
    SELECT users.fullname || ? || addresses.email_address AS title
    FROM users, addresses
    WHERE users.id = addresses.user_id AND users.name BETWEEN ? AND ? AND
    (addresses.email_address LIKE ? OR addresses.email_address LIKE ?)
    (', ', 'm', 'z', '%@aol.com', '%@msn.com')
    [(u'Wendy Williams, wendy@aol.com',)]

Once again, SQLAlchemy figured out the FROM clause for our statement. In fact
it will determine the FROM clause based on all of its other bits; the columns
clause, the where clause, and also some other elements which we haven't
covered yet, which include ORDER BY, GROUP BY, and HAVING.

.. _sqlexpression_text:

Using Text
===========

Our last example really became a handful to type. Going from what one
understands to be a textual SQL expression into a Python construct which
groups components together in a programmatic style can be hard. That's why
SQLAlchemy lets you just use strings too. The ``text()`` construct represents
any textual statement. To use bind parameters with ``text()``, always use the
named colon format. Such as below, we create a ``text()`` and execute it,
feeding in the bind parameters to the ``execute()`` method:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import text
    >>> s = text("""SELECT users.fullname || ', ' || addresses.email_address AS title
    ...            FROM users, addresses
    ...            WHERE users.id = addresses.user_id AND users.name BETWEEN :x AND :y AND
    ...            (addresses.email_address LIKE :e1 OR addresses.email_address LIKE :e2)
    ...        """)
    {sql}>>> print conn.execute(s, x='m', y='z', e1='%@aol.com', e2='%@msn.com').fetchall() # doctest:+NORMALIZE_WHITESPACE
    SELECT users.fullname || ', ' || addresses.email_address AS title
    FROM users, addresses
    WHERE users.id = addresses.user_id AND users.name BETWEEN ? AND ? AND
    (addresses.email_address LIKE ? OR addresses.email_address LIKE ?)
    ('m', 'z', '%@aol.com', '%@msn.com')
    {stop}[(u'Wendy Williams, wendy@aol.com',)]

To gain a "hybrid" approach, the `select()` construct accepts strings for most
of its arguments. Below we combine the usage of strings with our constructed
:func:`.select` object, by using the :func:`.select` object to structure the
statement, and strings to provide all the content within the structure. For
this example, SQLAlchemy is not given any :class:`~sqlalchemy.schema.Column`
or :class:`~sqlalchemy.schema.Table` objects in any of its expressions, so it
cannot generate a FROM clause. So we also give it the ``from_obj`` keyword
argument, which is a list of ``ClauseElements`` (or strings) to be placed
within the FROM clause:

.. sourcecode:: pycon+sql

    >>> s = select(["users.fullname || ', ' || addresses.email_address AS title"],
    ...        and_(
    ...            "users.id = addresses.user_id",
    ...             "users.name BETWEEN 'm' AND 'z'",
    ...             "(addresses.email_address LIKE :x OR addresses.email_address LIKE :y)"
    ...        ),
    ...         from_obj=['users', 'addresses']
    ...    )
    {sql}>>> print conn.execute(s, x='%@aol.com', y='%@msn.com').fetchall() #doctest: +NORMALIZE_WHITESPACE
    SELECT users.fullname || ', ' || addresses.email_address AS title
    FROM users, addresses
    WHERE users.id = addresses.user_id AND users.name BETWEEN 'm' AND 'z' AND (addresses.email_address LIKE ? OR addresses.email_address LIKE ?)
    ('%@aol.com', '%@msn.com')
    {stop}[(u'Wendy Williams, wendy@aol.com',)]

Going from constructed SQL to text, we lose some capabilities. We lose the
capability for SQLAlchemy to compile our expression to a specific target
database; above, our expression won't work with MySQL since it has no ``||``
construct. It also becomes more tedious for SQLAlchemy to be made aware of the
datatypes in use; for example, if our bind parameters required UTF-8 encoding
before going in, or conversion from a Python ``datetime`` into a string (as is
required with SQLite), we would have to add extra information to our
``text()`` construct. Similar issues arise on the result set side, where
SQLAlchemy also performs type-specific data conversion in some cases; still
more information can be added to ``text()`` to work around this. But what we
really lose from our statement is the ability to manipulate it, transform it,
and analyze it. These features are critical when using the ORM, which makes
heavy usage of relational transformations. To show off what we mean, we'll
first introduce the ALIAS construct and the JOIN construct, just so we have
some juicier bits to play with.

Using Aliases
==============

The alias in SQL corresponds to a "renamed" version of a table or SELECT
statement, which occurs anytime you say "SELECT .. FROM sometable AS
someothername". The ``AS`` creates a new name for the table. Aliases are a key
construct as they allow any table or subquery to be referenced by a unique
name. In the case of a table, this allows the same table to be named in the
FROM clause multiple times. In the case of a SELECT statement, it provides a
parent name for the columns represented by the statement, allowing them to be
referenced relative to this name.

In SQLAlchemy, any :class:`.Table`, :func:`.select` construct, or
other selectable can be turned into an alias using the :meth:`.FromClause.alias`
method, which produces a :class:`.Alias` construct.  As an example, suppose we know that our user ``jack`` has two
particular email addresses. How can we locate jack based on the combination of those two
addresses?   To accomplish this, we'd use a join to the ``addresses`` table,
once for each address.   We create two :class:`.Alias` constructs against
``addresses``, and then use them both within a :func:`.select` construct:

.. sourcecode:: pycon+sql

    >>> a1 = addresses.alias()
    >>> a2 = addresses.alias()
    >>> s = select([users], and_(
    ...        users.c.id==a1.c.user_id,
    ...        users.c.id==a2.c.user_id,
    ...        a1.c.email_address=='jack@msn.com',
    ...        a2.c.email_address=='jack@yahoo.com'
    ...   ))
    {sql}>>> print conn.execute(s).fetchall()  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname
    FROM users, addresses AS addresses_1, addresses AS addresses_2
    WHERE users.id = addresses_1.user_id AND users.id = addresses_2.user_id AND addresses_1.email_address = ? AND addresses_2.email_address = ?
    ('jack@msn.com', 'jack@yahoo.com')
    {stop}[(1, u'jack', u'Jack Jones')]

Note that the :class:`.Alias` construct generated the names ``addresses_1`` and 
``addresses_2`` in the final SQL result.  The generation of these names is determined
by the position of the construct within the statement.   If we created a query using
only the second ``a2`` alias, the name would come out as ``addresses_1``.  The 
generation of the names is also *deterministic*, meaning the same SQLAlchemy 
statement construct will produce the identical SQL string each time it is 
rendered for a particular dialect.

Since on the outside, we refer to the alias using the :class:`.Alias` construct
itself, we don't need to be concerned about the generated name.  However, for
the purposes of debugging, it can be specified by passing a string name
to the :meth:`.FromClause.alias` method::

    >>> a1 = addresses.alias('a1')

Aliases can of course be used for anything which you can SELECT from,
including SELECT statements themselves. We can self-join the ``users`` table
back to the :func:`.select` we've created by making an alias of the entire
statement. The ``correlate(None)`` directive is to avoid SQLAlchemy's attempt
to "correlate" the inner ``users`` table with the outer one:

.. sourcecode:: pycon+sql

    >>> a1 = s.correlate(None).alias()
    >>> s = select([users.c.name], users.c.id==a1.c.id)
    {sql}>>> print conn.execute(s).fetchall()  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.name
    FROM users, (SELECT users.id AS id, users.name AS name, users.fullname AS fullname
    FROM users, addresses AS addresses_1, addresses AS addresses_2
    WHERE users.id = addresses_1.user_id AND users.id = addresses_2.user_id AND addresses_1.email_address = ? AND addresses_2.email_address = ?) AS anon_1
    WHERE users.id = anon_1.id
    ('jack@msn.com', 'jack@yahoo.com')
    {stop}[(u'jack',)]

Using Joins
============


We're halfway along to being able to construct any SELECT expression. The next
cornerstone of the SELECT is the JOIN expression. We've already been doing
joins in our examples, by just placing two tables in either the columns clause
or the where clause of the :func:`.select` construct. But if we want to make a
real "JOIN" or "OUTERJOIN" construct, we use the ``join()`` and
``outerjoin()`` methods, most commonly accessed from the left table in the
join:

.. sourcecode:: pycon+sql

    >>> print users.join(addresses)
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

    >>> print users.join(addresses, addresses.c.email_address.like(users.c.name + '%'))
    users JOIN addresses ON addresses.email_address LIKE users.name || :name_1

When we create a :func:`.select` construct, SQLAlchemy looks around at the
tables we've mentioned and then places them in the FROM clause of the
statement. When we use JOINs however, we know what FROM clause we want, so
here we make usage of the ``from_obj`` keyword argument:

.. sourcecode:: pycon+sql

    >>> s = select([users.c.fullname], from_obj=[
    ...    users.join(addresses, addresses.c.email_address.like(users.c.name + '%'))
    ...    ])
    {sql}>>> print conn.execute(s).fetchall()  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.fullname
    FROM users JOIN addresses ON addresses.email_address LIKE users.name || ?
    ('%',)
    {stop}[(u'Jack Jones',), (u'Jack Jones',), (u'Wendy Williams',)]

The ``outerjoin()`` function just creates ``LEFT OUTER JOIN`` constructs. It's
used just like ``join()``:

.. sourcecode:: pycon+sql

    >>> s = select([users.c.fullname], from_obj=[users.outerjoin(addresses)])
    >>> print s  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.fullname
    FROM users LEFT OUTER JOIN addresses ON users.id = addresses.user_id

That's the output ``outerjoin()`` produces, unless, of course, you're stuck in
a gig using Oracle prior to version 9, and you've set up your engine (which
would be using ``OracleDialect``) to use Oracle-specific SQL:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.dialects.oracle import dialect as OracleDialect
    >>> print s.compile(dialect=OracleDialect(use_ansi=False))  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.fullname
    FROM users, addresses
    WHERE users.id = addresses.user_id(+)

If you don't know what that SQL means, don't worry ! The secret tribe of
Oracle DBAs don't want their black magic being found out ;).

Intro to Generative Selects
================================================

We've now gained the ability to construct very sophisticated statements. We
can use all kinds of operators, table constructs, text, joins, and aliases.
The point of all of this, as mentioned earlier, is not that it's an "easier"
or "better" way to write SQL than just writing a SQL statement yourself; the
point is that it's better for writing *programmatically generated* SQL which
can be morphed and adapted as needed in automated scenarios.

To support this, the :func:`.select` construct we've been working with
supports piecemeal construction, in addition to the "all at once" method we've
been doing. Suppose you're writing a search function, which receives criterion
and then must construct a select from it. To accomplish this, upon each
criterion encountered, you apply "generative" criterion to an existing
:func:`.select` construct with new elements, one at a time. We start with a
basic :func:`.select` constructed with the shortcut method available on the
``users`` table:

.. sourcecode:: pycon+sql

    >>> query = users.select()
    >>> print query  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname
    FROM users

We encounter search criterion of "name='jack'". So we apply WHERE criterion
stating such:

.. sourcecode:: pycon+sql

    >>> query = query.where(users.c.name=='jack')

Next, we encounter that they'd like the results in descending order by full
name. We apply ORDER BY, using an extra modifier ``desc``:

.. sourcecode:: pycon+sql

    >>> query = query.order_by(users.c.fullname.desc())

We also come across that they'd like only users who have an address at MSN. A
quick way to tack this on is by using an EXISTS clause, which we correlate to
the ``users`` table in the enclosing SELECT:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import exists
    >>> query = query.where(
    ...    exists([addresses.c.id],
    ...        and_(addresses.c.user_id==users.c.id, addresses.c.email_address.like('%@msn.com'))
    ...    ).correlate(users))

And finally, the application also wants to see the listing of email addresses
at once; so to save queries, we outerjoin the ``addresses`` table (using an
outer join so that users with no addresses come back as well; since we're
programmatic, we might not have kept track that we used an EXISTS clause
against the ``addresses`` table too...). Additionally, since the ``users`` and
``addresses`` table both have a column named ``id``, let's isolate their names
from each other in the COLUMNS clause by using labels:

.. sourcecode:: pycon+sql

    >>> query = query.column(addresses).select_from(users.outerjoin(addresses)).apply_labels()

Let's bake for .0001 seconds and see what rises:

.. sourcecode:: pycon+sql

    >>> conn.execute(query).fetchall()  # doctest: +NORMALIZE_WHITESPACE
    {opensql}SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, addresses.id AS addresses_id, addresses.user_id AS addresses_user_id, addresses.email_address AS addresses_email_address
    FROM users LEFT OUTER JOIN addresses ON users.id = addresses.user_id
    WHERE users.name = ? AND (EXISTS (SELECT addresses.id
    FROM addresses
    WHERE addresses.user_id = users.id AND addresses.email_address LIKE ?)) ORDER BY users.fullname DESC
    ('jack', '%@msn.com')
    {stop}[(1, u'jack', u'Jack Jones', 1, 1, u'jack@yahoo.com'), (1, u'jack', u'Jack Jones', 2, 1, u'jack@msn.com')]

The generative approach is about starting small, adding one thing at a time,
to arrive with a full statement.

Transforming a Statement
------------------------

We've seen how methods like :meth:`.Select.where` and :meth:`._SelectBase.order_by` are 
part of the so-called *Generative* family of methods on the :func:`.select` construct,
where one :func:`.select` copies itself to return a new one with modifications.
SQL constructs also support another form of generative behavior which is
the *transformation*.   This is an advanced technique that most core applications
won't use directly; however, it is a system which the ORM relies on heavily,
and can be useful for any system that deals with generalized behavior of Core SQL
constructs.

Using a transformation we can take our ``users``/``addresses`` query and replace
all occurrences of ``addresses`` with an alias of itself.   That is, anywhere
that ``addresses`` is referred to in the original query, the new query will
refer to ``addresses_1``, which is selected as ``addresses AS addresses_1``.
The :meth:`.FromClause.replace_selectable` method can achieve this:

.. sourcecode:: pycon+sql

    >>> a1 = addresses.alias()
    >>> query = query.replace_selectable(addresses, a1)
    >>> print query  # doctest: +NORMALIZE_WHITESPACE
    {opensql}SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, addresses_1.id AS addresses_1_id, addresses_1.user_id AS addresses_1_user_id, addresses_1.email_address AS addresses_1_email_address
    FROM users LEFT OUTER JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE users.name = :name_1 AND (EXISTS (SELECT addresses_1.id
    FROM addresses AS addresses_1
    WHERE addresses_1.user_id = users.id AND addresses_1.email_address LIKE :email_address_1)) ORDER BY users.fullname DESC

For a query such as the above, we can access the columns referred
to by the ``a1`` alias in a result set using the :class:`.Column` objects
present directly on ``a1``:

.. sourcecode:: pycon+sql

    {sql}>>> for row in conn.execute(query):
    ...     print "Name:", row[users.c.name], "; Email Address", row[a1.c.email_address]  # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, addresses_1.id AS addresses_1_id, addresses_1.user_id AS addresses_1_user_id, addresses_1.email_address AS addresses_1_email_address
    FROM users LEFT OUTER JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE users.name = ? AND (EXISTS (SELECT addresses_1.id
    FROM addresses AS addresses_1
    WHERE addresses_1.user_id = users.id AND addresses_1.email_address LIKE ?)) ORDER BY users.fullname DESC
    ('jack', '%@msn.com')
    {stop}Name: jack ; Email Address jack@yahoo.com
    Name: jack ; Email Address jack@msn.com

Everything Else
================

The concepts of creating SQL expressions have been introduced. What's left are
more variants of the same themes. So now we'll catalog the rest of the
important things we'll need to know.

Bind Parameter Objects
----------------------

Throughout all these examples, SQLAlchemy is busy creating bind parameters
wherever literal expressions occur. You can also specify your own bind
parameters with your own names, and use the same statement repeatedly. The
database dialect converts to the appropriate named or positional style, as
here where it converts to positional for SQLite:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import bindparam
    >>> s = users.select(users.c.name==bindparam('username'))
    {sql}>>> conn.execute(s, username='wendy').fetchall() # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname
    FROM users
    WHERE users.name = ?
    ('wendy',)
    {stop}[(2, u'wendy', u'Wendy Williams')]

Another important aspect of bind parameters is that they may be assigned a
type. The type of the bind parameter will determine its behavior within
expressions and also how the data bound to it is processed before being sent
off to the database:

.. sourcecode:: pycon+sql

    >>> s = users.select(users.c.name.like(bindparam('username', type_=String) + text("'%'")))
    {sql}>>> conn.execute(s, username='wendy').fetchall() # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname
    FROM users
    WHERE users.name LIKE ? || '%'
    ('wendy',)
    {stop}[(2, u'wendy', u'Wendy Williams')]


Bind parameters of the same name can also be used multiple times, where only a
single named value is needed in the execute parameters:

.. sourcecode:: pycon+sql

    >>> s = select([users, addresses],
    ...    users.c.name.like(bindparam('name', type_=String) + text("'%'")) |
    ...    addresses.c.email_address.like(bindparam('name', type_=String) + text("'@%'")),
    ...    from_obj=[users.outerjoin(addresses)])
    {sql}>>> conn.execute(s, name='jack').fetchall() # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname, addresses.id, addresses.user_id, addresses.email_address
    FROM users LEFT OUTER JOIN addresses ON users.id = addresses.user_id
    WHERE users.name LIKE ? || '%' OR addresses.email_address LIKE ? || '@%'
    ('jack', 'jack')
    {stop}[(1, u'jack', u'Jack Jones', 1, 1, u'jack@yahoo.com'), (1, u'jack', u'Jack Jones', 2, 1, u'jack@msn.com')]

Functions
---------

SQL functions are created using the :attr:`~.expression.func` keyword, which
generates functions using attribute access:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import func
    >>> print func.now()
    now()

    >>> print func.concat('x', 'y')
    concat(:param_1, :param_2)

By "generates", we mean that **any** SQL function is created based on the word
you choose::

    >>> print func.xyz_my_goofy_function() # doctest: +NORMALIZE_WHITESPACE
    xyz_my_goofy_function()

Certain function names are known by SQLAlchemy, allowing special behavioral
rules to be applied. Some for example are "ANSI" functions, which mean they
don't get the parenthesis added after them, such as CURRENT_TIMESTAMP:

.. sourcecode:: pycon+sql

    >>> print func.current_timestamp()
    CURRENT_TIMESTAMP

Functions are most typically used in the columns clause of a select statement,
and can also be labeled as well as given a type. Labeling a function is
recommended so that the result can be targeted in a result row based on a
string name, and assigning it a type is required when you need result-set
processing to occur, such as for Unicode conversion and date conversions.
Below, we use the result function ``scalar()`` to just read the first column
of the first row and then close the result; the label, even though present, is
not important in this case:

.. sourcecode:: pycon+sql

    >>> print conn.execute(
    ...     select([func.max(addresses.c.email_address, type_=String).label('maxemail')])
    ... ).scalar() # doctest: +NORMALIZE_WHITESPACE
    {opensql}SELECT max(addresses.email_address) AS maxemail
    FROM addresses
    ()
    {stop}www@www.org

Databases such as PostgreSQL and Oracle which support functions that return
whole result sets can be assembled into selectable units, which can be used in
statements. Such as, a database function ``calculate()`` which takes the
parameters ``x`` and ``y``, and returns three columns which we'd like to name
``q``, ``z`` and ``r``, we can construct using "lexical" column objects as
well as bind parameters:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import column
    >>> calculate = select([column('q'), column('z'), column('r')],
    ...     from_obj=[func.calculate(bindparam('x'), bindparam('y'))])

    >>> print select([users], users.c.id > calculate.c.z) # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname
    FROM users, (SELECT q, z, r
    FROM calculate(:x, :y))
    WHERE users.id > z

If we wanted to use our ``calculate`` statement twice with different bind
parameters, the :func:`~sqlalchemy.sql.expression.ClauseElement.unique_params`
function will create copies for us, and mark the bind parameters as "unique"
so that conflicting names are isolated. Note we also make two separate aliases
of our selectable:

.. sourcecode:: pycon+sql

    >>> s = select([users], users.c.id.between(
    ...    calculate.alias('c1').unique_params(x=17, y=45).c.z,
    ...    calculate.alias('c2').unique_params(x=5, y=12).c.z))

    >>> print s # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, users.name, users.fullname
    FROM users, (SELECT q, z, r
    FROM calculate(:x_1, :y_1)) AS c1, (SELECT q, z, r
    FROM calculate(:x_2, :y_2)) AS c2
    WHERE users.id BETWEEN c1.z AND c2.z

    >>> s.compile().params
    {u'x_2': 5, u'y_2': 12, u'y_1': 45, u'x_1': 17}

See also :attr:`sqlalchemy.sql.expression.func`.

Window Functions
-----------------

Any :class:`.FunctionElement`, including functions generated by
:attr:`~.expression.func`, can be turned into a "window function", that is an
OVER clause, using the :meth:`~.FunctionElement.over` method:

.. sourcecode:: pycon+sql

    >>> s = select([users.c.id, func.row_number().over(order_by=users.c.name)])
    >>> print s # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id, row_number() OVER (ORDER BY users.name) AS anon_1 
    FROM users

Unions and Other Set Operations
-------------------------------

Unions come in two flavors, UNION and UNION ALL, which are available via
module level functions:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import union
    >>> u = union(
    ...     addresses.select(addresses.c.email_address=='foo@bar.com'),
    ...    addresses.select(addresses.c.email_address.like('%@yahoo.com')),
    ... ).order_by(addresses.c.email_address)

    {sql}>>> print conn.execute(u).fetchall() # doctest: +NORMALIZE_WHITESPACE
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address = ? UNION SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address LIKE ? ORDER BY addresses.email_address
    ('foo@bar.com', '%@yahoo.com')
    {stop}[(1, 1, u'jack@yahoo.com')]

Also available, though not supported on all databases, are ``intersect()``,
``intersect_all()``, ``except_()``, and ``except_all()``:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.sql import except_
    >>> u = except_(
    ...    addresses.select(addresses.c.email_address.like('%@%.com')),
    ...    addresses.select(addresses.c.email_address.like('%@msn.com'))
    ... )

    {sql}>>> print conn.execute(u).fetchall() # doctest: +NORMALIZE_WHITESPACE
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address LIKE ? EXCEPT SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address LIKE ?
    ('%@%.com', '%@msn.com')
    {stop}[(1, 1, u'jack@yahoo.com'), (4, 2, u'wendy@aol.com')]

A common issue with so-called "compound" selectables arises due to the fact
that they nest with parenthesis. SQLite in particular doesn't like a statement
that starts with parenthesis. So when nesting a "compound" inside a
"compound", it's often necessary to apply ``.alias().select()`` to the first
element of the outermost compound, if that element is also a compound. For
example, to nest a "union" and a "select" inside of "except\_", SQLite will
want the "union" to be stated as a subquery:

.. sourcecode:: pycon+sql

    >>> u = except_(
    ...    union(
    ...         addresses.select(addresses.c.email_address.like('%@yahoo.com')),
    ...         addresses.select(addresses.c.email_address.like('%@msn.com'))
    ...     ).alias().select(),   # apply subquery here
    ...    addresses.select(addresses.c.email_address.like('%@msn.com'))
    ... )
    {sql}>>> print conn.execute(u).fetchall()   # doctest: +NORMALIZE_WHITESPACE
    SELECT anon_1.id, anon_1.user_id, anon_1.email_address
    FROM (SELECT addresses.id AS id, addresses.user_id AS user_id,
    addresses.email_address AS email_address FROM addresses
    WHERE addresses.email_address LIKE ? UNION SELECT addresses.id AS id,
    addresses.user_id AS user_id, addresses.email_address AS email_address
    FROM addresses WHERE addresses.email_address LIKE ?) AS anon_1 EXCEPT
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    WHERE addresses.email_address LIKE ?
    ('%@yahoo.com', '%@msn.com', '%@msn.com')
    {stop}[(1, 1, u'jack@yahoo.com')]


Scalar Selects
--------------

To embed a SELECT in a column expression, use
:func:`~sqlalchemy.sql.expression._SelectBaseMixin.as_scalar`:

.. sourcecode:: pycon+sql

    {sql}>>> print conn.execute(select([   # doctest: +NORMALIZE_WHITESPACE
    ...       users.c.name,
    ...       select([func.count(addresses.c.id)], users.c.id==addresses.c.user_id).as_scalar()
    ...    ])).fetchall()
    SELECT users.name, (SELECT count(addresses.id) AS count_1
    FROM addresses
    WHERE users.id = addresses.user_id) AS anon_1
    FROM users
    ()
    {stop}[(u'jack', 2), (u'wendy', 2), (u'fred', 0), (u'mary', 0)]

Alternatively, applying a ``label()`` to a select evaluates it as a scalar as
well:

.. sourcecode:: pycon+sql

    {sql}>>> print conn.execute(select([    # doctest: +NORMALIZE_WHITESPACE
    ...       users.c.name,
    ...       select([func.count(addresses.c.id)], users.c.id==addresses.c.user_id).label('address_count')
    ...    ])).fetchall()
    SELECT users.name, (SELECT count(addresses.id) AS count_1
    FROM addresses
    WHERE users.id = addresses.user_id) AS address_count
    FROM users
    ()
    {stop}[(u'jack', 2), (u'wendy', 2), (u'fred', 0), (u'mary', 0)]

Correlated Subqueries
---------------------

Notice in the examples on "scalar selects", the FROM clause of each embedded
select did not contain the ``users`` table in its FROM clause. This is because
SQLAlchemy automatically attempts to correlate embedded FROM objects to that
of an enclosing query. To disable this, or to specify explicit FROM clauses to
be correlated, use ``correlate()``::

    >>> s = select([users.c.name], users.c.id==select([users.c.id]).correlate(None))
    >>> print s # doctest: +NORMALIZE_WHITESPACE
    SELECT users.name
    FROM users
    WHERE users.id = (SELECT users.id
    FROM users)

    >>> s = select([users.c.name, addresses.c.email_address], users.c.id==
    ...        select([users.c.id], users.c.id==addresses.c.user_id).correlate(addresses)
    ...    )
    >>> print s # doctest: +NORMALIZE_WHITESPACE
    SELECT users.name, addresses.email_address
    FROM users, addresses
    WHERE users.id = (SELECT users.id
    FROM users
    WHERE users.id = addresses.user_id)

Ordering, Grouping, Limiting, Offset...ing...
---------------------------------------------


The :func:`.select` function can take keyword arguments ``order_by``,
``group_by`` (as well as ``having``), ``limit``, and ``offset``. There's also
``distinct=True``. These are all also available as generative functions.
``order_by()`` expressions can use the modifiers ``asc()`` or ``desc()`` to
indicate ascending or descending.

.. sourcecode:: pycon+sql

    >>> s = select([addresses.c.user_id, func.count(addresses.c.id)]).\
    ...     group_by(addresses.c.user_id).having(func.count(addresses.c.id)>1)
    {sql}>>> print conn.execute(s).fetchall() # doctest: +NORMALIZE_WHITESPACE
    SELECT addresses.user_id, count(addresses.id) AS count_1
    FROM addresses GROUP BY addresses.user_id
    HAVING count(addresses.id) > ?
    (1,)
    {stop}[(1, 2), (2, 2)]

    >>> s = select([addresses.c.email_address, addresses.c.id]).distinct().\
    ...     order_by(addresses.c.email_address.desc(), addresses.c.id)
    {sql}>>> conn.execute(s).fetchall() # doctest: +NORMALIZE_WHITESPACE
    SELECT DISTINCT addresses.email_address, addresses.id
    FROM addresses ORDER BY addresses.email_address DESC, addresses.id
    ()
    {stop}[(u'www@www.org', 3), (u'wendy@aol.com', 4), (u'jack@yahoo.com', 1), (u'jack@msn.com', 2)]

    >>> s = select([addresses]).offset(1).limit(1)
    {sql}>>> print conn.execute(s).fetchall() # doctest: +NORMALIZE_WHITESPACE
    SELECT addresses.id, addresses.user_id, addresses.email_address
    FROM addresses
    LIMIT 1 OFFSET 1
    ()
    {stop}[(2, 1, u'jack@msn.com')]

.. _inserts_and_updates:

Inserts and Updates
===================

Finally, we're back to INSERT for some more detail. The
:func:`~sqlalchemy.sql.expression.insert` construct provides a :meth:`~.ValuesBase.values`
method which can be used to send any value or clause expression to the VALUES
portion of the INSERT::

    # insert from a function
    users.insert().values(id=12, name=func.upper('jack'))

    # insert from a concatenation expression
    addresses.insert().values(email_address = name + '@' + host)

``values()`` can be mixed with per-execution values::

    conn.execute(
        users.insert().values(name=func.upper('jack')),
        fullname='Jack Jones'
    )

:func:`~sqlalchemy.sql.expression.bindparam` constructs can be passed, however
the names of the table's columns are reserved for the "automatic" generation
of bind names::

    users.insert().values(id=bindparam('_id'), name=bindparam('_name'))

    # insert many rows at once:
    conn.execute(
        users.insert().values(id=bindparam('_id'), name=bindparam('_name')),
        [
            {'_id':1, '_name':'name1'},
            {'_id':2, '_name':'name2'},
            {'_id':3, '_name':'name3'},
        ]
    )

An UPDATE statement is emitted using the :func:`.update` construct.  These
work much like an INSERT, except there is an additional WHERE clause
that can be specified:

.. sourcecode:: pycon+sql

    >>> # change 'jack' to 'ed'
    {sql}>>> conn.execute(users.update().
    ...                    where(users.c.name=='jack').
    ...                    values(name='ed')
    ...                ) #doctest: +ELLIPSIS
    UPDATE users SET name=? WHERE users.name = ?
    ('ed', 'jack')
    COMMIT
    {stop}<sqlalchemy.engine.base.ResultProxy object at 0x...>

    >>> # use bind parameters
    >>> u = users.update().\
    ...             where(users.c.name==bindparam('oldname')).\
    ...             values(name=bindparam('newname'))
    {sql}>>> conn.execute(u, oldname='jack', newname='ed') #doctest: +ELLIPSIS
    UPDATE users SET name=? WHERE users.name = ?
    ('ed', 'jack')
    COMMIT
    {stop}<sqlalchemy.engine.base.ResultProxy object at 0x...>

    >>> # with binds, you can also update many rows at once
    {sql}>>> conn.execute(u, 
    ...     {'oldname':'jack', 'newname':'ed'},
    ...     {'oldname':'wendy', 'newname':'mary'},
    ...     {'oldname':'jim', 'newname':'jake'},
    ...     ) #doctest: +ELLIPSIS
    UPDATE users SET name=? WHERE users.name = ?
    [('ed', 'jack'), ('mary', 'wendy'), ('jake', 'jim')]
    COMMIT
    {stop}<sqlalchemy.engine.base.ResultProxy object at 0x...>

    >>> # update a column to an expression.:
    {sql}>>> conn.execute(users.update().
    ...                     values(fullname="Fullname: " + users.c.name)
    ...                 ) #doctest: +ELLIPSIS
    UPDATE users SET fullname=(? || users.name)
    ('Fullname: ',)
    COMMIT
    {stop}<sqlalchemy.engine.base.ResultProxy object at 0x...>

Correlated Updates
------------------

A correlated update lets you update a table using selection from another
table, or the same table:

.. sourcecode:: pycon+sql

    >>> s = select([addresses.c.email_address], addresses.c.user_id==users.c.id).limit(1)
    {sql}>>> conn.execute(users.update().values(fullname=s)) #doctest: +ELLIPSIS,+NORMALIZE_WHITESPACE
    UPDATE users SET fullname=(SELECT addresses.email_address
    FROM addresses
    WHERE addresses.user_id = users.id
    LIMIT 1 OFFSET 0)
    ()
    COMMIT
    {stop}<sqlalchemy.engine.base.ResultProxy object at 0x...>

Multiple Table Updates
----------------------

.. note:: 

   This feature is new as of version 0.7.4.

The Postgresql, Microsoft SQL Server, and MySQL backends all support UPDATE statements
that refer to multiple tables.   For PG and MSSQL, this is the "UPDATE FROM" syntax,
which updates one table at a time, but can reference additional tables in an additional
"FROM" clause that can then be referenced in the WHERE clause directly.   On MySQL,
multiple tables can be embedded into a single UPDATE statement separated by a comma.
The SQLAlchemy :func:`.update` construct supports both of these modes 
implicitly, by specifying multiple tables in the WHERE clause::

    stmt = users.update().\
            values(name='ed wood').\
            where(users.c.id==addresses.c.id).\
            where(addresses.c.email_address.startswith('ed%'))
    conn.execute(stmt)

The resulting SQL from the above statement would render as::

    UPDATE users SET name=:name FROM addresses 
    WHERE users.id = addresses.id AND 
    addresses.email_address LIKE :email_address_1 || '%%'

When using MySQL, columns from each table can be assigned to in the
SET clause directly, using the dictionary form passed to :meth:`.Update.values`::

    stmt = users.update().\
            values({
                users.c.name:'ed wood', 
                addresses.c.email_address:'ed.wood@foo.com'
            }).\
            where(users.c.id==addresses.c.id).\
            where(addresses.c.email_address.startswith('ed%'))

The tables are referenced explicitly in the SET clause::

    UPDATE users, addresses SET addresses.email_address=%s, 
            users.name=%s WHERE users.id = addresses.id 
            AND addresses.email_address LIKE concat(%s, '%%')

SQLAlchemy doesn't do anything special when these constructs are used on 
a non-supporting database.  The ``UPDATE FROM`` syntax generates by default
when multiple tables are present, and the statement will be rejected
by the database if this syntax is not supported.

.. _deletes:

Deletes
========

Finally, a delete.  This is accomplished easily enough using the
:func:`~.expression.delete` construct:

.. sourcecode:: pycon+sql

    {sql}>>> conn.execute(addresses.delete()) #doctest: +ELLIPSIS
    DELETE FROM addresses
    ()
    COMMIT
    {stop}<sqlalchemy.engine.base.ResultProxy object at 0x...>

    {sql}>>> conn.execute(users.delete().where(users.c.name > 'm')) #doctest: +ELLIPSIS
    DELETE FROM users WHERE users.name > ?
    ('m',)
    COMMIT
    {stop}<sqlalchemy.engine.base.ResultProxy object at 0x...>

Further Reference
==================

Expression Language Reference: :ref:`expression_api_toplevel`

Database Metadata Reference: :ref:`metadata_toplevel`

Engine Reference: :ref:`engines_toplevel`

Connection Reference: :ref:`connections_toplevel`

Types Reference: :ref:`types_toplevel`


