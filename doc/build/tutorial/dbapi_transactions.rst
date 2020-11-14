.. |prev| replace:: :doc:`engine`
.. |next| replace:: :doc:`metadata`

.. include:: tutorial_nav_include.rst


.. _tutorial_working_with_transactions:

Working with Transactions and the DBAPI
========================================



With the :class:`_future.Engine` object ready to go, we may now proceed
to dive into the basic operation of an :class:`_future.Engine` and
its primary interactive endpoints, the :class:`_future.Connection` and
:class:`_engine.Result`.   We will additionally introduce the ORM's
:term:`facade` for these objects, known as the :class:`_orm.Session`.

.. container:: orm-header

    **Note to ORM readers**

    When using the ORM, the :class:`_future.Engine` is managed by another
    object called the :class:`_orm.Session`.  The :class:`_orm.Session` in
    modern SQLAlchemy emphasizes a transactional and SQL execution pattern that
    is largely identical to that of the :class:`_future.Connection` discussed
    below, so while this subsection is Core-centric, all of the concepts here
    are essentially relevant to ORM use as well and is recommended for all ORM
    learners.   The execution pattern used by the :class:`_future.Connection`
    will be contrasted with that of the :class:`_orm.Session` at the end
    of this section.

As we have yet to introduce the SQLAlchemy Expression Language that is the
primary feature of SQLAlchemy, we will make use of one simple construct within
this package called the :func:`_sql.text` construct, which allows us to write
SQL statements as **textual SQL**.   Rest assured that textual SQL in
day-to-day SQLAlchemy use is by far the exception rather than the rule for most
tasks, even though it always remains fully available.

.. rst-class:: core-header

.. _tutorial_getting_connection:

Getting a Connection
---------------------

The sole purpose of the :class:`_future.Engine` object from a user-facing
perspective is to provide a unit of
connectivity to the database called the :class:`_future.Connection`.   When
working with the Core directly, the :class:`_future.Connection` object
is how all interaction with the database is done.   As the :class:`_future.Connection`
represents an open resource against the database, we want to always limit
the scope of our use of this object to a specific context, and the best
way to do that is by using Python context manager form, also known as
`the with statement <https://docs.python.org/3/reference/compound_stmts.html#with>`_.
Below we illustrate "Hello World", using a textual SQL statement.  Textual
SQL is emitted using a construct called :func:`_sql.text` that will be discussed
in more detail later:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import text

    >>> with engine.connect() as conn:
    ...     result = conn.execute(text("select 'hello world'"))
    ...     print(result.all())
    {opensql}BEGIN (implicit)
    select 'hello world'
    [...] ()
    {stop}[('hello world',)]
    {opensql}ROLLBACK{stop}

In the above example, the context manager provided for a database connection
and also framed the operation inside of a transaction. The default behavior of
the Python DBAPI includes that a transaction is always in progress; when the
scope of the connection is :term:`released`, a ROLLBACK is emitted to end the
transaction.   The transaction is **not committed automatically**; when we want
to commit data we normally need to call :meth:`_future.Connection.commit`
as we'll see in the next section.

.. tip::  "autocommit" mode is available for special cases.  The section
   :ref:`dbapi_autocommit` discusses this.

The result of our SELECT was also returned in an object called
:class:`_engine.Result` that will be discussed later, however for the moment
we'll add that it's best to ensure this object is consumed within the
"connect" block, and is not passed along outside of the scope of our connection.

.. rst-class:: core-header

.. _tutorial_committing_data:

Committing Changes
------------------

We just learned that the DBAPI connection is non-autocommitting.  What if
we want to commit some data?   We can alter our above example to create a
table and insert some data, and the transaction is then committed using
the :meth:`_future.Connection.commit` method, invoked **inside** the block
where we acquired the :class:`_future.Connection` object:

.. sourcecode:: pycon+sql

    # "commit as you go"
    >>> with engine.connect() as conn:
    ...     conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    ...     conn.execute(
    ...         text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
    ...         [{"x": 1, "y": 1}, {"x": 2, "y": 4}]
    ...     )
    ...     conn.commit()
    {opensql}BEGIN (implicit)
    CREATE TABLE some_table (x int, y int)
    [...] ()
    <sqlalchemy.engine.cursor.CursorResult object at 0x...>
    INSERT INTO some_table (x, y) VALUES (?, ?)
    [...] ((1, 1), (2, 4))
    <sqlalchemy.engine.cursor.CursorResult object at 0x...>
    COMMIT

Above, we emitted two SQL statements that are generally transactional, a
"CREATE TABLE" statement [1]_ and an "INSERT" statement that's parameterized
(the parameterization syntax above is discussed a few sections below in
:ref:`tutorial_multiple_parameters`).  As we want the work we've done to be
committed within our block, we invoke the
:meth:`_future.Connection.commit` method which commits the transaction. After
we call this method inside the block, we can continue to run more SQL
statements and if we choose we may call :meth:`_future.Connection.commit`
again for subsequent statements.  SQLAlchemy refers to this style as **commit as
you go**.

There is also another style of committing data, which is that we can declare
our "connect" block to be a transaction block up front.   For this mode of
operation, we use the :meth:`_future.Engine.begin` method to acquire the
connection, rather than the :meth:`_future.Engine.connect` method.  This method
will both manage the scope of the :class:`_future.Connection` and also
enclose everything inside of a transaction with COMMIT at the end, assuming
a successful block, or ROLLBACK in case of exception raise.  This style
may be referred towards as **begin once**:

.. sourcecode:: pycon+sql

    # "begin once"
    >>> with engine.begin() as conn:
    ...     conn.execute(
    ...         text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
    ...         [{"x": 6, "y": 8}, {"x": 9, "y": 10}]
    ...     )
    {opensql}BEGIN (implicit)
    INSERT INTO some_table (x, y) VALUES (?, ?)
    [...] ((6, 8), (9, 10))
    <sqlalchemy.engine.cursor.CursorResult object at 0x...>
    COMMIT

"Begin once" style is often preferred as it is more succinct and indicates the
intention of the entire block up front.   However, within this tutorial we will
normally use "commit as you go" style as it is more flexible for demonstration
purposes.

.. topic::  What's "BEGIN (implicit)"?

    You might have noticed the log line "BEGIN (implicit)" at the start of a
    transaction block.  "implicit" here means that SQLAlchemy **did not
    actually send any command** to the database; it just considers this to be
    the start of the DBAPI's implicit transaction.   You can register
    :ref:`event hooks <core_sql_events>` to intercept this event, for example.


.. [1] :term:`DDL` refers to the subset of SQL that instructs the database
   to create, modify, or remove schema-level constructs such as tables. DDL
   such as "CREATE TABLE" is recommended to be within a transaction block that
   ends with COMMIT, as many databases uses transactional DDL such that the
   schema changes don't take place until the transaction is committed. However,
   as we'll see later, we usually let SQLAlchemy run DDL sequences for us as
   part of a higher level operation where we don't generally need to worry
   about the COMMIT.


.. rst-class:: core-header


Basics of Statement Execution
-----------------------------

We have seen a few examples that run SQL statements against a database, making
use of a method called :meth:`_future.Connection.execute`, in conjunction with
an object called :func:`_sql.text`, and returning an object called
:class:`_engine.Result`.  In this section we'll illustrate more closely the
mechanics and interactions of these components.

.. container:: orm-header

  Most of the content in this section applies equally well to modern ORM
  use when using the :meth:`_orm.Session.execute` method, which works
  very similarly to that of :meth:`_future.Connection.execute`, including that
  ORM result rows are delivered using the same :class:`_engine.Result`
  interface used by Core.

.. rst-class:: orm-addin

.. _tutorial_fetching_rows:

Fetching Rows
^^^^^^^^^^^^^

We'll first illustrate the :class:`_engine.Result` object more closely by
making use of the rows we've inserted previously, running a textual SELECT
statement on the table we've created:


.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...     result = conn.execute(text("SELECT x, y FROM some_table"))
    ...     for row in result:
    ...         print(f"x: {row.x}  y: {row.y}")
    {opensql}BEGIN (implicit)
    SELECT x, y FROM some_table
    [...] ()
    {stop}x: 1  y: 1
    x: 2  y: 4
    x: 6  y: 8
    x: 9  y: 10
    {opensql}ROLLBACK{stop}

Above, the "SELECT" string we executed selected all rows from our table.
The object returned is called :class:`_engine.Result` and represents an
iterable object of result rows.

:class:`_engine.Result` has lots of methods for
fetching and transforming rows, such as the :meth:`_engine.Result.all`
method illustrated previously, which returns a list of all :class:`_engine.Row`
objects.   It also implements the Python iterator interface so that we can
iterate over the collection of :class:`_engine.Row` objects directly.

The :class:`_engine.Row` objects themselves are intended to act like Python
`named tuples
<https://docs.python.org/3/library/collections.html#collections.namedtuple>`_.
Below we illustrate a variety of ways to access rows.

* **Tuple Assignment** - This is the most Python-idiomatic style, which is to assign variables
  to each row positionally as they are received:

  ::

      result = conn.execute(text("select x, y from some_table"))

      for x, y in result:
          # ...

* **Integer Index** - Tuples are Python sequences, so regular integer access is available too:

  ::

      result = conn.execute(text("select x, y from some_table"))

        for row in result:
            x = row[0]

* **Attribute Name** - As these are Python named tuples, the tuples have dynamic attribute names
  matching the names of each column.  These names are normally the names that the
  SQL statement assigns to the columns in each row.  While they are usually
  fairly predictable and can also be controlled by labels, in less defined cases
  they may be subject to database-specific behaviors::

      result = conn.execute(text("select x, y from some_table"))

      for row in result:
          y = row.y

          # illustrate use with Python f-strings
          print(f"Row: {row.x} {row.y}")

  ..

* **Mapping Access** - To receive rows as Python **mapping** objects, which is
  essentially a read-only version of Python's interface to the common ``dict``
  object, the :class:`_engine.Result` may be **transformed** into a
  :class:`_engine.MappingResult` object using the
  :meth:`_engine.Result.mappings` modifier; this is a result object that yields
  dictionary-like :class:`_engine.RowMapping` objects rather than
  :class:`_engine.Row` objects::

      result = conn.execute(text("select x, y from some_table"))

      for dict_row in result.mappings():
          x = dict_row['x']
          y = dict_row['y']

  ..

.. rst-class:: orm-addin

.. _tutorial_sending_parameters:

Sending Parameters
^^^^^^^^^^^^^^^^^^

SQL statements are usually accompanied by data that is to be passed with the
statement itself, as we saw in the INSERT example previously. The
:meth:`_future.Connection.execute` method therefore also accepts parameters,
which are referred towards as :term:`bound parameters`.  A rudimentary example
might be if we wanted to limit our SELECT statement only to rows that meet a
certain criteria, such as rows where the "y" value were greater than a certain
value that is passed in to a function.

In order to achieve this such that the SQL statement can remain fixed and
that the driver can properly sanitize the value, we add a WHERE criteria to
our statement that names a new parameter called "y"; the :func:`_sql.text`
construct accepts these using a colon format "``:y``".   The actual value for
"``:y``" is then passed as the second argument to
:meth:`_future.Connection.execute` in the form of a dictionary:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...     result = conn.execute(
    ...         text("SELECT x, y FROM some_table WHERE y > :y"),
    ...         {"y": 2}
    ...     )
    ...     for row in result:
    ...        print(f"x: {row.x}  y: {row.y}")
    {opensql}BEGIN (implicit)
    SELECT x, y FROM some_table WHERE y > ?
    [...] (2,)
    {stop}x: 2  y: 4
    x: 6  y: 8
    x: 9  y: 10
    {opensql}ROLLBACK{stop}


In the logged SQL output, we can see that the bound parameter ``:y`` was
converted into a question mark when it was sent to the SQLite database.
This is because the SQLite database driver uses a format called "qmark parameter style",
which is one of six different formats allowed by the DBAPI specification.
SQLAlchemy abstracts these formats into just one, which is the "named" format
using a colon.

.. topic:: Always use bound parameters

    As mentioned at the beginning of this section, textual SQL is not the usual
    way we work with SQLAlchemy.  However, when using textual SQL, a Python
    literal value, even non-strings like integers or dates, should **never be
    stringified into SQL string directly**; a parameter should **always** be
    used.  This is most famously known as how to avoid SQL injection attacks
    when the data is untrusted.  However it also allows the SQLAlchemy dialects
    and/or DBAPI to correctly handle the incoming input for the backend.
    Outside of plain textual SQL use cases, SQLAlchemy's Core Expression API
    otherwise ensures that Python literal values are passed as bound parameters
    where appropriate.

.. _tutorial_multiple_parameters:

Sending Multiple Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the example at :ref:`tutorial_committing_data`, we executed an INSERT
statement where it appeared that we were able to INSERT multiple rows into the
database at once.  For statements that **operate upon data, but do not return
result sets**, namely :term:`DML` statements such as "INSERT" which don't
include a phrase like "RETURNING", we can send **multi params** to the
:meth:`_future.Connection.execute` method by passing a list of dictionaries
instead of a single dictionary, thus allowing the single SQL statement to
be invoked against each parameter set individually:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...     conn.execute(
    ...         text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
    ...         [{"x": 11, "y": 12}, {"x": 13, "y": 14}]
    ...     )
    ...     conn.commit()
    {opensql}BEGIN (implicit)
    INSERT INTO some_table (x, y) VALUES (?, ?)
    [...] ((11, 12), (13, 14))
    <sqlalchemy.engine.cursor.CursorResult object at 0x...>
    COMMIT

Behind the scenes, the :class:`_future.Connection` objects uses a DBAPI feature
known as `cursor.executemany()
<https://www.python.org/dev/peps/pep-0249/#id18>`_. This method performs the
equivalent operation of invoking the given SQL statement against each parameter
set individually.   The DBAPI may optimize this operation in a variety of ways,
by using prepared statements, or by concatenating the parameter sets into a
single SQL statement in some cases.  Some SQLAlchemy dialects may also use
alternate APIs for this case, such as the :ref:`psycopg2 dialect for PostgreSQL
<postgresql_psycopg2>` which uses more performant APIs
for this use case.

.. tip::  you may have noticed this section isn't tagged as an ORM concept.
   That's because the "multiple parameters" use case is **usually** used
   for INSERT statements, which when using the ORM are invoked in a different
   way.   Multiple parameters also may be used with UPDATE and DELETE
   statements to emit distinct UPDATE/DELETE operations on a per-row basis,
   however again when using the ORM, there is a different technique
   generally used for updating or deleting many individual rows separately.

.. rst-class:: orm-addin

.. _tutorial_bundling_parameters:

Bundling Parameters with a Statement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The two previous cases illustrate a series of parameters being passed to
accompany a SQL statement.    For single-parameter statement executions,
SQLAlchemy's use of parameters is in fact more often than not done by
**bundling** the parameters with the statement itself, which is a primary
feature of the SQL Expression Language and makes for queries that can be
composed naturally while still making use of parameterization in all cases.
This concept will be discussed in much more detail in the sections that follow;
for a brief preview, the :func:`_sql.text` construct itself being part of the
SQL Expression Language supports this feature by using the
:meth:`_sql.TextClause.bindparams` method; this is a :term:`generative` method that
returns a new copy of the SQL construct with additional state added, in this
case the parameter values we want to pass along:


.. sourcecode:: pycon+sql

    >>> stmt = text("SELECT x, y FROM some_table WHERE y > :y ORDER BY x, y").bindparams(y=6)
    >>> with engine.connect() as conn:
    ...     result = conn.execute(stmt)
    ...     for row in result:
    ...        print(f"x: {row.x}  y: {row.y}")
    {opensql}BEGIN (implicit)
    SELECT x, y FROM some_table WHERE y > ? ORDER BY x, y
    [...] (6,)
    {stop}x: 6  y: 8
    x: 9  y: 10
    x: 11  y: 12
    x: 13  y: 14
    {opensql}ROLLBACK{stop}


The interesting thing to note above is that even though we passed only a single
argument, ``stmt``, to the :meth:`_future.Connection.execute` method, the
execution of the statement illustrated both the SQL string as well as the
separate parameter tuple.

.. rst-class:: orm-addin

.. _tutorial_executing_orm_session:

Executing with an ORM Session
-----------------------------

As mentioned previously, most of the patterns and examples above apply to
use with the ORM as well, so here we will introduce this usage so that
as the tutorial proceeds, we will be able to illustrate each pattern in
terms of Core and ORM use together.

The fundamental transactional / database interactive object when using the
ORM is called the :class:`_orm.Session`.  In modern SQLAlchemy, this object
is used in a manner very similar to that of the :class:`_future.Connection`,
and in fact as the :class:`_orm.Session` is used, it refers to a
:class:`_future.Connection` internally which it uses to emit SQL.

When the :class:`_orm.Session` is used with non-ORM constructs, it
passes through the SQL statements we give it and does not generally do things
much differently from how the :class:`_future.Connection` does directly, so
we can illustrate it here in terms of the simple textual SQL
operations we've already learned.

The :class:`_orm.Session` has a few different creational patterns, but
here we will illustrate the most basic one that tracks exactly with how
the :class:`_future.Connection` is used which is to construct it within
a context manager:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy.orm import Session

    >>> stmt = text("SELECT x, y FROM some_table WHERE y > :y ORDER BY x, y").bindparams(y=6)
    >>> with Session(engine) as session:
    ...     result = session.execute(stmt)
    ...     for row in result:
    ...        print(f"x: {row.x}  y: {row.y}")
    {opensql}BEGIN (implicit)
    SELECT x, y FROM some_table WHERE y > ? ORDER BY x, y
    [...] (6,){stop}
    x: 6  y: 8
    x: 9  y: 10
    x: 11  y: 12
    x: 13  y: 14
    {opensql}ROLLBACK{stop}

The example above can be compared to the example in the preceding section
in :ref:`tutorial_bundling_parameters` - we directly replace the call to
``with engine.connect() as conn`` with ``with Session(engine) as session``,
and then make use of the :meth:`_orm.Session.execute` method just like we
do with the :meth:`_future.Connection.execute` method.

Also, like the :class:`_future.Connection`, the :class:`_orm.Session` features
"commit as you go" behavior using the :meth:`_orm.Session.commit` method,
illustrated below using a textual UPDATE statement to alter some of
our data:

.. sourcecode:: pycon+sql

    >>> with Session(engine) as session:
    ...     result = session.execute(
    ...         text("UPDATE some_table SET y=:y WHERE x=:x"),
    ...         [{"x": 9, "y":11}, {"x": 13, "y": 15}]
    ...     )
    ...     session.commit()
    {opensql}BEGIN (implicit)
    UPDATE some_table SET y=? WHERE x=?
    [...] ((11, 9), (15, 13))
    COMMIT{stop}

Above, we invoked an UPDATE statement using the bound-parameter, "executemany"
style of execution introduced at :ref:`tutorial_multiple_parameters`, ending
the block with a "commit as you go" commit.

.. tip:: The :class:`_orm.Session` doesn't actually hold onto the
   :class:`_future.Connection` object after it ends the transaction.  It
   gets a new :class:`_future.Connection` from the :class:`_future.Engine`
   when executing SQL against the database is next needed.

The :class:`_orm.Session` obviously has a lot more tricks up its sleeve
than that, however understanding that it has an :meth:`_orm.Session.execute`
method that's used the same way as :meth:`_future.Connection.execute` will
get us started with the examples that follow later.





