.. highlight:: pycon+sql

.. |prev| replace:: :doc:`metadata`
.. |next| replace:: :doc:`orm_data_manipulation`

.. include:: tutorial_nav_include.rst

.. _tutorial_working_with_data:

Working with Data
==================

In :ref:`tutorial_working_with_transactions`, we learned the basics of how to
interact with the Python DBAPI and its transactional state.  Then, in
:ref:`tutorial_working_with_metadata`, we learned how to represent database
tables, columns, and constraints within SQLAlchemy using the
:class:`_schema.MetaData` and related objects.  In this section we will combine
both concepts above to create, select and manipulate data within a relational
database.   Our interaction with the database is **always** in terms
of a transaction, even if we've set our database driver to use :ref:`autocommit
<dbapi_autocommit>` behind the scenes.

The components of this section are as follows:

* :ref:`tutorial_core_insert` - to get some data into the database, we introduce
  and demonstrate the Core :class:`_sql.Insert` construct.   INSERTs from an
  ORM perspective are described later, at :ref:`tutorial_orm_data_manipulation`.

* :ref:`tutorial_selecting_data` - this section will describe in detail
  the :class:`_sql.Select` construct, which is the most commonly used object
  in SQLAlchemy.  The :class:`_sql.Select` construct emits SELECT statements
  for both Core and ORM centric applications and both use cases will be
  described here.   Additional ORM use cases are also noted in he later
  section :ref:`tutorial_select_relationships` as well as the
  :ref:`queryguide_toplevel`.

* :ref:`tutorial_core_update_delete` - Rounding out the INSERT and SELECtion
  of data, this section will describe from a Core perspective the use of the
  :class:`_sql.Update` and :class:`_sql.Delete` constructs.  ORM-specific
  UPDATE and DELETE is similarly described in the
  :ref:`tutorial_orm_data_manipulation` section.

.. rst-class:: core-header

.. _tutorial_core_insert:

Core Insert
-----------

When using Core, a SQL INSERT statement is generated using the
:func:`_sql.insert` function - this function generates a new instance of
:class:`_sql.Insert` which represents an INSERT statement in SQL, that adds
new data into a table.

.. container:: orm-header

    **ORM Readers** - The way that rows are INSERTed into the database from an ORM
    perspective makes use of object-centric APIs on the :class:`_orm.Session` object known as the
    :term:`unit of work` process,
    and is fairly different from the Core-only approach described here.
    The more ORM-focused sections later starting at :ref:`tutorial_inserting_orm`
    subsequent to the Expression Language sections introduce this.

The insert() SQL Expression Construct
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A simple example of :class:`_sql.Insert` illustrates the target table
and the VALUES clause at once::

    >>> from sqlalchemy import insert
    >>> stmt = insert(user_table).values(name='spongebob', fullname="Spongebob Squarepants")

The above ``stmt`` variable is an instance of :class:`_sql.Insert`.  Most
SQL expressions can be stringified in place as a means to see the general
form of what's being produced::

    >>> print(stmt)
    {opensql}INSERT INTO user_account (name, fullname) VALUES (:name, :fullname)

The stringified form is created by producing a :class:`_engine.Compiled` form
of the object which includes a database-specific string SQL representation of
the statement; we can acquire this object directly using the
:meth:`_sql.ClauseElement.compile` method::

    >>> compiled = stmt.compile()

Our :class:`_sql.Insert` construct is an example of a "parameterized"
construct, illustrated previously at :ref:`tutorial_sending_parameters`; to
view the ``name`` and ``fullname`` :term:`bound parameters`, these are
available from the :class:`_engine.Compiled` construct as well::

    >>> compiled.params
    {'name': 'spongebob', 'fullname': 'Spongebob Squarepants'}

Executing the Statement
^^^^^^^^^^^^^^^^^^^^^^^

Invoking the statement we can INSERT a row into ``user_table``.
The INSERT SQL as well as the bundled parameters can be seen in the
SQL logging:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...     result = conn.execute(stmt)
    ...     conn.commit()
    {opensql}BEGIN (implicit)
    INSERT INTO user_account (name, fullname) VALUES (?, ?)
    [...] ('spongebob', 'Spongebob Squarepants')
    COMMIT

In its simple form above, the INSERT statement does not return any rows, and if
only a single row is inserted, it will usually include the ability to return
information about column-level default values that were generated during the
INSERT of that row, most commonly an integer primary key value.  In the above
case the first row in a SQLite database will normally return ``1`` for the
first integer primary key value, which we can acquire using the
:attr:`_engine.CursorResult.inserted_primary_key` accessor:

.. sourcecode:: pycon+sql

    >>> result.inserted_primary_key
    (1,)

.. tip:: :attr:`_engine.CursorResult.inserted_primary_key` returns a tuple
   because a primary key may contain multiple columns.  This is known as
   a :term:`composite primary key`.  The :attr:`_engine.CursorResult.inserted_primary_key`
   is intended to always contain the complete primary key of the record just
   inserted, not just a "cursor.lastrowid" kind of value, and is also intended
   to be populated regardless of whether or not "autoincrement" were used, hence
   to express a complete primary key it's a tuple.

.. _tutorial_core_insert_values_clause:

INSERT usually generates the "values" clause automatically
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The example above made use of the :meth:`_sql.Insert.values` method to
explicitly create the VALUES clause of the SQL INSERT statement.   This method
in fact has some variants that allow for special forms such as multiple rows in
one statement and insertion of SQL expressions.   However the usual way that
:class:`_sql.Insert` is used is such that the VALUES clause is generated
automatically from the parameters passed to the
:meth:`_future.Connection.execute` method; below we INSERT two more rows to
illustrate this:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...     result = conn.execute(
    ...         insert(user_table),
    ...         [
    ...             {"name": "sandy", "fullname": "Sandy Cheeks"},
    ...             {"name": "patrick", "fullname": "Patrick Star"}
    ...         ]
    ...     )
    ...     conn.commit()
    {opensql}BEGIN (implicit)
    INSERT INTO user_account (name, fullname) VALUES (?, ?)
    [...] (('sandy', 'Sandy Cheeks'), ('patrick', 'Patrick Star'))
    COMMIT{stop}

The execution above features "executemany" form first illustrated at
:ref:`tutorial_multiple_parameters`, however unlike when using the
:func:`_sql.text` construct, we didn't have to spell out any SQL.
By passing a dictionary or list of dictionaries to the :meth:`_future.Connection.execute`
method in conjunction with the :class:`_sql.Insert` construct, the
:class:`_future.Connection` ensures that the column names which are passed
will be expressed in the VALUES clause of the :class:`_sql.Insert`
construct automatically.

.. deepalchemy::

    Hi, welcome to the first edition of **Deep Alchemy**.   The person on the
    left is known as **The Alchemist**, and you'll note they are **not** a wizard,
    as the pointy hat is not sticking upwards.   The Alchemist comes around to
    describe things that are generally **more advanced and/or tricky** and
    additionally **not usually needed**, but for whatever reason they feel you
    should know about this thing that SQLAlchemy can do.

    In this edition, towards the goal of having some interesting data in the
    ``address_table`` as well, below is a more advanced example illustrating
    how the :meth:`_sql.Insert.values` method may be used explicitly while at
    the same time including for additional VALUES generated from the
    parameters.    A :term:`scalar subquery` is constructed, making use of the
    :func:`_sql.select` construct introduced in the next section, and the
    parameters used in the subquery are set up using an explicit bound
    parameter name, established using the :func:`_sql.bindparam` construct.

    This is some slightly **deeper** alchemy just so that we can add related
    rows without fetching the primary key identifiers from the ``user_table``
    operation into the application.   Most Alchemists will simply use the ORM
    which takes care of things like this for us.

    .. sourcecode:: pycon+sql

        >>> from sqlalchemy import select, bindparam
        >>> scalar_subquery = (
        ...     select(user_table.c.id).
        ...     where(user_table.c.name==bindparam('username')).
        ...     scalar_subquery()
        ... )

        >>> with engine.connect() as conn:
        ...     result = conn.execute(
        ...         insert(address_table).values(user_id=scalar_subquery),
        ...         [
        ...             {"username": 'spongebob', "email_address": "spongebob@sqlalchemy.org"},
        ...             {"username": 'sandy', "email_address": "sandy@sqlalchemy.org"},
        ...             {"username": 'sandy', "email_address": "sandy@squirrelpower.org"},
        ...         ]
        ...     )
        ...     conn.commit()
        {opensql}BEGIN (implicit)
        INSERT INTO address (user_id, email_address) VALUES ((SELECT user_account.id
        FROM user_account
        WHERE user_account.name = ?), ?)
        [...] (('spongebob', 'spongebob@sqlalchemy.org'), ('sandy', 'sandy@sqlalchemy.org'),
        ('sandy', 'sandy@squirrelpower.org'))
        COMMIT{stop}

Other INSERT Options
^^^^^^^^^^^^^^^^^^^^^

A quick overview of some other patterns that are available with :func:`_sql.insert`:

* **INSERT..FROM SELECT** - the :class:`_sql.Insert` construct can compose
  an INSERT that gets rows directly from a SELECT using the :meth:`_sql.Insert.from_select`
  method::

    >>> select_stmt = select(user_table.c.id, user_table.c.name + "@aol.com")
    >>> insert_stmt = insert(address_table).from_select(
    ...     ["user_id", "email_address"], select_stmt
    ... )
    >>> print(insert_stmt)
    {opensql}INSERT INTO address (user_id, email_address)
    SELECT user_account.id, user_account.name || :name_1 AS anon_1
    FROM user_account

  ..

* **RETURNING clause** - the RETURNING clause for supported backends is used
  automatically in order to retrieve the last inserted primary key value
  as well as the values for server defaults.   However the RETURNING clause
  may also be specified explicitly using the :meth:`_sql.Insert.returning`
  method; in this case, the :class:`_engine.Result`
  object that's returned when the statement is executed has rows which
  can be fetched.  It is only supported for single-statement
  forms, and for some backends may only support single-row INSERT statements
  overall.   It can also be combined with :meth:`_sql.Insert.from_select`,
  as in the example below that builds upon the previous example::

    >>> print(insert_stmt.returning(address_table.c.id, address_table.c.email_address))
    {opensql}INSERT INTO address (user_id, email_address)
    SELECT user_account.id, user_account.name || :name_1 AS anon_1
    FROM user_account RETURNING address.id, address.email_address

  ..

.. seealso::

    :class:`_sql.Insert` - in the SQL Expression API documentation


.. _tutorial_selecting_data:

.. rst-class:: core-header, orm-dependency

Selecting Data
--------------

For both Core and ORM, the :func:`_sql.select` function generates a
:class:`_sql.Select` construct which is used for all SELECT queries.
Passed to methods like :meth:`_future.Connection.execute` in Core and
:meth:`_orm.Session.execute` in ORM, a SELECT statement is emitted in the
current transaction and the result rows available via the returned
:class:`_engine.Result` object.

.. container:: orm-header

    **ORM Readers** - the content here applies equally well to both Core and ORM
    use and basic ORM variant use cases are mentioned here.  However there are
    a lot more ORM-specific features available as well; these are documented
    at :ref:`queryguide_toplevel`.


The select() SQL Expression Construct
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_sql.select` construct builds up a statement in the same way
as that of :func:`_sql.insert`, using a :term:`generative` approach where
each method builds more state onto the object.  Like the other SQL constructs,
it can be stringified in place::

    >>> from sqlalchemy import select
    >>> stmt = select(user_table).where(user_table.c.name == 'spongebob')
    >>> print(stmt)
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.name = :name_1

Also in the same manner as all other statement-level SQL constructs, to
actually run the statement we pass it to an execution method.
Since a SELECT statement returns
rows we can always iterate the result object to get :class:`_engine.Row`
objects back:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...     for row in conn.execute(stmt):
    ...         print(row)
    {opensql}BEGIN (implicit)
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.name = ?
    [...] ('spongebob',){stop}
    (1, 'spongebob', 'Spongebob Squarepants')
    {opensql}ROLLBACK{stop}

When using the ORM, particularly with a :func:`_sql.select` construct that's
composed against ORM entities, we will want to execute it using the
:meth:`_orm.Session.execute` method on the :class:`_orm.Session`; using
this approach, we continue to get :class:`_engine.Row` objects from the
result, however these rows are now capable of including
complete entities, such as instances of the ``User`` class, as column values:

.. sourcecode:: pycon+sql

    >>> stmt = select(User).where(User.name == 'spongebob')
    >>> with Session(engine) as session:
    ...     for row in session.execute(stmt):
    ...         print(row)
    {opensql}BEGIN (implicit)
    SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.name = ?
    [...] ('spongebob',){stop}
    (User(id=1, name='spongebob', fullname='Spongebob Squarepants'),)
    {opensql}ROLLBACK{stop}

The following sections will discuss the SELECT construct in more detail.


Setting the COLUMNS and FROM clause
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_sql.select` function accepts positional elements representing any
number of :class:`_schema.Column` and/or :class:`_schema.Table` expressions, as
well as a wide range of compatible objects, which are resolved into a list of SQL
expressions to be SELECTed from that will be returned as columns in the result
set.  These elements also serve in simpler cases to create the FROM clause,
which is inferred from the columns and table-like expressions passed::

    >>> print(select(user_table))
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account

To SELECT from individual columns using a Core approach,
:class:`_schema.Column` objects are accessed from the :attr:`_schema.Table.c`
accessor and can be sent directly; the FROM clause will be inferred as the set
of all :class:`_schema.Table` and other :class:`_sql.FromClause` objects that
are represented by those columns::

    >>> print(select(user_table.c.name, user_table.c.fullname))
    {opensql}SELECT user_account.name, user_account.fullname
    FROM user_account

.. _tutorial_selecting_orm_entities:

Selecting ORM Entities and Columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

ORM entities, such our ``User`` class as well as the column-mapped
attributes upon it such as ``User.name``, also participate in the SQL Expression
Language system representing tables and columns.    Below illustrates an
example of SELECTing from the ``User`` entity, which ultimately renders
in the same way as if we had used ``user_table`` directly::

    >>> print(select(User))
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account

To select from individual columns using ORM entities, the class-bound
attributes can be passed directly which are resolved into the
:class:`_schema.Column` or other SQL expression represented by each attribute::

    >>> print(select(User.name, User.fullname))
    {opensql}SELECT user_account.name, user_account.fullname
    FROM user_account

.. tip::

    When ORM-related objects are used within the :class:`_sql.Select`
    construct, they are resolved into the underlying :class:`_schema.Table` and
    :class:`_schema.Column` and similar Core constructs they represent; at the
    same time, they apply a **plugin** to the core :class:`_sql.Select`
    construct such that a new set of ORM-specific behaviors make take
    effect when the construct is being compiled.

.. seealso::

    :ref:`orm_queryguide_select_columns` - in the :ref:`queryguide_toplevel`

Selecting from Labeled SQL Expressions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :meth:`_sql.ColumnElement.label` method as well as the same-named method
available on ORM attributes provides a SQL label of a column or expression,
allowing it to have a specific name in a result set.  This can be helpful
when referring to arbitrary SQL expressions in a result row by name:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import func, cast
    >>> stmt = (
    ...     select(
    ...         ("Username: " + user_table.c.name).label("username"),
    ...     ).order_by(user_table.c.name)
    ... )
    >>> with engine.connect() as conn:
    ...     for row in conn.execute(stmt):
    ...         print(f"{row.username}")
    {opensql}BEGIN (implicit)
    SELECT ? || user_account.name AS username
    FROM user_account ORDER BY user_account.name
    [...] ('Username: ',){stop}
    Username: patrick
    Username: sandy
    Username: spongebob
    {opensql}ROLLBACK{stop}

.. _tutorial_select_where_clause:

The WHERE clause
^^^^^^^^^^^^^^^^

SQLAlchemy allows us to compose SQL expressions, such as ``name = 'squidward'``
or ``user_id > 10``, by making use of standard Python operators in
conjunction with
:class:`_schema.Column` and similar objects.   For boolean expressions, most
Python operators such as ``==``, ``!=``, ``<``, ``>=`` etc. generate new
SQL Expression objects, rather than plain boolean True/False values::

    >>> print(user_table.c.name == 'squidward')
    user_account.name = :name_1

    >>> print(address_table.c.user_id > 10)
    address.user_id > :user_id_1


We can use expressions like these to generate the WHERE clause by passing
the resulting objects to the :meth:`_sql.Select.where` method::

    >>> print(select(user_table).where(user_table.c.name == 'squidward'))
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.name = :name_1


To produce multiple expressions joined by AND, the :meth:`_sql.Select.where`
method may be invoked any number of times::

    >>> print(
    ...     select(address_table.c.email_address).
    ...     where(user_table.c.name == 'squidward').
    ...     where(address_table.c.user_id == user_table.c.id)
    ... )
    {opensql}SELECT address.email_address
    FROM address, user_account
    WHERE user_account.name = :name_1 AND address.user_id = user_account.id

A single call to :meth:`_sql.Select.where` also accepts multiple expressions
with the same effect::

    >>> print(
    ...     select(address_table.c.email_address).
    ...     where(
    ...          user_table.c.name == 'squidward',
    ...          address_table.c.user_id == user_table.c.id
    ...     )
    ... )
    {opensql}SELECT address.email_address
    FROM address, user_account
    WHERE user_account.name = :name_1 AND address.user_id = user_account.id

"AND" and "OR" conjunctions are both available directly using the
:func:`_sql.and_` and :func:`_sql.or_` functions, illustrated below in terms
of ORM entities::

    >>> from sqlalchemy import and_, or_
    >>> print(
    ...     select(Address.email_address).
    ...     where(
    ...         and_(
    ...             or_(User.name == 'squidward', User.name == 'sandy'),
    ...             Address.user_id == User.id
    ...         )
    ...     )
    ... )
    {opensql}SELECT address.email_address
    FROM address, user_account
    WHERE (user_account.name = :name_1 OR user_account.name = :name_2)
    AND address.user_id = user_account.id

For simple "equality" comparisons against a single entity, there's also a
popular method known as :meth:`_sql.Select.filter_by` which accepts keyword
arguments that match to column keys or ORM attribute names.  It will filter
against the leftmost FROM clause or the last entity joined::

    >>> print(
    ...     select(User).filter_by(name='spongebob', fullname='Spongebob Squarepants')
    ... )
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    WHERE user_account.name = :name_1 AND user_account.fullname = :fullname_1


.. seealso::


    :doc:`/core/operators` - descriptions of most SQL operator functions in SQLAlchemy


.. _tutorial_select_join:

Explicit FROM clauses and JOINs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As mentioned previously, the FROM clause is usually **inferred**
based on the expressions that we are setting in the columns
clause as well as other elements of the :class:`_sql.Select`.

If we set a single column from a particular :class:`_schema.Table`
in the COLUMNS clause, it puts that :class:`_schema.Table` in the FROM
clause as well::

    >>> print(select(user_table.c.name))
    {opensql}SELECT user_account.name
    FROM user_account

If we were to put columns from two tables, then we get a comma-separated FROM
clause::

    >>> print(select(user_table.c.name, address_table.c.email_address))
    {opensql}SELECT user_account.name, address.email_address
    FROM user_account, address

In order to JOIN these two tables together, two methods that are
most straightforward are :meth:`_sql.Select.join_from`, which
allows us to indicate the left and right side of the JOIN explicitly::

    >>> print(
    ...     select(user_table.c.name, address_table.c.email_address).
    ...     join_from(user_table, address_table)
    ... )
    {opensql}SELECT user_account.name, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id


the other is the :meth:`_sql.Select.join` method, which indicates only the
right side of the JOIN, the left hand-side is inferred::

    >>> print(
    ...     select(user_table.c.name, address_table.c.email_address).
    ...     join(address_table)
    ... )
    {opensql}SELECT user_account.name, address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id

.. sidebar::  The ON Clause is inferred

    When using :meth:`_sql.Select.join_from` or :meth:`_sql.Select.join`, we may
    observe that the ON clause of the join is also inferred for us in simple cases.
    More on that in the next section.

We also have the option add elements to the FROM clause explicitly, if it is not
inferred the way we want from the columns clause.  We use the
:meth:`_sql.Select.select_from` method to achieve this, as below
where we establish ``user_table`` as the first element in the FROM
clause and :meth:`_sql.Select.join` to establish ``address_table`` as
the second::

    >>> print(
    ...     select(address_table.c.email_address).
    ...     select_from(user_table).join(address_table)
    ... )
    {opensql}SELECT address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id

Another example where we might want to use :meth:`_sql.Select.select_from`
is if our columns clause doesn't have enough information to provide for a
FROM clause.  For example, to SELECT from the common SQL expression
``count(*)``, we use a SQLAlchemy element known as :attr:`_sql.func` to
produce the SQL ``count()`` function::

    >>> from sqlalchemy import func
    >>> print (
    ...     select(func.count('*')).select_from(user_table)
    ... )
    {opensql}SELECT count(:count_2) AS count_1
    FROM user_account

.. _tutorial_select_join_onclause:

Setting the ON Clause
~~~~~~~~~~~~~~~~~~~~~

The previous examples on JOIN illustrated that the :class:`_sql.Select` construct
can join between two tables and produce the ON clause automatically.  This
occurs in those examples because the ``user_table`` and ``address_table``
:class:`_sql.Table` objects include a single :class:`_schema.ForeignKeyConstraint`
definition which is used to form this ON clause.

If the left and right targets of the join do not have such a constraint, or
there are multiple constraints in place, we need to specify the ON clause
directly.   Both :meth:`_sql.Select.join` and :meth:`_sql.Select.join_from`
accept an additional argument for the ON clause, which is stated using the
same SQL Expression mechanics as we saw about in :ref:`tutorial_select_where_clause`::

    >>> print(
    ...     select(address_table.c.email_address).
    ...     select_from(user_table).
    ...     join(address_table, user_table.c.id == address_table.c.user_id)
    ... )
    {opensql}SELECT address.email_address
    FROM user_account JOIN address ON user_account.id = address.user_id

.. container:: orm-header

    **ORM Tip** - there's another way to generate the ON clause when using
    ORM entities as well, when using the :func:`_orm.relationship` construct
    that can be seen in the mapping set up at :ref:`tutorial_declaring_mapped_classes`.
    This is a whole subject onto itself, which is introduced more fully
    at :ref:`tutorial_joining_relationships`.

OUTER and FULL join
~~~~~~~~~~~~~~~~~~~

Both the :meth:`_sql.Select.join` and :meth:`_sql.Select.join_from` methods
accept keyword arguments :paramref:`_sql.Select.join.isouter` and
:paramref:`_sql.Select.join.full` which will render LEFT OUTER JOIN
and FULL OUTER JOIN, respectively::

    >>> print(
    ...     select(user_table).join(address_table, isouter=True)
    ... )
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account LEFT OUTER JOIN address ON user_account.id = address.user_id

    >>> print(
    ...     select(user_table).join(address_table, full=True)
    ... )
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account FULL OUTER JOIN address ON user_account.id = address.user_id

There is also a method :meth:`_sql.Select.outerjoin` that is equivalent to
using ``.join(..., isouter=True)``.

ORDER BY
^^^^^^^^^

The ORDER BY clause is constructed in terms
of SQL Expression constructs typically based on :class:`_schema.Column` or
similar objects.  The :meth:`_sql.Select.order_by` method accepts one or
more of these expressions positionally::

    >>> print(select(user_table).order_by(user_table.c.name))
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account ORDER BY user_account.name

Ascending / descending is available from the :meth:`_sql.ColumnElement.asc`
and :meth:`_sql.ColumnElement.desc` modifiers, which are present
from ORM-bound attributes as well::


    >>> print(select(User).order_by(User.name.asc(), User.fullname.desc()))
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account ORDER BY user_account.name ASC, user_account.fullname DESC

.. _tutorial_group_by_w_aggregates:

Aggregate functions with GROUP BY / HAVING
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In SQL, aggregate functions allow column expressions across multiple rows
to be aggregated together to produce a single result.  Examples include
counting, computing averages, as well as locating the maximum or minimum
value in a set of values.

SQLAlchemy provides for SQL functions in an open-ended way using a namespace
known as :data:`_sql.func`.  This is a special constructor object which
will create new instances of :class:`_functions.Function` when given the name
of a particular SQL function, which can be any name, as well as zero or
more arguments to pass to the function, which are like in all other cases
SQL Expression constructs.   For example, to
render the SQL COUNT() function against the ``user_account.id`` column,
we call upon the name ``count()`` name::

    >>> from sqlalchemy import func
    >>> count_fn = func.count(user_table.c.id)
    >>> print(count_fn)
    {opensql}count(user_account.id)

When using aggregate functions in SQL, the GROUP BY clause is essential in that
it allows rows to be partitioned into groups where aggregate functions will
be applied to each group individually.  When requesting non-aggregated columns
in the COLUMNS clause of a SELECT statement, SQL requires that these columns
all be subject to a GROUP BY clause, either directly or indirectly based on
a primary key association.    The HAVING clause is then used in a similar
manner as the WHERE clause, except that it filters out rows based on aggregated
values rather than direct row contents.

SQLAlchemy provides for these two clauses using the :meth:`_sql.Select.group_by`
and :meth:`_sql.Select.having` methods.   Below we illustrate selecting
user name fields as well as count of addresses, for those users that have more
than one address:

.. sourcecode:: python+sql

    >>> with engine.connect() as conn:
    ...     result = conn.execute(
    ...         select(User.name, func.count(Address.id).label("count")).
    ...         join(Address).
    ...         group_by(User.name).
    ...         having(func.count(Address.id) > 1)
    ...     )
    ...     print(result.all())
    {opensql}BEGIN (implicit)
    SELECT user_account.name, count(address.id) AS count
    FROM user_account JOIN address ON user_account.id = address.user_id GROUP BY user_account.name
    HAVING count(address.id) > ?
    [...] (1,){stop}
    [('sandy', 2)]
    {opensql}ROLLBACK{stop}

Ordering or Grouping by a Label
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An important technique in particular on some database backends is the ability
to ORDER BY or GROUP BY an expression that is already stated in the columns
clause, without re-stating the expression in the ORDER BY or GROUP BY clause
and instead using the column name or labeled name from the COLUMNS clause.
This form is available by passing the string text of the name to the
:meth:`_sql.Select.order_by` or :meth:`_sql.Select.group_by` method.  The text
passed is **not rendered directly**; instead, the name given to an expression
in the columns clause and rendered as that expression name in context, raising an
error if no match is found.   The unary modifiers
:func:`.asc` and :func:`.desc` may also be used in this form:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import func, desc
    >>> stmt = select(
    ...         Address.user_id,
    ...         func.count(Address.id).label('num_addresses')).\
    ...         group_by("user_id").order_by("user_id", desc("num_addresses"))
    >>> print(stmt)
    {opensql}SELECT address.user_id, count(address.id) AS num_addresses
    FROM address GROUP BY address.user_id ORDER BY address.user_id, num_addresses DESC

.. _tutorial_using_aliases:

Using Aliases
^^^^^^^^^^^^^

Now that we are selecting from multiple tables and using joins, we quickly
run into the case where we need to refer to the same table mutiple times
in the FROM clause of a statement.  We accomplish this using SQL **aliases**,
which are a syntax that supplies an alternative name to a table or subquery
from which it can be referred towards in the statement.

In the SQLAlchemy Expression Language, these "names" are instead represented by
:class:`_sql.FromClause` objects known as the :class:`_sql.Alias` construct,
which is constructed in Core using the :meth:`_sql.FromClause.alias`
method. An :class:`_sql.Alias` construct is just like a :class:`_sql.Table`
construct in that it also has a namespace of :class:`_schema.Column`
objects within the :attr:`_sql.Alias.c` collection.  The SELECT statement
below for example returns all unique pairs of user names::

    >>> user_alias_1 = user_table.alias()
    >>> user_alias_2 = user_table.alias()
    >>> print(
    ...     select(user_alias_1.c.name, user_alias_2.c.name).
    ...     join_from(user_alias_1, user_alias_2, user_alias_1.c.id > user_alias_2.c.id)
    ... )
    {opensql}SELECT user_account_1.name, user_account_2.name
    FROM user_account AS user_account_1
    JOIN user_account AS user_account_2 ON user_account_1.id > user_account_2.id

.. _tutorial_orm_entity_aliases:

ORM Entity Aliases
~~~~~~~~~~~~~~~~~~

The ORM equivalent of the :meth:`_sql.FromClause.alias` method is the
ORM :func:`_orm.aliased` function, which may be applied to an entity
such as ``User`` and ``Address``.  This produces a :class:`_sql.Alias` object
internally that's against the original mapped :class:`_schema.Table` object,
while maintaining ORM functionality.  The SELECT below selects from the
``User`` entity all objects that include two particular email addresses::

    >>> from sqlalchemy.orm import aliased
    >>> address_alias_1 = aliased(Address)
    >>> address_alias_2 = aliased(Address)
    >>> print(
    ...     select(User).
    ...     join_from(User, address_alias_1).
    ...     where(address_alias_1.email_address == 'patrick@aol.com').
    ...     join_from(User, address_alias_2).
    ...     where(address_alias_2.email_address == 'patrick@gmail.com')
    ... )
    {opensql}SELECT user_account.id, user_account.name, user_account.fullname
    FROM user_account
    JOIN address AS address_1 ON user_account.id = address_1.user_id
    JOIN address AS address_2 ON user_account.id = address_2.user_id
    WHERE address_1.email_address = :email_address_1
    AND address_2.email_address = :email_address_2

.. tip::

    As mentioned in :ref:`tutorial_select_join_onclause`, the ORM provides
    for another way to join using the :func:`_orm.relationship` construct.
    The above example using aliases is demonstrated using :func:`_orm.relationship`
    at :ref:`tutorial_joining_relationships_aliased`.


.. _tutorial_subqueries_ctes:

Subqueries and CTEs
^^^^^^^^^^^^^^^^^^^^

A subquery in SQL is a SELECT statement that is rendered within parenthesis and
placed within the context of an enclosing statement, typically a SELECT
statement but not necessarily.

This section will cover a so-called "non-scalar" subquery, which is typically
placed in the FROM clause of an enclosing SELECT.   We will also cover the
Common Table Expression or CTE, which is used in a similar way as a subquery,
but includes additional features.

SQLAlchemy uses the :class:`_sql.Subquery` object to represent a subquery and
the :class:`_sql.CTE` to represent a CTE, usually obtained from the
:meth:`_sql.Select.subquery` and :meth:`_sql.Select.cte` methods, respectively.
Either object can be used as a FROM element inside of a larger
:func:`_sql.select` construct.

We can construct a :class:`_sql.Subquery` that will select an aggregate count
of rows from the ``address`` table (aggregate functions and GROUP BY were
introduced previously at :ref:`tutorial_group_by_w_aggregates`):

    >>> subq = select(
    ...     func.count(address_table.c.id).label("count"),
    ...     address_table.c.user_id
    ... ).group_by(address_table.c.user_id).subquery()

Stringifying the subquery by itself without it being embedded inside of another
:class:`_sql.Select` or other statement produces the plain SELECT statement
without any enclosing parenthesis::

    >>> print(subq)
    {opensql}SELECT count(address.id) AS count, address.user_id
    FROM address GROUP BY address.user_id


The :class:`_sql.Subquery` object behaves like any other FROM object such
as a :class:`_schema.Table`, notably that it includes a :attr:`_sql.Subquery.c`
namespace of the columns which it selects.  We can use this namespace to
refer to both the ``user_id`` column as well as our custom labeled
``count`` expression::

    >>> print(select(subq.c.user_id, subq.c.count))
    {opensql}SELECT anon_1.user_id, anon_1.count
    FROM (SELECT count(address.id) AS count, address.user_id AS user_id
    FROM address GROUP BY address.user_id) AS anon_1

With a selection of rows contained within the ``subq`` object, we can apply
the object to a larger :class:`_sql.Select` that will join the data to
the ``user_account`` table::

    >>> stmt = select(
    ...    user_table.c.name,
    ...    user_table.c.fullname,
    ...    subq.c.count
    ... ).join_from(user_table, subq)

    >>> print(stmt)
    {opensql}SELECT user_account.name, user_account.fullname, anon_1.count
    FROM user_account JOIN (SELECT count(address.id) AS count, address.user_id AS user_id
    FROM address GROUP BY address.user_id) AS anon_1 ON user_account.id = anon_1.user_id

In order to join from ``user_account`` to ``address``, we made use of the
:meth:`_sql.Select.join_from` method.   As has been illustrated previously, the
ON clause of this join was again **inferred** based on foreign key constraints.
Even though a SQL subquery does not itself have any constraints, SQLAlchemy can
act upon constraints represented on the columns by determining that the
``subq.c.user_id`` column is **derived** from the ``address_table.c.user_id``
column, which does express a foreign key relationship back to the
``user_table.c.id`` column which is then used to generate the ON clause.

Common Table Expressions (CTEs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Usage of the :class:`_sql.CTE` construct in SQLAlchemy is virtually
the same as how the :class:`_sql.Subquery` construct is used.  By changing
the invocation of the :meth:`_sql.Select.subquery` method to use
:meth:`_sql.Select.cte` instead, we can use the resulting object as a FROM
element in the same way, but the SQL rendered is the very different common
table expression syntax::

    >>> subq = select(
    ...     func.count(address_table.c.id).label("count"),
    ...     address_table.c.user_id
    ... ).group_by(address_table.c.user_id).cte()

    >>> stmt = select(
    ...    user_table.c.name,
    ...    user_table.c.fullname,
    ...    subq.c.count
    ... ).join_from(user_table, subq)

    >>> print(stmt)
    {opensql}WITH anon_1 AS
    (SELECT count(address.id) AS count, address.user_id AS user_id
    FROM address GROUP BY address.user_id)
     SELECT user_account.name, user_account.fullname, anon_1.count
    FROM user_account JOIN anon_1 ON user_account.id = anon_1.user_id

The :class:`_sql.CTE` construct also features the ability to be used
in a "recursive" style, and may in more elaborate cases be composed from the
RETURNING clause of an INSERT, UPDATE or DELETE statement.  The docstring
for :class:`_sql.CTE` includes details on these additional patterns.

.. seealso::

    :meth:`_sql.Select.subquery` - further detail on subqueries

    :meth:`_sql.Select.cte` - examples for CTE including how to use
    RECURSIVE as well as DML-oriented CTEs

ORM Entity Subqueries/CTEs
~~~~~~~~~~~~~~~~~~~~~~~~~~

In the ORM, the :func:`_orm.aliased` construct may be used to associate an ORM
entity, such as our ``User`` or ``Address`` class, with any :class:`_sql.FromClause`
concept that represents a source of rows.  The preceding section
:ref:`tutorial_orm_entity_aliases` illustrates using :func:`_orm.aliased`
to associate the mapped class with an :class:`_sql.Alias` of its
mapped :class:`_schema.Table`.   Here we illustrate :func:`_orm.aliased` doing the same
thing against both a :class:`_sql.Subquery` as well as a :class:`_sql.CTE`
generated against a :class:`_sql.Select` construct, that ultimately derives
from that same mapped :class:`_schema.Table`.

Below is an example of applying :func:`_orm.aliased` to the :class:`_sql.Subquery`
construct, so that ORM entities can be extracted from its rows.  The result
shows a series of ``User`` and ``Address`` objects, where the data for
each ``Address`` object ultimately came from a subquery against the
``address`` table rather than that table directly:

.. sourcecode:: python+sql

    >>> subq = select(Address).where(~Address.email_address.like('%@aol.com')).subquery()
    >>> address_subq = aliased(Address, subq)
    >>> stmt = select(User, address_subq).join_from(User, address_subq).order_by(User.id, address_subq.id)
    >>> with Session(engine) as session:
    ...     for user, address in session.execute(stmt):
    ...         print(f"{user} {address}")
    {opensql}BEGIN (implicit)
    SELECT user_account.id, user_account.name, user_account.fullname,
    anon_1.id AS id_1, anon_1.email_address, anon_1.user_id
    FROM user_account JOIN
    (SELECT address.id AS id, address.email_address AS email_address, address.user_id AS user_id
    FROM address
    WHERE address.email_address NOT LIKE ?) AS anon_1 ON user_account.id = anon_1.user_id
    ORDER BY user_account.id, anon_1.id
    [...] ('%@aol.com',){stop}
    User(id=1, name='spongebob', fullname='Spongebob Squarepants') Address(id=1, email_address='spongebob@sqlalchemy.org')
    User(id=2, name='sandy', fullname='Sandy Cheeks') Address(id=2, email_address='sandy@sqlalchemy.org')
    User(id=2, name='sandy', fullname='Sandy Cheeks') Address(id=3, email_address='sandy@squirrelpower.org')
    {opensql}ROLLBACK{stop}

Another example follows, which is exactly the same except it makes use of the
:class:`_sql.CTE` construct instead:

.. sourcecode:: python+sql

    >>> cte = select(Address).where(~Address.email_address.like('%@aol.com')).cte()
    >>> address_cte = aliased(Address, cte)
    >>> stmt = select(User, address_cte).join_from(User, address_cte).order_by(User.id, address_cte.id)
    >>> with Session(engine) as session:
    ...     for user, address in session.execute(stmt):
    ...         print(f"{user} {address}")
    {opensql}BEGIN (implicit)
    WITH anon_1 AS
    (SELECT address.id AS id, address.email_address AS email_address, address.user_id AS user_id
    FROM address
    WHERE address.email_address NOT LIKE ?)
    SELECT user_account.id, user_account.name, user_account.fullname,
    anon_1.id AS id_1, anon_1.email_address, anon_1.user_id
    FROM user_account
    JOIN anon_1 ON user_account.id = anon_1.user_id
    ORDER BY user_account.id, anon_1.id
    [...] ('%@aol.com',){stop}
    User(id=1, name='spongebob', fullname='Spongebob Squarepants') Address(id=1, email_address='spongebob@sqlalchemy.org')
    User(id=2, name='sandy', fullname='Sandy Cheeks') Address(id=2, email_address='sandy@sqlalchemy.org')
    User(id=2, name='sandy', fullname='Sandy Cheeks') Address(id=3, email_address='sandy@squirrelpower.org')
    {opensql}ROLLBACK{stop}

In both cases, the subquery and CTE were named at the SQL level using an
"anonymous" name.  In the Python code, we don't need to provide these names
at all.  The object identity of the :class:`_sql.Subquery` or :class:`_sql.CTE`
instances serves as the syntactical identity of the object when rendered.

.. _tutorial_scalar_subquery:

Scalar and Correlated Subqueries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A scalar subquery is a subquery that returns exactly zero or one row and
exactly one column.  The subquery is then used in the COLUMNS or WHERE clause
of an enclosing SELECT statement and is different than a regular subquery in
that it is not used in the FROM clause.   A :term:`correlated subquery` is a
scalar subquery that refers to a table in the enclosing SELECT statement.

SQLAlchemy represents the scalar subquery using the
:class:`_sql.ScalarSelect` construct, which is part of the
:class:`_sql.ColumnElement` expression hierarchy, in contrast to the regular
subquery which is represented by the :class:`_sql.Subquery` construct, which is
in the :class:`_sql.FromClause` hierarchy.

Scalar subqueries are often, but not necessarily, used with aggregate functions,
introduced previously at :ref:`tutorial_group_by_w_aggregates`.   A scalar
subquery is indicated explicitly by making use of the :meth:`_sql.Select.scalar_subquery`
method as below.  It's default string form when stringified by itself
renders as an ordinary SELECT statement that is selecting from two tables::

    >>> subq = select(func.count(address_table.c.id)).\
    ...             where(user_table.c.id == address_table.c.user_id).\
    ...             scalar_subquery()
    >>> print(subq)
    {opensql}(SELECT count(address.id) AS count_1
    FROM address, user_account
    WHERE user_account.id = address.user_id)

The above ``subq`` object now falls within the :class:`_sql.ColumnElement`
SQL expression hierarchy, in that it may be used like any other column
expression::

    >>> print(subq == 5)
    {opensql}(SELECT count(address.id) AS count_1
    FROM address, user_account
    WHERE user_account.id = address.user_id) = :param_1


Although the scalar subquery by itself renders both ``user_account`` and
``address`` in its FROM clause when stringified by itself, when embedding it
into an enclosing :func:`_sql.select` construct that deals with the
``user_account`` table, the ``user_account`` table is automatically
**correlated**, meaning it does not render in the FROM clause of the subquery::

    >>> stmt = select(user_table.c.name, subq.label("address_count"))
    >>> print(stmt)
    {opensql}SELECT user_account.name, (SELECT count(address.id) AS count_1
    FROM address
    WHERE user_account.id = address.user_id) AS address_count
    FROM user_account

Simple correlated subqueries will usually do the right thing that's desired.
However, in the case where the correlation is ambiguous, SQLAlchemy will let
us know that more clarity is needed::

    >>> stmt = select(
    ...     user_table.c.name,
    ...     address_table.c.email_address,
    ...     subq.label("address_count")
    ... ).\
    ... join_from(user_table, address_table).\
    ... order_by(user_table.c.id, address_table.c.id)
    >>> print(stmt)
    Traceback (most recent call last):
    ...
    InvalidRequestError: Select statement '<... Select object at ...>' returned
    no FROM clauses due to auto-correlation; specify correlate(<tables>) to
    control correlation manually.

To specify that the ``user_table`` is the one we seek to correlate we specify
this using the :meth:`_sql.ScalarSelect.correlate` or
:meth:`_sql.ScalarSelect.correlate_except` methods::

    >>> subq = select(func.count(address_table.c.id)).\
    ...             where(user_table.c.id == address_table.c.user_id).\
    ...             scalar_subquery().correlate(user_table)

The statement then can return the data for this column like any other:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...     result = conn.execute(
    ...         select(
    ...             user_table.c.name,
    ...             address_table.c.email_address,
    ...             subq.label("address_count")
    ...         ).
    ...         join_from(user_table, address_table).
    ...         order_by(user_table.c.id, address_table.c.id)
    ...     )
    ...     print(result.all())
    {opensql}BEGIN (implicit)
    SELECT user_account.name, address.email_address, (SELECT count(address.id) AS count_1
    FROM address
    WHERE user_account.id = address.user_id) AS address_count
    FROM user_account JOIN address ON user_account.id = address.user_id ORDER BY user_account.id, address.id
    [...] (){stop}
    [('spongebob', 'spongebob@sqlalchemy.org', 1), ('sandy', 'sandy@sqlalchemy.org', 2),
     ('sandy', 'sandy@squirrelpower.org', 2)]
    {opensql}ROLLBACK{stop}

.. _tutorial_exists:

EXISTS subqueries
^^^^^^^^^^^^^^^^^^

The SQL EXISTS keyword is an operator that is used with :ref:`scalar subqueries
<tutorial_scalar_subquery>` to return a boolean true or false depending on if
the SELECT statement would return a row.  SQLAlchemy includes a variant of the
:class:`_sql.ScalarSelect` object called :class:`_sql.Exists`, which will
generate an EXISTS subquery and is most conveniently generated using the
:meth:`_sql.SelectBase.exists` method.  Below we produce an EXISTS so that we
can return ``user_account`` rows that have more than one related row in
``address``:

.. sourcecode:: pycon+sql

    >>> subq = (
    ...     select(func.count(address_table.c.id)).
    ...     where(user_table.c.id == address_table.c.user_id).
    ...     group_by(address_table.c.user_id).
    ...     having(func.count(address_table.c.id) > 1)
    ... ).exists()
    >>> with engine.connect() as conn:
    ...     result = conn.execute(
    ...         select(user_table.c.name).where(subq)
    ...     )
    ...     print(result.all())
    {opensql}BEGIN (implicit)
    SELECT user_account.name
    FROM user_account
    WHERE EXISTS (SELECT count(address.id) AS count_1
    FROM address
    WHERE user_account.id = address.user_id GROUP BY address.user_id
    HAVING count(address.id) > ?)
    [...] (1,){stop}
    [('sandy',)]
    {opensql}ROLLBACK{stop}

The EXISTS construct is more often than not used as a negation, e.g. NOT EXISTS,
as it provides a SQL-efficient form of locating rows for which a related
table has no rows.  Below we select user names that have no email addresses;
note the binary negation operator (``~``) used inside the second WHERE
clause:

.. sourcecode:: pycon+sql

    >>> subq = (
    ...     select(address_table.c.id).
    ...     where(user_table.c.id == address_table.c.user_id)
    ... ).exists()
    >>> with engine.connect() as conn:
    ...     result = conn.execute(
    ...         select(user_table.c.name).where(~subq)
    ...     )
    ...     print(result.all())
    {opensql}BEGIN (implicit)
    SELECT user_account.name
    FROM user_account
    WHERE NOT (EXISTS (SELECT address.id
    FROM address
    WHERE user_account.id = address.user_id))
    [...] (){stop}
    [('patrick',)]
    {opensql}ROLLBACK{stop}


.. rst-class:: core-header, orm-addin

.. _tutorial_core_update_delete:

Core UPDATE and DELETE
----------------------

So far we've covered :class:`_sql.Insert`, so that we can get some data into
our database, and then spent a lot of time on :class:`_sql.Select` which
handles the broad range of usage patterns used for retrieving data from the
database.   In this section we will cover the :class:`_sql.Update` and
:class:`_sql.Delete` constructs, which are used to modify existing rows
as well as delete existing rows.    This section will cover these constructs
from a Core-centric perspective.


.. container:: orm-header

    **ORM Readers** - As was the case mentioned at :ref:`tutorial_core_insert`,
    the :class:`_sql.Update` and :class:`_sql.Delete` operations when used with
    the ORM are usually invoked internally from the :class:`_orm.Session`
    object as part of the :term:`unit of work` process.

    However, unlike :class:`_sql.Insert`, the :class:`_sql.Update` and
    :class:`_sql.Delete` constructs can also be used directly with the ORM,
    using a pattern known as "ORM-enabled update and delete"; for this reason,
    familiarity with these constructs is useful for ORM use.  Both styles of
    use are discussed in the sections :ref:`tutorial_orm_updating` and
    :ref:`tutorial_orm_deleting`.

The update() SQL Expression Construct
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_sql.update` function generates a new instance of
:class:`_sql.Update` which represents an UPDATE statement in SQL, that will
update existing data in a table.

Like the :func:`_sql.insert` construct, there is a "traditional" form of
:func:`_sql.update`, which emits UPDATE against a single table at a time and
does not return any rows.   However some backends support an UPDATE statement
that may modify multiple tables at once, and the UPDATE statement also
supports RETURNING such that columns contained in matched rows may be returned
in the result set.

A basic UPDATE looks like::

    >>> from sqlalchemy import update
    >>> stmt = (
    ...     update(user_table).where(user_table.c.name == 'patrick').
    ...     values(fullname='Patrick the Star')
    ... )
    >>> print(stmt)
    {opensql}UPDATE user_account SET fullname=:fullname WHERE user_account.name = :name_1

The :meth:`_sql.Update.values` method controls the contents of the SET elements
of the UPDATE statement.  This is the same method shared by the :class:`_sql.Insert`
construct.   Parameters can normally be passed using the column names as
keyword arguments.

UPDATE supports all the major SQL forms of UPDATE, including updates against expressions,
where we can make use of :class:`_schema.Column` expressions::

    >>> stmt = (
    ...     update(user_table).
    ...     values(fullname="Username: " + user_table.c.name)
    ... )
    >>> print(stmt)
    {opensql}UPDATE user_account SET fullname=(:name_1 || user_account.name)

To support UPDATE in an "executemany" context, where many parameter sets will
be invoked against the same statement, the :func:`_sql.bindparam`
construct may be used to set up bound parameters; these replace the places
that literal values would normally go:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import bindparam
    >>> stmt = (
    ...   update(user_table).
    ...   where(user_table.c.name == bindparam('oldname')).
    ...   values(name=bindparam('newname'))
    ... )
    >>> with engine.begin() as conn:
    ...   conn.execute(
    ...       stmt,
    ...       [
    ...          {'oldname':'jack', 'newname':'ed'},
    ...          {'oldname':'wendy', 'newname':'mary'},
    ...          {'oldname':'jim', 'newname':'jake'},
    ...       ]
    ...   )
    {opensql}BEGIN (implicit)
    UPDATE user_account SET name=? WHERE user_account.name = ?
    [...] (('ed', 'jack'), ('mary', 'wendy'), ('jake', 'jim'))
    <sqlalchemy.engine.cursor.CursorResult object at 0x...>
    COMMIT{stop}

Other techniques which may be applied to UPDATE include:

* **Correlated Updates**:  a :ref:`correlated subquery <tutorial_scalar_subquery>`
  may be used anywhere a column expression might be
  placed::

    >>> scalar_subq = (
    ...   select(address_table.c.email_address).
    ...   where(address_table.c.user_id == user_table.c.id).
    ...   order_by(address_table.c.id).
    ...   limit(1).
    ...   scalar_subquery()
    ... )
    >>> update_stmt = update(user_table).values(fullname=scalar_subq)
    >>> print(update_stmt)
    {opensql}UPDATE user_account SET fullname=(SELECT address.email_address
    FROM address
    WHERE address.user_id = user_account.id ORDER BY address.id
    LIMIT :param_1)

  ..


* **UPDATE..FROM**:  Some databases such as PostgreSQL and MySQL support a syntax
  "UPDATE FROM" where additional tables may be stated in the FROM clause.
  This syntax will be generated implicitly when additional tables are located
  in the WHERE clause of the statement::

    >>> update_stmt = (
    ...    update(user_table).
    ...    where(user_table.c.id == address_table.c.user_id).
    ...    where(address_table.c.email_address == 'patrick@aol.com').
    ...    values(fullname='Pat')
    ...  )
    >>> print(update_stmt)
    {opensql}UPDATE user_account SET fullname=:fullname FROM address
    WHERE user_account.id = address.user_id AND address.email_address = :email_address_1

  ..

* **UPDATE..FROM updating multiple tables**: this is a MySQL specific syntax which
  requires we refer to :class:`_schema.Table` objects in the VALUES
  clause in order to refer to additional tables::

    >>> update_stmt = (
    ...    update(user_table).
    ...    where(user_table.c.id == address_table.c.user_id).
    ...    where(address_table.c.email_address == 'patrick@aol.com').
    ...    values(
    ...        {
    ...            user_table.c.fullname: "Pat",
    ...            address_table.c.email_address: "pat@aol.com"
    ...        }
    ...    )
    ...  )
    >>> from sqlalchemy.dialects import mysql
    >>> print(update_stmt.compile(dialect=mysql.dialect()))
    {opensql}UPDATE user_account, address
    SET address.email_address=%s, user_account.fullname=%s
    WHERE user_account.id = address.user_id AND address.email_address = %s

  ..

* **Parameter Ordered Updates**: Another MySQL-only behavior is that the order
  of parameters in the SET clause of an UPDATE actually impacts the evaluation
  of each expression.   For this use case, the :meth:`_sql.Update.ordered_values`
  method accepts a sequence of tuples so that this order may be controlled [1]_::

    >>> update_stmt = (
    ...     update(some_table).
    ...     ordered_values(
    ...         (some_table.c.y, 20),
    ...         (some_table.c.x, some_table.c.y + 10)
    ...     )
    ... )
    >>> print(update_stmt)
    {opensql}UPDATE some_table SET y=:y, x=(some_table.y + :y_1)

  ..


.. [1] While Python dictionaries are `guaranteed to be insert ordered
   <https://mail.python.org/pipermail/python-dev/2017-December/151283.html>`_
   as of Python 3.7, the
   :meth:`_sql.Update.ordered_values` method stilll provides an additional
   measure of clarity of intent when it is essential that the SET clause
   of a MySQL UPDATE statement proceed in a specific way.


The delete() SQL Expression Construct
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`_sql.delete` function generates a new instance of
:class:`_sql.Delete` which represents an DELETE statement in SQL, that will
delete rows from a table.

The :func:`_sql.delete` statement from an API perspective is very similar to
that of the :func:`_sql.update` construct, traditionally returning no rows but
allowing for a RETURNING variant.

::

    >>> from sqlalchemy import delete
    >>> stmt = (
    ...     delete(user_table).where(user_table.c.name == 'patrick')
    ... )
    >>> print(stmt)
    {opensql}DELETE FROM user_account WHERE user_account.name = :name_1

Like :class:`_sql.Update`, :class:`_sql.Delete` supports the use of correlated
subqueries in the WHERE clause as well as backend-specific multiple table
syntaxes, such as ``DELETE FROM..USING`` on MySQL::

    >>> delete_stmt = (
    ...    delete(user_table).
    ...    where(user_table.c.id == address_table.c.user_id).
    ...    where(address_table.c.email_address == 'patrick@aol.com')
    ...  )
    >>> from sqlalchemy.dialects import mysql
    >>> print(delete_stmt.compile(dialect=mysql.dialect()))
    {opensql}DELETE FROM user_account USING user_account, address
    WHERE user_account.id = address.user_id AND address.email_address = %s

Getting Affected Row Count from UPDATE, DELETE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Both :class:`_sql.Update` and :class:`_sql.Delete` support the ability to
return the number of rows matched after the statement proceeds, for statements
that are invoked using Core :class:`_engine.Connection`, i.e.
:meth:`_engine.Connection.execute`. Per the caveats mentioned below, this value
is available from the :attr:`_engine.CursorResult.rowcount` attribute:

.. sourcecode:: pycon+sql

    >>> with engine.begin() as conn:
    ...     result = conn.execute(
    ...         update(user_table).
    ...         values(fullname="Patrick McStar").
    ...         where(user_table.c.name == 'patrick')
    ...     )
    ...     print(result.rowcount)
    {opensql}BEGIN (implicit)
    UPDATE user_account SET fullname=? WHERE user_account.name = ?
    [...] ('Patrick McStar', 'patrick'){stop}
    1
    {opensql}COMMIT{stop}

.. tip::

    The :class:`_engine.CursorResult` class is a subclass of
    :class:`_engine.Result` which contains additional attributes that are
    specific to the DBAPI ``cursor`` object.  An instance of this subclass is
    returned when a statement is invoked via the
    :meth:`_engine.Connection.execute` method. When using the ORM, the
    :meth:`_orm.Session.execute` method returns an object of this type for
    all INSERT, UPDATE, and DELETE statements.

Facts about :attr:`_engine.CursorResult.rowcount`:

* The value returned is the number of rows **matched** by the WHERE clause of
  the statement.   It does not matter if the row were actually modified or not.

* :attr:`_engine.CursorResult.rowcount` is not necessarily available for an UPDATE
  or DELETE statement that uses RETURNING.

* For an :ref:`executemany <tutorial_multiple_parameters>` execution,
  :attr:`_engine.CursorResult.rowcount` may not be available either, which depends
  highly on the DBAPI module in use as well as configured options.  The
  attribute :attr:`_engine.CursorResult.supports_sane_multi_rowcount` indicates
  if this value will be available for the current backend in use.

* Some drivers, particularly third party dialects for non-relational databases,
  may not support :attr:`_engine.CursorResult.rowcount` at all.   The
  :attr:`_engine.CursorResult.supports_sane_rowcount` will indicate this.

* "rowcount" is used by the ORM :term:`unit of work` process to validate that
  an UPDATE or DELETE statement matched the expected number of rows, and is
  also essential for the ORM versioning feature documented at
  :ref:`mapper_version_counter`.

Using RETURNING with UPDATE, DELETE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Like the :class:`_sql.Insert` construct, :class:`_sql.Update` and :class:`_sql.Delete`
also support the RETURNING clause which is added by using the
:meth:`_sql.Update.returning` and :meth:`_sql.Delete.returning` methods.
When these methods are used on a backend that supports RETURNING, selected
columns from all rows that match the WHERE criteria of the statement
will be returned in the :class:`_engine.Result` object as rows that can
be iterated::


    >>> update_stmt = (
    ...     update(user_table).where(user_table.c.name == 'patrick').
    ...     values(fullname='Patrick the Star').
    ...     returning(user_table.c.id, user_table.c.name)
    ... )
    >>> print(update_stmt)
    {opensql}UPDATE user_account SET fullname=:fullname
    WHERE user_account.name = :name_1
    RETURNING user_account.id, user_account.name

    >>> delete_stmt = (
    ...     delete(user_table).where(user_table.c.name == 'patrick').
    ...     returning(user_table.c.id, user_table.c.name)
    ... )
    >>> print(delete_stmt.returning(user_table.c.id, user_table.c.name))
    {opensql}DELETE FROM user_account
    WHERE user_account.name = :name_1
    RETURNING user_account.id, user_account.name

Further Reading for UPDATE, DELETE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. seealso::

    API documentation for UPDATE / DELETE:

    * :class:`_sql.Update`

    * :class:`_sql.Delete`

    ORM-enabled UPDATE and DELETE:

    * :ref:`tutorial_orm_enabled_update`

    * :ref:`tutorial_orm_enabled_delete`

