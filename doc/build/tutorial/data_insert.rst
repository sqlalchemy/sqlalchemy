.. highlight:: pycon+sql

.. |prev| replace:: :doc:`data`
.. |next| replace:: :doc:`data_select`

.. include:: tutorial_nav_include.rst


.. rst-class:: core-header


.. _tutorial_core_insert:

Using INSERT Statements
-----------------------

When using Core as well as when using the ORM for bulk operations, a SQL INSERT
statement is generated directly using the :func:`_sql.insert` function - this
function generates a new instance of :class:`_sql.Insert` which represents an
INSERT statement in SQL, that adds new data into a table.

.. container:: orm-header

    **ORM Readers** -

    The ORM's means of generating INSERT statements is described in
    one of two ways; the most common is by using
    the :term:`unit of work` process which automates the generation of
    INSERT statements from object state, and is introduced
    at :ref:`tutorial_inserting_orm`.  The other is by using
    the :class:`_sql.Insert` construct directly
    in a manner very similar to that described in this section; this use
    is introduced at :ref:`tutorial_orm_bulk`.


The insert() SQL Expression Construct
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A simple example of :class:`_sql.Insert` illustrating the target table
and the VALUES clause at once::

    >>> from sqlalchemy import insert
    >>> stmt = insert(user_table).values(name="spongebob", fullname="Spongebob Squarepants")

The above ``stmt`` variable is an instance of :class:`_sql.Insert`.  Most
SQL expressions can be stringified in place as a means to see the general
form of what's being produced::

    >>> print(stmt)
    {printsql}INSERT INTO user_account (name, fullname) VALUES (:name, :fullname)

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
    {execsql}BEGIN (implicit)
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

.. versionchanged:: 1.4.8 the tuple returned by
   :attr:`_engine.CursorResult.inserted_primary_key` is now a named tuple
   fulfilled by returning it as a :class:`_result.Row` object.

.. _tutorial_core_insert_values_clause:

INSERT usually generates the "values" clause automatically
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The example above made use of the :meth:`_sql.Insert.values` method to
explicitly create the VALUES clause of the SQL INSERT statement.   This method
in fact has some variants that allow for special forms such as multiple rows in
one statement and insertion of SQL expressions.   However the usual way that
:class:`_sql.Insert` is used is such that the VALUES clause is generated
automatically from the parameters passed to the
:meth:`_engine.Connection.execute` method; below we INSERT two more rows to
illustrate this:

.. sourcecode:: pycon+sql

    >>> with engine.connect() as conn:
    ...     result = conn.execute(
    ...         insert(user_table),
    ...         [
    ...             {"name": "sandy", "fullname": "Sandy Cheeks"},
    ...             {"name": "patrick", "fullname": "Patrick Star"},
    ...         ],
    ...     )
    ...     conn.commit()
    {execsql}BEGIN (implicit)
    INSERT INTO user_account (name, fullname) VALUES (?, ?)
    [...] [('sandy', 'Sandy Cheeks'), ('patrick', 'Patrick Star')]
    COMMIT{stop}

The execution above features "executemany" form first illustrated at
:ref:`tutorial_multiple_parameters`, however unlike when using the
:func:`_sql.text` construct, we didn't have to spell out any SQL.
By passing a dictionary or list of dictionaries to the :meth:`_engine.Connection.execute`
method in conjunction with the :class:`_sql.Insert` construct, the
:class:`_engine.Connection` ensures that the column names which are passed
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
        >>> scalar_subq = (
        ...     select(user_table.c.id)
        ...     .where(user_table.c.name == bindparam("username"))
        ...     .scalar_subquery()
        ... )

        >>> with engine.connect() as conn:
        ...     result = conn.execute(
        ...         insert(address_table).values(user_id=scalar_subq),
        ...         [
        ...             {
        ...                 "username": "spongebob",
        ...                 "email_address": "spongebob@sqlalchemy.org",
        ...             },
        ...             {"username": "sandy", "email_address": "sandy@sqlalchemy.org"},
        ...             {"username": "sandy", "email_address": "sandy@squirrelpower.org"},
        ...         ],
        ...     )
        ...     conn.commit()
        {execsql}BEGIN (implicit)
        INSERT INTO address (user_id, email_address) VALUES ((SELECT user_account.id
        FROM user_account
        WHERE user_account.name = ?), ?)
        [...] [('spongebob', 'spongebob@sqlalchemy.org'), ('sandy', 'sandy@sqlalchemy.org'),
        ('sandy', 'sandy@squirrelpower.org')]
        COMMIT{stop}

.. _tutorial_insert_from_select:

INSERT...FROM SELECT
^^^^^^^^^^^^^^^^^^^^^

The :class:`_sql.Insert` construct can compose
an INSERT that gets rows directly from a SELECT using the :meth:`_sql.Insert.from_select`
method::

    >>> select_stmt = select(user_table.c.id, user_table.c.name + "@aol.com")
    >>> insert_stmt = insert(address_table).from_select(
    ...     ["user_id", "email_address"], select_stmt
    ... )
    >>> print(insert_stmt)
    {printsql}INSERT INTO address (user_id, email_address)
    SELECT user_account.id, user_account.name || :name_1 AS anon_1
    FROM user_account

.. _tutorial_insert_returning:

INSERT...RETURNING
^^^^^^^^^^^^^^^^^^^^^

The RETURNING clause for supported backends is used
automatically in order to retrieve the last inserted primary key value
as well as the values for server defaults.   However the RETURNING clause
may also be specified explicitly using the :meth:`_sql.Insert.returning`
method; in this case, the :class:`_engine.Result`
object that's returned when the statement is executed has rows which
can be fetched::

    >>> insert_stmt = insert(address_table).returning(
    ...     address_table.c.id, address_table.c.email_address
    ... )
    >>> print(insert_stmt)
    {printsql}INSERT INTO address (id, user_id, email_address)
    VALUES (:id, :user_id, :email_address)
    RETURNING address.id, address.email_address

It can also be combined with :meth:`_sql.Insert.from_select`,
as in the example below that builds upon the example stated in
:ref:`tutorial_insert_from_select`::

    >>> select_stmt = select(user_table.c.id, user_table.c.name + "@aol.com")
    >>> insert_stmt = insert(address_table).from_select(
    ...     ["user_id", "email_address"], select_stmt
    ... )
    >>> print(insert_stmt.returning(address_table.c.id, address_table.c.email_address))
    {printsql}INSERT INTO address (user_id, email_address)
    SELECT user_account.id, user_account.name || :name_1 AS anon_1
    FROM user_account RETURNING address.id, address.email_address

.. tip::

    The RETURNING feature is also supported by UPDATE and DELETE statements,
    which will be introduced later in this tutorial.

    For INSERT statements, the RETURNING feature may be used
    both for single-row statements as well as for statements that INSERT
    multiple rows at once.  Support for multiple-row INSERT with RETURNING
    is dialect specific, however is supported for all the dialects
    that are included in SQLAlchemy which support RETURNING.  See the section
    :ref:`engine_insertmanyvalues` for background on this feature.


.. seealso::

    :class:`_sql.Insert` - in the SQL Expression API documentation

