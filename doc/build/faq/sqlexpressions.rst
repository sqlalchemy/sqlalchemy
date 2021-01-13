SQL Expressions
===============

.. contents::
    :local:
    :class: faq
    :backlinks: none

.. _faq_sql_expression_string:

How do I render SQL expressions as strings, possibly with bound parameters inlined?
------------------------------------------------------------------------------------

The "stringification" of a SQLAlchemy Core statement object or
expression fragment, as well as that of an ORM :class:`_query.Query` object,
in the majority of simple cases is as simple as using
the ``str()`` builtin function, as below when use it with the ``print``
function (note the Python ``print`` function also calls ``str()`` automatically
if we don't use it explicitly)::

    >>> from sqlalchemy import table, column, select
    >>> t = table('my_table', column('x'))
    >>> statement = select(t)
    >>> print(str(statement))
    SELECT my_table.x
    FROM my_table

The ``str()`` builtin, or an equivalent, can be invoked on ORM
:class:`_query.Query`  object as well as any statement such as that of
:func:`_expression.select`, :func:`_expression.insert` etc. and also any expression fragment, such
as::

    >>> from sqlalchemy import column
    >>> print(column('x') == 'some value')
    x = :x_1

Stringifying for Specific Databases
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A complication arises when the statement or fragment we are stringifying
contains elements that have a database-specific string format, or when it
contains elements that are only available within a certain kind of database.
In these cases, we might get a stringified statement that is not in the correct
syntax for the database we are targeting, or the operation may raise a
:class:`.UnsupportedCompilationError` exception.   In these cases, it is
necessary that we stringify the statement using the
:meth:`_expression.ClauseElement.compile` method, while passing along an :class:`_engine.Engine`
or :class:`.Dialect` object that represents the target database.  Such as
below, if we have a MySQL database engine, we can stringify a statement in
terms of the MySQL dialect::

    from sqlalchemy import create_engine

    engine = create_engine("mysql+pymysql://scott:tiger@localhost/test")
    print(statement.compile(engine))

More directly, without building up an :class:`_engine.Engine` object we can
instantiate a :class:`.Dialect` object directly, as below where we
use a PostgreSQL dialect::

    from sqlalchemy.dialects import postgresql
    print(statement.compile(dialect=postgresql.dialect()))

When given an ORM :class:`~.orm.query.Query` object, in order to get at the
:meth:`_expression.ClauseElement.compile`
method we only need access the :attr:`~.orm.query.Query.statement`
accessor first::

    statement = query.statement
    print(statement.compile(someengine))

Rendering Bound Parameters Inline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning:: **Never** use this technique with string content received from
   untrusted input, such as from web forms or other user-input applications.
   SQLAlchemy's facilities to  coerce Python values into direct SQL string
   values are **not secure against untrusted input and do not validate the type
   of data being passed**. Always use bound parameters when programmatically
   invoking non-DDL SQL statements against a relational database.

The above forms will render the SQL statement as it is passed to the Python
:term:`DBAPI`, which includes that bound parameters are not rendered inline.
SQLAlchemy normally does not stringify bound parameters, as this is handled
appropriately by the Python DBAPI, not to mention bypassing bound
parameters is probably the most widely exploited security hole in
modern web applications.   SQLAlchemy has limited ability to do this
stringification in certain circumstances such as that of emitting DDL.
In order to access this functionality one can use the ``literal_binds``
flag, passed to ``compile_kwargs``::

    from sqlalchemy.sql import table, column, select

    t = table('t', column('x'))

    s = select(t).where(t.c.x == 5)

    # **do not use** with untrusted input!!!
    print(s.compile(compile_kwargs={"literal_binds": True}))

The above approach has the caveats that it is only supported for basic
types, such as ints and strings, and furthermore if a :func:`.bindparam`
without a pre-set value is used directly, it won't be able to
stringify that either.

This functionality is provided mainly for
logging or debugging purposes, where having the raw sql string of a query
may prove useful.  Note that the ``dialect`` parameter should also
passed to the :meth:`_expression.ClauseElement.compile` method to render
the query that will be sent to the database.

To support inline literal rendering for types not supported, implement
a :class:`.TypeDecorator` for the target type which includes a
:meth:`.TypeDecorator.process_literal_param` method::

    from sqlalchemy import TypeDecorator, Integer


    class MyFancyType(TypeDecorator):
        impl = Integer

        def process_literal_param(self, value, dialect):
            return "my_fancy_formatting(%s)" % value

    from sqlalchemy import Table, Column, MetaData

    tab = Table('mytable', MetaData(), Column('x', MyFancyType()))

    stmt = tab.select().where(tab.c.x > 5)
    print(stmt.compile(compile_kwargs={"literal_binds": True}))

producing output like::

    SELECT mytable.x
    FROM mytable
    WHERE mytable.x > my_fancy_formatting(5)


.. _faq_sql_expression_percent_signs:

Why are percent signs being doubled up when stringifying SQL statements?
------------------------------------------------------------------------

Many :term:`DBAPI` implementations make use of the ``pyformat`` or ``format``
`paramstyle <https://www.python.org/dev/peps/pep-0249/#paramstyle>`_, which
necessarily involve percent signs in their syntax.  Most DBAPIs that do this
expect percent signs used for other reasons to be doubled up (i.e. escaped) in
the string form of the statements used, e.g.::

    SELECT a, b FROM some_table WHERE a = %s AND c = %s AND num %% modulus = 0

When SQL statements are passed to the underlying DBAPI by SQLAlchemy,
substitution of bound parameters works in the same way as the Python string
interpolation operator ``%``, and in many cases the DBAPI actually uses this
operator directly.  Above, the substitution of bound parameters would then look
like::

    SELECT a, b FROM some_table WHERE a = 5 AND c = 10 AND num % modulus = 0

The default compilers for databases like PostgreSQL (default DBAPI is psycopg2)
and MySQL (default DBAPI is mysqlclient) will have this percent sign
escaping behavior::

    >>> from sqlalchemy import table, column
    >>> from sqlalchemy.dialects import postgresql
    >>> t = table("my_table", column("value % one"), column("value % two"))
    >>> print(t.select().compile(dialect=postgresql.dialect()))
    SELECT my_table."value %% one", my_table."value %% two"
    FROM my_table

When such a dialect is being used, if non-DBAPI statements are desired that
don't include bound parameter symbols, one quick way to remove the percent
signs is to simply substitute in an empty set of parameters using Python's
``%`` operator directly::

    >>> strstmt = str(t.select().compile(dialect=postgresql.dialect()))
    >>> print(strstmt % ())
    SELECT my_table."value % one", my_table."value % two"
    FROM my_table

The other is to set a different parameter style on the dialect being used; all
:class:`.Dialect` implementations accept a parameter
``paramstyle`` which will cause the compiler for that
dialect to use the given parameter style.  Below, the very common ``named``
parameter style is set within the dialect used for the compilation so that
percent signs are no longer significant in the compiled form of SQL, and will
no longer be escaped::

    >>> print(t.select().compile(dialect=postgresql.dialect(paramstyle="named")))
    SELECT my_table."value % one", my_table."value % two"
    FROM my_table


.. _faq_sql_expression_op_parenthesis:

I'm using op() to generate a custom operator and my parenthesis are not coming out correctly
---------------------------------------------------------------------------------------------

The :meth:`.Operators.op` method allows one to create a custom database operator
otherwise not known by SQLAlchemy::

    >>> print(column('q').op('->')(column('p')))
    q -> p

However, when using it on the right side of a compound expression, it doesn't
generate parenthesis as we expect::

    >>> print((column('q1') + column('q2')).op('->')(column('p')))
    q1 + q2 -> p

Where above, we probably want ``(q1 + q2) -> p``.

The solution to this case is to set the precedence of the operator, using
the :paramref:`.Operators.op.precedence` parameter, to a high
number, where 100 is the maximum value, and the highest number used by any
SQLAlchemy operator is currently 15::

    >>> print((column('q1') + column('q2')).op('->', precedence=100)(column('p')))
    (q1 + q2) -> p

We can also usually force parenthesization around a binary expression (e.g.
an expression that has left/right operands and an operator) using the
:meth:`_expression.ColumnElement.self_group` method::

    >>> print((column('q1') + column('q2')).self_group().op('->')(column('p')))
    (q1 + q2) -> p

Why are the parentheses rules like this?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A lot of databases barf when there are excessive parenthesis or when
parenthesis are in unusual places they doesn't expect, so SQLAlchemy does not
generate parenthesis based on groupings, it uses operator precedence and if the
operator is known to be associative, so that parenthesis are generated
minimally. Otherwise, an expression like::

    column('a') & column('b') & column('c') & column('d')

would produce::

    (((a AND b) AND c) AND d)

which is fine but would probably annoy people (and be reported as a bug). In
other cases, it leads to things that are more likely to confuse databases or at
the very least readability, such as::

  column('q', ARRAY(Integer, dimensions=2))[5][6]

would produce::

    ((q[5])[6])

There are also some edge cases where we get things like ``"(x) = 7"`` and databases
really don't like that either.  So parenthesization doesn't naively
parenthesize, it uses operator precedence and associativity to determine
groupings.

For :meth:`.Operators.op`, the value of precedence defaults to zero.

What if we defaulted the value of :paramref:`.Operators.op.precedence` to 100,
e.g. the highest?  Then this expression makes more parenthesis, but is
otherwise OK, that is, these two are equivalent::

    >>> print((column('q') - column('y')).op('+', precedence=100)(column('z')))
    (q - y) + z
    >>> print((column('q') - column('y')).op('+')(column('z')))
    q - y + z

but these two are not::

    >>> print(column('q') - column('y').op('+', precedence=100)(column('z')))
    q - y + z
    >>> print(column('q') - column('y').op('+')(column('z')))
    q - (y + z)

For now, it's not clear that as long as we are doing parenthesization based on
operator precedence and associativity, if there is really a way to parenthesize
automatically for a generic operator with no precedence given that is going to
work in all cases, because sometimes you want a custom op to have a lower
precedence than the other operators and sometimes you want it to be higher.

It is possible that maybe if the "binary" expression above forced the use of
the ``self_group()`` method when ``op()`` is called, making the assumption that
a compound expression on the left side can always be parenthesized harmlessly.
Perhaps this change can be made at some point, however for the time being
keeping the parenthesization rules more internally consistent seems to be
the safer approach.

