SQL Expressions
===============

.. contents::
    :local:
    :class: faq
    :backlinks: none

.. _faq_sql_expression_string:

How do I render SQL expressions as strings, possibly with bound parameters inlined?
------------------------------------------------------------------------------------

The "stringification" of a SQLAlchemy statement or Query in the vast majority
of cases is as simple as::

    print(str(statement))

this applies both to an ORM :class:`~.orm.query.Query` as well as any :func:`.select` or other
statement.   Additionally, to get the statement as compiled to a
specific dialect or engine, if the statement itself is not already
bound to one you can pass this in to :meth:`.ClauseElement.compile`::

    print(statement.compile(someengine))

or without an :class:`.Engine`::

    from sqlalchemy.dialects import postgresql
    print(statement.compile(dialect=postgresql.dialect()))

When given an ORM :class:`~.orm.query.Query` object, in order to get at the
:meth:`.ClauseElement.compile`
method we only need access the :attr:`~.orm.query.Query.statement`
accessor first::

    statement = query.statement
    print(statement.compile(someengine))

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

    s = select([t]).where(t.c.x == 5)

    print(s.compile(compile_kwargs={"literal_binds": True}))

the above approach has the caveats that it is only supported for basic
types, such as ints and strings, and furthermore if a :func:`.bindparam`
without a pre-set value is used directly, it won't be able to
stringify that either.

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

    print(
        tab.select().where(tab.c.x > 5).compile(
            compile_kwargs={"literal_binds": True})
    )

producing output like::

    SELECT mytable.x
    FROM mytable
    WHERE mytable.x > my_fancy_formatting(5)


Why does ``.col.in_([])`` Produce ``col != col``? Why not ``1=0``?
-------------------------------------------------------------------

.. note:: This section refers to SQLAlchemy 1.1 and earlier.  The behavior has
   been revised in version 1.2 of SQLAlchemy to actually produce ``1=0`` in
   the default case.

A little introduction to the issue. The IN operator in SQL, given a list of
elements to compare against a column, generally does not accept an empty list,
that is while it is valid to say::

    column IN (1, 2, 3)

it's not valid to say::

    column IN ()

SQLAlchemy's :meth:`.Operators.in_` operator, when given an empty list, produces this
expression::

    column != column

As of version 0.6, it also produces a warning stating that a less efficient
comparison operation will be rendered. This expression is the only one that is
both database agnostic and produces correct results.

For example, the naive approach of "just evaluate to false, by comparing 1=0
or 1!=1", does not handle nulls properly. An expression like::

    NOT column != column

will not return a row when "column" is null, but an expression which does not
take the column into account::

    NOT 1=0

will.

Closer to the mark is the following CASE expression::

    CASE WHEN column IS NOT NULL THEN 1=0 ELSE NULL END

We don't use this expression due to its verbosity, and its also not
typically accepted by Oracle within a WHERE clause - depending
on how you phrase it, you'll either get "ORA-00905: missing keyword" or
"ORA-00920: invalid relational operator". It's also still less efficient than
just rendering SQL without the clause altogether (or not issuing the SQL at
all, if the statement is just a simple search).

The best approach therefore is to avoid the usage of IN given an argument list
of zero length.  Instead, don't emit the Query in the first place, if no rows
should be returned.  The warning is best promoted to a full error condition
using the Python warnings filter (see http://docs.python.org/library/warnings.html).

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
:meth:`.ColumnElement.self_group` method::

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

    >>> print (column('q') - column('y')).op('+', precedence=100)(column('z'))
    (q - y) + z
    >>> print (column('q') - column('y')).op('+')(column('z'))
    q - y + z

but these two are not::

    >>> print column('q') - column('y').op('+', precedence=100)(column('z'))
    q - y + z
    >>> print column('q') - column('y').op('+')(column('z'))
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

