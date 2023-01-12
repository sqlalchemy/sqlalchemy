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
if we don't use it explicitly):

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import table, column, select
    >>> t = table("my_table", column("x"))
    >>> statement = select(t)
    >>> print(str(statement))
    {printsql}SELECT my_table.x
    FROM my_table

The ``str()`` builtin, or an equivalent, can be invoked on ORM
:class:`_query.Query`  object as well as any statement such as that of
:func:`_expression.select`, :func:`_expression.insert` etc. and also any expression fragment, such
as:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import column
    >>> print(column("x") == "some value")
    {printsql}x = :x_1

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

Note that any dialect can be assembled using :func:`_sa.create_engine` itself
with a dummy URL and then accessing the :attr:`_engine.Engine.dialect` attribute,
such as if we wanted a dialect object for psycopg2::

    e = create_engine("postgresql+psycopg2://")
    psycopg2_dialect = e.dialect

When given an ORM :class:`~.orm.query.Query` object, in order to get at the
:meth:`_expression.ClauseElement.compile`
method we only need access the :attr:`~.orm.query.Query.statement`
accessor first::

    statement = query.statement
    print(statement.compile(someengine))

Rendering Bound Parameters Inline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning:: **Never** use these techniques with string content received from
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

    t = table("t", column("x"))

    s = select(t).where(t.c.x == 5)

    # **do not use** with untrusted input!!!
    print(s.compile(compile_kwargs={"literal_binds": True}))

    # to render for a specific dialect
    print(s.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))

    # or if you have an Engine, pass as first argument
    print(s.compile(some_engine, compile_kwargs={"literal_binds": True}))

This functionality is provided mainly for logging or debugging purposes, where
having the raw sql string of a query may prove useful.

The above approach has the caveats that it is only supported for basic types,
such as ints and strings, and furthermore if a :func:`.bindparam` without a
pre-set value is used directly, it won't be able to stringify that either.
Methods of stringifying all parameters unconditionally are detailed below.

.. tip::

   The reason SQLAlchemy does not support full stringification of all
   datatypes is threefold:

   1. This is a functionality that is already supported by the DBAPI in use
      when the DBAPI is used normally.   The SQLAlchemy project cannot be
      tasked with duplicating this functionality for every datatype for
      all backends, as this is redundant work which also incurs significant
      testing and ongoing support overhead.

   2. Stringifying with bound parameters inlined for specific databases
      suggests a usage that is actually passing these fully stringified
      statements onto the database for execution. This is unnecessary and
      insecure, and SQLAlchemy does not want to encourage this use in any
      way.

   3. The area of rendering literal values is the most likely area for
      security issues to be reported.  SQLAlchemy tries to keep the area of
      safe parameter stringification an issue for the DBAPI drivers as much
      as possible where the specifics for each DBAPI can be handled
      appropriately and securely.

As SQLAlchemy intentionally does not support full stringification of literal
values, techniques to do so within specific debugging scenarios include the
following. As an example, we will use the PostgreSQL :class:`_postgresql.UUID`
datatype::

    import uuid

    from sqlalchemy import Column
    from sqlalchemy import create_engine
    from sqlalchemy import Integer
    from sqlalchemy import select
    from sqlalchemy.dialects.postgresql import UUID
    from sqlalchemy.orm import declarative_base


    Base = declarative_base()


    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)
        data = Column(UUID)


    stmt = select(A).where(A.data == uuid.uuid4())

Given the above model and statement which will compare a column to a single
UUID value, options for stringifying this statement with inline values
include:

* Some DBAPIs such as psycopg2 support helper functions like
  `mogrify() <https://www.psycopg.org/docs/cursor.html#cursor.mogrify>`_ which
  provide access to their literal-rendering functionality.   To use such
  features, render the SQL string without using ``literal_binds`` and pass
  the parameters separately via the :attr:`.SQLCompiler.params` accessor::

      e = create_engine("postgresql+psycopg2://scott:tiger@localhost/test")

      with e.connect() as conn:
          cursor = conn.connection.cursor()
          compiled = stmt.compile(e)

          print(cursor.mogrify(str(compiled), compiled.params))

  The above code will produce psycopg2's raw bytestring:

  .. sourcecode:: sql

      b"SELECT a.id, a.data \nFROM a \nWHERE a.data = 'a511b0fc-76da-4c47-a4b4-716a8189b7ac'::uuid"

* Render the :attr:`.SQLCompiler.params` directly into the statement, using
  the appropriate `paramstyle <https://www.python.org/dev/peps/pep-0249/#paramstyle>`_
  of the target DBAPI.  For example, the psycopg2 DBAPI uses the named ``pyformat``
  style.  The meaning of ``render_postcompile`` will be discussed in the next
  section.   **WARNING this is NOT secure, do NOT use untrusted input**::

    e = create_engine("postgresql+psycopg2://")

    # will use pyformat style, i.e. %(paramname)s for param
    compiled = stmt.compile(e, compile_kwargs={"render_postcompile": True})

    print(str(compiled) % compiled.params)

  This will produce a non-working string, that nonetheless is suitable for
  debugging:

  .. sourcecode:: sql

    SELECT a.id, a.data
    FROM a
    WHERE a.data = 9eec1209-50b4-4253-b74b-f82461ed80c1

  Another example using a positional paramstyle such as ``qmark``, we can render
  our above statement in terms of SQLite by also using the
  :attr:`.SQLCompiler.positiontup` collection in conjunction with
  :attr:`.SQLCompiler.params`, in order to retrieve the parameters in
  their positional order for the statement as compiled::

    import re

    e = create_engine("sqlite+pysqlite://")

    # will use qmark style, i.e. ? for param
    compiled = stmt.compile(e, compile_kwargs={"render_postcompile": True})

    # params in positional order
    params = (repr(compiled.params[name]) for name in compiled.positiontup)

    print(re.sub(r"\?", lambda m: next(params), str(compiled)))

  The above snippet prints:

  .. sourcecode:: sql

    SELECT a.id, a.data
    FROM a
    WHERE a.data = UUID('1bd70375-db17-4d8c-94f1-fc2ef3aada26')

* Use the :ref:`sqlalchemy.ext.compiler_toplevel` extension to render
  :class:`_sql.BindParameter` objects in a custom way when a user-defined
  flag is present.  This flag is sent through the ``compile_kwargs``
  dictionary like any other flag::

    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.sql.expression import BindParameter


    @compiles(BindParameter)
    def _render_literal_bindparam(element, compiler, use_my_literal_recipe=False, **kw):
        if not use_my_literal_recipe:
            # use normal bindparam processing
            return compiler.visit_bindparam(element, **kw)

        # if use_my_literal_recipe was passed to compiler_kwargs,
        # render the value directly
        return repr(element.value)


    e = create_engine("postgresql+psycopg2://")
    print(stmt.compile(e, compile_kwargs={"use_my_literal_recipe": True}))

  The above recipe will print:

  .. sourcecode:: sql

    SELECT a.id, a.data
    FROM a
    WHERE a.data = UUID('47b154cd-36b2-42ae-9718-888629ab9857')

* For type-specific stringification that's built into a model or a statement, the
  :class:`_types.TypeDecorator` class may be used to provide custom stringification
  of any datatype using the :meth:`.TypeDecorator.process_literal_param` method::

    from sqlalchemy import TypeDecorator


    class UUIDStringify(TypeDecorator):
        impl = UUID

        def process_literal_param(self, value, dialect):
            return repr(value)

  The above datatype needs to be used either explicitly within the model
  or locally within the statement using :func:`_sql.type_coerce`, such as ::

    from sqlalchemy import type_coerce

    stmt = select(A).where(type_coerce(A.data, UUIDStringify) == uuid.uuid4())

    print(stmt.compile(e, compile_kwargs={"literal_binds": True}))

  Again printing the same form:

  .. sourcecode:: sql

    SELECT a.id, a.data
    FROM a
    WHERE a.data = UUID('47b154cd-36b2-42ae-9718-888629ab9857')

Rendering "POSTCOMPILE" Parameters as Bound Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

SQLAlchemy includes a variant on a bound parameter known as
:paramref:`_sql.BindParameter.expanding`, which is a "late evaluated" parameter
that is rendered in an intermediary state when a SQL construct is compiled,
which is then further processed at statement execution time when the actual
known values are passed.   "Expanding" parameters are used for
:meth:`_sql.ColumnOperators.in_` expressions by default so that the SQL
string can be safely cached independently of the actual lists of values
being passed to a particular invocation of :meth:`_sql.ColumnOperators.in_`::

  >>> stmt = select(A).where(A.id.in_[1, 2, 3])

To render the IN clause with real bound parameter symbols, use the
``render_postcompile=True`` flag with :meth:`_sql.ClauseElement.compile`:

.. sourcecode:: pycon+sql

  >>> e = create_engine("postgresql+psycopg2://")
  >>> print(stmt.compile(e, compile_kwargs={"render_postcompile": True}))
  {printsql}SELECT a.id, a.data
  FROM a
  WHERE a.id IN (%(id_1_1)s, %(id_1_2)s, %(id_1_3)s)

The ``literal_binds`` flag, described in the previous section regarding
rendering of bound parameters, automatically sets ``render_postcompile`` to
True, so for a statement with simple ints/strings, these can be stringified
directly:

.. sourcecode:: pycon+sql

  # render_postcompile is implied by literal_binds
  >>> print(stmt.compile(e, compile_kwargs={"literal_binds": True}))
  {printsql}SELECT a.id, a.data
  FROM a
  WHERE a.id IN (1, 2, 3)

The :attr:`.SQLCompiler.params` and :attr:`.SQLCompiler.positiontup` are
also compatible with ``render_postcompile``, so that
the previous recipes for rendering inline bound parameters will work here
in the same way, such as SQLite's positional form:

.. sourcecode:: pycon+sql

  >>> u1, u2, u3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
  >>> stmt = select(A).where(A.data.in_([u1, u2, u3]))

  >>> import re
  >>> e = create_engine("sqlite+pysqlite://")
  >>> compiled = stmt.compile(e, compile_kwargs={"render_postcompile": True})
  >>> params = (repr(compiled.params[name]) for name in compiled.positiontup)
  >>> print(re.sub(r"\?", lambda m: next(params), str(compiled)))
  {printsql}SELECT a.id, a.data
  FROM a
  WHERE a.data IN (UUID('aa1944d6-9a5a-45d5-b8da-0ba1ef0a4f38'), UUID('a81920e6-15e2-4392-8a3c-d775ffa9ccd2'), UUID('b5574cdb-ff9b-49a3-be52-dbc89f087bfa'))

.. warning::

    Remember, **all** of the above code recipes which stringify literal
    values, bypassing the use of bound parameters when sending statements
    to the database, are **only to be used when**:

    1. the use is **debugging purposes only**

    2. the string **is not to be passed to a live production database**

    3. only with **local, trusted input**

    The above recipes for stringification of literal values are **not secure in
    any way and should never be used against production databases**.

.. _faq_sql_expression_percent_signs:

Why are percent signs being doubled up when stringifying SQL statements?
------------------------------------------------------------------------

Many :term:`DBAPI` implementations make use of the ``pyformat`` or ``format``
`paramstyle <https://www.python.org/dev/peps/pep-0249/#paramstyle>`_, which
necessarily involve percent signs in their syntax.  Most DBAPIs that do this
expect percent signs used for other reasons to be doubled up (i.e. escaped) in
the string form of the statements used, e.g.:

.. sourcecode:: sql

    SELECT a, b FROM some_table WHERE a = %s AND c = %s AND num %% modulus = 0

When SQL statements are passed to the underlying DBAPI by SQLAlchemy,
substitution of bound parameters works in the same way as the Python string
interpolation operator ``%``, and in many cases the DBAPI actually uses this
operator directly.  Above, the substitution of bound parameters would then look
like:

.. sourcecode:: sql

    SELECT a, b FROM some_table WHERE a = 5 AND c = 10 AND num % modulus = 0

The default compilers for databases like PostgreSQL (default DBAPI is psycopg2)
and MySQL (default DBAPI is mysqlclient) will have this percent sign
escaping behavior:

.. sourcecode:: pycon+sql

    >>> from sqlalchemy import table, column
    >>> from sqlalchemy.dialects import postgresql
    >>> t = table("my_table", column("value % one"), column("value % two"))
    >>> print(t.select().compile(dialect=postgresql.dialect()))
    {printsql}SELECT my_table."value %% one", my_table."value %% two"
    FROM my_table

When such a dialect is being used, if non-DBAPI statements are desired that
don't include bound parameter symbols, one quick way to remove the percent
signs is to simply substitute in an empty set of parameters using Python's
``%`` operator directly:

.. sourcecode:: pycon+sql

    >>> strstmt = str(t.select().compile(dialect=postgresql.dialect()))
    >>> print(strstmt % ())
    {printsql}SELECT my_table."value % one", my_table."value % two"
    FROM my_table

The other is to set a different parameter style on the dialect being used; all
:class:`.Dialect` implementations accept a parameter
``paramstyle`` which will cause the compiler for that
dialect to use the given parameter style.  Below, the very common ``named``
parameter style is set within the dialect used for the compilation so that
percent signs are no longer significant in the compiled form of SQL, and will
no longer be escaped:

.. sourcecode:: pycon+sql

    >>> print(t.select().compile(dialect=postgresql.dialect(paramstyle="named")))
    {printsql}SELECT my_table."value % one", my_table."value % two"
    FROM my_table


.. _faq_sql_expression_op_parenthesis:

I'm using op() to generate a custom operator and my parenthesis are not coming out correctly
---------------------------------------------------------------------------------------------

The :meth:`.Operators.op` method allows one to create a custom database operator
otherwise not known by SQLAlchemy:

.. sourcecode:: pycon+sql

    >>> print(column("q").op("->")(column("p")))
    {printsql}q -> p

However, when using it on the right side of a compound expression, it doesn't
generate parenthesis as we expect:

.. sourcecode:: pycon+sql

    >>> print((column("q1") + column("q2")).op("->")(column("p")))
    {printsql}q1 + q2 -> p

Where above, we probably want ``(q1 + q2) -> p``.

The solution to this case is to set the precedence of the operator, using
the :paramref:`.Operators.op.precedence` parameter, to a high
number, where 100 is the maximum value, and the highest number used by any
SQLAlchemy operator is currently 15:

.. sourcecode:: pycon+sql

    >>> print((column("q1") + column("q2")).op("->", precedence=100)(column("p")))
    {printsql}(q1 + q2) -> p

We can also usually force parenthesization around a binary expression (e.g.
an expression that has left/right operands and an operator) using the
:meth:`_expression.ColumnElement.self_group` method:

.. sourcecode:: pycon+sql

    >>> print((column("q1") + column("q2")).self_group().op("->")(column("p")))
    {printsql}(q1 + q2) -> p

Why are the parentheses rules like this?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A lot of databases barf when there are excessive parenthesis or when
parenthesis are in unusual places they doesn't expect, so SQLAlchemy does not
generate parenthesis based on groupings, it uses operator precedence and if the
operator is known to be associative, so that parenthesis are generated
minimally. Otherwise, an expression like::

    column("a") & column("b") & column("c") & column("d")

would produce:

.. sourcecode:: sql

    (((a AND b) AND c) AND d)

which is fine but would probably annoy people (and be reported as a bug). In
other cases, it leads to things that are more likely to confuse databases or at
the very least readability, such as::

    column("q", ARRAY(Integer, dimensions=2))[5][6]

would produce:

.. sourcecode:: sql

    ((q[5])[6])

There are also some edge cases where we get things like ``"(x) = 7"`` and databases
really don't like that either.  So parenthesization doesn't naively
parenthesize, it uses operator precedence and associativity to determine
groupings.

For :meth:`.Operators.op`, the value of precedence defaults to zero.

What if we defaulted the value of :paramref:`.Operators.op.precedence` to 100,
e.g. the highest?  Then this expression makes more parenthesis, but is
otherwise OK, that is, these two are equivalent:

.. sourcecode:: pycon+sql

    >>> print((column("q") - column("y")).op("+", precedence=100)(column("z")))
    {printsql}(q - y) + z{stop}
    >>> print((column("q") - column("y")).op("+")(column("z")))
    {printsql}q - y + z{stop}

but these two are not:

.. sourcecode:: pycon+sql

    >>> print(column("q") - column("y").op("+", precedence=100)(column("z")))
    {printsql}q - y + z{stop}
    >>> print(column("q") - column("y").op("+")(column("z")))
    {printsql}q - (y + z){stop}

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

