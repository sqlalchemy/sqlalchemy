Operator Reference
===============================

..  Setup code, not for display

    >>> from sqlalchemy import column, select
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
    >>> from sqlalchemy import MetaData, Table, Column, Integer, String
    >>> metadata = MetaData()
    >>> user_table = Table(
    ...     "user_account",
    ...     metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String(30)),
    ...     Column('fullname', String)
    ... )
    >>> from sqlalchemy import ForeignKey
    >>> address_table = Table(
    ...     "address",
    ...     metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('user_id', None, ForeignKey('user_account.id')),
    ...     Column('email_address', String, nullable=False)
    ... )
    >>> metadata.create_all(engine)
    BEGIN (implicit)
    ...
    >>> from sqlalchemy.orm import declarative_base
    >>> Base = declarative_base()
    >>> from sqlalchemy.orm import relationship
    >>> class User(Base):
    ...     __tablename__ = 'user_account'
    ...
    ...     id = Column(Integer, primary_key=True)
    ...     name = Column(String(30))
    ...     fullname = Column(String)
    ...
    ...     addresses = relationship("Address", back_populates="user")
    ...
    ...     def __repr__(self):
    ...        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

    >>> class Address(Base):
    ...     __tablename__ = 'address'
    ...
    ...     id = Column(Integer, primary_key=True)
    ...     email_address = Column(String, nullable=False)
    ...     user_id = Column(Integer, ForeignKey('user_account.id'))
    ...
    ...     user = relationship("User", back_populates="addresses")
    ...
    ...     def __repr__(self):
    ...         return f"Address(id={self.id!r}, email_address={self.email_address!r})"
    >>> conn = engine.connect()
    >>> from sqlalchemy.orm import Session
    >>> session = Session(conn)
    >>> session.add_all([
    ... User(name="spongebob", fullname="Spongebob Squarepants", addresses=[
    ...    Address(email_address="spongebob@sqlalchemy.org")
    ... ]),
    ... User(name="sandy", fullname="Sandy Cheeks", addresses=[
    ...    Address(email_address="sandy@sqlalchemy.org"),
    ...     Address(email_address="squirrel@squirrelpower.org")
    ...     ]),
    ...     User(name="patrick", fullname="Patrick Star", addresses=[
    ...         Address(email_address="pat999@aol.com")
    ...     ]),
    ...     User(name="squidward", fullname="Squidward Tentacles", addresses=[
    ...         Address(email_address="stentcl@sqlalchemy.org")
    ...     ]),
    ...     User(name="ehkrabs", fullname="Eugene H. Krabs"),
    ... ])
    >>> session.commit()
    BEGIN ...
    >>> conn.begin()
    BEGIN ...


This section details usage of the operators that are available
to construct SQL expressions.

These methods are presented in terms of the :class:`_sql.Operators`
and :class:`_sql.ColumnOperators` base classes.   The methods are then
available on descendants of these classes, including:

* :class:`_schema.Column` objects

* :class:`_sql.ColumnElement` objects more generally, which are the root
  of all Core SQL Expression language column-level expressions

* :class:`_orm.InstrumentedAttribute` objects, which are ORM
  level mapped attributes.

The operators are first introduced in the tutorial sections, including:

* :doc:`/tutorial/index` - unified tutorial in :term:`2.0 style`

* :doc:`/orm/tutorial` - ORM tutorial in :term:`1.x style`

* :doc:`/core/tutorial` - Core tutorial in :term:`1.x style`

Comparison Operators
^^^^^^^^^^^^^^^^^^^^

Basic comparisons which apply to many datatypes, including numerics,
strings, dates, and many others:

* :meth:`_sql.ColumnOperators.__eq__` (Python "``==``" operator)::

    >>> print(column('x') == 5)
    x = :x_1

  ..

* :meth:`_sql.ColumnOperators.__ne__` (Python "``!=``" operator)::

    >>> print(column('x') != 5)
    x != :x_1

  ..

* :meth:`_sql.ColumnOperators.__gt__` (Python "``>``" operator)::

    >>> print(column('x') > 5)
    x > :x_1

  ..

* :meth:`_sql.ColumnOperators.__lt__` (Python "``<``" operator)::

    >>> print(column('x') < 5)
    x < :x_1

  ..

* :meth:`_sql.ColumnOperators.__ge__` (Python "``>=``" operator)::

    >>> print(column('x') >= 5)
    x >= :x_1

  ..

* :meth:`_sql.ColumnOperators.__le__` (Python "``<=``" operator)::

    >>> print(column('x') <= 5)
    x <= :x_1

  ..

* :meth:`_sql.ColumnOperators.between`::

    >>> print(column('x').between(5, 10))
    x BETWEEN :x_1 AND :x_2

  ..

IN Comparisons
^^^^^^^^^^^^^^
The SQL IN operator is a subject all its own in SQLAlchemy.   As the IN
operator is usually used against a list of fixed values, SQLAlchemy's
feature of bound parameter coercion makes use of a special form of SQL
compilation that renders an interim SQL string for compilation that's formed
into the final list of bound parameters in a second step.   In other words,
"it just works".

IN against a list of values
~~~~~~~~~~~~~~~~~~~~~~~~~~~

IN is available most typically by passing a list of
values to the :meth:`_sql.ColumnOperators.in_` method::


    >>> print(column('x').in_([1, 2, 3]))
    x IN ([POSTCOMPILE_x_1])

The special bound form ``POSTCOMPILE`` is rendered into individual parameters
at execution time, illustrated below:

.. sourcecode:: pycon+sql

    >>> stmt = select(User.id).where(User.id.in_([1, 2, 3]))
    >>> result = conn.execute(stmt)
    {opensql}SELECT user_account.id
    FROM user_account
    WHERE user_account.id IN (?, ?, ?)
    [...] (1, 2, 3){stop}

Empty IN Expressions
~~~~~~~~~~~~~~~~~~~~

SQLAlchemy produces a mathematically valid result for an empty IN expression
by rendering a backend-specific subquery that returns no rows.   Again
in other words, "it just works":

.. sourcecode:: pycon+sql

    >>> stmt = select(User.id).where(User.id.in_([]))
    >>> result = conn.execute(stmt)
    {opensql}SELECT user_account.id
    FROM user_account
    WHERE user_account.id IN (SELECT 1 FROM (SELECT 1) WHERE 1!=1)
    [...] ()

The "empty set" subquery above generalizes correctly and is also rendered
in terms of the IN operator which remains in place.


NOT IN
~~~~~~~

"NOT IN" is available via the :meth:`_sql.ColumnOperators.not_in` operator::

    >>> print(column('x').not_in([1, 2, 3]))
    x NOT IN ([POSTCOMPILE_x_1])

This is typically more easily available by negating with the ``~`` operator::

    >>> print(~column('x').in_([1, 2, 3]))
    x NOT IN ([POSTCOMPILE_x_1])

Tuple IN Expressions
~~~~~~~~~~~~~~~~~~~~

Comparison of tuples to tuples is common with IN, as among other use cases
accommodates for the case when matching rows to a set of potential composite
primary key values.  The :func:`_sql.tuple_` construct provides the basic
building block for tuple comparisons.  The :meth:`_sql.Tuple.in_` operator
then receives a list of tuples::

    >>> from sqlalchemy import tuple_
    >>> tup = tuple_(column('x', Integer), column('y', Integer))
    >>> expr = tup.in_([(1, 2), (3, 4)])
    >>> print(expr)
    (x, y) IN ([POSTCOMPILE_param_1])

To illustrate the parameters rendered:

.. sourcecode:: pycon+sql


    >>> tup = tuple_(User.id, Address.id)
    >>> stmt = select(User.name).join(Address).where(tup.in_([(1, 1), (2, 2)]))
    >>> conn.execute(stmt).all()
    {opensql}SELECT user_account.name
    FROM user_account JOIN address ON user_account.id = address.user_id
    WHERE (user_account.id, address.id) IN (VALUES (?, ?), (?, ?))
    [...] (1, 1, 2, 2){stop}
    [('spongebob',), ('sandy',)]

Subquery IN
~~~~~~~~~~~

Finally, the :meth:`_sql.ColumnOperators.in_` and :meth:`_sql.ColumnOperators.not_in`
operators work with subqueries.   The form provides that a :class:`_sql.Select`
construct is passed in directly, without any explicit conversion to a named
subquery::

    >>> print(column('x').in_(select(user_table.c.id)))
    x IN (SELECT user_account.id
    FROM user_account)

Tuples work as expected::

    >>> print(
    ...     tuple_(column('x'), column('y')).in_(
    ...         select(user_table.c.id, address_table.c.id).join(address_table)
    ...     )
    ... )
    (x, y) IN (SELECT user_account.id, address.id
    FROM user_account JOIN address ON user_account.id = address.user_id)

Identity Comparisons
^^^^^^^^^^^^^^^^^^^^

These operators involve testing for special SQL values such as
``NULL``, boolean constants such as ``true`` or ``false`` which some
databases support:

* :meth:`_sql.ColumnOperators.is_`:

  This operator will provide exactly the SQL for "x IS y", most often seen
  as "<expr> IS NULL".   The ``NULL`` constant is most easily acquired
  using regular Python ``None``::

    >>> print(column('x').is_(None))
    x IS NULL

  SQL NULL is also explicitly available, if needed, using the
  :func:`_sql.null` construct::

    >>> from sqlalchemy import null
    >>> print(column('x').is_(null()))
    x IS NULL

  The :meth:`_sql.ColumnOperators.is_` operator is automatically invoked when
  using the :meth:`_sql.ColumnOperators.__eq__` overloaded operator, i.e.
  ``==``, in conjunction with the ``None`` or :func:`_sql.null` value. In this
  way, there's typically not a need to use :meth:`_sql.ColumnOperators.is_`
  explicitly, paricularly when used with a dynamic value::

    >>> a = None
    >>> print(column('x') == a)
    x IS NULL

  Note that the Python ``is`` operator is **not overloaded**.  Even though
  Python provides hooks to overload operators such as ``==`` and ``!=``,
  it does **not** provide any way to redefine ``is``.

* :meth:`_sql.ColumnOperators.is_not`:

  Similar to :meth:`_sql.ColumnOperators.is_`, produces "IS NOT"::

    >>> print(column('x').is_not(None))
    x IS NOT NULL

  Is similarly equivalent to ``!= None``::

    >>> print(column('x') != None)
    x IS NOT NULL

* :meth:`_sql.ColumnOperators.is_distinct_from`:

  Produces SQL IS DISTINCT FROM::

    >>> print(column('x').is_distinct_from('some value'))
    x IS DISTINCT FROM :x_1

* :meth:`_sql.ColumnOperators.isnot_distinct_from`:

  Produces SQL IS NOT DISTINCT FROM::

    >>> print(column('x').isnot_distinct_from('some value'))
    x IS NOT DISTINCT FROM :x_1

String Comparisons
^^^^^^^^^^^^^^^^^^

* :meth:`_sql.ColumnOperators.like`::

    >>> print(column('x').like('word'))
    x LIKE :x_1

  ..

* :meth:`_sql.ColumnOperators.ilike`:

  Case insensitive LIKE makes use of the SQL ``lower()`` function on a
  generic backend.  On the PostgreSQL backend it will use ``ILIKE``::

    >>> print(column('x').ilike('word'))
    lower(x) LIKE lower(:x_1)

  ..

* :meth:`_sql.ColumnOperators.notlike`::

    >>> print(column('x').notlike('word'))
    x NOT LIKE :x_1

  ..


* :meth:`_sql.ColumnOperators.notilike`::

    >>> print(column('x').notilike('word'))
    lower(x) NOT LIKE lower(:x_1)

  ..

String Containment
^^^^^^^^^^^^^^^^^^^

String containment operators are basically built as a combination of
LIKE and the string concatenation operator, which is ``||`` on most
backends or sometimes a function like ``concat()``:

* :meth:`_sql.ColumnOperators.startswith`::

    The string containment operators
    >>> print(column('x').startswith('word'))
    x LIKE :x_1 || '%'

  ..

* :meth:`_sql.ColumnOperators.endswith`::

    >>> print(column('x').endswith('word'))
    x LIKE '%' || :x_1

  ..

* :meth:`_sql.ColumnOperators.contains`::

    >>> print(column('x').contains('word'))
    x LIKE '%' || :x_1 || '%'

  ..

String matching
^^^^^^^^^^^^^^^^

Matching operators are always backend-specific and may provide different
behaviors and results on different databases:

* :meth:`_sql.ColumnOperators.match`:

  This is a dialect-specific operator that makes use of the MATCH
  feature of the underlying database, if available::

    >>> print(column('x').match('word'))
    x MATCH :x_1

  ..

* :meth:`_sql.ColumnOperators.regexp_match`:

  This operator is dialect specific.  We can illustrate it in terms of
  for example the PostgreSQL dialect::

    >>> from sqlalchemy.dialects import postgresql
    >>> print(column('x').regexp_match('word').compile(dialect=postgresql.dialect()))
    x ~ %(x_1)s

  Or MySQL::

    >>> from sqlalchemy.dialects import mysql
    >>> print(column('x').regexp_match('word').compile(dialect=mysql.dialect()))
    x REGEXP %s

  ..


.. _queryguide_operators_concat_op:

String Alteration
^^^^^^^^^^^^^^^^^

* :meth:`_sql.ColumnOperators.concat`:

  String concatenation::

    >>> print(column('x').concat("some string"))
    x || :x_1

  This operator is available via :meth:`_sql.ColumnOperators.__add__`, that
  is, the Python ``+`` operator, when working with a column expression that
  derives from :class:`_types.String`::

    >>> print(column('x', String) + "some string")
    x || :x_1

  The operator will produce the appropriate database-specific construct,
  such as on MySQL it's historically been the ``concat()`` SQL function::

    >>> print((column('x', String) + "some string").compile(dialect=mysql.dialect()))
    concat(x, %s)

  ..

* :meth:`_sql.ColumnOperators.regexp_replace`:

  Complementary to :meth:`_sql.ColumnOperators.regexp` this produces REGEXP
  REPLACE equivalent for the backends which support it::

    >>> print(column('x').regexp_replace('foo', 'bar').compile(dialect=postgresql.dialect()))
    REGEXP_REPLACE(x, %(x_1)s, %(x_2)s)

  ..

* :meth:`_sql.ColumnOperators.collate`:

  Produces the COLLATE SQL operator which provides for specific collations
  at expression time::

    >>> print((column('x').collate('latin1_german2_ci') == 'Müller').compile(dialect=mysql.dialect()))
    (x COLLATE latin1_german2_ci) = %s


  To use COLLATE against a literal value, use the :func:`_sql.literal` construct::


    >>> from sqlalchemy import literal
    >>> print((literal('Müller').collate('latin1_german2_ci') == column('x')).compile(dialect=mysql.dialect()))
    (%s COLLATE latin1_german2_ci) = x

  ..

Arithmetic Operators
^^^^^^^^^^^^^^^^^^^^

* :meth:`_sql.ColumnOperators.__add__`, :meth:`_sql.ColumnOperators.__radd__` (Python "``+``" operator)::

    >>> print(column('x') + 5)
    x + :x_1

    >>> print(5 + column('x'))
    :x_1 + x

  ..


  Note that when the datatype of the expression is :class:`_types.String`
  or similar, the :meth:`_sql.ColumnOperators.__add__` operator instead produces
  :ref:`string concatenation <queryguide_operators_concat_op>`.


* :meth:`_sql.ColumnOperators.__sub__`, :meth:`_sql.ColumnOperators.__rsub__` (Python "``-``" operator)::

    >>> print(column('x') - 5)
    x - :x_1

    >>> print(5 - column('x'))
    :x_1 - x

  ..


* :meth:`_sql.ColumnOperators.__mul__`, :meth:`_sql.ColumnOperators.__rmul__` (Python "``*``" operator)::

    >>> print(column('x') * 5)
    x * :x_1

    >>> print(5 * column('x'))
    :x_1 * x

  ..

* :meth:`_sql.ColumnOperators.__div__`, :meth:`_sql.ColumnOperators.__rdiv__` (Python "``/``" operator)::

    >>> print(column('x') / 5)
    x / :x_1
    >>> print(5 / column('x'))
    :x_1 / x

  ..


* :meth:`_sql.ColumnOperators.__mod__`, :meth:`_sql.ColumnOperators.__rmod__` (Python "``%``" operator)::

    >>> print(column('x') % 5)
    x % :x_1
    >>> print(5 % column('x'))
    :x_1 % x

  ..


Using Conjunctions and Negations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The most common conjunction, "AND", is automatically applied if we make repeated use of the :meth:`_sql.Select.where` method, as well as similar methods such as
:meth:`_sql.Update.where` and :meth:`_sql.Delete.where`::

    >>> print(
    ...        select(address_table.c.email_address).
    ...        where(user_table.c.name == 'squidward').
    ...        where(address_table.c.user_id == user_table.c.id)
    ...    )
    SELECT address.email_address
    FROM address, user_account
    WHERE user_account.name = :name_1 AND address.user_id = user_account.id

:meth:`_sql.Select.where`, :meth:`_sql.Update.where` and :meth:`_sql.Delete.where` also accept multiple expressions with the same effect::

    >>> print(
    ...        select(address_table.c.email_address).
    ...        where(
    ...            user_table.c.name == 'squidward',
    ...            address_table.c.user_id == user_table.c.id
    ...        )
    ...    )
    SELECT address.email_address
    FROM address, user_account
    WHERE user_account.name = :name_1 AND address.user_id = user_account.id

The "AND" conjunction, as well as its partner "OR", are both available directly using the :func:`_sql.and_` and :func:`_sql.or_` functions::


    >>> from sqlalchemy import and_, or_
    >>> print(
    ...     select(address_table.c.email_address).
    ...     where(
    ...         and_(
    ...             or_(user_table.c.name == 'squidward', user_table.c.name == 'sandy'),
    ...             address_table.c.user_id == user_table.c.id
    ...         )
    ...     )
    ... )
    SELECT address.email_address
    FROM address, user_account
    WHERE (user_account.name = :name_1 OR user_account.name = :name_2)
    AND address.user_id = user_account.id

A negation is available using the :func:`_sql.not_` function.  This will
typically invert the operator in a boolean expression::

    >>> from sqlalchemy import not_
    >>> print(not_(column('x') == 5))
    x != :x_1

It also may apply a keyword such as ``NOT`` when appropriate::

    >>> from sqlalchemy import Boolean
    >>> print(not_(column('x', Boolean)))
    NOT x


Conjunction Operators
^^^^^^^^^^^^^^^^^^^^^^

The above conjunction functions :func:`_sql.and_`, :func:`_sql.or_`,
:func:`_sql.not_` are also available as overloaded Python operators:

.. note:: The Python ``&``, ``|`` and ``~`` operators take high precedence
   in the language; as a result, parenthesis must usually be applied
   for operands that themselves contain expressions, as indicated in the
   examples below.

* :meth:`_sql.Operators.__and__` (Python "``&``" operator):

  The Python binary ``&`` operator is overloaded to behave the same
  as :func:`_sql.and_` (note parenthesis around the two operands)::

     >>> print((column('x') == 5) & (column('y') == 10))
     x = :x_1 AND y = :y_1

  ..


* :meth:`_sql.Operators.__or__` (Python "``|``" operator):

  The Python binary ``|`` operator is overloaded to behave the same
  as :func:`_sql.or_` (note parenthesis around the two operands)::

    >>> print((column('x') == 5) | (column('y') == 10))
    x = :x_1 OR y = :y_1

  ..


* :meth:`_sql.Operators.__invert__` (Python "``~``" operator):

  The Python binary ``~`` operator is overloaded to behave the same
  as :func:`_sql.not_`, either inverting the existing operator, or
  applying the ``NOT`` keyword to the expression as a whole::

    >>> print(~(column('x') == 5))
    x != :x_1

    >>> from sqlalchemy import Boolean
    >>> print(~column('x', Boolean))
    NOT x

  ..



Operator Customization
^^^^^^^^^^^^^^^^^^^^^^

TODO


..  Setup code, not for display

    >>> conn.close()
    ROLLBACK