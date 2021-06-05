.. currentmodule:: sqlalchemy.orm

.. _mapper_sql_expressions:

SQL Expressions as Mapped Attributes
====================================

Attributes on a mapped class can be linked to SQL expressions, which can
be used in queries.

Using a Hybrid
--------------

The easiest and most flexible way to link relatively simple SQL expressions to a class is to use a so-called
"hybrid attribute",
described in the section :ref:`hybrids_toplevel`.  The hybrid provides
for an expression that works at both the Python level as well as at the
SQL expression level.  For example, below we map a class ``User``,
containing attributes ``firstname`` and ``lastname``, and include a hybrid that
will provide for us the ``fullname``, which is the string concatenation of the two::

    from sqlalchemy.ext.hybrid import hybrid_property

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))

        @hybrid_property
        def fullname(self):
            return self.firstname + " " + self.lastname

Above, the ``fullname`` attribute is interpreted at both the instance and
class level, so that it is available from an instance::

    some_user = session.query(User).first()
    print(some_user.fullname)

as well as usable within queries::

    some_user = session.query(User).filter(User.fullname == "John Smith").first()

The string concatenation example is a simple one, where the Python expression
can be dual purposed at the instance and class level.  Often, the SQL expression
must be distinguished from the Python expression, which can be achieved using
:meth:`.hybrid_property.expression`.  Below we illustrate the case where a conditional
needs to be present inside the hybrid, using the ``if`` statement in Python and the
:func:`_expression.case` construct for SQL expressions::

    from sqlalchemy.ext.hybrid import hybrid_property
    from sqlalchemy.sql import case

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))

        @hybrid_property
        def fullname(self):
            if self.firstname is not None:
                return self.firstname + " " + self.lastname
            else:
                return self.lastname

        @fullname.expression
        def fullname(cls):
            return case([
                (cls.firstname != None, cls.firstname + " " + cls.lastname),
            ], else_ = cls.lastname)

.. _mapper_column_property_sql_expressions:

Using column_property
---------------------

The :func:`_orm.column_property` function can be used to map a SQL
expression in a manner similar to a regularly mapped :class:`_schema.Column`.
With this technique, the attribute is loaded
along with all other column-mapped attributes at load time.  This is in some
cases an advantage over the usage of hybrids, as the value can be loaded
up front at the same time as the parent row of the object, particularly if
the expression is one which links to other tables (typically as a correlated
subquery) to access data that wouldn't normally be
available on an already loaded object.

Disadvantages to using :func:`_orm.column_property` for SQL expressions include that
the expression must be compatible with the SELECT statement emitted for the class
as a whole, and there are also some configurational quirks which can occur
when using :func:`_orm.column_property` from declarative mixins.

Our "fullname" example can be expressed using :func:`_orm.column_property` as
follows::

    from sqlalchemy.orm import column_property

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))
        fullname = column_property(firstname + " " + lastname)

Correlated subqueries may be used as well. Below we use the
:func:`_expression.select` construct to create a :class:`_sql.ScalarSelect`,
representing a column-oriented SELECT statement, that links together the count
of ``Address`` objects available for a particular ``User``::

    from sqlalchemy.orm import column_property
    from sqlalchemy import select, func
    from sqlalchemy import Column, Integer, String, ForeignKey

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        user_id = Column(Integer, ForeignKey('user.id'))

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        address_count = column_property(
            select(func.count(Address.id)).
            where(Address.user_id==id).
            correlate_except(Address).
            scalar_subquery()
        )

In the above example, we define a :func:`_expression.ScalarSelect` construct like the following::

    stmt = (
        select(func.count(Address.id)).
        where(Address.user_id==id).
        correlate_except(Address).
        scalar_subquery()
    )

Above, we first use :func:`_sql.select` to create a :class:`_sql.Select`
construct, which we then convert into a :term:`scalar subquery` using the
:meth:`_sql.Select.scalar_subquery` method, indicating our intent to use this
:class:`_sql.Select` statement in a column expression context.

Within the :class:`_sql.Select` itself, we select the count of ``Address.id`` rows
where the ``Address.user_id`` column is equated to ``id``, which in the context
of the ``User`` class is the :class:`_schema.Column` named ``id`` (note that ``id`` is
also the name of a Python built in function, which is not what we want to use
here - if we were outside of the ``User`` class definition, we'd use ``User.id``).

The :meth:`_sql.Select.correlate_except` method indicates that each element in the
FROM clause of this :func:`_expression.select` may be omitted from the FROM list (that is, correlated
to the enclosing SELECT statement against ``User``) except for the one corresponding
to ``Address``.  This isn't strictly necessary, but prevents ``Address`` from
being inadvertently omitted from the FROM list in the case of a long string
of joins between ``User`` and ``Address`` tables where SELECT statements against
``Address`` are nested.

If import issues prevent the :func:`.column_property` from being defined
inline with the class, it can be assigned to the class after both
are configured.   When using mappings that make use of a :func:`_orm.declarative_base`
base class, this attribute assignment has the effect of calling :meth:`_orm.Mapper.add_property`
to add an additional property after the fact::

    # only works if a declarative base class is in use
    User.address_count = column_property(
        select(func.count(Address.id)).
        where(Address.user_id==User.id).
        scalar_subquery()
    )

When using mapping styles that don't use :func:`_orm.declarative_base`,
such as the :meth:`_orm.registry.mapped` decorator, the :meth:`_orm.Mapper.add_property`
method may be invoked explicitly on the underlying :class:`_orm.Mapper` object,
which can be obtained using :func:`_sa.inspect`::

    from sqlalchemy.orm import registry

    reg = registry()

    @reg.mapped
    class User:
        __tablename__ = 'user'

        # ... additional mapping directives


    # later ...

    # works for any kind of mapping
    from sqlalchemy import inspect
    inspect(User).add_property(
        column_property(
           select(func.count(Address.id)).
           where(Address.user_id==User.id).
           scalar_subquery()
        )
    )

For a :func:`.column_property` that refers to columns linked from a
many-to-many relationship, use :func:`.and_` to join the fields of the
association table to both tables in a relationship::

    from sqlalchemy import and_

    class Author(Base):
        # ...

        book_count = column_property(
            select(func.count(books.c.id)
            ).where(
                and_(
                    book_authors.c.author_id==authors.c.id,
                    book_authors.c.book_id==books.c.id
                )
            ).scalar_subquery()
        )

.. _mapper_column_property_sql_expressions_composed:

Composing from Column Properties at Mapping Time
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is possible to create mappings that combine multiple
:class:`.ColumnProperty` objects together.  The :class:`.ColumnProperty` will
be interpreted as a SQL expression when used in a Core expression context,
provided that it is targeted by an existing expression object; this works by
the Core detecting that the object has a ``__clause_element__()`` method which
returns a SQL expression.   However, if the :class:`.ColumnProperty` is used as
a lead object in an expression where there is no other Core SQL expression
object to target it, the :attr:`.ColumnProperty.expression` attribute will
return the underlying SQL expression so that it can be used to build SQL
expressions consistently.  Below, the ``File`` class contains an attribute
``File.path`` that concatenates a string token to the ``File.filename``
attribute, which is itself a :class:`.ColumnProperty`::


    class File(Base):
        __tablename__ = 'file'

        id = Column(Integer, primary_key=True)
        name = Column(String(64))
        extension = Column(String(8))
        filename = column_property(name + '.' + extension)
        path = column_property('C:/' + filename.expression)

When the ``File`` class is used in expressions normally, the attributes
assigned to ``filename`` and ``path`` are usable directly.  The use of the
:attr:`.ColumnProperty.expression` attribute is only necessary when using
the :class:`.ColumnProperty` directly within the mapping definition::

    q = session.query(File.path).filter(File.filename == 'foo.txt')


Using a plain descriptor
------------------------

In cases where a SQL query more elaborate than what :func:`_orm.column_property`
or :class:`.hybrid_property` can provide must be emitted, a regular Python
function accessed as an attribute can be used, assuming the expression
only needs to be available on an already-loaded instance.   The function
is decorated with Python's own ``@property`` decorator to mark it as a read-only
attribute.   Within the function, :func:`.object_session`
is used to locate the :class:`.Session` corresponding to the current object,
which is then used to emit a query::

    from sqlalchemy.orm import object_session
    from sqlalchemy import select, func

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))

        @property
        def address_count(self):
            return object_session(self).\
                scalar(
                    select(func.count(Address.id)).\
                        where(Address.user_id==self.id)
                )

The plain descriptor approach is useful as a last resort, but is less performant
in the usual case than both the hybrid and column property approaches, in that
it needs to emit a SQL query upon each access.

.. _mapper_querytime_expression:

Query-time SQL expressions as mapped attributes
-----------------------------------------------

When using :meth:`.Session.query`, we have the option to specify not just
mapped entities but ad-hoc SQL expressions as well.  Suppose if a class
``A`` had integer attributes ``.x`` and ``.y``, we could query for ``A``
objects, and additionally the sum of ``.x`` and ``.y``, as follows::

    q = session.query(A, A.x + A.y)

The above query returns tuples of the form ``(A object, integer)``.

An option exists which can apply the ad-hoc ``A.x + A.y`` expression to the
returned ``A`` objects instead of as a separate tuple entry; this is the
:func:`.with_expression` query option in conjunction with the
:func:`.query_expression` attribute mapping.    The class is mapped
to include a placeholder attribute where any particular SQL expression
may be applied::

    from sqlalchemy.orm import query_expression

    class A(Base):
        __tablename__ = 'a'
        id = Column(Integer, primary_key=True)
        x = Column(Integer)
        y = Column(Integer)

        expr = query_expression()

We can then query for objects of type ``A``, applying an arbitrary
SQL expression to be populated into ``A.expr``::

    from sqlalchemy.orm import with_expression
    q = session.query(A).options(
        with_expression(A.expr, A.x + A.y))

The :func:`.query_expression` mapping has these caveats:

* On an object where :func:`.query_expression` were not used to populate
  the attribute, the attribute on an object instance will have the value
  ``None``, unless the :paramref:`_orm.query_expression.default_expr`
  parameter is set to an alternate SQL expression.

* The query_expression value **does not populate on an object that is
  already loaded**.  That is, this will **not work**::

    obj = session.query(A).first()

    obj = session.query(A).options(with_expression(A.expr, some_expr)).first()

  To ensure the attribute is re-loaded, use :meth:`_orm.Query.populate_existing`::

    obj = session.query(A).populate_existing().options(
        with_expression(A.expr, some_expr)).first()

* The query_expression value **does not refresh when the object is
  expired**.  Once the object is expired, either via :meth:`.Session.expire`
  or via the expire_on_commit behavior of :meth:`.Session.commit`, the value is
  removed from the attribute and will return ``None`` on subsequent access.
  Only by running a new :class:`_query.Query` that touches the object which includes
  a new :func:`.with_expression` directive will the attribute be set to a
  non-None value.

* The mapped attribute currently **cannot** be applied to other parts of the
  query, such as the WHERE clause, the ORDER BY clause, and make use of the
  ad-hoc expression; that is, this won't work::

    # wont work
    q = session.query(A).options(
        with_expression(A.expr, A.x + A.y)
    ).filter(A.expr > 5).order_by(A.expr)

  The ``A.expr`` expression will resolve to NULL in the above WHERE clause
  and ORDER BY clause. To use the expression throughout the query, assign to a
  variable and use that::

    a_expr = A.x + A.y
    q = session.query(A).options(
        with_expression(A.expr, a_expr)
    ).filter(a_expr > 5).order_by(a_expr)

.. versionadded:: 1.2

