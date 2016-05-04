.. module:: sqlalchemy.orm

.. _mapper_sql_expressions:

SQL Expressions as Mapped Attributes
=====================================

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
:func:`.sql.expression.case` construct for SQL expressions::

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

The :func:`.orm.column_property` function can be used to map a SQL
expression in a manner similar to a regularly mapped :class:`.Column`.
With this technique, the attribute is loaded
along with all other column-mapped attributes at load time.  This is in some
cases an advantage over the usage of hybrids, as the value can be loaded
up front at the same time as the parent row of the object, particularly if
the expression is one which links to other tables (typically as a correlated
subquery) to access data that wouldn't normally be
available on an already loaded object.

Disadvantages to using :func:`.orm.column_property` for SQL expressions include that
the expression must be compatible with the SELECT statement emitted for the class
as a whole, and there are also some configurational quirks which can occur
when using :func:`.orm.column_property` from declarative mixins.

Our "fullname" example can be expressed using :func:`.orm.column_property` as
follows::

    from sqlalchemy.orm import column_property

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))
        fullname = column_property(firstname + " " + lastname)

Correlated subqueries may be used as well.  Below we use the :func:`.select`
construct to create a SELECT that links together the count of ``Address``
objects available for a particular ``User``::

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
            select([func.count(Address.id)]).\
                where(Address.user_id==id).\
                correlate_except(Address)
        )

In the above example, we define a :func:`.select` construct like the following::

    select([func.count(Address.id)]).\
        where(Address.user_id==id).\
        correlate_except(Address)

The meaning of the above statement is, select the count of ``Address.id`` rows
where the ``Address.user_id`` column is equated to ``id``, which in the context
of the ``User`` class is the :class:`.Column` named ``id`` (note that ``id`` is
also the name of a Python built in function, which is not what we want to use
here - if we were outside of the ``User`` class definition, we'd use ``User.id``).

The :meth:`.select.correlate_except` directive indicates that each element in the
FROM clause of this :func:`.select` may be omitted from the FROM list (that is, correlated
to the enclosing SELECT statement against ``User``) except for the one corresponding
to ``Address``.  This isn't strictly necessary, but prevents ``Address`` from
being inadvertently omitted from the FROM list in the case of a long string
of joins between ``User`` and ``Address`` tables where SELECT statements against
``Address`` are nested.

If import issues prevent the :func:`.column_property` from being defined
inline with the class, it can be assigned to the class after both
are configured.   In Declarative this has the effect of calling :meth:`.Mapper.add_property`
to add an additional property after the fact::

    User.address_count = column_property(
            select([func.count(Address.id)]).\
                where(Address.user_id==User.id)
        )

For many-to-many relationships, use :func:`.and_` to join the fields of the
association table to both tables in a relation, illustrated
here with a classical mapping::

    from sqlalchemy import and_

    mapper(Author, authors, properties={
        'book_count': column_property(
                            select([func.count(books.c.id)],
                                and_(
                                    book_authors.c.author_id==authors.c.id,
                                    book_authors.c.book_id==books.c.id
                                )))
        })

Using a plain descriptor
-------------------------

In cases where a SQL query more elaborate than what :func:`.orm.column_property`
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
                    select([func.count(Address.id)]).\
                        where(Address.user_id==self.id)
                )

The plain descriptor approach is useful as a last resort, but is less performant
in the usual case than both the hybrid and column property approaches, in that
it needs to emit a SQL query upon each access.

