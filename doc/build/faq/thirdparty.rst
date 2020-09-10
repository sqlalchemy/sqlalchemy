Third Party Integration Issues
===============================

.. contents::
    :local:
    :class: faq
    :backlinks: none

.. _numpy_int64:

I'm getting errors related to "``numpy.int64``", "``numpy.bool_``", etc.
------------------------------------------------------------------------

The numpy_ package has its own numeric datatypes that extend from Python's
numeric types, but contain some behaviors that in some cases make them impossible
to reconcile with some of SQLAlchemy's behaviors, as well as in some cases
with those of the underlying DBAPI driver in use.

Two errors which can occur are ``ProgrammingError: can't adapt type 'numpy.int64'``
on a backend such as psycopg2, and ``ArgumentError: SQL expression object
expected, got object of type <class 'numpy.bool_'> instead``; in
more recent versions of SQLAlchemy this may be ``ArgumentError: SQL expression
for WHERE/HAVING role expected, got True``.

In the first case, the issue is due to psycopg2 not having an appropriate
lookup entry for the ``int64`` datatype such that it is not accepted directly
by queries.   This may be illustrated from code based on the following::

    import numpy

    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)
        data = Column(Integer)

    # .. later
    session.add(A(data=numpy.int64(10)))
    session.commit()


In the latter case, the issue is due to the ``numpy.int64`` datatype overriding
the ``__eq__()`` method and enforcing that the return type of an expression is
``numpy.True`` or ``numpy.False``, which breaks SQLAlchemy's expression
language behavior that expects to return :class:`_sql.ColumnElement`
expressions from Python equality comparisons::

    >>> import numpy
    >>> from sqlalchemy import column, Integer
    >>> print(column('x', Integer) == numpy.int64(10))  # works
    x = :x_1
    >>> print(numpy.int64(10) == column('x', Integer))  # breaks
    False

These errors are both solved in the same way, which is that special numpy
datatypes need to be replaced with regular Python values.  Examples include
applying the Python ``int()`` function to types like ``numpy.int32`` and
``numpy.int64`` and the Python ``float()`` function to ``numpy.float32``::

    data = numpy.int64(10)

    session.add(A(data=int(data)))

    result = session.execute(
        select(A.data).where(int(data) == A.data)
    )

    session.commit()

.. _numpy: https://numpy.org

SQL expression for WHERE/HAVING role expected, got True
-------------------------------------------------------

See :ref:`numpy_int64`.