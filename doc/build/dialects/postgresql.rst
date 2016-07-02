.. _postgresql_toplevel:

PostgreSQL
==========

.. automodule:: sqlalchemy.dialects.postgresql.base

PostgreSQL Data Types
------------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with PostgreSQL are importable from the top level dialect, whether
they originate from :mod:`sqlalchemy.types` or from the local dialect::

    from sqlalchemy.dialects.postgresql import \
        ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
        DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, \
        INTERVAL, JSON, JSONB, MACADDR, NUMERIC, OID, REAL, SMALLINT, TEXT, \
        TIME, TIMESTAMP, UUID, VARCHAR, INT4RANGE, INT8RANGE, NUMRANGE, \
        DATERANGE, TSRANGE, TSTZRANGE, TSVECTOR

Types which are specific to PostgreSQL, or have PostgreSQL-specific
construction arguments, are as follows:

.. currentmodule:: sqlalchemy.dialects.postgresql

.. autoclass:: aggregate_order_by

.. autoclass:: array

.. autoclass:: ARRAY
    :members: __init__, Comparator

.. autofunction:: array_agg

.. autofunction:: Any

.. autofunction:: All

.. autoclass:: BIT
    :members: __init__


.. autoclass:: BYTEA
    :members: __init__

.. autoclass:: CIDR


.. autoclass:: DOUBLE_PRECISION
    :members: __init__


.. autoclass:: ENUM
    :members: __init__, create, drop


.. autoclass:: HSTORE
    :members:


.. autoclass:: hstore
    :members:


.. autoclass:: INET
    :members: __init__


.. autoclass:: INTERVAL
    :members: __init__

.. autoclass:: JSON
    :members:

.. autoclass:: JSONB
    :members:

.. autoclass:: MACADDR
    :members: __init__

.. autoclass:: OID
    :members: __init__

.. autoclass:: REAL
    :members: __init__

.. autoclass:: TSVECTOR
    :members: __init__

.. autoclass:: UUID
    :members: __init__


Range Types
~~~~~~~~~~~

The new range column types found in PostgreSQL 9.2 onwards are
catered for by the following types:

.. autoclass:: INT4RANGE


.. autoclass:: INT8RANGE


.. autoclass:: NUMRANGE


.. autoclass:: DATERANGE


.. autoclass:: TSRANGE


.. autoclass:: TSTZRANGE


The types above get most of their functionality from the following
mixin:

.. autoclass:: sqlalchemy.dialects.postgresql.ranges.RangeOperators
    :members:

.. warning::

  The range type DDL support should work with any Postgres DBAPI
  driver, however the data types returned may vary. If you are using
  ``psycopg2``, it's recommended to upgrade to version 2.5 or later
  before using these column types.

When instantiating models that use these column types, you should pass
whatever data type is expected by the DBAPI driver you're using for
the column type. For :mod:`psycopg2` these are
:class:`~psycopg2.extras.NumericRange`,
:class:`~psycopg2.extras.DateRange`,
:class:`~psycopg2.extras.DateTimeRange` and
:class:`~psycopg2.extras.DateTimeTZRange` or the class you've
registered with :func:`~psycopg2.extras.register_range`.

For example:

.. code-block:: python

  from psycopg2.extras import DateTimeRange
  from sqlalchemy.dialects.postgresql import TSRANGE

  class RoomBooking(Base):

      __tablename__ = 'room_booking'

      room = Column(Integer(), primary_key=True)
      during = Column(TSRANGE())

  booking = RoomBooking(
      room=101,
      during=DateTimeRange(datetime(2013, 3, 23), None)
  )

PostgreSQL Constraint Types
---------------------------

SQLAlchemy supports PostgreSQL EXCLUDE constraints via the
:class:`ExcludeConstraint` class:

.. autoclass:: ExcludeConstraint
   :members: __init__

For example::

  from sqlalchemy.dialects.postgresql import ExcludeConstraint, TSRANGE

  class RoomBooking(Base):

      __tablename__ = 'room_booking'

      room = Column(Integer(), primary_key=True)
      during = Column(TSRANGE())

      __table_args__ = (
          ExcludeConstraint(('room', '='), ('during', '&&')),
      )

PostgreSQL DML Constructs
---------------------------

.. autofunction:: sqlalchemy.dialects.postgresql.dml.insert

.. autoclass:: sqlalchemy.dialects.postgresql.dml.Insert
  :members:

psycopg2
--------

.. automodule:: sqlalchemy.dialects.postgresql.psycopg2

pg8000
------

.. automodule:: sqlalchemy.dialects.postgresql.pg8000

psycopg2cffi
------------

.. automodule:: sqlalchemy.dialects.postgresql.psycopg2cffi

py-postgresql
-------------

.. automodule:: sqlalchemy.dialects.postgresql.pypostgresql

.. _dialect-postgresql-pygresql:

pygresql
--------

.. automodule:: sqlalchemy.dialects.postgresql.pygresql

zxjdbc
------

.. automodule:: sqlalchemy.dialects.postgresql.zxjdbc

