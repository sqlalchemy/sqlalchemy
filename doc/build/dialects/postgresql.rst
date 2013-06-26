.. _postgresql_toplevel:

PostgreSQL
==========

.. automodule:: sqlalchemy.dialects.postgresql.base

PostgreSQL Data Types
------------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with Postgresql are importable from the top level dialect, whether
they originate from :mod:`sqlalchemy.types` or from the local dialect::

    from sqlalchemy.dialects.postgresql import \
        ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
        DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, \
        INTERVAL, MACADDR, NUMERIC, REAL, SMALLINT, TEXT, TIME, \
        TIMESTAMP, UUID, VARCHAR, INT4RANGE, INT8RANGE, NUMRANGE, \
        DATERANGE, TSRANGE, TSTZRANGE

Types which are specific to PostgreSQL, or have PostgreSQL-specific
construction arguments, are as follows:

.. currentmodule:: sqlalchemy.dialects.postgresql

.. autoclass:: array

.. autoclass:: ARRAY
    :members: __init__, Comparator
    :show-inheritance:

.. autoclass:: Any

.. autoclass:: All

.. autoclass:: BIT
    :members: __init__
    :show-inheritance:

.. autoclass:: BYTEA
    :members: __init__
    :show-inheritance:

.. autoclass:: CIDR
    :members: __init__
    :show-inheritance:

.. autoclass:: DOUBLE_PRECISION
    :members: __init__
    :show-inheritance:

.. autoclass:: ENUM
    :members: __init__, create, drop
    :show-inheritance:

.. autoclass:: HSTORE
    :members:
    :show-inheritance:

.. autoclass:: hstore
    :members:
    :show-inheritance:

.. autoclass:: INET
    :members: __init__
    :show-inheritance:

.. autoclass:: INTERVAL
    :members: __init__
    :show-inheritance:

.. autoclass:: MACADDR
    :members: __init__
    :show-inheritance:

.. autoclass:: REAL
    :members: __init__
    :show-inheritance:

.. autoclass:: UUID
    :members: __init__
    :show-inheritance:

Range Types
~~~~~~~~~~~

The new range column types founds in PostgreSQL 9.2 onwards are
catered for by the following types:

.. autoclass:: INT4RANGE
   :show-inheritance:

.. autoclass:: INT8RANGE
   :show-inheritance:

.. autoclass:: NUMRANGE
   :show-inheritance:

.. autoclass:: DATERANGE
   :show-inheritance:

.. autoclass:: TSRANGE
   :show-inheritance:

.. autoclass:: TSTZRANGE
   :show-inheritance:

The types above get most of their functionality from the following
mixin:

.. autoclass:: sqlalchemy.dialects.postgresql.ranges.RangeOperators
    :members:

.. warning::

  The range type DDL support should work with any Postgres DBAPI
  driver, however the data types returned may vary. If you are using
  ``psycopg2``, it's recommended to upgrade to version 2.5 or later
  before using these column types.


PostgreSQL Constraint Types
---------------------------

SQLAlchemy supports Postgresql EXCLUDE constraints via the
:class:`ExcludeConstraint` class:

.. autoclass:: ExcludeConstraint
   :show-inheritance:
   :members: __init__

For example::

  from sqlalchemy.dialects.postgresql import ExcludeConstraint, TSRANGE

  class RoomBookings(Base):

      room = Column(Integer(), primary_key=True)
      during = Column(TSRANGE())

      __table_args__ = (
          ExcludeConstraint(('room', '='), ('during', '&&')),
      )

psycopg2
--------------

.. automodule:: sqlalchemy.dialects.postgresql.psycopg2

py-postgresql
--------------------

.. automodule:: sqlalchemy.dialects.postgresql.pypostgresql

pg8000
--------------

.. automodule:: sqlalchemy.dialects.postgresql.pg8000

zxjdbc
--------------

.. automodule:: sqlalchemy.dialects.postgresql.zxjdbc
