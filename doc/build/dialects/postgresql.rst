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
        DOUBLE_PRECISION, ENUM, FLOAT, INET, INTEGER, INTERVAL, \
        MACADDR, NUMERIC, REAL, SMALLINT, TEXT, TIME, TIMESTAMP, \
        UUID, VARCHAR

Types which are specific to PostgreSQL, or have PostgreSQL-specific
construction arguments, are as follows:

.. currentmodule:: sqlalchemy.dialects.postgresql

.. autoclass:: array

.. autoclass:: ARRAY
    :members: __init__, Comparator
    :show-inheritance:

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
