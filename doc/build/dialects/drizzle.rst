Drizzle
=======

.. automodule:: sqlalchemy.dialects.drizzle.base

Drizzle Data Types
------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with Drizzle are importable from the top level dialect::

    from sqlalchemy.dialects.drizzle import \
            BIGINT, BINARY, BLOB, BOOLEAN, CHAR, DATE, DATETIME,
            DECIMAL, DOUBLE, ENUM, FLOAT, INT, INTEGER,
            NUMERIC, TEXT, TIME, TIMESTAMP, VARBINARY, VARCHAR

Types which are specific to Drizzle, or have Drizzle-specific 
construction arguments, are as follows:

.. currentmodule:: sqlalchemy.dialects.drizzle

.. autoclass:: BIGINT
    :members: __init__
    :show-inheritance:

.. autoclass:: CHAR
    :members: __init__
    :show-inheritance:

.. autoclass:: DECIMAL
    :members: __init__
    :show-inheritance:

.. autoclass:: DOUBLE
    :members: __init__
    :show-inheritance:

.. autoclass:: ENUM
    :members: __init__
    :show-inheritance:

.. autoclass:: FLOAT
    :members: __init__
    :show-inheritance:

.. autoclass:: INTEGER
    :members: __init__
    :show-inheritance:

.. autoclass:: NUMERIC
    :members: __init__
    :show-inheritance:

.. autoclass:: REAL
    :members: __init__
    :show-inheritance:

.. autoclass:: TEXT
    :members: __init__
    :show-inheritance:

.. autoclass:: TIMESTAMP
    :members: __init__
    :show-inheritance:

.. autoclass:: VARCHAR
    :members: __init__
    :show-inheritance:


MySQL-Python Notes
--------------------

.. automodule:: sqlalchemy.dialects.drizzle.mysqldb
