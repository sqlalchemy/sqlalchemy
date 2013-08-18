.. _drizzle_toplevel:

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
     

.. autoclass:: CHAR
    :members: __init__
     

.. autoclass:: DECIMAL
    :members: __init__
     

.. autoclass:: DOUBLE
    :members: __init__
     

.. autoclass:: ENUM
    :members: __init__
     

.. autoclass:: FLOAT
    :members: __init__
     

.. autoclass:: INTEGER
    :members: __init__
     

.. autoclass:: NUMERIC
    :members: __init__
     

.. autoclass:: REAL
    :members: __init__
     

.. autoclass:: TEXT
    :members: __init__
     

.. autoclass:: TIMESTAMP
    :members: __init__
     

.. autoclass:: VARCHAR
    :members: __init__
     


MySQL-Python
------------

.. automodule:: sqlalchemy.dialects.drizzle.mysqldb
