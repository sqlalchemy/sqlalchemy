.. _oracle_toplevel:

Oracle
======

.. automodule:: sqlalchemy.dialects.oracle.base

Oracle Database Data Types
--------------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be valid
with Oracle Database are importable from the top level dialect, whether they
originate from :mod:`sqlalchemy.types` or from the local dialect::

    from sqlalchemy.dialects.oracle import (
        BFILE,
        BLOB,
        CHAR,
        CLOB,
        DATE,
        DOUBLE_PRECISION,
        FLOAT,
        INTERVAL,
        LONG,
        NCLOB,
        NCHAR,
        NUMBER,
        NVARCHAR,
        NVARCHAR2,
        RAW,
        TIMESTAMP,
        VARCHAR,
        VARCHAR2,
        VECTOR,
    )

Types which are specific to Oracle Database, or have Oracle-specific
construction arguments, are as follows:

.. currentmodule:: sqlalchemy.dialects.oracle

.. autoclass:: BFILE
  :members: __init__

.. autoclass:: BINARY_DOUBLE
  :members: __init__

.. autoclass:: BINARY_FLOAT
  :members: __init__

.. autoclass:: DATE
   :members: __init__

.. autoclass:: FLOAT
   :members: __init__

.. autoclass:: INTERVAL
  :members: __init__

.. autoclass:: NCLOB
  :members: __init__

.. autoclass:: NVARCHAR2
   :members: __init__

.. autoclass:: NUMBER
   :members: __init__

.. autoclass:: LONG
  :members: __init__

.. autoclass:: RAW
  :members: __init__

.. autoclass:: ROWID
  :members: __init__

.. autoclass:: TIMESTAMP
  :members: __init__

.. autoclass:: VECTOR
  :members: __init__

.. autoclass:: VectorIndexType
  :members:

.. autoclass:: VectorIndexConfig
  :members:
  :undoc-members:

.. autoclass:: VectorStorageFormat
  :members:

.. autoclass:: VectorDistanceType
  :members:

.. autoclass:: VectorStorageType
  :members:

.. autoclass:: SparseVector
  :members:


.. _oracledb:

python-oracledb
---------------

.. automodule:: sqlalchemy.dialects.oracle.oracledb

.. _cx_oracle:

cx_Oracle
---------

.. automodule:: sqlalchemy.dialects.oracle.cx_oracle
