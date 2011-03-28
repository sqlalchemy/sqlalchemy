Oracle
======

.. automodule:: sqlalchemy.dialects.oracle.base

Oracle Data Types
-------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with Oracle are importable from the top level dialect, whether
they originate from :mod:`sqlalchemy.types` or from the local dialect::

    from sqlalchemy.dialects.oracle import \
                BFILE, BLOB, CHAR, CLOB, DATE, DATETIME, \
                DOUBLE_PRECISION, FLOAT, INTERVAL, LONG, NCLOB, \
                NUMBER, NVARCHAR, NVARCHAR2, RAW, TIMESTAMP, VARCHAR, \
                VARCHAR2

Types which are specific to Oracle, or have Oracle-specific 
construction arguments, are as follows:

.. currentmodule:: sqlalchemy.dialects.oracle

.. autoclass:: BFILE
  :members: __init__
  :show-inheritance:

.. autoclass:: DOUBLE_PRECISION
   :members: __init__
   :show-inheritance:

.. autoclass:: INTERVAL
  :members: __init__
  :show-inheritance:

.. autoclass:: NCLOB
  :members: __init__
  :show-inheritance:

.. autoclass:: NUMBER
   :members: __init__
   :show-inheritance:

.. autoclass:: LONG
  :members: __init__
  :show-inheritance:

.. autoclass:: RAW
  :members: __init__
  :show-inheritance:

cx_Oracle Notes
---------------

.. automodule:: sqlalchemy.dialects.oracle.cx_oracle

zxjdbc Notes
--------------

.. automodule:: sqlalchemy.dialects.oracle.zxjdbc
