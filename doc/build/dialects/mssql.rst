Microsoft SQL Server
====================

.. automodule:: sqlalchemy.dialects.mssql.base

SQL Server Data Types
-----------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with SQL server are importable from the top level dialect, whether
they originate from :mod:`sqlalchemy.types` or from the local dialect::

    from sqlalchemy.dialects.mssql import \
        BIGINT, BINARY, BIT, CHAR, DATE, DATETIME, DATETIME2, \
        DATETIMEOFFSET, DECIMAL, FLOAT, IMAGE, INTEGER, MONEY, \
        NCHAR, NTEXT, NUMERIC, NVARCHAR, REAL, SMALLDATETIME, \
        SMALLINT, SMALLMONEY, SQL_VARIANT, TEXT, TIME, \
        TIMESTAMP, TINYINT, UNIQUEIDENTIFIER, VARBINARY, VARCHAR

Types which are specific to SQL Server, or have SQL Server-specific 
construction arguments, are as follows:

.. currentmodule:: sqlalchemy.dialects.mssql

.. autoclass:: BIT
   :members: __init__
   :show-inheritance:

.. autoclass:: CHAR
   :members: __init__
   :show-inheritance:

.. autoclass:: DATETIME2
   :members: __init__
   :show-inheritance:

.. autoclass:: DATETIMEOFFSET
   :members: __init__
   :show-inheritance:

.. autoclass:: IMAGE
   :members: __init__
   :show-inheritance:

.. autoclass:: MONEY
   :members: __init__
   :show-inheritance:

.. autoclass:: NCHAR
   :members: __init__
   :show-inheritance:

.. autoclass:: NTEXT
   :members: __init__
   :show-inheritance:

.. autoclass:: NVARCHAR
   :members: __init__
   :show-inheritance:

.. autoclass:: REAL
   :members: __init__
   :show-inheritance:

.. autoclass:: SMALLDATETIME
   :members: __init__
   :show-inheritance:

.. autoclass:: SMALLMONEY
   :members: __init__
   :show-inheritance:

.. autoclass:: SQL_VARIANT
   :members: __init__
   :show-inheritance:

.. autoclass:: TEXT
   :members: __init__
   :show-inheritance:

.. autoclass:: TIME
   :members: __init__
   :show-inheritance:

.. autoclass:: TINYINT
   :members: __init__
   :show-inheritance:

.. autoclass:: UNIQUEIDENTIFIER
   :members: __init__
   :show-inheritance:

.. autoclass:: VARCHAR
   :members: __init__
   :show-inheritance:


PyODBC
------
.. automodule:: sqlalchemy.dialects.mssql.pyodbc

mxODBC
------
.. automodule:: sqlalchemy.dialects.mssql.mxodbc

pymssql
-------
.. automodule:: sqlalchemy.dialects.mssql.pymssql

zxjdbc Notes
--------------

.. automodule:: sqlalchemy.dialects.mssql.zxjdbc

AdoDBAPI
--------
.. automodule:: sqlalchemy.dialects.mssql.adodbapi

