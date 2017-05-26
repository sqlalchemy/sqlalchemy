.. _mssql_toplevel:

Microsoft SQL Server
====================

.. automodule:: sqlalchemy.dialects.mssql.base

SQL Server Data Types
---------------------

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


.. autoclass:: CHAR
   :members: __init__


.. autoclass:: DATETIME2
   :members: __init__


.. autoclass:: DATETIMEOFFSET
   :members: __init__


.. autoclass:: IMAGE
   :members: __init__


.. autoclass:: MONEY
   :members: __init__


.. autoclass:: NCHAR
   :members: __init__


.. autoclass:: NTEXT
   :members: __init__


.. autoclass:: NVARCHAR
   :members: __init__


.. autoclass:: REAL
   :members: __init__


.. autoclass:: SMALLDATETIME
   :members: __init__


.. autoclass:: SMALLMONEY
   :members: __init__


.. autoclass:: SQL_VARIANT
   :members: __init__


.. autoclass:: TEXT
   :members: __init__


.. autoclass:: TIME
   :members: __init__


.. autoclass:: TINYINT
   :members: __init__


.. autoclass:: UNIQUEIDENTIFIER
   :members: __init__


.. autoclass:: VARCHAR
   :members: __init__


.. autoclass:: XML
   :members: __init__


PyODBC
------
.. automodule:: sqlalchemy.dialects.mssql.pyodbc

mxODBC
------
.. automodule:: sqlalchemy.dialects.mssql.mxodbc

pymssql
-------
.. automodule:: sqlalchemy.dialects.mssql.pymssql

zxjdbc
------

.. automodule:: sqlalchemy.dialects.mssql.zxjdbc

AdoDBAPI
--------
.. automodule:: sqlalchemy.dialects.mssql.adodbapi

