.. _mssql_toplevel:

Microsoft SQL Server
====================

.. automodule:: sqlalchemy.dialects.mssql.base

SQL Server SQL Constructs
-------------------------

.. currentmodule:: sqlalchemy.dialects.mssql

.. autofunction:: try_cast

SQL Server Data Types
---------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with SQL server are importable from the top level dialect, whether
they originate from :mod:`sqlalchemy.types` or from the local dialect::

    from sqlalchemy.dialects.mssql import (
        BIGINT,
        BINARY,
        BIT,
        CHAR,
        DATE,
        DATETIME,
        DATETIME2,
        DATETIMEOFFSET,
        DECIMAL,
        DOUBLE_PRECISION,
        FLOAT,
        IMAGE,
        INTEGER,
        JSON,
        MONEY,
        NCHAR,
        NTEXT,
        NUMERIC,
        NVARCHAR,
        REAL,
        SMALLDATETIME,
        SMALLINT,
        SMALLMONEY,
        SQL_VARIANT,
        TEXT,
        TIME,
        TIMESTAMP,
        TINYINT,
        UNIQUEIDENTIFIER,
        VARBINARY,
        VARCHAR,
    )

Types which are specific to SQL Server, or have SQL Server-specific
construction arguments, are as follows:

.. note: where :noindex: is used, indicates a type that is not redefined
   in the dialect module, just imported from sqltypes.  this avoids warnings
   in the sphinx build

.. currentmodule:: sqlalchemy.dialects.mssql

.. autoclass:: BIT
   :members: __init__


.. autoclass:: CHAR
   :members: __init__
   :noindex:


.. autoclass:: DATETIME2
   :members: __init__


.. autoclass:: DATETIMEOFFSET
   :members: __init__

.. autoclass:: DOUBLE_PRECISION
   :members: __init__

.. autoclass:: IMAGE
   :members: __init__


.. autoclass:: JSON
   :members: __init__


.. autoclass:: MONEY
   :members: __init__


.. autoclass:: NCHAR
   :members: __init__
   :noindex:


.. autoclass:: NTEXT
   :members: __init__


.. autoclass:: NVARCHAR
   :members: __init__
   :noindex:

.. autoclass:: REAL
   :members: __init__

.. autoclass:: ROWVERSION
   :members: __init__

.. autoclass:: SMALLDATETIME
   :members: __init__


.. autoclass:: SMALLMONEY
   :members: __init__


.. autoclass:: SQL_VARIANT
   :members: __init__


.. autoclass:: TEXT
   :members: __init__
   :noindex:

.. autoclass:: TIME
   :members: __init__


.. autoclass:: TIMESTAMP
   :members: __init__

.. autoclass:: TINYINT
   :members: __init__


.. autoclass:: UNIQUEIDENTIFIER
   :members: __init__


.. autoclass:: VARBINARY
   :members: __init__
   :noindex:

.. autoclass:: VARCHAR
   :members: __init__
   :noindex:


.. autoclass:: XML
   :members: __init__


PyODBC
------
.. automodule:: sqlalchemy.dialects.mssql.pyodbc

pymssql
-------
.. automodule:: sqlalchemy.dialects.mssql.pymssql

