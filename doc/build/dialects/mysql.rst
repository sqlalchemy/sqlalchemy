MySQL
=====

.. automodule:: sqlalchemy.dialects.mysql.base

MySQL Data Types
------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with MySQL are importable from the top level dialect::

    from sqlalchemy.dialects.mysql import \
            BIGINT, BINARY, BIT, BLOB, BOOLEAN, CHAR, DATE, \
            DATETIME, DECIMAL, DECIMAL, DOUBLE, ENUM, FLOAT, INTEGER, \
            LONGBLOB, LONGTEXT, MEDIUMBLOB, MEDIUMINT, MEDIUMTEXT, NCHAR, \
            NUMERIC, NVARCHAR, REAL, SET, SMALLINT, TEXT, TIME, TIMESTAMP, \
            TINYBLOB, TINYINT, TINYTEXT, VARBINARY, VARCHAR, YEAR

Types which are specific to MySQL, or have MySQL-specific 
construction arguments, are as follows:

.. currentmodule:: sqlalchemy.dialects.mysql

.. autoclass:: BIGINT
    :members: __init__
    :show-inheritance:

.. autoclass:: BINARY
    :members: __init__
    :show-inheritance:

.. autoclass:: BIT
    :members: __init__
    :show-inheritance:

.. autoclass:: BLOB
    :members: __init__
    :show-inheritance:

.. autoclass:: BOOLEAN
    :members: __init__
    :show-inheritance:

.. autoclass:: CHAR
    :members: __init__
    :show-inheritance:

.. autoclass:: DATE
    :members: __init__
    :show-inheritance:

.. autoclass:: DATETIME
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

.. autoclass:: LONGBLOB
    :members: __init__
    :show-inheritance:

.. autoclass:: LONGTEXT
    :members: __init__
    :show-inheritance:

.. autoclass:: MEDIUMBLOB
    :members: __init__
    :show-inheritance:

.. autoclass:: MEDIUMINT
    :members: __init__
    :show-inheritance:

.. autoclass:: MEDIUMTEXT
    :members: __init__
    :show-inheritance:

.. autoclass:: NCHAR
    :members: __init__
    :show-inheritance:

.. autoclass:: NUMERIC
    :members: __init__
    :show-inheritance:

.. autoclass:: NVARCHAR
    :members: __init__
    :show-inheritance:

.. autoclass:: REAL
    :members: __init__
    :show-inheritance:

.. autoclass:: SET
    :members: __init__
    :show-inheritance:

.. autoclass:: SMALLINT
    :members: __init__
    :show-inheritance:

.. autoclass:: TEXT
    :members: __init__
    :show-inheritance:

.. autoclass:: TIME
    :members: __init__
    :show-inheritance:

.. autoclass:: TIMESTAMP
    :members: __init__
    :show-inheritance:

.. autoclass:: TINYBLOB
    :members: __init__
    :show-inheritance:

.. autoclass:: TINYINT
    :members: __init__
    :show-inheritance:

.. autoclass:: TINYTEXT
    :members: __init__
    :show-inheritance:

.. autoclass:: VARBINARY
    :members: __init__
    :show-inheritance:

.. autoclass:: VARCHAR
    :members: __init__
    :show-inheritance:

.. autoclass:: YEAR
    :members: __init__
    :show-inheritance:


MySQL-Python Notes
--------------------

.. automodule:: sqlalchemy.dialects.mysql.mysqldb

OurSQL Notes
--------------

.. automodule:: sqlalchemy.dialects.mysql.oursql

pymysql Notes
-------------

.. automodule:: sqlalchemy.dialects.mysql.pymysql

MySQL-Connector Notes
----------------------

.. automodule:: sqlalchemy.dialects.mysql.mysqlconnector

pyodbc Notes
--------------

.. automodule:: sqlalchemy.dialects.mysql.pyodbc

zxjdbc Notes
--------------

.. automodule:: sqlalchemy.dialects.mysql.zxjdbc
