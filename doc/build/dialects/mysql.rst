.. _mysql_toplevel:

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
     

.. autoclass:: BINARY
    :members: __init__
     

.. autoclass:: BIT
    :members: __init__
     

.. autoclass:: BLOB
    :members: __init__
     

.. autoclass:: BOOLEAN
    :members: __init__
     

.. autoclass:: CHAR
    :members: __init__
     

.. autoclass:: DATE
    :members: __init__
     

.. autoclass:: DATETIME
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
     

.. autoclass:: LONGBLOB
    :members: __init__
     

.. autoclass:: LONGTEXT
    :members: __init__
     

.. autoclass:: MEDIUMBLOB
    :members: __init__
     

.. autoclass:: MEDIUMINT
    :members: __init__
     

.. autoclass:: MEDIUMTEXT
    :members: __init__
     

.. autoclass:: NCHAR
    :members: __init__
     

.. autoclass:: NUMERIC
    :members: __init__
     

.. autoclass:: NVARCHAR
    :members: __init__
     

.. autoclass:: REAL
    :members: __init__
     

.. autoclass:: SET
    :members: __init__
     

.. autoclass:: SMALLINT
    :members: __init__
     

.. autoclass:: TEXT
    :members: __init__
     

.. autoclass:: TIME
    :members: __init__
     

.. autoclass:: TIMESTAMP
    :members: __init__
     

.. autoclass:: TINYBLOB
    :members: __init__
     

.. autoclass:: TINYINT
    :members: __init__
     

.. autoclass:: TINYTEXT
    :members: __init__
     

.. autoclass:: VARBINARY
    :members: __init__
     

.. autoclass:: VARCHAR
    :members: __init__
     

.. autoclass:: YEAR
    :members: __init__
     

MySQL-Python
--------------------

.. automodule:: sqlalchemy.dialects.mysql.mysqldb

OurSQL
--------------

.. automodule:: sqlalchemy.dialects.mysql.oursql

pymysql
-------------

.. automodule:: sqlalchemy.dialects.mysql.pymysql

MySQL-Connector
----------------------

.. automodule:: sqlalchemy.dialects.mysql.mysqlconnector

cymysql
------------

.. automodule:: sqlalchemy.dialects.mysql.cymysql

Google App Engine
-----------------------

.. automodule:: sqlalchemy.dialects.mysql.gaerdbms

pyodbc
------

.. automodule:: sqlalchemy.dialects.mysql.pyodbc

zxjdbc
--------------

.. automodule:: sqlalchemy.dialects.mysql.zxjdbc
