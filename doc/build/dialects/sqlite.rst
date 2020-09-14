.. _sqlite_toplevel:

SQLite
======

.. automodule:: sqlalchemy.dialects.sqlite.base

SQLite Data Types
-----------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with SQLite are importable from the top level dialect, whether
they originate from :mod:`sqlalchemy.types` or from the local dialect::

    from sqlalchemy.dialects.sqlite import \
                BLOB, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, FLOAT, \
                INTEGER, NUMERIC, JSON, SMALLINT, TEXT, TIME, TIMESTAMP, \
                VARCHAR

.. module:: sqlalchemy.dialects.sqlite

.. autoclass:: DATETIME

.. autoclass:: DATE

.. autoclass:: JSON

.. autoclass:: TIME

SQLite DML Constructs
-------------------------

.. autofunction:: sqlalchemy.dialects.sqlite.insert

.. autoclass:: sqlalchemy.dialects.sqlite.Insert
  :members:

Pysqlite
--------

.. automodule:: sqlalchemy.dialects.sqlite.pysqlite

Pysqlcipher
-----------

.. automodule:: sqlalchemy.dialects.sqlite.pysqlcipher
