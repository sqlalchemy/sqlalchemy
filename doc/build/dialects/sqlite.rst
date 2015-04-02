.. _sqlite_toplevel:

SQLite
======

.. automodule:: sqlalchemy.dialects.sqlite.base

SQLite Data Types
------------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with SQLite are importable from the top level dialect, whether
they originate from :mod:`sqlalchemy.types` or from the local dialect::

    from sqlalchemy.dialects.sqlite import \
                BLOB, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, FLOAT, \
                INTEGER, NUMERIC, SMALLINT, TEXT, TIME, TIMESTAMP, \
                VARCHAR

.. module:: sqlalchemy.dialects.sqlite

.. autoclass:: DATETIME

.. autoclass:: DATE

.. autoclass:: TIME

Pysqlite
--------

.. automodule:: sqlalchemy.dialects.sqlite.pysqlite

Pysqlcipher
-----------

.. automodule:: sqlalchemy.dialects.sqlite.pysqlcipher
