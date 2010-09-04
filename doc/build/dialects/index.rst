.. _dialect_toplevel:

Dialects
========

The *dialect* is the system SQLAlchemy uses to communicate with various types of DBAPIs and databases.
A compatibility chart of supported backends can be found at :ref:`supported_dbapis`.

This section contains all notes and documentation specific to the usage of various backends.

Supported Databases
-------------------

These backends are fully operational with 
current versions of SQLAlchemy.

.. toctree::
    :maxdepth: 1
    :glob:

    firebird
    mssql
    mysql
    oracle
    postgresql
    sqlite
    sybase

Unsupported Databases
---------------------

These backends are untested and may not be completely
ported to current versions of SQLAlchemy.

.. toctree::
    :maxdepth: 1
    :glob:

    access
    informix
    maxdb

