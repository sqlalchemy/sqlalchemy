.. _dialect_toplevel:

Dialects
========

The **dialect** is the system SQLAlchemy uses to communicate with various types of DBAPIs and databases.
A compatibility chart of supported backends can be found at :ref:`supported_dbapis`.  The sections that 
follow contain reference documentation and notes specific to the usage of each backend, as well as notes
for the various DBAPIs.

Note that not all backends are fully ported and tested with
current versions of SQLAlchemy. The compatibility chart
should be consulted to check for current support level.

.. toctree::
    :maxdepth: 1
    :glob:

    drizzle
    firebird
    informix
    maxdb
    access
    mssql
    mysql
    oracle
    postgresql
    sqlite
    sybase



