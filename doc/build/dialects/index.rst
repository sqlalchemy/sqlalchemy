.. _dialect_toplevel:

Dialects
========

The **dialect** is the system SQLAlchemy uses to communicate with various types of :term:`DBAPI` implementations and databases.
The sections that follow contain reference documentation and notes specific to the usage of each backend, as well as notes
for the various DBAPIs.

All dialects require that an appropriate DBAPI driver is installed.

Included Dialects
-----------------

.. toctree::
    :maxdepth: 1
    :glob:

    drizzle
    firebird
    informix
    mssql
    mysql
    oracle
    postgresql
    sqlite
    sybase

.. _external_toplevel:

External Dialects
-----------------

.. versionchanged:: 0.8
   As of SQLAlchemy 0.8, several dialects have been moved to external
   projects, and dialects for new databases will also be published
   as external projects.   The rationale here is to keep the base
   SQLAlchemy install and test suite from growing inordinately large.

   The "classic" dialects such as SQLite, MySQL, Postgresql, Oracle,
   SQL Server, and Firebird will remain in the Core for the time being.

Current external dialect projects for SQLAlchemy include:

* `ibm_db_sa <http://code.google.com/p/ibm-db/wiki/README>`_ - driver for IBM DB2, developed jointly by IBM and SQLAlchemy developers.
* `sqlalchemy-access <https://bitbucket.org/zzzeek/sqlalchemy-access>`_ - driver for Microsoft Access.
* `sqlalchemy-akiban <https://github.com/zzzeek/sqlalchemy_akiban>`_ - driver and ORM extensions for the `Akiban <http://www.akiban.com>`_ database.
* `sqlalchemy-cubrid <https://bitbucket.org/zzzeek/sqlalchemy-cubrid>`_ - driver for the CUBRID database.
* `sqlalchemy-maxdb <https://bitbucket.org/zzzeek/sqlalchemy-maxdb>`_ - driver for the MaxDB database.
* `CALCHIPAN <https://bitbucket.org/zzzeek/calchipan/>`_ - Adapts `Pandas <http://pandas.pydata.org/>`_ dataframes to SQLAlchemy.


