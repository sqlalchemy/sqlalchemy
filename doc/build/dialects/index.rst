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

    postgresql
    mysql
    sqlite
    oracle
    mssql
    firebird
    sybase

.. _external_toplevel:

External Dialects
-----------------

Currently maintained external dialect projects for SQLAlchemy include:


* `ibm_db_sa <http://code.google.com/p/ibm-db/wiki/README>`_ - driver for IBM DB2 and Informix.
* `sqlalchemy-redshift <https://pypi.python.org/pypi/sqlalchemy-redshift>`_ - driver for Amazon Redshift, adapts
  the existing Postgresql/psycopg2 driver.
* `sqlalchemy_exasol <https://github.com/blue-yonder/sqlalchemy_exasol>`_ - driver for EXASolution.
* `sqlalchemy-sqlany <https://github.com/sqlanywhere/sqlalchemy-sqlany>`_ - driver for SAP Sybase SQL
  Anywhere, developed by SAP.
* `sqlalchemy-monetdb <https://github.com/gijzelaerr/sqlalchemy-monetdb>`_ - driver for MonetDB.
* `snowflake-sqlalchemy <https://github.com/snowflakedb/snowflake-sqlalchemy>`_ - driver for `Snowflake <https://www.snowflake.net/>`_.

