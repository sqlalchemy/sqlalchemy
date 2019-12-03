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

Included, but not currently supported dialects
----------------------------------------------

The following dialects have implementations within SQLAlchemy, but they are not
part of continuous integration testing nor are they actively developed.
These dialects may be removed in future major releases.

.. toctree::
    :maxdepth: 1
    :glob:

    firebird
    sybase

.. _external_toplevel:

External Dialects
-----------------

Currently maintained external dialect projects for SQLAlchemy include:


* `ibm_db_sa <http://code.google.com/p/ibm-db/wiki/README>`_ - driver for IBM DB2 and Informix.
* `PyHive <https://github.com/dropbox/PyHive#sqlalchemy>`_ - driver for `Apache Hive <https://hive.apache.org/>`_ and `Presto <https://prestodb.github.io/>`_.
* `pybigquery <https://github.com/mxmzdlv/pybigquery/>`_ - driver for Google BigQuery.
* `sqlalchemy-redshift <https://pypi.python.org/pypi/sqlalchemy-redshift>`_ - driver for Amazon Redshift, adapts
  the existing PostgreSQL/psycopg2 driver.
* `sqlalchemy-drill <https://github.com/JohnOmernik/sqlalchemy-drill>`_ - driver for Apache Drill.
* `sqlalchemy-hana <https://github.com/SAP/sqlalchemy-hana>`_ - driver for SAP Hana.
* `sqlalchemy-solr <https://github.com/aadel/sqlalchemy-solr>`_ - driver for Apache Solr.
* `sqlalchemy_exasol <https://github.com/blue-yonder/sqlalchemy_exasol>`_ - driver for EXASolution.
* `sqlalchemy-sqlany <https://github.com/sqlanywhere/sqlalchemy-sqlany>`_ - driver for SAP Sybase SQL
  Anywhere, developed by SAP.
* `sqlalchemy-monetdb <https://github.com/gijzelaerr/sqlalchemy-monetdb>`_ - driver for MonetDB.
* `snowflake-sqlalchemy <https://github.com/snowflakedb/snowflake-sqlalchemy>`_ - driver for `Snowflake <https://www.snowflake.net/>`_.
* `sqlalchemy-tds <https://github.com/m32/sqlalchemy-tds>`_ - driver for MS-SQL,
  on top of `python-tds <https://github.com/denisenkom/pytds>`_.
* `crate <https://github.com/crate/crate-python>`_ - driver for `CrateDB <https://crate.io/>`_.
* `sqlalchemy-access <https://pypi.org/project/sqlalchemy-access/>`_ - driver for Microsoft Access,
  on top of `pyodbc <https://pypi.org/project/pyodbc/>`_.
* `elasticsearch-dbapi <https://github.com/preset-io/elasticsearch-dbapi/>`_ - readonly driver for Elasticsearch.
* `pydruid <https://github.com/druid-io/pydruid>`_ - driver for Apache Druid.
* `gsheets <https://github.com/betodealmeida/gsheets-db-api>`_ - driver for Google Sheets.
