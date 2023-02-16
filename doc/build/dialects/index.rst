.. _dialect_toplevel:

Dialects
========

The **dialect** is the system SQLAlchemy uses to communicate with various types of :term:`DBAPI` implementations and databases.
The sections that follow contain reference documentation and notes specific to the usage of each backend, as well as notes
for the various DBAPIs.

All dialects require that an appropriate DBAPI driver is installed.

.. _included_dialects:

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

Support Levels for Included Dialects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following table summarizes the support level for each included dialect.

.. dialect-table:: **Supported database versions for included dialects**
  :header-rows: 1

Support Definitions
^^^^^^^^^^^^^^^^^^^

.. glossary::

    Fully tested in CI
        **Fully tested in CI** indicates a version that is tested in the sqlalchemy
        CI system and passes all the tests in the test suite.

    Normal support
        **Normal support** indicates that most features should work,
        but not all versions are tested in the ci configuration so there may
        be some not supported edge cases. We will try to fix issues that affect
        these versions.

    Best effort
        **Best effort** indicates that we try to support basic features on them,
        but most likely there will be unsupported features or errors in some use cases.
        Pull requests with associated issues may be accepted to continue supporting
        older versions, which are reviewed on a case-by-case basis.

.. _external_toplevel:

External Dialects
-----------------

Currently maintained external dialect projects for SQLAlchemy include:

+------------------------------------------------+---------------------------------------+
| Database                                       | Dialect                               |
+================================================+=======================================+
| Actian Avalanche, Vector, Actian X, and Ingres | sqlalchemy-ingres_                    |
+------------------------------------------------+---------------------------------------+
| Amazon Athena                                  | pyathena_                             |
+------------------------------------------------+---------------------------------------+
| Amazon Redshift (via psycopg2)                 | sqlalchemy-redshift_                  |
+------------------------------------------------+---------------------------------------+
| Apache Drill                                   | sqlalchemy-drill_                     |
+------------------------------------------------+---------------------------------------+
| Apache Druid                                   | pydruid_                              |
+------------------------------------------------+---------------------------------------+
| Apache Hive and Presto                         | PyHive_                               |
+------------------------------------------------+---------------------------------------+
| Apache Solr                                    | sqlalchemy-solr_                      |
+------------------------------------------------+---------------------------------------+
| CockroachDB                                    | sqlalchemy-cockroachdb_               |
+------------------------------------------------+---------------------------------------+
| CrateDB                                        | crate-python_                         |
+------------------------------------------------+---------------------------------------+
| EXASolution                                    | sqlalchemy_exasol_                    |
+------------------------------------------------+---------------------------------------+
| Elasticsearch (readonly)                       | elasticsearch-dbapi_                  |
+------------------------------------------------+---------------------------------------+
| Firebird                                       | sqlalchemy-firebird_                  |
+------------------------------------------------+---------------------------------------+
| Firebolt                                       | firebolt-sqlalchemy_                  |
+------------------------------------------------+---------------------------------------+
| Google BigQuery                                | pybigquery_                           |
+------------------------------------------------+---------------------------------------+
| Google Sheets                                  | gsheets_                              |
+------------------------------------------------+---------------------------------------+
| IBM DB2 and Informix                           | ibm-db-sa_                            |
+------------------------------------------------+---------------------------------------+
| IBM Netezza Performance Server [1]_            | nzalchemy_                            |
+------------------------------------------------+---------------------------------------+
| Microsoft Access (via pyodbc)                  | sqlalchemy-access_                    |
+------------------------------------------------+---------------------------------------+
| Microsoft SQL Server (via python-tds)          | sqlalchemy-tds_                       |
+------------------------------------------------+---------------------------------------+
| Microsoft SQL Server (via turbodbc)            | sqlalchemy-turbodbc_                  |
+------------------------------------------------+---------------------------------------+
| MonetDB [1]_                                   | sqlalchemy-monetdb_                   |
+------------------------------------------------+---------------------------------------+
| OpenGauss                                      | openGauss-sqlalchemy_                 |
+------------------------------------------------+---------------------------------------+
| SAP ASE (fork of former Sybase dialect)        | sqlalchemy-sybase_                    |
+------------------------------------------------+---------------------------------------+
| SAP Hana [1]_                                  | sqlalchemy-hana_                      |
+------------------------------------------------+---------------------------------------+
| SAP Sybase SQL Anywhere                        | sqlalchemy-sqlany_                    |
+------------------------------------------------+---------------------------------------+
| Snowflake                                      | snowflake-sqlalchemy_                 |
+------------------------------------------------+---------------------------------------+
| Teradata Vantage                               | teradatasqlalchemy_                   |
+------------------------------------------------+---------------------------------------+

.. [1] Supports version 1.3.x only at the moment.

.. _openGauss-sqlalchemy: https://gitee.com/opengauss/openGauss-sqlalchemy
.. _sqlalchemy-ingres: https://github.com/clach04/ingres_sa_dialect
.. _nzalchemy: https://pypi.org/project/nzalchemy/
.. _ibm-db-sa: https://pypi.org/project/ibm-db-sa/
.. _PyHive: https://github.com/dropbox/PyHive#sqlalchemy
.. _teradatasqlalchemy: https://pypi.org/project/teradatasqlalchemy/
.. _pybigquery: https://github.com/mxmzdlv/pybigquery/
.. _sqlalchemy-redshift: https://pypi.org/project/sqlalchemy-redshift
.. _sqlalchemy-drill: https://github.com/JohnOmernik/sqlalchemy-drill
.. _sqlalchemy-hana: https://github.com/SAP/sqlalchemy-hana
.. _sqlalchemy-solr: https://github.com/aadel/sqlalchemy-solr
.. _sqlalchemy_exasol: https://github.com/blue-yonder/sqlalchemy_exasol
.. _sqlalchemy-sqlany: https://github.com/sqlanywhere/sqlalchemy-sqlany
.. _sqlalchemy-monetdb: https://github.com/gijzelaerr/sqlalchemy-monetdb
.. _snowflake-sqlalchemy: https://github.com/snowflakedb/snowflake-sqlalchemy
.. _sqlalchemy-tds: https://github.com/m32/sqlalchemy-tds
.. _crate-python: https://github.com/crate/crate-python
.. _sqlalchemy-access: https://pypi.org/project/sqlalchemy-access/
.. _elasticsearch-dbapi: https://github.com/preset-io/elasticsearch-dbapi/
.. _pydruid: https://github.com/druid-io/pydruid
.. _gsheets: https://github.com/betodealmeida/gsheets-db-api
.. _sqlalchemy-firebird: https://github.com/pauldex/sqlalchemy-firebird
.. _sqlalchemy-cockroachdb: https://github.com/cockroachdb/sqlalchemy-cockroachdb
.. _sqlalchemy-turbodbc: https://pypi.org/project/sqlalchemy-turbodbc/
.. _sqlalchemy-sybase: https://pypi.org/project/sqlalchemy-sybase/
.. _firebolt-sqlalchemy: https://pypi.org/project/firebolt-sqlalchemy/
.. _pyathena: https://github.com/laughingman7743/PyAthena/
