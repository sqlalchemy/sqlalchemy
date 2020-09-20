=====================
SQLALCHEMY UNIT TESTS
=====================

Basic Test Running
==================

Tox is used to run the test suite fully.   For basic test runs against
a single Python interpreter::

    tox


Advanced Tox Options
====================

For more elaborate CI-style test running, the tox script provided will
run against various Python / database targets.   For a basic run against
Python 2.7 using an in-memory SQLite database::

    tox -e py38-sqlite

The tox runner contains a series of target combinations that can run
against various combinations of databases.  The test suite can be
run against SQLite with "backend" tests also running against a PostgreSQL
database::

    tox -e py38-sqlite-postgresql

Or to run just "backend" tests against a MySQL databases::

    tox -e py38-mysql-backendonly

Running against backends other than SQLite requires that a database of that
vendor be available at a specific URL.  See "Setting Up Databases" below
for details.

The pytest Engine
==================

The tox runner is using pytest to invoke the test suite.   Within the realm of
pytest, SQLAlchemy itself is adding a large series of option and
customizations to the pytest runner using plugin points, to allow for
SQLAlchemy's multiple database support, database setup/teardown and
connectivity, multi process support, as well as lots of skip / database
selection rules.

Running tests with pytest directly grants more immediate control over
database options and test selection.

A generic pytest run looks like::

    pytest -n4

Above, the full test suite will run against SQLite, using four processes.
If the "-n" flag is not used, the pytest-xdist is skipped and the tests will
run linearly, which will take a pretty long time.

The pytest command line is more handy for running subsets of tests and to
quickly allow for custom database connections.  Example::

    pytest --dburi=postgresql+psycopg2://scott:tiger@localhost/test  test/sql/test_query.py

Above will run the tests in the test/sql/test_query.py file (a pretty good
file for basic "does this database work at all?" to start with) against a
running PostgreSQL database at the given URL.

The pytest frontend can also run tests against multiple kinds of databases at
once - a large subset of tests are marked as "backend" tests, which will be run
against each available backend, and additionally lots of tests are targeted at
specific backends only, which only run if a matching backend is made available.
For example, to run the test suite against both PostgreSQL and MySQL at the
same time::

    pytest -n4 --db postgresql --db mysql


Setting Up Databases
====================

The test suite identifies several built-in database tags that run against
a pre-set URL.  These can be seen using --dbs::

    $ pytest --dbs
    Available --db options (use --dburi to override)
                 default    sqlite:///:memory:
                firebird    firebird://sysdba:masterkey@localhost//Users/classic/foo.fdb
                 mariadb    mariadb://scott:tiger@192.168.0.199:3307/test
                   mssql    mssql+pyodbc://scott:tiger^5HHH@mssql2017:1433/test?driver=ODBC+Driver+13+for+SQL+Server
           mssql_pymssql    mssql+pymssql://scott:tiger@ms_2008
                   mysql    mysql://scott:tiger@127.0.0.1:3306/test?charset=utf8mb4
                  oracle    oracle://scott:tiger@127.0.0.1:1521
                 oracle8    oracle://scott:tiger@127.0.0.1:1521/?use_ansi=0
                  pg8000    postgresql+pg8000://scott:tiger@127.0.0.1:5432/test
              postgresql    postgresql://scott:tiger@127.0.0.1:5432/test
    postgresql_psycopg2cffi postgresql+psycopg2cffi://scott:tiger@127.0.0.1:5432/test
                 pymysql    mysql+pymysql://scott:tiger@127.0.0.1:3306/test?charset=utf8mb4
                  sqlite    sqlite:///:memory:
             sqlite_file    sqlite:///querytest.db

Note that a pyodbc URL **must be against a hostname / database name
combination, not a DSN name** when using the multiprocessing option; this is
because the test suite needs to generate new URLs to refer to per-process
databases that are created on the fly.

What those mean is that if you have a database running that can be accessed
by the above URL, you can run the test suite against it using ``--db <name>``.

The URLs are present in the ``setup.cfg`` file.   You can make your own URLs by
creating a new file called ``test.cfg`` and adding your own ``[db]`` section::

    # test.cfg file
    [db]
    my_postgresql=postgresql://username:pass@hostname/dbname

Above, we can now run the tests with ``my_postgresql``::

    pytest --db my_postgresql

We can also override the existing names in our ``test.cfg`` file, so that we can run
with the tox runner also::

    # test.cfg file
    [db]
    postgresql=postgresql://username:pass@hostname/dbname

Now when we run ``tox -e py27-postgresql``, it will use our custom URL instead
of the fixed one in setup.cfg.

Database Configuration
======================

Step one, the **database chosen for tests must be entirely empty**.  A lot
of what SQLAlchemy tests is creating and dropping lots of tables
as well as running database introspection to see what is there.  If there
are pre-existing tables or other objects in the target database already,
these will get in the way.   A failed test run can also be followed by
 a run that includes the "--dropfirst" option, which will try to drop
all existing tables in the target database.

The above paragraph changes somewhat when the multiprocessing option
is used, in that separate databases will be created instead, however
in the case of Postgresql, the starting database is used as a template,
so the starting database must still be empty.  See below for example
configurations using docker.

The test runner will by default create and drop tables within the default
database that's in the database URL, *unless* the multiprocessing option is in
use via the pytest "-n" flag, which invokes pytest-xdist.   The
multiprocessing option is **enabled by default** when using the tox runner.
When multiprocessing is used, the SQLAlchemy testing framework will create a
new database for each process, and then tear it down after the test run is
complete.    So it will be necessary for the database user to have access to
CREATE DATABASE in order for this to work.   Additionally, as mentioned
earlier, the database URL must be formatted such that it can be rewritten on
the fly to refer to these other databases, which means for pyodbc it must refer
to a hostname/database name combination, not a DSN name.

Several tests require alternate usernames or schemas to be present, which
are used to test dotted-name access scenarios.  On some databases such
as Oracle or Sybase, these are usernames, and others such as PostgreSQL
and MySQL they are schemas.   The requirement applies to all backends
except SQLite and Firebird.  The names are::

    test_schema
    test_schema_2 (only used on PostgreSQL and mssql)

Please refer to your vendor documentation for the proper syntax to create
these namespaces - the database user must have permission to create and drop
tables within these schemas.  Its perfectly fine to run the test suite
without these namespaces present, it only means that a handful of tests which
expect them to be present will fail.

Additional steps specific to individual databases are as follows::

    POSTGRESQL: To enable unicode testing with JSONB, create the
    database with UTF8 encoding::

        postgres=# create database test with owner=scott encoding='utf8' template=template0;

    To include tests for HSTORE, create the HSTORE type engine::

        postgres=# \c test;
        You are now connected to database "test" as user "postgresql".
        test=# create extension hstore;
        CREATE EXTENSION

    Full-text search configuration should be set to English, else
    several tests of ``.match()`` will fail. This can be set (if it isn't so
    already) with:

     ALTER DATABASE test SET default_text_search_config = 'pg_catalog.english'

    For two-phase transaction support, the max_prepared_transactions
    configuration variable must be set to a non-zero value in postgresql.conf.
    See
    https://www.postgresql.org/docs/current/runtime-config-resource.html#GUC-MAX-PREPARED-TRANSACTIONS
    for further background.

    ORACLE: a user named "test_schema" is created in addition to the default
    user.

    The primary database user needs to be able to create and drop tables,
    synonyms, and constraints within the "test_schema" user.   For this
    to work fully, including that the user has the "REFERENCES" role
    in a remote schema for tables not yet defined (REFERENCES is per-table),
    it is required that the test the user be present in the "DBA" role:

        grant dba to scott;

    MSSQL: Tests that involve multiple connections require Snapshot Isolation
    ability implemented on the test database in order to prevent deadlocks that
    will occur with record locking isolation. This feature is only available
    with MSSQL 2005 and greater. You must enable snapshot isolation at the
    database level and set the default cursor isolation with two SQL commands:

     ALTER DATABASE MyDatabase SET ALLOW_SNAPSHOT_ISOLATION ON

     ALTER DATABASE MyDatabase SET READ_COMMITTED_SNAPSHOT ON

Docker Configurations
---------------------

The SQLAlchemy test can run against database running in Docker containers.
This ensures that they are empty and that their configuration is not influenced
by any local usage.

The following configurations are just examples that developers can use to
quickly set up a local environment for SQLAlchemy development. They are **NOT**
intended for production use!

**PostgreSQL configuration**::

    # only needed if a local image of postgres is not already present
    docker pull postgres:12

    # create the container with the proper configuration for sqlalchemy
    docker run --rm -e POSTGRES_USER='scott' -e POSTGRES_PASSWORD='tiger' -e POSTGRES_DB='test' -p 127.0.0.1:5432:5432 -d --name postgres postgres:12-alpine

    # configure the database
    sleep 10
    docker exec -ti postgres psql -U scott -c 'CREATE SCHEMA test_schema; CREATE SCHEMA test_schema_2;' test
    # this last command is optional
    docker exec -ti postgres sed -i 's/#max_prepared_transactions = 0/max_prepared_transactions = 10/g' /var/lib/postgresql/data/postgresql.conf

    # To stop the container. It will also remove it.
    docker stop postgres

**MySQL configuration**::

    # only needed if a local image of mysql is not already present
    docker pull mysql:8

    # create the container with the proper configuration for sqlalchemy
    docker run --rm -e MYSQL_USER='scott' -e MYSQL_PASSWORD='tiger' -e MYSQL_DATABASE='test' -e MYSQL_ROOT_PASSWORD='password' -p 127.0.0.1:3306:3306 -d --name mysql mysql:8 --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci

    # configure the database
    sleep 20
    docker exec -ti mysql mysql -u root -ppassword -D test -w -e "GRANT ALL ON *.* TO scott@'%'; CREATE DATABASE test_schema CHARSET utf8mb4; CREATE DATABASE test_schema_2 CHARSET utf8mb4;"

    # To stop the container. It will also remove it.
    docker stop mysql

**MariaDB configuration**::

    # only needed if a local image of MariaDB is not already present
    docker pull mariadb

    # create the container with the proper configuration for sqlalchemy
    docker run --rm -e MYSQL_USER='scott' -e MYSQL_PASSWORD='tiger' -e MYSQL_DATABASE='test' -e MYSQL_ROOT_PASSWORD='password' -p 127.0.0.1:3306:3306 -d --name mariadb mariadb --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci

    # configure the database
    sleep 20
    docker exec -ti mariadb mysql -u root -ppassword -D test -w -e "GRANT ALL ON *.* TO scott@'%'; CREATE DATABASE test_schema CHARSET utf8mb4; CREATE DATABASE test_schema_2 CHARSET utf8mb4;"

    # To stop the container. It will also remove it.
    docker stop mariadb

**MSSQL configuration**::

    # only needed if a local image of mssql is not already present
    docker pull mcr.microsoft.com/mssql/server:2019-CU1-ubuntu-16.04

    # create the container with the proper configuration for sqlalchemy
    # it will use the Developer version
    docker run --rm -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=yourStrong(!)Password' -p 127.0.0.1:1433:1433 -d --name mssql mcr.microsoft.com/mssql/server:2019-CU2-ubuntu-16.04

    # configure the database
    sleep 20
    docker exec -it mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'yourStrong(!)Password' -Q "sp_configure 'contained database authentication', 1; RECONFIGURE; CREATE DATABASE test CONTAINMENT = PARTIAL; ALTER DATABASE test SET ALLOW_SNAPSHOT_ISOLATION ON; ALTER DATABASE test SET READ_COMMITTED_SNAPSHOT ON; CREATE LOGIN scott WITH PASSWORD = 'tiger^5HHH'; ALTER SERVER ROLE sysadmin ADD MEMBER scott;"
    docker exec -it mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'yourStrong(!)Password' -d test -Q "CREATE SCHEMA test_schema"
    docker exec -it mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'yourStrong(!)Password' -d test -Q "CREATE SCHEMA test_schema_2"

    # To stop the container. It will also remove it.
    docker stop mssql

NOTE: with this configuration the url to use is not the default one configured
in setup, but ``mssql+pymssql://scott:tiger^5HHH@127.0.0.1:1433/test``.  It can
be used with pytest by using ``--db docker_mssql``.

CONFIGURING LOGGING
-------------------
SQLAlchemy logs its activity and debugging through Python's logging package.
Any log target can be directed to the console with command line options, such
as::

    $ ./pytest test/orm/test_unitofwork.py -s \
      --log-debug=sqlalchemy.pool --log-info=sqlalchemy.engine

Above we add the pytest "-s" flag so that standard out is not suppressed.


DEVELOPING AND TESTING NEW DIALECTS
-----------------------------------

See the file README.dialects.rst for detail on dialects.


