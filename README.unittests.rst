=====================
SQLALCHEMY UNIT TESTS
=====================

Updated for 1.1, 1.2

Basic Test Running
==================

A test target exists within the setup.py script.  For basic test runs::

    python setup.py test


Running with Tox
================

For more elaborate CI-style test running, the tox script provided will
run against various Python / database targets.   For a basic run against
Python 2.7 using an in-memory SQLite database::

    tox -e py27-sqlite

The tox runner contains a series of target combinations that can run
against various combinations of databases.  The test suite can be
run against SQLite with "backend" tests also running against a Postgresql
database::

    tox -e py36-sqlite-postgresql

Or to run just "backend" tests against a MySQL database::

    tox -e py36-mysql-backendonly

Running against backends other than SQLite requires that a database of that
vendor be available at a specific URL.  See "Setting Up Databases" below
for details.

The py.test Engine
==================

Both the tox runner and the setup.py runner are using py.test to invoke
the test suite.   Within the realm of py.test, SQLAlchemy itself is adding
a large series of option and customizations to the py.test runner using
plugin points, to allow for SQLAlchemy's multiple database support,
database setup/teardown and connectivity, multi process support, as well as
lots of skip / database selection rules.

Running tests with py.test directly grants more immediate control over
database options and test selection.

A generic py.test run looks like::

    py.test -n4

Above, the full test suite will run against SQLite, using four processes.
If the "-n" flag is not used, the pytest-xdist is skipped and the tests will
run linearly, which will take a pretty long time.

The py.test command line is more handy for running subsets of tests and to
quickly allow for custom database connections.  Example::

    py.test --dburi=postgresql+psycopg2://scott:tiger@localhost/test  test/sql/test_query.py

Above will run the tests in the test/sql/test_query.py file (a pretty good
file for basic "does this database work at all?" to start with) against a
running Postgresql database at the given URL.

The py.test frontend can also run tests against multiple kinds of databases
at once - a large subset of tests are marked as "backend" tests, which will
be run against each available backend, and additionally lots of tests are targeted
at specific backends only, which only run if a matching backend is made available.
For example, to run the test suite against both Postgresql and MySQL at the same time::

    py.test -n4 --db postgresql --db mysql


Setting Up Databases
====================

The test suite identifies several built-in database tags that run against
a pre-set URL.  These can be seen using --dbs::

    $ py.test --dbs=.
    Available --db options (use --dburi to override)
                 default    sqlite:///:memory:
                firebird    firebird://sysdba:masterkey@localhost//Users/classic/foo.fdb
                   mssql    mssql+pyodbc://scott:tiger@ms_2008
           mssql_pymssql    mssql+pymssql://scott:tiger@ms_2008
                   mysql    mysql://scott:tiger@127.0.0.1:3306/test?charset=utf8
                  oracle    oracle://scott:tiger@127.0.0.1:1521
                 oracle8    oracle://scott:tiger@127.0.0.1:1521/?use_ansi=0
                  pg8000    postgresql+pg8000://scott:tiger@127.0.0.1:5432/test
              postgresql    postgresql://scott:tiger@127.0.0.1:5432/test
    postgresql_psycopg2cffi postgresql+psycopg2cffi://scott:tiger@127.0.0.1:5432/test
                 pymysql    mysql+pymysql://scott:tiger@127.0.0.1:3306/test?charset=utf8
                  sqlite    sqlite:///:memory:
             sqlite_file    sqlite:///querytest.db

What those mean is that if you have a database running that can be accessed
by the above URL, you can run the test suite against it using ``--db <name>``.

The URLs are present in the ``setup.cfg`` file.   You can make your own URLs by
creating a new file called ``test.cfg`` and adding your own ``[db]`` section::

    # test.cfg file
    [db]
    my_postgresql=postgresql://username:pass@hostname/dbname

Above, we can now run the tests with ``my_postgresql``::

    py.test --db my_postgresql

We can also override the existing names in our ``test.cfg`` file, so that we can run
with the tox runner also::

    # test.cfg file
    [db]
    postgresql=postgresql://username:pass@hostname/dbname

Now when we run ``tox -e py27-postgresql``, it will use our custom URL instead
of the fixed one in setup.cfg.

Database Configuration
======================

The test runner will by default create and drop tables within the default
database that's in the database URL, *unless* the multiprocessing option
is in use via the py.test "-n" flag, which invokes pytest-xdist.   The
multiprocessing option is **enabled by default** for both the tox runner
and the setup.py frontend.   When multiprocessing is used, the SQLAlchemy
testing framework will create a new database for each process, and then
tear it down after the test run is complete.    So it will be necessary
for the database user to have access to CREATE DATABASE in order for this
to work.

Several tests require alternate usernames or schemas to be present, which
are used to test dotted-name access scenarios.  On some databases such
as Oracle or Sybase, these are usernames, and others such as PostgreSQL
and MySQL they are schemas.   The requirement applies to all backends
except SQLite and Firebird.  The names are::

    test_schema
    test_schema_2 (only used on PostgreSQL)

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


CONFIGURING LOGGING
-------------------
SQLAlchemy logs its activity and debugging through Python's logging package.
Any log target can be directed to the console with command line options, such
as::

    $ ./py.test test/orm/test_unitofwork.py -s \
      --log-debug=sqlalchemy.pool --log-info=sqlalchemy.engine

Above we add the py.test "-s" flag so that standard out is not suppressed.


DEVELOPING AND TESTING NEW DIALECTS
-----------------------------------

See the file README.dialects.rst for detail on dialects.


