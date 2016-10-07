=====================
SQLALCHEMY UNIT TESTS
=====================

**NOTE:** SQLAlchemy as of 0.9.4 now standardizes on `pytest <http://pytest.org/>`_
for test running!  However, the existing support for Nose **still remains**!
That is, you can now run the tests via pytest or nose.  We hope to keep the
suite nose-compatible indefinitely however this might change at some point.

SQLAlchemy unit tests by default run using Python's built-in sqlite3
module.   If running on a Python installation that doesn't include this
module, then pysqlite or compatible must be installed.

Unit tests can be run with pytest or nose:

    py.test: http://pytest.org/

    nose: https://pypi.python.org/pypi/nose/

The suite includes enhanced support when running with pytest.

SQLAlchemy implements plugins for both pytest and nose that must be
present when tests are run.   In the case of pytest, this plugin is automatically
used when pytest is run against the SQLAlchemy source tree.  However,
for Nose support, a special test runner script must be used.


The test suite as also requires the mock library.  While
mock is part of the Python standard library as of 3.3, previous versions
will need to have it installed, and is available at::

    https://pypi.python.org/pypi/mock

RUNNING TESTS VIA SETUP.PY
--------------------------
A plain vanilla run of all tests using sqlite can be run via setup.py, and
requires that pytest is installed::

    $ python setup.py test


RUNNING ALL TESTS - PYTEST
--------------------------
To run all tests::

    $ py.test

The pytest configuration in setup.cfg will point the runner at the
test/ directory, where it consumes a conftest.py file that gets everything
else up and running.


RUNNING ALL TESTS - NOSE
--------------------------

When using Nose, a bootstrap script is provided which sets up sys.path
as well as installs the nose plugin::

    $ ./sqla_nose.py

Assuming all tests pass, this is a very unexciting output.  To make it more
interesting::

    $ ./sqla_nose.py -v

RUNNING INDIVIDUAL TESTS
---------------------------------

Any directory of test modules can be run at once by specifying the directory
path, and a specific file can be specified as well::

    $ py.test test/dialect

    $ py.test test/orm/test_mapper.py

When using nose, the setup.cfg currently sets "where" to "test/", so the
"test/" prefix is omitted::

    $ ./sqla_nose.py dialect/

    $ ./sqla_nose.py orm/test_mapper.py

With Nose, it is often more intuitive to specify tests as module paths::

    $ ./sqla_nose.py test.orm.test_mapper

Nose can also specify a test class and optional method using this syntax::

    $ ./sqla_nose.py test.orm.test_mapper:MapperTest.test_utils

With pytest, the -k flag is used to limit tests::

    $ py.test test/orm/test_mapper.py -k "MapperTest and test_utils"


COMMAND LINE OPTIONS
--------------------

SQLAlchemy-specific options are added to both runners, which are viewable
within the help screen.  With pytest, these options are easier to locate
as they are underneath the "sqlalchemy" grouping::

    $ py.test --help

    $ ./sqla_nose.py --help

The --help screen is a combination of common nose options and options which
the SQLAlchemy nose plugin adds.  The most commonly SQLAlchemy-specific
options used are '--db' and '--dburi'.

Both pytest and nose support the same set of SQLAlchemy options, though
pytest features a bit more capability with them.


DATABASE TARGETS
----------------

Tests will target an in-memory SQLite database by default.  To test against
another database, use the --dburi option with any standard SQLAlchemy URL::

    --dburi=postgresql://user:password@localhost/test

If you'll be running the tests frequently, database aliases can save a lot of
typing.  The --dbs option lists the built-in aliases and their matching URLs::

    $ py.test --dbs
    Available --db options (use --dburi to override)
               mysql    mysql://scott:tiger@127.0.0.1:3306/test
              oracle    oracle://scott:tiger@127.0.0.1:1521
            postgresql    postgresql://scott:tiger@127.0.0.1:5432/test
    [...]

To run tests against an aliased database::

    $ py.test --db postgresql

This list of database urls is present in the setup.cfg file.   The list
can be modified/extended by adding a file ``test.cfg`` at the
top level of the SQLAlchemy source distribution which includes
additional entries::

    [db]
    postgresql=postgresql://myuser:mypass@localhost/mydb

Your custom entries will override the defaults and you'll see them reflected
in the output of --dbs.

MULTIPLE DATABASE TARGETS
-------------------------

As of SQLAlchemy 0.9.4, the test runner supports **multiple databases at once**.
This doesn't mean that the entire test suite runs for each database, but
instead specific test suites may do so, while other tests may choose to
run on a specific target out of those available.   For example, if the tests underneath
test/dialect/ are run, the majority of these tests are either specific to
a particular backend, or are marked as "multiple", meaning they will run repeatedly
for each database in use.  If one runs the test suite as follows::

    $ py.test test/dialect --db sqlite --db postgresql --db mysql

The tests underneath test/dialect/test_suite.py will be tripled up, running
as appropriate for each target database, whereas dialect-specific tests
within test/dialect/mysql, test/dialect/postgresql/ test/dialect/test_sqlite.py
should run fully with no skips, as each suite has its target database available.

The multiple targets feature is available both under pytest and nose,
however when running nose, the "multiple runner" feature won't be available;
instead, the first database target will be used.

When running with multiple targets, tests that don't prefer a specific target
will be run against the first target specified.  Putting sqlite first in
the list will lead to a much faster suite as the in-memory database is
extremely fast for setting up and tearing down tables.



DATABASE CONFIGURATION
----------------------

Use an empty database and a database user with general DBA privileges.
The test suite will be creating and dropping many tables and other DDL, and
preexisting tables will interfere with the tests.

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

    MYSQL: Default storage engine should be "MyISAM".   Tests that require
    "InnoDB" as the engine will specify this explicitly.

    ORACLE: a user named "test_schema" is created.

    The primary database user needs to be able to create and drop tables,
    synonyms, and constraints within the "test_schema" user.   For this
    to work fully, including that the user has the "REFERENCES" role
    in a remote schema for tables not yet defined (REFERENCES is per-table),
    it is required that the test the user be present in the "DBA" role:

        grant dba to scott;

    SYBASE: Similar to Oracle, "test_schema" is created as a user, and the
    primary test user needs to have the "sa_role".

    It's also recommended to turn on "trunc log on chkpt" and to use a
    separate transaction log device - Sybase basically seizes up when
    the transaction log is full otherwise.

    A full series of setup assuming sa/master:

        disk init name="translog", physname="/opt/sybase/data/translog.dat", size="10M"
        create database sqlalchemy on default log on translog="10M"
        sp_dboption sqlalchemy, "trunc log on chkpt", true
        sp_addlogin scott, "tiger7"
        sp_addlogin test_schema, "tiger7"
        use sqlalchemy
        sp_adduser scott
        sp_adduser test_schema
        grant all to scott
        sp_role "grant", sa_role, scott

    Sybase will still freeze for up to a minute when the log becomes
    full.  To manually dump the log::

        dump tran sqlalchemy with truncate_only

    MSSQL: Tests that involve multiple connections require Snapshot Isolation
    ability implemented on the test database in order to prevent deadlocks that
    will occur with record locking isolation. This feature is only available
    with MSSQL 2005 and greater. You must enable snapshot isolation at the
    database level and set the default cursor isolation with two SQL commands:

     ALTER DATABASE MyDatabase SET ALLOW_SNAPSHOT_ISOLATION ON

     ALTER DATABASE MyDatabase SET READ_COMMITTED_SNAPSHOT ON

    MSSQL+zxJDBC: Trying to run the unit tests on Windows against SQL Server
    requires using a test.cfg configuration file as the cmd.exe shell won't
    properly pass the URL arguments into the nose test runner.

    POSTGRESQL: Full-text search configuration should be set to English, else
    several tests of ``.match()`` will fail. This can be set (if it isn't so
    already) with:

     ALTER DATABASE test SET default_text_search_config = 'pg_catalog.english'


CONFIGURING LOGGING
-------------------
SQLAlchemy logs its activity and debugging through Python's logging package.
Any log target can be directed to the console with command line options, such
as::

    $ ./sqla_nose.py test.orm.unitofwork --log-info=sqlalchemy.orm.mapper \
      --log-debug=sqlalchemy.pool --log-info=sqlalchemy.engine

This would log mapper configuration, connection pool checkouts, and SQL
statement execution.


BUILT-IN COVERAGE REPORTING
------------------------------
Coverage is tracked using the coverage plugins built for pytest or nose::

    $ py.test test/sql/test_query --cov=sqlalchemy

    $ ./sqla_nose.py test.sql.test_query --with-coverage

BIG COVERAGE TIP !!!  There is an issue where existing .pyc files may
store the incorrect filepaths, which will break the coverage system.  If
coverage numbers are coming out as low/zero, try deleting all .pyc files.

DEVELOPING AND TESTING NEW DIALECTS
-----------------------------------

See the file README.dialects.rst for detail on dialects.


TESTING WITH MULTIPLE PYTHON VERSIONS USING TOX
-----------------------------------------------

If you want to test across multiple versions of Python, you may find `tox
<http://tox.testrun.org/>`_ useful.  SQLAlchemy includes a tox.ini file::

    tox -e full

SQLAlchemy uses tox mostly for pre-fab testing configurations, to simplify
configuration of Jenkins jobs, and *not* for testing different Python
interpreters simultaneously.  You can of course create whatever alternate
tox.ini file you want.

Environments include::

    "full" - runs a full py.test

    "coverage" - runs a py.test plus coverage, skipping memory/timing
    intensive tests

    "pep8" - runs flake8 against the codebase (useful with --diff to check
    against a patch)


PARALLEL TESTING
----------------

Parallel testing is supported using the Pytest xdist plugin.   Supported
databases currently include sqlite, postgresql, and mysql.  The username
for the database should have CREATE DATABASE and DROP DATABASE privileges.
After installing pytest-xdist, testing is run adding the -n<num> option.
For example, to run against sqlite, mysql, postgresql with four processes::

    tox -e -- -n 4 --db sqlite --db postgresql --db mysql

Each backend has a different scheme for setting up the database.  PostgreSQL
still needs the "test_schema" and "test_schema_2" schemas present, as the
parallel databases are created using the base database as a "template".
