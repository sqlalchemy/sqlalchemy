=====================
SQLALCHEMY UNIT TESTS
=====================

SQLAlchemy unit tests by default run using Python's built-in sqlite3
module.  If running on Python 2.4, pysqlite must be installed.

Unit tests are run using nose.  Nose is available at::

    https://pypi.python.org/pypi/nose/

SQLAlchemy implements a nose plugin that must be present when tests are run.
This plugin is invoked when the test runner script provided with
SQLAlchemy is used.

The test suite as of version 0.8.2 also requires the mock library.  While
mock is part of the Python standard library as of 3.3, previous versions
will need to have it installed, and is available at::

    https://pypi.python.org/pypi/mock

**NOTE:** - the nose plugin is no longer installed by setuptools as of
version 0.7 !  Use "python setup.py test" or "./sqla_nose.py".

RUNNING TESTS VIA SETUP.PY
--------------------------
A plain vanilla run of all tests using sqlite can be run via setup.py:

    $ python setup.py test

The -v flag also works here::

    $ python setup.py test -v

RUNNING ALL TESTS
------------------
To run all tests::

    $ ./sqla_nose.py

If you're running the tests on Microsoft Windows, then there is an additional
argument that must be passed to ./sqla_nose.py::

    > ./sqla_nose.py --first-package-wins

This is required because nose's importer will normally evict a package from
sys.modules if it sees a package with the same name in a different location.
Setting this argument disables that behavior.

Assuming all tests pass, this is a very unexciting output.  To make it more
interesting::

    $ ./sqla_nose.py -v

RUNNING INDIVIDUAL TESTS
-------------------------
Any directory of test modules can be run at once by specifying the directory
path::

    $ ./sqla_nose.py test/dialect

Any test module can be run directly by specifying its module name::

    $ ./sqla_nose.py test.orm.test_mapper

To run a specific test within the module, specify it as module:ClassName.methodname::

    $ ./sqla_nose.py test.orm.test_mapper:MapperTest.test_utils


COMMAND LINE OPTIONS
--------------------
Help is available via --help::

    $ ./sqla_nose.py --help

The --help screen is a combination of common nose options and options which
the SQLAlchemy nose plugin adds.  The most commonly SQLAlchemy-specific
options used are '--db' and '--dburi'.


DATABASE TARGETS
----------------

Tests will target an in-memory SQLite database by default.  To test against
another database, use the --dburi option with any standard SQLAlchemy URL::

    --dburi=postgresql://user:password@localhost/test

Use an empty database and a database user with general DBA privileges.
The test suite will be creating and dropping many tables and other DDL, and
preexisting tables will interfere with the tests.

Several tests require alternate usernames or schemas to be present, which
are used to test dotted-name access scenarios.  On some databases such
as Oracle or Sybase, these are usernames, and others such as Postgresql
and MySQL they are schemas.   The requirement applies to all backends
except SQLite and Firebird.  The names are::

    test_schema
    test_schema_2 (only used on Postgresql)

Please refer to your vendor documentation for the proper syntax to create
these namespaces - the database user must have permission to create and drop
tables within these schemas.  Its perfectly fine to run the test suite
without these namespaces present, it only means that a handful of tests which
expect them to be present will fail.

Additional steps specific to individual databases are as follows::

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

If you'll be running the tests frequently, database aliases can save a lot of
typing.  The --dbs option lists the built-in aliases and their matching URLs::

    $ ./sqla_nose.py --dbs
    Available --db options (use --dburi to override)
               mysql    mysql://scott:tiger@127.0.0.1:3306/test
              oracle    oracle://scott:tiger@127.0.0.1:1521
            postgresql    postgresql://scott:tiger@127.0.0.1:5432/test
    [...]

To run tests against an aliased database::

    $ ./sqla_nose.py --db=postgresql

To customize the URLs with your own users or hostnames, create a file
called `test.cfg` at the top level of the SQLAlchemy source distribution.
This file is in Python config format, and contains a [db] section which
lists out additional database configurations::

    [db]
    postgresql=postgresql://myuser:mypass@localhost/mydb

Your custom entries will override the defaults and you'll see them reflected
in the output of --dbs.

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
Coverage is tracked using Nose's coverage plugin.   See the nose
documentation for details.  Basic usage is::

    $ ./sqla_nose.py test.sql.test_query --with-coverage

BIG COVERAGE TIP !!!  There is an issue where existing .pyc files may
store the incorrect filepaths, which will break the coverage system.  If
coverage numbers are coming out as low/zero, try deleting all .pyc files.

DEVELOPING AND TESTING NEW DIALECTS
-----------------------------------

See the new file README.dialects.rst for detail on dialects.

