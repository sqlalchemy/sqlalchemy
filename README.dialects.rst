========================
Developing new Dialects
========================

.. note::

   When studying this file, it's probably a good idea to also
   familiarize with the  README.unittests.rst file, which discusses
   SQLAlchemy's usage and extension of the Nose test runner.

While SQLAlchemy includes many dialects within the core distribution, the
trend for new dialects should be that they are published as external
projects.   SQLAlchemy has since version 0.5 featured a "plugin" system
which allows external dialects to be integrated into SQLAlchemy using
standard setuptools entry points.  As of version 0.8, this system has
been enhanced, so that a dialect can also be "plugged in" at runtime.

On the testing side, SQLAlchemy as of 0.8 also includes a "dialect
compliance suite" that is usable by third party libraries.  There is no
longer a strong need for a new dialect to run through SQLAlchemy's full
testing suite, as a large portion of these tests do not have
dialect-sensitive functionality.  The "dialect compliance suite" should
be viewed as the primary target for new dialects, and as it continues
to grow and mature it should become a more thorough and efficient system
of testing new dialects.

As of SQLAlchemy 0.9.4, both nose and pytest are supported for running tests,
and pytest is now preferred.

Dialect Layout
===============

The file structure of a dialect is typically similar to the following::

    sqlalchemy-<dialect>/
                         setup.py
                         setup.cfg
                         run_tests.py
                         sqlalchemy_<dialect>/
                                              __init__.py
                                              base.py
                                              <dbapi>.py
                                              requirements.py
                         test/
                                              conftest.py
                                              __init__.py
                                              test_suite.py
                                              test_<dialect_specific_test>.py
                                              ...

An example of this structure can be seen in the Access dialect at
https://bitbucket.org/zzzeek/sqlalchemy-access/.

Key aspects of this file layout include:

* setup.py - should specify setuptools entrypoints, allowing the
  dialect to be usable from create_engine(), e.g.::

        entry_points={
         'sqlalchemy.dialects': [
              'access = sqlalchemy_access.pyodbc:AccessDialect_pyodbc',
              'access.pyodbc = sqlalchemy_access.pyodbc:AccessDialect_pyodbc',
              ]
        }

  Above, the two entrypoints ``access`` and ``access.pyodbc`` allow URLs to be
  used such as::

    create_engine("access://user:pw@dsn")

    create_engine("access+pyodbc://user:pw@dsn")

* setup.cfg - this file contains the traditional contents such as [egg_info],
  [pytest] and [nosetests] directives, but also contains new directives that are used
  by SQLAlchemy's testing framework.  E.g. for Access::

    [egg_info]
    tag_build = dev

    [pytest]
    addopts= --tb native -v -r fxX
    python_files=test/*test_*.py

    [nosetests]
    with-sqla_testing = true
    where = test
    cover-package = sqlalchemy_access
    with-coverage = 1
    cover-erase = 1

    [sqla_testing]
    requirement_cls=sqlalchemy_access.requirements:Requirements
    profile_file=.profiles.txt

    [db]
    default=access+pyodbc://admin@access_test
    sqlite=sqlite:///:memory:

  Above, the ``[sqla_testing]`` section contains configuration used by
  SQLAlchemy's test plugin.  The ``[pytest]`` and ``[nosetests]`` sections
  include directives to help with these runners; in the case of
  Nose, the directive ``with-sql_testing = true``, which indicates to Nose that
  the SQLAlchemy nose plugin should be used.  In the case of pytest, the
  test/conftest.py file will bootstrap SQLAlchemy's plugin.

* test/conftest.py - This script bootstraps SQLAlchemy's pytest plugin
  into the pytest runner.  This
  script can also be used to install your third party dialect into
  SQLAlchemy without using the setuptools entrypoint system; this allows
  your dialect to be present without any explicit setup.py step needed.
  The other portion invokes SQLAlchemy's pytest plugin::

    from sqlalchemy.dialects import registry

    registry.register("access", "sqlalchemy_access.pyodbc", "AccessDialect_pyodbc")
    registry.register("access.pyodbc", "sqlalchemy_access.pyodbc", "AccessDialect_pyodbc")

    from sqlalchemy.testing.plugin.pytestplugin import *

  Where above, the ``registry`` module, introduced in SQLAlchemy 0.8, provides
  an in-Python means of installing the dialect entrypoints without the use
  of setuptools, using the ``registry.register()`` function in a way that
  is similar to the ``entry_points`` directive we placed in our ``setup.py``.

* run_tests.py - This script is used when running the tests via Nose.
  The purpose of the script is to plug in SQLAlchemy's nose plugin into
  the Nose environment before the tests run.

  The format of this file is similar to that of conftest.py; first,
  the optional but helpful step of registering your third party plugin,
  then the other is to import SQLAlchemy's nose runner and invoke it::

    from sqlalchemy.dialects import registry

    registry.register("access", "sqlalchemy_access.pyodbc", "AccessDialect_pyodbc")
    registry.register("access.pyodbc", "sqlalchemy_access.pyodbc", "AccessDialect_pyodbc")

    from sqlalchemy.testing import runner

    # use this in setup.py 'test_suite':
    # test_suite="run_tests.setup_py_test"
    def setup_py_test():
        runner.setup_py_test()

    if __name__ == '__main__':
        runner.main()

  The call to ``runner.main()`` then runs the Nose front end, which installs
  SQLAlchemy's testing plugins.   Invoking our custom runner looks like the
  following::

    $ python run_tests.py -v

* requirements.py - The ``requirements.py`` file is where directives
  regarding database and dialect capabilities are set up.
  SQLAlchemy's tests are often annotated with decorators   that mark
  tests as "skip" or "fail" for particular backends.  Over time, this
  system   has been refined such that specific database and DBAPI names
  are mentioned   less and less, in favor of @requires directives which
  state a particular capability.   The requirement directive is linked
  to target dialects using a ``Requirements`` subclass.   The custom
  ``Requirements`` subclass is specified in the ``requirements.py`` file
  and   is made available to SQLAlchemy's test runner using the
  ``requirement_cls`` directive   inside the ``[sqla_testing]`` section.

  For a third-party dialect, the custom ``Requirements`` class can
  usually specify a simple yes/no answer for a particular system. For
  example, a requirements file that specifies a database that supports
  the RETURNING construct but does not support reflection of tables
  might look like this::

      # sqlalchemy_access/requirements.py

      from sqlalchemy.testing.requirements import SuiteRequirements

      from sqlalchemy.testing import exclusions

      class Requirements(SuiteRequirements):
          @property
          def table_reflection(self):
              return exclusions.closed()

          @property
          def returning(self):
              return exclusions.open()

  The ``SuiteRequirements`` class in
  ``sqlalchemy.testing.requirements`` contains a large number of
  requirements rules, which attempt to have reasonable defaults. The
  tests will report on those requirements found as they are run.

  The requirements system can also be used when running SQLAlchemy's
  primary test suite against the external dialect.  In this use case,
  a ``--dburi`` as well as a ``--requirements`` flag are passed to SQLAlchemy's
  main test runner ``./sqla_nose.py`` so that exclusions specific to the
  dialect take place::

    cd /path/to/sqlalchemy
    py.test -v \
      --requirements sqlalchemy_access.requirements:Requirements \
      --dburi access+pyodbc://admin@access_test

* test_suite.py - Finally, the ``test_suite.py`` module represents a
  stub test suite, which pulls in the actual SQLAlchemy test suite.
  To pull in the suite as a whole, it can   be imported in one step::

      # test/test_suite.py

      from sqlalchemy.testing.suite import *

  That's all that's needed - the ``sqlalchemy.testing.suite`` package
  contains an ever expanding series of tests, most of which should be
  annotated with specific requirement decorators so that they can be
  fully controlled. To specifically modify some of the tests, they can
  be imported by name and subclassed::

      from sqlalchemy.testing.suite import *

      from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest

      class ComponentReflectionTest(_ComponentReflectionTest):
          @classmethod
          def define_views(cls, metadata, schema):
              # bypass the "define_views" section of the
              # fixture
              return

Going Forward
==============

The third-party dialect can be distributed like any other Python
module on Pypi. Links to prominent dialects can be featured within
SQLAlchemy's own documentation; contact the developers (see AUTHORS)
for help with this.

While SQLAlchemy includes many dialects built in, it remains to be
seen if the project as a whole might move towards "plugin" model for
all dialects, including all those currently built in.  Now that
SQLAlchemy's dialect API is mature and the test suite is not far
behind, it may be that a better maintenance experience can be
delivered by having all dialects separately maintained and released.

As new versions of SQLAlchemy are released, the test suite and
requirements file will receive new tests and changes.  The dialect
maintainer would normally keep track of these changes and make
adjustments as needed.

Continuous Integration
======================

The most ideal scenario for ongoing dialect testing is continuous
integration, that is, an automated test runner that runs in response
to changes not just in the dialect itself but to new pushes to
SQLAlchemy as well.

The SQLAlchemy project features a Jenkins installation that runs tests
on Amazon EC2 instances.   It is possible for third-party dialect
developers to provide the SQLAlchemy project either with AMIs or EC2
instance keys which feature test environments appropriate to the
dialect - SQLAlchemy's own Jenkins suite can invoke tests on these
environments.  Contact the developers for further info.

