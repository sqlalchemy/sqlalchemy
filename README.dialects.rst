========================
Developing new Dialects
========================

.. note::

   When studying this file, it's probably a good idea to also
   familiarize with the  README.unittests.rst file, which discusses
   SQLAlchemy's usage and extension of the pytest test runner.

While SQLAlchemy includes many dialects within the core distribution, the
trend for new dialects should be that they are published as external
projects.   SQLAlchemy has since version 0.5 featured a "plugin" system
which allows external dialects to be integrated into SQLAlchemy using
standard setuptools entry points.  As of version 0.8, this system has
been enhanced, so that a dialect can also be "plugged in" at runtime.

On the testing side, SQLAlchemy includes a "dialect compliance
suite" that is usable by third party libraries, in the source tree
at ``lib/sqlalchemy/testing/suite``.   There's no need for a third party
dialect to run through SQLAlchemy's full testing suite, as a large portion of
these tests do not have dialect-sensitive functionality.  The "dialect
compliance suite" should be viewed as the primary target for new dialects.


Dialect Layout
===============

The file structure of a dialect is typically similar to the following:

.. sourcecode:: text

    sqlalchemy-<dialect>/
                         setup.py
                         setup.cfg
                         sqlalchemy_<dialect>/
                                              __init__.py
                                              base.py
                                              <dbapi>.py
                                              requirements.py
                         test/
                                              __init__.py
                                              conftest.py
                                              test_suite.py
                                              test_<dialect_specific_test>.py
                                              ...

An example of this structure can be seen in the MS Access dialect at
https://github.com/gordthompson/sqlalchemy-access .

Key aspects of this file layout include:

* setup.py - should specify setuptools entrypoints, allowing the
  dialect to be usable from create_engine(), e.g.::

        entry_points = {
            "sqlalchemy.dialects": [
                "access.pyodbc = sqlalchemy_access.pyodbc:AccessDialect_pyodbc",
            ]
        }

  Above, the entrypoint ``access.pyodbc`` allow URLs to be used such as::

    create_engine("access+pyodbc://user:pw@dsn")

* setup.cfg - this file contains the traditional contents such as
  [tool:pytest] directives, but also contains new directives that are used
  by SQLAlchemy's testing framework.  E.g. for Access:

  .. sourcecode:: text

    [tool:pytest]
    addopts= --tb native -v -r fxX --maxfail=25 -p no:warnings
    python_files=test/*test_*.py

    [sqla_testing]
    requirement_cls=sqlalchemy_access.requirements:Requirements
    profile_file=test/profiles.txt

    [db]
    default=access+pyodbc://admin@access_test
    sqlite=sqlite:///:memory:

  Above, the ``[sqla_testing]`` section contains configuration used by
  SQLAlchemy's test plugin.  The ``[tool:pytest]`` section
  include directives to help with these runners.  When using pytest
  the test/conftest.py file will bootstrap SQLAlchemy's plugin.

* test/conftest.py - This script bootstraps SQLAlchemy's pytest plugin
  into the pytest runner.  This
  script can also be used to install your third party dialect into
  SQLAlchemy without using the setuptools entrypoint system; this allows
  your dialect to be present without any explicit setup.py step needed.
  The other portion invokes SQLAlchemy's pytest plugin::

    from sqlalchemy.dialects import registry
    import pytest

    registry.register("access.pyodbc", "sqlalchemy_access.pyodbc", "AccessDialect_pyodbc")

    pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

    from sqlalchemy.testing.plugin.pytestplugin import *

  Where above, the ``registry`` module, introduced in SQLAlchemy 0.8, provides
  an in-Python means of installing the dialect entrypoint(s) without the use
  of setuptools, using the ``registry.register()`` function in a way that
  is similar to the ``entry_points`` directive we placed in our ``setup.py``.
  (The ``pytest.register_assert_rewrite`` is there just to suppress a spurious
  warning from pytest.)

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
  the RETURNING construct but does not support nullable boolean
  columns might look like this::

      # sqlalchemy_access/requirements.py

      from sqlalchemy.testing.requirements import SuiteRequirements

      from sqlalchemy.testing import exclusions


      class Requirements(SuiteRequirements):
          @property
          def nullable_booleans(self):
              """Target database allows boolean columns to store NULL."""
              # Access Yes/No doesn't allow null
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
  test runner so that exclusions specific to the dialect take place:

  .. sourcecode:: text

    cd /path/to/sqlalchemy
    pytest -v \
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
  fully controlled.  In the case that the decorators are not covering
  a particular test, a test can also be directly modified or bypassed.
  In the example below, the Access dialect test suite overrides the
  ``get_huge_int()`` test::

      from sqlalchemy.testing.suite import *

      from sqlalchemy.testing.suite import IntegerTest as _IntegerTest


      class IntegerTest(_IntegerTest):

          @testing.skip("access")
          def test_huge_int(self):
              # bypass this test because Access ODBC fails with
              # [ODBC Microsoft Access Driver] Optional feature not implemented.
              return

AsyncIO dialects
----------------

As of version 1.4 SQLAlchemy supports also dialects that use
asyncio drivers to interface with the database backend.

SQLAlchemy's approach to asyncio drivers is that the connection and cursor
objects of the driver (if any) are adapted into a pep-249 compliant interface,
using the ``AdaptedConnection`` interface class. Refer to the internal asyncio
driver implementations such as that of ``asyncpg``, ``asyncmy`` and
``aiosqlite`` for examples.

Going Forward
==============

The third-party dialect can be distributed like any other Python
module on PyPI. Links to prominent dialects can be featured within
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

