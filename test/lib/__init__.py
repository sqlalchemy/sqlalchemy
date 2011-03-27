"""Testing environment and utilities.

This package contains base classes and routines used by 
the unit tests.   Tests are based on Nose and bootstrapped
by noseplugin.NoseSQLAlchemy.

"""

from test.bootstrap import config
from test.lib import testing, engines, requires, profiling, pickleable, \
    fixtures
from test.lib.schema import Column, Table
from test.lib.testing import AssertsCompiledSQL, \
    AssertsExecutionResults, ComparesTables, rowset


__all__ = ('testing',
            'Column', 'Table',
           'rowset','fixtures',
           'AssertsExecutionResults',
           'AssertsCompiledSQL', 'ComparesTables',
           'engines', 'profiling', 'pickleable')


