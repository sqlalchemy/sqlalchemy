"""Testing environment and utilities.

This package contains base classes and routines used by 
the unit tests.   Tests are based on Nose and bootstrapped
by noseplugin.NoseSQLAlchemy.

"""

from sqlalchemy.test import testing, engines, requires, profiling, pickleable, config
from sqlalchemy.test.schema import Column, Table
from sqlalchemy.test.testing import \
     AssertsCompiledSQL, \
     AssertsExecutionResults, \
     ComparesTables, \
     TestBase, \
     rowset


__all__ = ('testing',
            'Column', 'Table',
           'rowset',
           'TestBase', 'AssertsExecutionResults',
           'AssertsCompiledSQL', 'ComparesTables',
           'engines', 'profiling', 'pickleable')


