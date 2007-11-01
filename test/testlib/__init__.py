"""Enhance unittest and instrument SQLAlchemy classes for testing.

Load after sqlalchemy imports to use instrumented stand-ins like Table.
"""

import testlib.config
from testlib.schema import Table, Column
from testlib.orm import mapper
import testlib.testing as testing
from testlib.testing import rowset
from testlib.testing import PersistTest, AssertMixin, ORMTest, SQLCompileTest
import testlib.profiling as profiling
import testlib.engines as engines


__all__ = ('testing',
           'mapper',
           'Table', 'Column',
           'rowset',
           'PersistTest', 'AssertMixin', 'ORMTest', 'SQLCompileTest',
           'profiling', 'engines')
