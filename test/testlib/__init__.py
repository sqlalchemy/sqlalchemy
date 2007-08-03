"""Enhance unittest and instrument SQLAlchemy classes for testing.

Load after sqlalchemy imports to use instrumented stand-ins like Table.
"""

import testlib.config
from testlib.schema import Table, Column
import testlib.testing as testing
from testlib.testing import PersistTest, AssertMixin, ORMTest
import testlib.profiling
import testlib.engines


__all__ = ('testing',
           'Table', 'Column',
           'PersistTest', 'AssertMixin', 'ORMTest')
