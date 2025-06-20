import os
import sys

from sqlalchemy.testing import fixtures


here = os.path.dirname(__file__)
sqla_base = os.path.normpath(os.path.join(here, "..", ".."))


sys.path.insert(0, sqla_base)

test_versioning = __import__(
    "examples.versioned_history.test_versioning"
).versioned_history.test_versioning


class VersionedRowsTestLegacyBase(
    test_versioning.TestVersioning,
    fixtures.RemoveORMEventsGlobally,
    fixtures.TestBase,
):
    pass


class VersionedRowsTestNewBase(
    test_versioning.TestVersioningNewBase,
    fixtures.RemoveORMEventsGlobally,
    fixtures.TestBase,
):
    pass


test_qualify = __import__(
    "examples.syntax_extensions.test_qualify"
).syntax_extensions.test_qualify


class QualifyCompileTest(test_qualify.QualifyCompileTest, fixtures.TestBase):
    pass
