import os
import sys

from sqlalchemy.testing import fixtures


here = os.path.dirname(__file__)
sqla_base = os.path.normpath(os.path.join(here, "..", ".."))


sys.path.insert(0, sqla_base)

test_versioning = __import__(
    "examples.versioned_history.test_versioning"
).versioned_history.test_versioning


class VersionedRowsTest(
    test_versioning.TestVersioning,
    fixtures.RemoveORMEventsGlobally,
    fixtures.TestBase,
):
    pass
