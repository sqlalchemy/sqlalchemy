from sqlalchemy import testing
from sqlalchemy.testing import fixtures
from sqlalchemy.util.compat import import_


class DatabaseRemovedTest(fixtures.TestBase):
    def test_deprecate_databases(self):
        with testing.expect_deprecated_20(
            "The `database` package is deprecated and will be removed in v2.0 "
        ):
            import_("sqlalchemy.databases")
