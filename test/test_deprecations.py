from sqlalchemy.testing import expect_deprecated_20, fixtures
from sqlalchemy.util.compat import import_


class DeprecationWarningsTest(fixtures.TestBase):
    __backend__ = False

    def test_deprecate_databases(self):
        with expect_deprecated_20(
            "The `database` package is deprecated and will be removed in v2.0 "
        ):
            import_("sqlalchemy.databases")
