from sqlalchemy.dialects.mysql import ENUM
from sqlalchemy.dialects.mysql import SET
from sqlalchemy.testing import expect_deprecated_20
from sqlalchemy.testing import fixtures


class DeprecateQuoting(fixtures.TestBase):
    def test_enum_warning(self):
        ENUM("a", "b")
        with expect_deprecated_20(
            "The 'quoting' parameter to :class:`.mysql.ENUM` is deprecated."
        ):
            ENUM("a", quoting="foo")

    def test_set_warning(self):
        SET("a", "b")
        with expect_deprecated_20(
            "The 'quoting' parameter to :class:`.mysql.SET` is deprecated.*"
        ):
            SET("a", quoting="foo")
