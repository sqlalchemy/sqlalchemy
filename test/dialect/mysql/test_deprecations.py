from sqlalchemy import select
from sqlalchemy import table
from sqlalchemy.dialects.mysql import base as mysql
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import fixtures


class CompileTest(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = mysql.dialect()

    def test_distinct_string(self):
        s = select("*").select_from(table("foo"))
        s._distinct = "foo"

        with expect_deprecated(
            "Sending string values for 'distinct' is deprecated in the MySQL "
            "dialect and will be removed in a future release"
        ):
            self.assert_compile(s, "SELECT FOO * FROM foo")
