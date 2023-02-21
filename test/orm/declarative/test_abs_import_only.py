"""
this file tests that absolute imports can be used in declarative
mappings while guaranteeing that the Mapped name is not locally present

"""

from __future__ import annotations

import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.orm
import sqlalchemy.testing
import sqlalchemy.testing.fixtures

try:
    x = Mapped  # type: ignore
except NameError:
    pass
else:
    raise Exception("Mapped name **must not be imported in this file**")


class MappedColumnTest(
    sqlalchemy.testing.fixtures.TestBase, sqlalchemy.testing.AssertsCompiledSQL
):
    __dialect__ = "default"

    def test_fully_qualified_mapped_name(self, decl_base):
        """test #8853 *again*, as reported in #9335 this failed to be fixed"""

        class Foo(decl_base):
            __tablename__ = "foo"

            id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
                primary_key=True
            )

            data: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column()

            data2: sqlalchemy.orm.Mapped[int]

            data3: orm.Mapped[int]

        self.assert_compile(
            sqlalchemy.select(Foo),
            "SELECT foo.id, foo.data, foo.data2, foo.data3 FROM foo",
        )
