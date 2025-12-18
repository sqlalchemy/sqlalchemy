"""
this file tests that absolute imports can be used in declarative
mappings while guaranteeing that the Mapped name is not locally present

"""

from __future__ import annotations

import typing

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

    @sqlalchemy.testing.variation(
        "construct", ["Mapped", "WriteOnlyMapped", "DynamicMapped"]
    )
    def test_fully_qualified_writeonly_mapped_name(self, decl_base, construct):
        """further variation in issue #10412"""

        class Foo(decl_base):
            __tablename__ = "foo"

            id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
                primary_key=True
            )

            if construct.Mapped:
                bars: orm.Mapped[typing.List[Bar]] = orm.relationship()
            elif construct.WriteOnlyMapped:
                bars: orm.WriteOnlyMapped[typing.List[Bar]] = (
                    orm.relationship()
                )
            elif construct.DynamicMapped:
                bars: orm.DynamicMapped[typing.List[Bar]] = orm.relationship()
            else:
                construct.fail()

        class Bar(decl_base):
            __tablename__ = "bar"

            id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
                primary_key=True
            )
            foo_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
                sqlalchemy.ForeignKey("foo.id")
            )

        self.assert_compile(
            sqlalchemy.select(Foo).join(Foo.bars),
            "SELECT foo.id FROM foo JOIN bar ON foo.id = bar.foo_id",
        )
