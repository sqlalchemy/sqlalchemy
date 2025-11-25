import random

from sqlalchemy import testing
from sqlalchemy.schema import Column
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import column
from sqlalchemy.sql import dml
from sqlalchemy.sql import func
from sqlalchemy.sql import select
from sqlalchemy.sql import text
from sqlalchemy.sql.base import ExecutableStatement
from sqlalchemy.sql.elements import literal
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_not
from sqlalchemy.testing import ne_
from sqlalchemy.testing.schema import Table
from sqlalchemy.types import Integer
from sqlalchemy.types import Text
from sqlalchemy.util.langhelpers import class_hierarchy


class BasicTests(fixtures.TestBase):
    def _all_subclasses(self, cls_):
        return dict.fromkeys(
            s
            for s in class_hierarchy(cls_)
            # class_hierarchy may return values that
            # aren't subclasses of cls
            if issubclass(s, cls_)
        )

    @staticmethod
    def _relevant_impls():
        return (
            text("select 1 + 2"),
            text("select 42 as q").columns(column("q", Integer)),
            func.max(42),
            select(1, 2).union(select(3, 4)),
            select(1, 2),
        )

    def test_params_impl(self):
        exclude = (dml.UpdateBase,)
        visit_names = set()
        for cls_ in self._all_subclasses(ExecutableStatement):
            if not issubclass(cls_, exclude):
                if "__visit_name__" in cls_.__dict__:
                    visit_names.add(cls_.__visit_name__)
                eq_(cls_.params, ExecutableStatement.params, cls_)
            else:
                ne_(cls_.params, ExecutableStatement.params, cls_)
                for other in exclude:
                    if issubclass(cls_, other):
                        eq_(cls_.params, other.params, cls_)
                        break
                else:
                    assert False

        extra = {"orm_from_statement"}
        eq_(
            visit_names - extra,
            {i.__visit_name__ for i in self._relevant_impls()},
        )

    @testing.combinations(*_relevant_impls())
    def test_compile_params(self, impl):
        new = impl.params(foo=5, bar=10)
        is_not(new, impl)
        eq_(impl.compile()._collected_params, {})
        eq_(new.compile()._collected_params, {"foo": 5, "bar": 10})
        eq_(new._generate_cache_key()[2], {"foo": 5, "bar": 10})


class CacheTests(fixtures.TablesTest):
    __sparse_driver_backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table("a", metadata, Column("data", Integer))
        Table("b", metadata, Column("data", Text))

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.a.insert(),
            [{"data": i} for i in range(1, 11)],
        )
        connection.execute(
            cls.tables.b.insert(),
            [{"data": "row %d" % i} for i in range(1, 11)],
        )

    def test_plain_select(self, connection):
        a = self.tables.a

        cs = connection.scalars

        for _ in range(3):
            x1 = random.randint(1, 10)

            eq_(cs(select(a).where(a.c.data == x1)).all(), [x1])
            stmt = select(a).where(a.c.data == bindparam("x", x1))
            eq_(cs(stmt).all(), [x1])

            x1 = random.randint(1, 10)
            eq_(cs(stmt.params({"x": x1})).all(), [x1])

            x1 = random.randint(1, 10)
            eq_(cs(stmt, {"x": x1}).all(), [x1])

            x1 = random.randint(1, 10)
            x2 = random.randint(1, 10)
            eq_(cs(stmt.params({"x": x1}), {"x": x2}).all(), [x2])

            stmt2 = stmt.params(x=6).subquery().select()
            eq_(cs(stmt2).all(), [6])
            eq_(cs(stmt2.params({"x": 2})).all(), [2])

            with expect_deprecated(
                r"The params\(\) and unique_params\(\) "
                "methods on non-statement"
            ):
                # NOTE: can't mix and match the two params styles here
                stmt3 = stmt.params(x=6).subquery().params(x=8).select()
            eq_(cs(stmt3).all(), [6])
            eq_(cs(stmt3.params({"x": 9})).all(), [9])

    def test_union(self, connection):
        a = self.tables.a

        cs = connection.scalars
        for _ in range(3):
            x1 = random.randint(1, 10)
            x2 = random.randint(1, 10)

            eq_(
                cs(
                    select(a)
                    .where(a.c.data == x1)
                    .union_all(select(a).where(a.c.data == x2))
                    .order_by(a.c.data)
                ).all(),
                sorted([x1, x2]),
            )

            x1 = random.randint(1, 10)
            x2 = random.randint(1, 10)
            stmt = (
                select(a, literal(1).label("ord"))
                .where(a.c.data == bindparam("x", x1))
                .union_all(
                    select(a, literal(2)).where(a.c.data == bindparam("y", x2))
                )
                .order_by("ord")
            )
            eq_(cs(stmt).all(), [x1, x2])

            x1a = random.randint(1, 10)
            eq_(cs(stmt.params({"x": x1a})).all(), [x1a, x2])

            x2 = random.randint(1, 10)
            eq_(cs(stmt, {"y": x2}).all(), [x1, x2])

            x1 = random.randint(1, 10)
            x2 = random.randint(1, 10)
            eq_(cs(stmt.params({"x": x1}), {"y": x2}).all(), [x1, x2])

            x1 = random.randint(1, 10)
            x2 = random.randint(1, 10)
            stmt2 = (
                stmt.params(x=x1)
                .subquery()
                .select()
                .params(y=x2)
                .order_by("ord")
            )
            eq_(cs(stmt2).all(), [x1, x2])
            eq_(cs(stmt2.params({"x": x1}).params({"y": x2})).all(), [x1, x2])

    def test_text(self, connection):
        a = self.tables.a

        cs = connection.scalars

        for _ in range(3):
            x0 = random.randint(1, 10)
            stmt = text("select data from a where data = :x").params(x=x0)
            eq_(cs(stmt).all(), [x0])

            x1 = random.randint(1, 10)
            eq_(cs(stmt.params({"x": x1})).all(), [x1])

            x2 = random.randint(1, 10)
            stmt2 = stmt.columns(a.c.data).params(x=x2)
            eq_(cs(stmt2).all(), [x2])
            eq_(cs(stmt2, {"x": 1}).all(), [1])
            eq_(cs(stmt2.params(x=1)).all(), [1])
