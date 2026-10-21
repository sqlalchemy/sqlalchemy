import random
import types

from sqlalchemy import testing
from sqlalchemy.engine import default
from sqlalchemy.schema import Column
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import column
from sqlalchemy.sql import delete
from sqlalchemy.sql import dml
from sqlalchemy.sql import exists
from sqlalchemy.sql import func
from sqlalchemy.sql import insert
from sqlalchemy.sql import select
from sqlalchemy.sql import table
from sqlalchemy.sql import text
from sqlalchemy.sql import tstring
from sqlalchemy.sql import union
from sqlalchemy.sql import update
from sqlalchemy.sql.base import ExecutableStatement
from sqlalchemy.sql.elements import literal
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_not
from sqlalchemy.testing import ne_
from sqlalchemy.testing.schema import Table
from sqlalchemy.types import Integer
from sqlalchemy.types import Text
from sqlalchemy.util import compat
from sqlalchemy.util.compat import Template
from sqlalchemy.util.langhelpers import class_hierarchy

if compat.py314:
    from string.templatelib import Interpolation
else:

    def Interpolation(value, expression):
        # the Template stand-in for Python < 3.14 accepts any object
        # with a .value attribute
        return types.SimpleNamespace(value=value, expression=expression)


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
            tstring(Template("select 1 + 2")),
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


def _all_statement_classes(include_orm):
    """all ExecutableStatement classes that use the statement-level
    params() implementation"""

    return {
        cls_
        for cls_ in class_hierarchy(ExecutableStatement)
        if issubclass(cls_, ExecutableStatement)
        and not issubclass(cls_, dml.UpdateBase)
        and "__visit_name__" in cls_.__dict__
        and cls_.__module__.startswith("sqlalchemy.orm") is include_orm
    }


class ParamsCompilePathsFixture:
    """Compile statements the way :class:`.Connection` does, both with and
    without the compiled cache, and assert that values given to
    ``.params()`` are present in the parameters for execution.

    Each statement construct has to collect its own ``.params()`` values,
    either through the cache key when the compiled cache is used or within
    the compiler when it is not; constructs that compile themselves
    separately can miss the second of these (#13709).

    """

    def _assert_params_all_paths(self, build, expected=None):
        """``build(value)`` returns a statement that will execute with
        bound parameter ``p`` set to ``value`` via ``.params()``."""

        dialect = default.DefaultDialect()
        cache = {}

        for value, expected_hit in (
            (5, dialect.CACHE_MISS),
            (6, dialect.CACHE_HIT),
            (5, dialect.CACHE_HIT),
        ):
            for compiled_cache, hit in (
                (cache, expected_hit),
                (None, dialect.CACHING_DISABLED),
            ):
                stmt = build(value)
                compiled, extracted, param_dict, cache_hit = (
                    stmt._compile_w_cache(
                        dialect,
                        compiled_cache=compiled_cache,
                        column_keys=[],
                        for_executemany=False,
                        schema_translate_map=None,
                    )
                )
                eq_(cache_hit, hit)
                eq_(
                    compiled.construct_params(
                        extracted_parameters=extracted,
                        _collected_params=param_dict,
                    ),
                    ({"p": value} if expected is None else expected(value)),
                )


def _p():
    return bindparam("p", type_=Integer)


_t = table("t", column("id", Integer), column("data", Integer))

# top level forms of each statement construct.  the core forms are
# required to cover every non-DML ExecutableStatement class
_statements = {
    "text": lambda v: text("select :p").params(p=v),
    "tstring": lambda v: tstring(
        Template("select ", Interpolation(_p(), "_p()"))
    ).params(p=v),
    "textual_select": lambda v: text("select :p as q")
    .columns(column("q", Integer))
    .params(p=v),
    "function": lambda v: func.abs(_p()).params(p=v),
    "select": lambda v: select(_p().label("q")).params(p=v),
    "compound_select": lambda v: union(
        select(_p().label("q")), select(_t.c.id)
    ).params(p=v),
}

# row returning statements, which are then embedded inside of
# other statements
_selectables = ("select", "compound_select", "textual_select")

_embeddings = {
    "toplevel": lambda s: s,
    "subquery": lambda s: select(s.subquery()),
    "cte": lambda s: select(s.cte()),
    "scalar_subquery": lambda s: select(s.scalar_subquery()),
    "exists": lambda s: select(exists(s)),
    "in": lambda s: select(_t.c.id).where(_t.c.id.in_(s)),
    # textual select can't be the first element of a UNION
    "union_member": lambda s: union(select(_t.c.id), s),
    "insert_from_select": lambda s: insert(_t).from_select(["id"], s),
    "update_where": lambda s: update(_t)
    .where(_t.c.id.in_(s))
    .values(data=_t.c.id),
    "update_values": lambda s: update(_t).values(data=s.scalar_subquery()),
    "delete_where": lambda s: delete(_t).where(_t.c.id.in_(s)),
}

# statements that are not row returning, embedded inside of other
# statements
_clause_embeddings = {
    "text_where": lambda v: select(_t.c.id).where(
        text("t.id = :p").params(p=v)
    ),
    "tstring_where": lambda v: select(_t.c.id).where(
        tstring(Template("t.id = ", Interpolation(_p(), "_p()"))).params(p=v)
    ),
    "function_column": lambda v: select(func.abs(_p()).params(p=v)),
    "function_where": lambda v: select(_t.c.id).where(
        _t.c.id == func.abs(_p()).params(p=v)
    ),
    "function_over": lambda v: select(func.max(_p()).params(p=v).over()),
    "function_filter": lambda v: select(
        func.max(_p()).params(p=v).filter(_t.c.id == _t.c.data)
    ),
    "function_within_group": lambda v: select(
        func.percentile_cont(_p()).params(p=v).within_group(_t.c.id)
    ),
    "function_table_valued": lambda v: select(
        func.json_each(_p()).params(p=v).table_valued("value")
    ),
    "function_column_valued": lambda v: select(
        func.json_each(_p()).params(p=v).column_valued("value")
    ),
    "function_insert_values": lambda v: insert(_t).values(
        data=func.abs(_p()).params(p=v)
    ),
    "function_update_values": lambda v: update(_t).values(
        data=func.abs(_p()).params(p=v)
    ),
}


class CompilePathsTest(ParamsCompilePathsFixture, fixtures.TestBase):
    """test that .params() values are applied for every statement
    construct, whether or not the compiled cache is used, both at the top
    level and embedded within other statements."""

    def test_all_statements_covered(self):
        eq_(
            {cls_.__visit_name__ for cls_ in _all_statement_classes(False)},
            {build(5).__visit_name__ for build in _statements.values()},
        )

    @testing.combinations(*_statements, argnames="name")
    def test_statement(self, name):
        self._assert_params_all_paths(_statements[name])

    @testing.combinations(*_selectables, argnames="name")
    @testing.combinations(*_embeddings, argnames="embedding")
    def test_selectable_embedded(self, name, embedding):
        build, embed = _statements[name], _embeddings[embedding]
        self._assert_params_all_paths(lambda v: embed(build(v)))

    @testing.combinations(*_clause_embeddings, argnames="name")
    def test_clause_embedded(self, name):
        self._assert_params_all_paths(_clause_embeddings[name])

    @testing.combinations(
        "subquery",
        "cte",
        "scalar_subquery",
        "exists",
        "in",
        "union_member",
        argnames="embedding",
    )
    def test_outer_params_take_precedence(self, embedding):
        """an outer .params() overrides that of an embedded statement
        with or without the compiled cache"""

        embed = _embeddings[embedding]

        self._assert_params_all_paths(
            lambda v: embed(select(_p().label("q")).params(p=3)).params(p=v)
        )


class LiteralBindsTest(fixtures.TestBase, AssertsCompiledSQL):
    """test #13635"""

    __dialect__ = "default"

    @testing.fixture
    def t(self):
        return table("t", column("id", Integer), column("data", Integer))

    def test_select(self, t):
        stmt = select(t.c.id).where(t.c.id == bindparam("x")).params(x=5)
        self.assert_compile(
            stmt,
            "SELECT t.id FROM t WHERE t.id = 5",
            literal_binds=True,
        )

    def test_override_bindparam_value(self, t):
        stmt = select(t.c.id).where(t.c.id == bindparam("x", 3)).params(x=5)
        self.assert_compile(
            stmt,
            "SELECT t.id FROM t WHERE t.id = 5",
            literal_binds=True,
        )

    def test_expanding(self, t):
        stmt = (
            select(t.c.id)
            .where(t.c.id.in_(bindparam("x", expanding=True)))
            .params(x=[1, 2, 3])
        )
        self.assert_compile(
            stmt,
            "SELECT t.id FROM t WHERE t.id IN (1, 2, 3)",
            literal_binds=True,
        )

    def test_outer_params_take_precedence(self, t):
        subq = (
            select(t.c.id)
            .where(t.c.data == bindparam("x"))
            .params(x=3)
            .scalar_subquery()
        )
        stmt = select(t.c.id).where(t.c.id == subq)

        self.assert_compile(
            stmt,
            "SELECT t.id FROM t WHERE t.id = "
            "(SELECT t.id FROM t WHERE t.data = 3)",
            literal_binds=True,
        )
        self.assert_compile(
            stmt.params(x=5),
            "SELECT t.id FROM t WHERE t.id = "
            "(SELECT t.id FROM t WHERE t.data = 5)",
            literal_binds=True,
        )

    def test_union(self, t):
        stmt = union(
            select(t.c.id).where(t.c.id == bindparam("x")),
            select(t.c.id).where(t.c.data == bindparam("y")),
        ).params(x=5, y=7)
        self.assert_compile(
            stmt,
            "SELECT t.id FROM t WHERE t.id = 5 "
            "UNION SELECT t.id FROM t WHERE t.data = 7",
            literal_binds=True,
        )

    def test_text(self):
        stmt = text("select :x").bindparams(bindparam("x", type_=Integer))
        self.assert_compile(stmt.params(x=5), "select 5", literal_binds=True)


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
