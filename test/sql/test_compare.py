import importlib
import itertools
import random

from sqlalchemy import and_
from sqlalchemy import Boolean
from sqlalchemy import case
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import dialects
from sqlalchemy import exists
from sqlalchemy import extract
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy import union
from sqlalchemy import union_all
from sqlalchemy import util
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql import False_
from sqlalchemy.sql import func
from sqlalchemy.sql import operators
from sqlalchemy.sql import True_
from sqlalchemy.sql import type_coerce
from sqlalchemy.sql import visitors
from sqlalchemy.sql.base import HasCacheKey
from sqlalchemy.sql.elements import _label_reference
from sqlalchemy.sql.elements import _textual_label_reference
from sqlalchemy.sql.elements import Annotated
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.elements import ClauseList
from sqlalchemy.sql.elements import CollationClause
from sqlalchemy.sql.elements import Immutable
from sqlalchemy.sql.elements import Null
from sqlalchemy.sql.elements import Slice
from sqlalchemy.sql.elements import UnaryExpression
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.sql.functions import ReturnTypeFromArgs
from sqlalchemy.sql.selectable import _OffsetLimitParam
from sqlalchemy.sql.selectable import AliasedReturnsRows
from sqlalchemy.sql.selectable import FromGrouping
from sqlalchemy.sql.selectable import Selectable
from sqlalchemy.sql.selectable import SelectStatementGrouping
from sqlalchemy.sql.visitors import InternalTraversal
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_not_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import ne_
from sqlalchemy.testing.util import random_choices
from sqlalchemy.util import class_hierarchy


meta = MetaData()
meta2 = MetaData()

table_a = Table("a", meta, Column("a", Integer), Column("b", String))
table_b_like_a = Table("b2", meta, Column("a", Integer), Column("b", String))

table_a_2 = Table("a", meta2, Column("a", Integer), Column("b", String))

table_a_2_fs = Table(
    "a", meta2, Column("a", Integer), Column("b", String), schema="fs"
)
table_a_2_bs = Table(
    "a", meta2, Column("a", Integer), Column("b", String), schema="bs"
)

table_b = Table("b", meta, Column("a", Integer), Column("b", Integer))

table_c = Table("c", meta, Column("x", Integer), Column("y", Integer))

table_d = Table("d", meta, Column("y", Integer), Column("z", Integer))


class MyEntity(HasCacheKey):
    def __init__(self, name, element):
        self.name = name
        self.element = element

    _cache_key_traversal = [
        ("name", InternalTraversal.dp_string),
        ("element", InternalTraversal.dp_clauseelement),
    ]


class CoreFixtures(object):
    # lambdas which return a tuple of ColumnElement objects.
    # must return at least two objects that should compare differently.
    # to test more varieties of "difference" additional objects can be added.
    fixtures = [
        lambda: (
            column("q"),
            column("x"),
            column("q", Integer),
            column("q", String),
        ),
        lambda: (~column("q", Boolean), ~column("p", Boolean)),
        lambda: (
            table_a.c.a.label("foo"),
            table_a.c.a.label("bar"),
            table_a.c.b.label("foo"),
        ),
        lambda: (
            _label_reference(table_a.c.a.desc()),
            _label_reference(table_a.c.a.asc()),
        ),
        lambda: (_textual_label_reference("a"), _textual_label_reference("b")),
        lambda: (
            text("select a, b from table").columns(a=Integer, b=String),
            text("select a, b, c from table").columns(
                a=Integer, b=String, c=Integer
            ),
            text("select a, b, c from table where foo=:bar").bindparams(
                bindparam("bar", type_=Integer)
            ),
            text("select a, b, c from table where foo=:foo").bindparams(
                bindparam("foo", type_=Integer)
            ),
            text("select a, b, c from table where foo=:bar").bindparams(
                bindparam("bar", type_=String)
            ),
        ),
        lambda: (
            column("q") == column("x"),
            column("q") == column("y"),
            column("z") == column("x"),
            column("z") + column("x"),
            column("z") - column("x"),
            column("x") - column("z"),
            column("z") > column("x"),
            column("x").in_([5, 7]),
            column("x").in_([10, 7, 8]),
            # note these two are mathematically equivalent but for now they
            # are considered to be different
            column("z") >= column("x"),
            column("x") <= column("z"),
            column("q").between(5, 6),
            column("q").between(5, 6, symmetric=True),
            column("q").like("somstr"),
            column("q").like("somstr", escape="\\"),
            column("q").like("somstr", escape="X"),
        ),
        lambda: (
            table_a.c.a,
            table_a.c.a._annotate({"orm": True}),
            table_a.c.a._annotate({"orm": True})._annotate({"bar": False}),
            table_a.c.a._annotate(
                {"orm": True, "parententity": MyEntity("a", table_a)}
            ),
            table_a.c.a._annotate(
                {"orm": True, "parententity": MyEntity("b", table_a)}
            ),
            table_a.c.a._annotate(
                {"orm": True, "parententity": MyEntity("b", select([table_a]))}
            ),
        ),
        lambda: (
            table_a,
            table_a._annotate({"orm": True}),
            table_a._annotate({"orm": True})._annotate({"bar": False}),
            table_a._annotate(
                {"orm": True, "parententity": MyEntity("a", table_a)}
            ),
            table_a._annotate(
                {"orm": True, "parententity": MyEntity("b", table_a)}
            ),
            table_a._annotate(
                {"orm": True, "parententity": MyEntity("b", select([table_a]))}
            ),
        ),
        lambda: (
            table("a", column("x"), column("y")),
            table("a", column("x"), column("y"))._annotate({"orm": True}),
            table("b", column("x"), column("y"))._annotate({"orm": True}),
        ),
        lambda: (
            cast(column("q"), Integer),
            cast(column("q"), Float),
            cast(column("p"), Integer),
        ),
        lambda: (
            bindparam("x"),
            bindparam("y"),
            bindparam("x", type_=Integer),
            bindparam("x", type_=String),
            bindparam(None),
        ),
        lambda: (_OffsetLimitParam("x"), _OffsetLimitParam("y")),
        lambda: (func.foo(), func.foo(5), func.bar()),
        lambda: (func.current_date(), func.current_time()),
        lambda: (
            func.next_value(Sequence("q")),
            func.next_value(Sequence("p")),
        ),
        lambda: (True_(), False_()),
        lambda: (Null(),),
        lambda: (ReturnTypeFromArgs("foo"), ReturnTypeFromArgs(5)),
        lambda: (FunctionElement(5), FunctionElement(5, 6)),
        lambda: (func.count(), func.not_count()),
        lambda: (func.char_length("abc"), func.char_length("def")),
        lambda: (GenericFunction("a", "b"), GenericFunction("a")),
        lambda: (CollationClause("foobar"), CollationClause("batbar")),
        lambda: (
            type_coerce(column("q", Integer), String),
            type_coerce(column("q", Integer), Float),
            type_coerce(column("z", Integer), Float),
        ),
        lambda: (table_a.c.a, table_b.c.a),
        lambda: (tuple_(1, 2), tuple_(3, 4)),
        lambda: (func.array_agg([1, 2]), func.array_agg([3, 4])),
        lambda: (
            func.percentile_cont(0.5).within_group(table_a.c.a),
            func.percentile_cont(0.5).within_group(table_a.c.b),
            func.percentile_cont(0.5).within_group(table_a.c.a, table_a.c.b),
            func.percentile_cont(0.5).within_group(
                table_a.c.a, table_a.c.b, column("q")
            ),
        ),
        lambda: (
            func.is_equal("a", "b").as_comparison(1, 2),
            func.is_equal("a", "c").as_comparison(1, 2),
            func.is_equal("a", "b").as_comparison(2, 1),
            func.is_equal("a", "b", "c").as_comparison(1, 2),
            func.foobar("a", "b").as_comparison(1, 2),
        ),
        lambda: (
            func.row_number().over(order_by=table_a.c.a),
            func.row_number().over(order_by=table_a.c.a, range_=(0, 10)),
            func.row_number().over(order_by=table_a.c.a, range_=(None, 10)),
            func.row_number().over(order_by=table_a.c.a, rows=(None, 20)),
            func.row_number().over(order_by=table_a.c.b),
            func.row_number().over(
                order_by=table_a.c.a, partition_by=table_a.c.b
            ),
        ),
        lambda: (
            func.count(1).filter(table_a.c.a == 5),
            func.count(1).filter(table_a.c.a == 10),
            func.foob(1).filter(table_a.c.a == 10),
        ),
        lambda: (
            and_(table_a.c.a == 5, table_a.c.b == table_b.c.a),
            and_(table_a.c.a == 5, table_a.c.a == table_b.c.a),
            or_(table_a.c.a == 5, table_a.c.b == table_b.c.a),
            ClauseList(table_a.c.a == 5, table_a.c.b == table_b.c.a),
            ClauseList(table_a.c.a == 5, table_a.c.b == table_a.c.a),
        ),
        lambda: (
            case(whens=[(table_a.c.a == 5, 10), (table_a.c.a == 10, 20)]),
            case(whens=[(table_a.c.a == 18, 10), (table_a.c.a == 10, 20)]),
            case(whens=[(table_a.c.a == 5, 10), (table_a.c.b == 10, 20)]),
            case(
                whens=[
                    (table_a.c.a == 5, 10),
                    (table_a.c.b == 10, 20),
                    (table_a.c.a == 9, 12),
                ]
            ),
            case(
                whens=[(table_a.c.a == 5, 10), (table_a.c.a == 10, 20)],
                else_=30,
            ),
            case({"wendy": "W", "jack": "J"}, value=table_a.c.a, else_="E"),
            case({"wendy": "W", "jack": "J"}, value=table_a.c.b, else_="E"),
            case({"wendy_w": "W", "jack": "J"}, value=table_a.c.a, else_="E"),
        ),
        lambda: (
            extract("foo", table_a.c.a),
            extract("foo", table_a.c.b),
            extract("bar", table_a.c.a),
        ),
        lambda: (
            Slice(1, 2, 5),
            Slice(1, 5, 5),
            Slice(1, 5, 10),
            Slice(2, 10, 15),
        ),
        lambda: (
            select([table_a.c.a]),
            select([table_a.c.a, table_a.c.b]),
            select([table_a.c.b, table_a.c.a]),
            select([table_a.c.a]).where(table_a.c.b == 5),
            select([table_a.c.a])
            .where(table_a.c.b == 5)
            .where(table_a.c.a == 10),
            select([table_a.c.a]).where(table_a.c.b == 5).with_for_update(),
            select([table_a.c.a])
            .where(table_a.c.b == 5)
            .with_for_update(nowait=True),
            select([table_a.c.a]).where(table_a.c.b == 5).correlate(table_b),
            select([table_a.c.a])
            .where(table_a.c.b == 5)
            .correlate_except(table_b),
        ),
        lambda: (
            select([table_a.c.a]).cte(),
            select([table_a.c.a]).cte(recursive=True),
            select([table_a.c.a]).cte(name="some_cte", recursive=True),
            select([table_a.c.a]).cte(name="some_cte"),
            select([table_a.c.a]).cte(name="some_cte").alias("other_cte"),
            select([table_a.c.a])
            .cte(name="some_cte")
            .union_all(select([table_a.c.a])),
            select([table_a.c.a])
            .cte(name="some_cte")
            .union_all(select([table_a.c.b])),
            select([table_a.c.a]).lateral(),
            select([table_a.c.a]).lateral(name="bar"),
            table_a.tablesample(func.bernoulli(1)),
            table_a.tablesample(func.bernoulli(1), seed=func.random()),
            table_a.tablesample(func.bernoulli(1), seed=func.other_random()),
            table_a.tablesample(func.hoho(1)),
            table_a.tablesample(func.bernoulli(1), name="bar"),
            table_a.tablesample(
                func.bernoulli(1), name="bar", seed=func.random()
            ),
        ),
        lambda: (
            select([table_a.c.a]),
            select([table_a.c.a]).prefix_with("foo"),
            select([table_a.c.a]).prefix_with("foo", dialect="mysql"),
            select([table_a.c.a]).prefix_with("foo", dialect="postgresql"),
            select([table_a.c.a]).prefix_with("bar"),
            select([table_a.c.a]).suffix_with("bar"),
        ),
        lambda: (
            select([table_a_2.c.a]),
            select([table_a_2_fs.c.a]),
            select([table_a_2_bs.c.a]),
        ),
        lambda: (
            select([table_a.c.a]),
            select([table_a.c.a]).with_hint(None, "some hint"),
            select([table_a.c.a]).with_hint(None, "some other hint"),
            select([table_a.c.a]).with_hint(table_a, "some hint"),
            select([table_a.c.a])
            .with_hint(table_a, "some hint")
            .with_hint(None, "some other hint"),
            select([table_a.c.a]).with_hint(table_a, "some other hint"),
            select([table_a.c.a]).with_hint(
                table_a, "some hint", dialect_name="mysql"
            ),
            select([table_a.c.a]).with_hint(
                table_a, "some hint", dialect_name="postgresql"
            ),
        ),
        lambda: (
            table_a.join(table_b, table_a.c.a == table_b.c.a),
            table_a.join(
                table_b, and_(table_a.c.a == table_b.c.a, table_a.c.b == 1)
            ),
            table_a.outerjoin(table_b, table_a.c.a == table_b.c.a),
        ),
        lambda: (
            table_a.alias("a"),
            table_a.alias("b"),
            table_a.alias(),
            table_b.alias("a"),
            select([table_a.c.a]).alias("a"),
        ),
        lambda: (
            FromGrouping(table_a.alias("a")),
            FromGrouping(table_a.alias("b")),
        ),
        lambda: (
            SelectStatementGrouping(select([table_a])),
            SelectStatementGrouping(select([table_b])),
        ),
        lambda: (
            select([table_a.c.a]).scalar_subquery(),
            select([table_a.c.a]).where(table_a.c.b == 5).scalar_subquery(),
        ),
        lambda: (
            exists().where(table_a.c.a == 5),
            exists().where(table_a.c.b == 5),
        ),
        lambda: (
            union(select([table_a.c.a]), select([table_a.c.b])),
            union(select([table_a.c.a]), select([table_a.c.b])).order_by("a"),
            union_all(select([table_a.c.a]), select([table_a.c.b])),
            union(select([table_a.c.a])),
            union(
                select([table_a.c.a]),
                select([table_a.c.b]).where(table_a.c.b > 5),
            ),
        ),
        lambda: (
            table("a", column("x"), column("y")),
            table("a", column("y"), column("x")),
            table("b", column("x"), column("y")),
            table("a", column("x"), column("y"), column("z")),
            table("a", column("x"), column("y", Integer)),
            table("a", column("q"), column("y", Integer)),
        ),
        lambda: (table_a, table_b),
    ]

    dont_compare_values_fixtures = [
        lambda: (
            # note the in_(...) all have different column names becuase
            # otherwise all IN expressions would compare as equivalent
            column("x").in_(random_choices(range(10), k=3)),
            column("y").in_(
                bindparam(
                    "q",
                    random_choices(range(10), k=random.randint(0, 7)),
                    expanding=True,
                )
            ),
            column("z").in_(random_choices(range(10), k=random.randint(0, 7))),
            column("x") == random.randint(1, 10),
        )
    ]

    def _complex_fixtures():
        def one():
            a1 = table_a.alias()
            a2 = table_b_like_a.alias()

            stmt = (
                select([table_a.c.a, a1.c.b, a2.c.b])
                .where(table_a.c.b == a1.c.b)
                .where(a1.c.b == a2.c.b)
                .where(a1.c.a == 5)
            )

            return stmt

        def one_diff():
            a1 = table_b_like_a.alias()
            a2 = table_a.alias()

            stmt = (
                select([table_a.c.a, a1.c.b, a2.c.b])
                .where(table_a.c.b == a1.c.b)
                .where(a1.c.b == a2.c.b)
                .where(a1.c.a == 5)
            )

            return stmt

        def two():
            inner = one().subquery()

            stmt = select([table_b.c.a, inner.c.a, inner.c.b]).select_from(
                table_b.join(inner, table_b.c.b == inner.c.b)
            )

            return stmt

        def three():

            a1 = table_a.alias()
            a2 = table_a.alias()
            ex = exists().where(table_b.c.b == a1.c.a)

            stmt = (
                select([a1.c.a, a2.c.a])
                .select_from(a1.join(a2, a1.c.b == a2.c.b))
                .where(ex)
            )
            return stmt

        return [one(), one_diff(), two(), three()]

    fixtures.append(_complex_fixtures)


class CacheKeyFixture(object):
    def _run_cache_key_fixture(self, fixture, compare_values):
        case_a = fixture()
        case_b = fixture()

        for a, b in itertools.combinations_with_replacement(
            range(len(case_a)), 2
        ):
            if a == b:
                a_key = case_a[a]._generate_cache_key()
                b_key = case_b[b]._generate_cache_key()
                is_not_(a_key, None)
                is_not_(b_key, None)

                eq_(a_key.key, b_key.key)
                eq_(hash(a_key), hash(b_key))

                for a_param, b_param in zip(
                    a_key.bindparams, b_key.bindparams
                ):
                    assert a_param.compare(
                        b_param, compare_values=compare_values
                    )
            else:
                a_key = case_a[a]._generate_cache_key()
                b_key = case_b[b]._generate_cache_key()

                if a_key.key == b_key.key:
                    for a_param, b_param in zip(
                        a_key.bindparams, b_key.bindparams
                    ):
                        if not a_param.compare(
                            b_param, compare_values=compare_values
                        ):
                            break
                    else:
                        # this fails unconditionally since we could not
                        # find bound parameter values that differed.
                        # Usually we intended to get two distinct keys here
                        # so the failure will be more descriptive using the
                        # ne_() assertion.
                        ne_(a_key.key, b_key.key)
                else:
                    ne_(a_key.key, b_key.key)

            # ClauseElement-specific test to ensure the cache key
            # collected all the bound parameters
            if isinstance(case_a[a], ClauseElement) and isinstance(
                case_b[b], ClauseElement
            ):
                assert_a_params = []
                assert_b_params = []
                visitors.traverse_depthfirst(
                    case_a[a], {}, {"bindparam": assert_a_params.append}
                )
                visitors.traverse_depthfirst(
                    case_b[b], {}, {"bindparam": assert_b_params.append}
                )

                # note we're asserting the order of the params as well as
                # if there are dupes or not.  ordering has to be
                # deterministic and matches what a traversal would provide.
                # regular traverse_depthfirst does produce dupes in cases
                # like
                # select([some_alias]).
                #    select_from(join(some_alias, other_table))
                # where a bound parameter is inside of some_alias.  the
                # cache key case is more minimalistic
                eq_(
                    sorted(a_key.bindparams, key=lambda b: b.key),
                    sorted(
                        util.unique_list(assert_a_params), key=lambda b: b.key
                    ),
                )
                eq_(
                    sorted(b_key.bindparams, key=lambda b: b.key),
                    sorted(
                        util.unique_list(assert_b_params), key=lambda b: b.key
                    ),
                )


class CacheKeyTest(CacheKeyFixture, CoreFixtures, fixtures.TestBase):
    def test_cache_key(self):
        for fixtures_, compare_values in [
            (self.fixtures, True),
            (self.dont_compare_values_fixtures, False),
        ]:
            for fixture in fixtures_:
                self._run_cache_key_fixture(fixture, compare_values)

    def test_cache_key_unknown_traverse(self):
        class Foobar1(ClauseElement):
            _traverse_internals = [
                ("key", InternalTraversal.dp_anon_name),
                ("type_", InternalTraversal.dp_unknown_structure),
            ]

            def __init__(self, key, type_):
                self.key = key
                self.type_ = type_

        f1 = Foobar1("foo", String())
        eq_(f1._generate_cache_key(), None)

    def test_cache_key_no_method(self):
        class Foobar1(ClauseElement):
            pass

        class Foobar2(ColumnElement):
            pass

        # the None for cache key will prevent objects
        # which contain these elements from being cached.
        f1 = Foobar1()
        eq_(f1._generate_cache_key(), None)

        f2 = Foobar2()
        eq_(f2._generate_cache_key(), None)

        s1 = select([column("q"), Foobar2()])

        eq_(s1._generate_cache_key(), None)

    def test_get_children_no_method(self):
        class Foobar1(ClauseElement):
            pass

        class Foobar2(ColumnElement):
            pass

        f1 = Foobar1()
        eq_(f1.get_children(), [])

        f2 = Foobar2()
        eq_(f2.get_children(), [])

    def test_copy_internals_no_method(self):
        class Foobar1(ClauseElement):
            pass

        class Foobar2(ColumnElement):
            pass

        f1 = Foobar1()
        f2 = Foobar2()

        f1._copy_internals()
        f2._copy_internals()


class CompareAndCopyTest(CoreFixtures, fixtures.TestBase):
    @classmethod
    def setup_class(cls):
        # TODO: we need to get dialects here somehow, perhaps in test_suite?
        [
            importlib.import_module("sqlalchemy.dialects.%s" % d)
            for d in dialects.__all__
            if not d.startswith("_")
        ]

    def test_all_present(self):
        need = set(
            cls
            for cls in class_hierarchy(ClauseElement)
            if issubclass(cls, (ColumnElement, Selectable))
            and (
                "__init__" in cls.__dict__
                or issubclass(cls, AliasedReturnsRows)
            )
            and not issubclass(cls, (Annotated))
            and "orm" not in cls.__module__
            and "compiler" not in cls.__module__
            and "crud" not in cls.__module__
            and "dialects" not in cls.__module__  # TODO: dialects?
        ).difference({ColumnElement, UnaryExpression})

        for fixture in self.fixtures + self.dont_compare_values_fixtures:
            case_a = fixture()
            for elem in case_a:
                for mro in type(elem).__mro__:
                    need.discard(mro)

        is_false(bool(need), "%d Remaining classes: %r" % (len(need), need))

    def test_compare_labels(self):
        for fixtures_, compare_values in [
            (self.fixtures, True),
            (self.dont_compare_values_fixtures, False),
        ]:
            for fixture in fixtures_:
                case_a = fixture()
                case_b = fixture()

                for a, b in itertools.combinations_with_replacement(
                    range(len(case_a)), 2
                ):
                    if a == b:
                        is_true(
                            case_a[a].compare(
                                case_b[b],
                                compare_annotations=True,
                                compare_values=compare_values,
                            ),
                            "%r != %r" % (case_a[a], case_b[b]),
                        )

                    else:
                        is_false(
                            case_a[a].compare(
                                case_b[b],
                                compare_annotations=True,
                                compare_values=compare_values,
                            ),
                            "%r == %r" % (case_a[a], case_b[b]),
                        )

    def test_compare_col_identity(self):
        stmt1 = (
            select([table_a.c.a, table_b.c.b])
            .where(table_a.c.a == table_b.c.b)
            .alias()
        )
        stmt1_c = (
            select([table_a.c.a, table_b.c.b])
            .where(table_a.c.a == table_b.c.b)
            .alias()
        )

        stmt2 = union(select([table_a]), select([table_b]))

        equivalents = {table_a.c.a: [table_b.c.a]}

        is_false(
            stmt1.compare(stmt2, use_proxies=True, equivalents=equivalents)
        )

        is_true(
            stmt1.compare(stmt1_c, use_proxies=True, equivalents=equivalents)
        )
        is_true(
            (table_a.c.a == table_b.c.b).compare(
                stmt1.c.a == stmt1.c.b,
                use_proxies=True,
                equivalents=equivalents,
            )
        )

    def test_copy_internals(self):
        for fixtures_, compare_values in [
            (self.fixtures, True),
            (self.dont_compare_values_fixtures, False),
        ]:
            for fixture in fixtures_:
                case_a = fixture()
                case_b = fixture()

                assert case_a[0].compare(
                    case_b[0], compare_values=compare_values
                )

                clone = visitors.replacement_traverse(
                    case_a[0], {}, lambda elem: None
                )

                assert clone.compare(case_b[0], compare_values=compare_values)

                stack = [clone]
                seen = {clone}
                found_elements = False
                while stack:
                    obj = stack.pop(0)

                    items = [
                        subelem
                        for key, elem in clone.__dict__.items()
                        if key != "_is_clone_of" and elem is not None
                        for subelem in util.to_list(elem)
                        if (
                            isinstance(subelem, (ColumnElement, ClauseList))
                            and subelem not in seen
                            and not isinstance(subelem, Immutable)
                            and subelem is not case_a[0]
                        )
                    ]
                    stack.extend(items)
                    seen.update(items)

                    if obj is not clone:
                        found_elements = True
                        # ensure the element will not compare as true
                        obj.compare = lambda other, **kw: False
                        obj.__visit_name__ = "dont_match"

                if found_elements:
                    assert not clone.compare(
                        case_b[0], compare_values=compare_values
                    )
                assert case_a[0].compare(
                    case_b[0], compare_values=compare_values
                )


class CompareClausesTest(fixtures.TestBase):
    def test_compare_metadata_tables(self):
        # metadata Table objects cache on their own identity, not their
        # structure.   This is mainly to reduce the size of cache keys
        # as well as reduce computational overhead, as Table objects have
        # very large internal state and they are also generally global
        # objects.

        t1 = Table("a", MetaData(), Column("q", Integer), Column("p", Integer))
        t2 = Table("a", MetaData(), Column("q", Integer), Column("p", Integer))

        ne_(t1._generate_cache_key(), t2._generate_cache_key())

        eq_(t1._generate_cache_key().key, (t1, "_annotations", ()))

    def test_compare_metadata_tables_annotations(self):
        # metadata Table objects cache on their own identity, not their
        # structure.   This is mainly to reduce the size of cache keys
        # as well as reduce computational overhead, as Table objects have
        # very large internal state and they are also generally global
        # objects.

        t1 = Table("a", MetaData(), Column("q", Integer), Column("p", Integer))
        t2 = Table("a", MetaData(), Column("q", Integer), Column("p", Integer))

        t1 = t1._annotate({"orm": True})
        t2 = t2._annotate({"orm": True})

        ne_(t1._generate_cache_key(), t2._generate_cache_key())

        eq_(
            t1._generate_cache_key().key,
            (t1, "_annotations", (("orm", True),)),
        )

    def test_compare_adhoc_tables(self):
        # non-metadata tables compare on their structure.  these objects are
        # not commonly used.

        # note this test is a bit redundant as we have a similar test
        # via the fixtures also
        t1 = table("a", Column("q", Integer), Column("p", Integer))
        t2 = table("a", Column("q", Integer), Column("p", Integer))
        t3 = table("b", Column("q", Integer), Column("p", Integer))
        t4 = table("a", Column("q", Integer), Column("x", Integer))

        eq_(t1._generate_cache_key(), t2._generate_cache_key())

        ne_(t1._generate_cache_key(), t3._generate_cache_key())
        ne_(t1._generate_cache_key(), t4._generate_cache_key())
        ne_(t3._generate_cache_key(), t4._generate_cache_key())

    def test_compare_comparison_associative(self):

        l1 = table_c.c.x == table_d.c.y
        l2 = table_d.c.y == table_c.c.x
        l3 = table_c.c.x == table_d.c.z

        is_true(l1.compare(l1))
        is_true(l1.compare(l2))
        is_false(l1.compare(l3))

    def test_compare_comparison_non_commutative_inverses(self):
        l1 = table_c.c.x >= table_d.c.y
        l2 = table_d.c.y < table_c.c.x
        l3 = table_d.c.y <= table_c.c.x

        # we're not doing this kind of commutativity right now.
        is_false(l1.compare(l2))
        is_false(l1.compare(l3))

    def test_compare_clauselist_associative(self):

        l1 = and_(table_c.c.x == table_d.c.y, table_c.c.y == table_d.c.z)

        l2 = and_(table_c.c.y == table_d.c.z, table_c.c.x == table_d.c.y)

        l3 = and_(table_c.c.x == table_d.c.z, table_c.c.y == table_d.c.y)

        is_true(l1.compare(l1))
        is_true(l1.compare(l2))
        is_false(l1.compare(l3))

    def test_compare_clauselist_not_associative(self):

        l1 = ClauseList(
            table_c.c.x, table_c.c.y, table_d.c.y, operator=operators.sub
        )

        l2 = ClauseList(
            table_d.c.y, table_c.c.x, table_c.c.y, operator=operators.sub
        )

        is_true(l1.compare(l1))
        is_false(l1.compare(l2))

    def test_compare_clauselist_assoc_different_operator(self):

        l1 = and_(table_c.c.x == table_d.c.y, table_c.c.y == table_d.c.z)

        l2 = or_(table_c.c.y == table_d.c.z, table_c.c.x == table_d.c.y)

        is_false(l1.compare(l2))

    def test_compare_clauselist_not_assoc_different_operator(self):

        l1 = ClauseList(
            table_c.c.x, table_c.c.y, table_d.c.y, operator=operators.sub
        )

        l2 = ClauseList(
            table_c.c.x, table_c.c.y, table_d.c.y, operator=operators.div
        )

        is_false(l1.compare(l2))

    def test_compare_labels(self):
        is_true(column("q").label(None).compare(column("q").label(None)))

        is_false(column("q").label("foo").compare(column("q").label(None)))

        is_false(column("q").label(None).compare(column("q").label("foo")))

        is_false(column("q").label("foo").compare(column("q").label("bar")))

        is_true(column("q").label("foo").compare(column("q").label("foo")))

    def test_compare_binds(self):
        b1 = bindparam("foo", type_=Integer())
        b2 = bindparam("foo", type_=Integer())
        b3 = bindparam("foo", type_=String())

        def c1():
            return 5

        def c2():
            return 6

        b4 = bindparam("foo", type_=Integer(), callable_=c1)
        b5 = bindparam("foo", type_=Integer(), callable_=c2)
        b6 = bindparam("foo", type_=Integer(), callable_=c1)

        b7 = bindparam("foo", type_=Integer, value=5)
        b8 = bindparam("foo", type_=Integer, value=6)

        is_false(b1.compare(b4))
        is_true(b4.compare(b6))
        is_false(b4.compare(b5))
        is_true(b1.compare(b2))

        # currently not comparing "key", as we often have to compare
        # anonymous names.  however we should really check for that
        # is_true(b1.compare(b3))

        is_false(b1.compare(b3))
        is_false(b1.compare(b7))
        is_false(b7.compare(b8))
        is_true(b7.compare(b7))

    def test_compare_tables(self):
        is_true(table_a.compare(table_a_2))

        # the "proxy" version compares schema tables on metadata identity
        is_false(table_a.compare(table_a_2, use_proxies=True))

        # same for lower case tables since it compares lower case columns
        # using proxies, which makes it very unlikely to have multiple
        # table() objects with columns that compare equally
        is_false(
            table("a", column("x", Integer), column("q", String)).compare(
                table("a", column("x", Integer), column("q", String)),
                use_proxies=True,
            )
        )

    def test_compare_annotated_clears_mapping(self):
        t = table("t", column("x"), column("y"))
        x_a = t.c.x._annotate({"foo": True})
        x_b = t.c.x._annotate({"foo": True})

        is_true(x_a.compare(x_b, compare_annotations=True))
        is_false(
            x_a.compare(x_b._annotate({"bar": True}), compare_annotations=True)
        )

        s1 = select([t.c.x])._annotate({"foo": True})
        s2 = select([t.c.x])._annotate({"foo": True})

        is_true(s1.compare(s2, compare_annotations=True))

        is_false(
            s1.compare(s2._annotate({"bar": True}), compare_annotations=True)
        )

    def test_compare_annotated_wo_annotations(self):
        t = table("t", column("x"), column("y"))
        x_a = t.c.x._annotate({})
        x_b = t.c.x._annotate({"foo": True})

        is_true(t.c.x.compare(x_a))
        is_true(x_b.compare(x_a))

        is_true(x_a.compare(t.c.x))
        is_false(x_a.compare(t.c.y))
        is_false(t.c.y.compare(x_a))
        is_true((t.c.x == 5).compare(x_a == 5))
        is_false((t.c.y == 5).compare(x_a == 5))

        s = select([t]).subquery()
        x_p = s.c.x
        is_false(x_a.compare(x_p))
        is_false(t.c.x.compare(x_p))
        x_p_a = x_p._annotate({})
        is_true(x_p_a.compare(x_p))
        is_true(x_p.compare(x_p_a))
        is_false(x_p_a.compare(x_a))
