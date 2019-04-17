import importlib
import itertools

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
from sqlalchemy.sql.selectable import FromGrouping
from sqlalchemy.sql.selectable import Selectable
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import ne_
from sqlalchemy.util import class_hierarchy


meta = MetaData()
meta2 = MetaData()

table_a = Table("a", meta, Column("a", Integer), Column("b", String))
table_a_2 = Table("a", meta2, Column("a", Integer), Column("b", String))

table_b = Table("b", meta, Column("a", Integer), Column("b", Integer))

table_c = Table("c", meta, Column("x", Integer), Column("y", Integer))

table_d = Table("d", meta, Column("y", Integer), Column("z", Integer))


class CompareAndCopyTest(fixtures.TestBase):

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
        ),
        lambda: (
            column("q") == column("x"),
            column("q") == column("y"),
            column("z") == column("x"),
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
        lambda: (tuple_([1, 2]), tuple_([3, 4])),
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
            select([table_a.c.a]).as_scalar(),
            select([table_a.c.a]).where(table_a.c.b == 5).as_scalar(),
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
        lambda: (
            Table("a", MetaData(), Column("q", Integer), Column("b", String)),
            Table("b", MetaData(), Column("q", Integer), Column("b", String)),
        ),
    ]

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
            and "__init__" in cls.__dict__
            and not issubclass(cls, (Annotated))
            and "orm" not in cls.__module__
            and "crud" not in cls.__module__
            and "dialects" not in cls.__module__  # TODO: dialects?
        ).difference({ColumnElement, UnaryExpression})
        for fixture in self.fixtures:
            case_a = fixture()
            for elem in case_a:
                for mro in type(elem).__mro__:
                    need.discard(mro)

        is_false(bool(need), "%d Remaining classes: %r" % (len(need), need))

    def test_compare(self):
        for fixture in self.fixtures:
            case_a = fixture()
            case_b = fixture()

            for a, b in itertools.combinations_with_replacement(
                range(len(case_a)), 2
            ):
                if a == b:
                    is_true(
                        case_a[a].compare(
                            case_b[b], arbitrary_expression=True
                        ),
                        "%r != %r" % (case_a[a], case_b[b]),
                    )

                else:
                    is_false(
                        case_a[a].compare(
                            case_b[b], arbitrary_expression=True
                        ),
                        "%r == %r" % (case_a[a], case_b[b]),
                    )

    def test_cache_key(self):
        def assert_params_append(assert_params):
            def append(param):
                if param._value_required_for_cache:
                    assert_params.append(param)
                else:
                    is_(param.value, None)

            return append

        for fixture in self.fixtures:
            case_a = fixture()
            case_b = fixture()

            for a, b in itertools.combinations_with_replacement(
                range(len(case_a)), 2
            ):

                assert_a_params = []
                assert_b_params = []

                visitors.traverse_depthfirst(
                    case_a[a],
                    {},
                    {"bindparam": assert_params_append(assert_a_params)},
                )
                visitors.traverse_depthfirst(
                    case_b[b],
                    {},
                    {"bindparam": assert_params_append(assert_b_params)},
                )
                if assert_a_params:
                    assert_raises_message(
                        NotImplementedError,
                        "bindparams collection argument required ",
                        case_a[a]._cache_key,
                    )
                if assert_b_params:
                    assert_raises_message(
                        NotImplementedError,
                        "bindparams collection argument required ",
                        case_b[b]._cache_key,
                    )

                if not assert_a_params and not assert_b_params:
                    if a == b:
                        eq_(case_a[a]._cache_key(), case_b[b]._cache_key())
                    else:
                        ne_(case_a[a]._cache_key(), case_b[b]._cache_key())

    def test_cache_key_gather_bindparams(self):
        for fixture in self.fixtures:
            case_a = fixture()
            case_b = fixture()

            # in the "bindparams" case, the cache keys for bound parameters
            # with only different values will be the same, but the params
            # themselves are gathered into a collection.
            for a, b in itertools.combinations_with_replacement(
                range(len(case_a)), 2
            ):
                a_params = {"bindparams": []}
                b_params = {"bindparams": []}
                if a == b:
                    a_key = case_a[a]._cache_key(**a_params)
                    b_key = case_b[b]._cache_key(**b_params)
                    eq_(a_key, b_key)

                    if a_params["bindparams"]:
                        for a_param, b_param in zip(
                            a_params["bindparams"], b_params["bindparams"]
                        ):
                            assert a_param.compare(b_param)
                else:
                    a_key = case_a[a]._cache_key(**a_params)
                    b_key = case_b[b]._cache_key(**b_params)

                    if a_key == b_key:
                        for a_param, b_param in zip(
                            a_params["bindparams"], b_params["bindparams"]
                        ):
                            if not a_param.compare(b_param):
                                break
                        else:
                            assert False, "Bound parameters are all the same"
                    else:
                        ne_(a_key, b_key)

                assert_a_params = []
                assert_b_params = []
                visitors.traverse_depthfirst(
                    case_a[a], {}, {"bindparam": assert_a_params.append}
                )
                visitors.traverse_depthfirst(
                    case_b[b], {}, {"bindparam": assert_b_params.append}
                )

                # note we're asserting the order of the params as well as
                # if there are dupes or not.  ordering has to be deterministic
                # and matches what a traversal would provide.
                eq_(a_params["bindparams"], assert_a_params)
                eq_(b_params["bindparams"], assert_b_params)

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

        stmt3 = select([table_b])

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
        for fixture in self.fixtures:
            case_a = fixture()
            case_b = fixture()

            assert case_a[0].compare(case_b[0])

            clone = case_a[0]._clone()
            clone._copy_internals()

            assert clone.compare(case_b[0])

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
                assert not clone.compare(case_b[0])
            assert case_a[0].compare(case_b[0])


class CompareClausesTest(fixtures.TestBase):
    def test_compare_comparison_associative(self):

        l1 = table_c.c.x == table_d.c.y
        l2 = table_d.c.y == table_c.c.x
        l3 = table_c.c.x == table_d.c.z

        is_true(l1.compare(l1))
        is_true(l1.compare(l2))
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

    def test_compare_binds(self):
        b1 = bindparam("foo", type_=Integer())
        b2 = bindparam("foo", type_=Integer())
        b3 = bindparam("bar", type_=Integer())
        b4 = bindparam("foo", type_=String())

        def c1():
            return 5

        def c2():
            return 6

        b5 = bindparam("foo", type_=Integer(), callable_=c1)
        b6 = bindparam("foo", type_=Integer(), callable_=c2)
        b7 = bindparam("foo", type_=Integer(), callable_=c1)

        b8 = bindparam("foo", type_=Integer, value=5)
        b9 = bindparam("foo", type_=Integer, value=6)

        is_false(b1.compare(b5))
        is_true(b5.compare(b7))
        is_false(b5.compare(b6))
        is_true(b1.compare(b2))

        # currently not comparing "key", as we often have to compare
        # anonymous names.  however we should really check for that
        # is_true(b1.compare(b3))

        is_false(b1.compare(b4))
        is_false(b1.compare(b8))
        is_false(b8.compare(b9))
        is_true(b8.compare(b8))

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
