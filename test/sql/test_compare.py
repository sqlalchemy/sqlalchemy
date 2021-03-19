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
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy import union
from sqlalchemy import union_all
from sqlalchemy import util
from sqlalchemy import values
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql import dml
from sqlalchemy.sql import False_
from sqlalchemy.sql import func
from sqlalchemy.sql import operators
from sqlalchemy.sql import roles
from sqlalchemy.sql import True_
from sqlalchemy.sql import type_coerce
from sqlalchemy.sql import visitors
from sqlalchemy.sql.base import HasCacheKey
from sqlalchemy.sql.elements import _label_reference
from sqlalchemy.sql.elements import _textual_label_reference
from sqlalchemy.sql.elements import Annotated
from sqlalchemy.sql.elements import BindParameter
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
from sqlalchemy.sql.lambdas import lambda_stmt
from sqlalchemy.sql.lambdas import LambdaElement
from sqlalchemy.sql.lambdas import LambdaOptions
from sqlalchemy.sql.selectable import _OffsetLimitParam
from sqlalchemy.sql.selectable import AliasedReturnsRows
from sqlalchemy.sql.selectable import FromGrouping
from sqlalchemy.sql.selectable import LABEL_STYLE_NONE
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql.selectable import Select
from sqlalchemy.sql.selectable import Selectable
from sqlalchemy.sql.selectable import SelectStatementGrouping
from sqlalchemy.sql.visitors import InternalTraversal
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing import ne_
from sqlalchemy.testing.util import random_choices
from sqlalchemy.types import ARRAY
from sqlalchemy.types import JSON
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

table_b_b = Table(
    "b_b",
    meta,
    Column("a", Integer),
    Column("b", Integer),
    Column("c", Integer),
    Column("d", Integer),
    Column("e", Integer),
)

table_c = Table("c", meta, Column("x", Integer), Column("y", Integer))

table_d = Table("d", meta, Column("y", Integer), Column("z", Integer))


def opt1(ctx):
    pass


def opt2(ctx):
    pass


def opt3(ctx):
    pass


class MyEntity(HasCacheKey):
    def __init__(self, name, element):
        self.name = name
        self.element = element

    _cache_key_traversal = [
        ("name", InternalTraversal.dp_string),
        ("element", InternalTraversal.dp_clauseelement),
    ]


class Foo:
    x = 10
    y = 15


dml.Insert.argument_for("sqlite", "foo", None)
dml.Update.argument_for("sqlite", "foo", None)
dml.Delete.argument_for("sqlite", "foo", None)


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
            column("q", ARRAY(Integer))[3] == 5,
            column("q", ARRAY(Integer))[3:5] == 5,
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
                {"orm": True, "parententity": MyEntity("b", select(table_a))}
            ),
            table_a.c.a._annotate(
                {
                    "orm": True,
                    "parententity": MyEntity(
                        "b", select(table_a).where(table_a.c.a == 5)
                    ),
                }
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
                {"orm": True, "parententity": MyEntity("b", select(table_a))}
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
            column("x", JSON)["key1"],
            column("x", JSON)["key1"].as_boolean(),
            column("x", JSON)["key1"].as_float(),
            column("x", JSON)["key1"].as_integer(),
            column("x", JSON)["key1"].as_string(),
            column("y", JSON)["key1"].as_integer(),
            column("y", JSON)["key1"].as_string(),
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
        lambda: (
            func.package1.foo(5),
            func.package2.foo(5),
            func.packge1.bar(5),
            func.foo(),
        ),
        lambda: (func.current_date(), func.current_time()),
        lambda: (
            func.next_value(Sequence("q")),
            func.next_value(Sequence("p")),
        ),
        lambda: (
            func.json_to_recordset("{foo}"),
            func.json_to_recordset("{foo}").table_valued("a", "b"),
            func.jsonb_to_recordset("{foo}").table_valued("a", "b"),
            func.json_to_recordset("{foo}")
            .table_valued("a", "b")
            .render_derived(),
            func.json_to_recordset("{foo}")
            .table_valued("a", with_ordinality="b")
            .render_derived(),
            func.json_to_recordset("{foo}")
            .table_valued("a", with_ordinality="c")
            .render_derived(),
            func.json_to_recordset("{foo}")
            .table_valued(column("a", Integer), column("b", String))
            .render_derived(),
            func.json_to_recordset("{foo}")
            .table_valued(column("a", Integer), column("b", String))
            .render_derived(with_types=True),
            func.json_to_recordset("{foo}")
            .table_valued("b", "c")
            .render_derived(),
            func.json_to_recordset("{foo}")
            .table_valued("a", "b")
            .alias("foo")
            .render_derived(with_types=True),
            func.json_to_recordset("{foo}")
            .table_valued("a", "b")
            .alias("foo"),
            func.json_to_recordset("{foo}").column_valued(),
            func.json_to_recordset("{foo}").scalar_table_valued("foo"),
        ),
        lambda: (table_a.table_valued(), table_b.table_valued()),
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
            case((table_a.c.a == 5, 10), (table_a.c.a == 10, 20)),
            case((table_a.c.a == 18, 10), (table_a.c.a == 10, 20)),
            case((table_a.c.a == 5, 10), (table_a.c.b == 10, 20)),
            case(
                (table_a.c.a == 5, 10),
                (table_a.c.b == 10, 20),
                (table_a.c.a == 9, 12),
            ),
            case(
                (table_a.c.a == 5, 10),
                (table_a.c.a == 10, 20),
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
            select(table_a.c.a),
            select(table_a.c.a, table_a.c.b),
            select(table_a.c.b, table_a.c.a),
            select(table_a.c.b, table_a.c.a).limit(5),
            select(table_a.c.b, table_a.c.a).limit(5).offset(10),
            select(table_a.c.b, table_a.c.a)
            .limit(literal_column("foobar"))
            .offset(10),
            select(table_a.c.b, table_a.c.a).set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ),
            select(table_a.c.b, table_a.c.a).set_label_style(LABEL_STYLE_NONE),
            select(table_a.c.a).where(table_a.c.b == 5),
            select(table_a.c.a)
            .where(table_a.c.b == 5)
            .where(table_a.c.a == 10),
            select(table_a.c.a).where(table_a.c.b == 5).with_for_update(),
            select(table_a.c.a)
            .where(table_a.c.b == 5)
            .with_for_update(nowait=True),
            select(table_a.c.a).where(table_a.c.b == 5).correlate(table_b),
            select(table_a.c.a)
            .where(table_a.c.b == 5)
            .correlate_except(table_b),
        ),
        lambda: (
            select(table_a.c.a),
            select(table_a.c.a).limit(2),
            select(table_a.c.a).limit(3),
            select(table_a.c.a).fetch(3),
            select(table_a.c.a).fetch(2),
            select(table_a.c.a).fetch(2, percent=True),
            select(table_a.c.a).fetch(2, with_ties=True),
            select(table_a.c.a).fetch(2, with_ties=True, percent=True),
            select(table_a.c.a).fetch(2).offset(3),
            select(table_a.c.a).fetch(2).offset(5),
            select(table_a.c.a).limit(2).offset(5),
            select(table_a.c.a).limit(2).offset(3),
            select(table_a.c.a).union(select(table_a.c.a)).limit(2).offset(3),
            union(select(table_a.c.a), select(table_a.c.b)).limit(2).offset(3),
            union(select(table_a.c.a), select(table_a.c.b)).limit(6).offset(3),
            union(select(table_a.c.a), select(table_a.c.b)).limit(6).offset(8),
            union(select(table_a.c.a), select(table_a.c.b)).fetch(2).offset(8),
            union(select(table_a.c.a), select(table_a.c.b)).fetch(6).offset(8),
            union(select(table_a.c.a), select(table_a.c.b)).fetch(6).offset(3),
            union(select(table_a.c.a), select(table_a.c.b))
            .fetch(6, percent=True)
            .offset(3),
            union(select(table_a.c.a), select(table_a.c.b))
            .fetch(6, with_ties=True)
            .offset(3),
            union(select(table_a.c.a), select(table_a.c.b))
            .fetch(6, with_ties=True, percent=True)
            .offset(3),
            union(select(table_a.c.a), select(table_a.c.b)).limit(6),
            union(select(table_a.c.a), select(table_a.c.b)).offset(6),
        ),
        lambda: (
            select(table_a.c.a),
            select(table_a.c.a).join(table_b, table_a.c.a == table_b.c.a),
            select(table_a.c.a).join_from(
                table_a, table_b, table_a.c.a == table_b.c.a
            ),
            select(table_a.c.a).join_from(table_a, table_b),
            select(table_a.c.a).join_from(table_c, table_b),
            select(table_a.c.a)
            .join(table_b, table_a.c.a == table_b.c.a)
            .join(table_c, table_b.c.b == table_c.c.x),
            select(table_a.c.a).join(table_b),
            select(table_a.c.a).join(table_c),
            select(table_a.c.a).join(table_b, table_a.c.a == table_b.c.b),
            select(table_a.c.a).join(table_c, table_a.c.a == table_c.c.x),
        ),
        lambda: (
            select(table_a.c.a).cte(),
            select(table_a.c.a).cte(recursive=True),
            select(table_a.c.a).cte(name="some_cte", recursive=True),
            select(table_a.c.a).cte(name="some_cte"),
            select(table_a.c.a).cte(name="some_cte").alias("other_cte"),
            select(table_a.c.a)
            .cte(name="some_cte")
            .union_all(select(table_a.c.a)),
            select(table_a.c.a)
            .cte(name="some_cte")
            .union_all(select(table_a.c.b)),
            select(table_a.c.a).lateral(),
            select(table_a.c.a).lateral(name="bar"),
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
            table_a.insert(),
            table_a.insert().values({})._annotate({"nocache": True}),
            table_b.insert(),
            table_b.insert().with_dialect_options(sqlite_foo="some value"),
            table_b.insert().from_select(["a", "b"], select(table_a)),
            table_b.insert().from_select(
                ["a", "b"], select(table_a).where(table_a.c.a > 5)
            ),
            table_b.insert().from_select(["a", "b"], select(table_b)),
            table_b.insert().from_select(["c", "d"], select(table_a)),
            table_b.insert().returning(table_b.c.a),
            table_b.insert().returning(table_b.c.a, table_b.c.b),
            table_b.insert().inline(),
            table_b.insert().prefix_with("foo"),
            table_b.insert().with_hint("RUNFAST"),
            table_b.insert().values(a=5, b=10),
            table_b.insert().values(a=5),
            table_b.insert()
            .values({table_b.c.a: 5, "b": 10})
            ._annotate({"nocache": True}),
            table_b.insert().values(a=7, b=10),
            table_b.insert().values(a=5, b=10).inline(),
            table_b.insert()
            .values([{"a": 5, "b": 10}, {"a": 8, "b": 12}])
            ._annotate({"nocache": True}),
            table_b.insert()
            .values([{"a": 9, "b": 10}, {"a": 8, "b": 7}])
            ._annotate({"nocache": True}),
            table_b.insert()
            .values([(5, 10), (8, 12)])
            ._annotate({"nocache": True}),
            table_b.insert()
            .values([(5, 9), (5, 12)])
            ._annotate({"nocache": True}),
        ),
        lambda: (
            table_b.update(),
            table_b.update().where(table_b.c.a == 5),
            table_b.update().where(table_b.c.b == 5),
            table_b.update()
            .where(table_b.c.b == 5)
            .with_dialect_options(mysql_limit=10),
            table_b.update()
            .where(table_b.c.b == 5)
            .with_dialect_options(mysql_limit=10, sqlite_foo="some value"),
            table_b.update().where(table_b.c.a == 5).values(a=5, b=10),
            table_b.update().where(table_b.c.a == 5).values(a=5, b=10, c=12),
            table_b.update()
            .where(table_b.c.b == 5)
            .values(a=5, b=10)
            ._annotate({"nocache": True}),
            table_b.update().values(a=5, b=10),
            table_b.update()
            .values({"a": 5, table_b.c.b: 10})
            ._annotate({"nocache": True}),
            table_b.update().values(a=7, b=10),
            table_b.update().ordered_values(("a", 5), ("b", 10)),
            table_b.update().ordered_values(("b", 10), ("a", 5)),
            table_b.update().ordered_values((table_b.c.a, 5), ("b", 10)),
        ),
        lambda: (
            table_b.delete(),
            table_b.delete().with_dialect_options(sqlite_foo="some value"),
            table_b.delete().where(table_b.c.a == 5),
            table_b.delete().where(table_b.c.b == 5),
        ),
        lambda: (
            values(
                column("mykey", Integer),
                column("mytext", String),
                column("myint", Integer),
                name="myvalues",
            )
            .data([(1, "textA", 99), (2, "textB", 88)])
            ._annotate({"nocache": True}),
            values(
                column("mykey", Integer),
                column("mytext", String),
                column("myint", Integer),
                name="myothervalues",
            )
            .data([(1, "textA", 99), (2, "textB", 88)])
            ._annotate({"nocache": True}),
            values(
                column("mykey", Integer),
                column("mytext", String),
                column("myint", Integer),
                name="myvalues",
            )
            .data([(1, "textA", 89), (2, "textG", 88)])
            ._annotate({"nocache": True}),
            values(
                column("mykey", Integer),
                column("mynottext", String),
                column("myint", Integer),
                name="myvalues",
            )
            .data([(1, "textA", 99), (2, "textB", 88)])
            ._annotate({"nocache": True}),
            # TODO: difference in type
            # values(
            #    [
            #        column("mykey", Integer),
            #        column("mytext", Text),
            #        column("myint", Integer),
            #    ],
            #    (1, "textA", 99),
            #    (2, "textB", 88),
            #    alias_name="myvalues",
            # ),
        ),
        lambda: (
            select(table_a.c.a),
            select(table_a.c.a).prefix_with("foo"),
            select(table_a.c.a).prefix_with("foo", dialect="mysql"),
            select(table_a.c.a).prefix_with("foo", dialect="postgresql"),
            select(table_a.c.a).prefix_with("bar"),
            select(table_a.c.a).suffix_with("bar"),
        ),
        lambda: (
            select(table_a_2.c.a),
            select(table_a_2_fs.c.a),
            select(table_a_2_bs.c.a),
        ),
        lambda: (
            select(table_a.c.a),
            select(table_a.c.a).with_hint(None, "some hint"),
            select(table_a.c.a).with_hint(None, "some other hint"),
            select(table_a.c.a).with_hint(table_a, "some hint"),
            select(table_a.c.a)
            .with_hint(table_a, "some hint")
            .with_hint(None, "some other hint"),
            select(table_a.c.a).with_hint(table_a, "some other hint"),
            select(table_a.c.a).with_hint(
                table_a, "some hint", dialect_name="mysql"
            ),
            select(table_a.c.a).with_hint(
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
            select(table_a.c.a).alias("a"),
        ),
        lambda: (
            FromGrouping(table_a.alias("a")),
            FromGrouping(table_a.alias("b")),
        ),
        lambda: (
            SelectStatementGrouping(select(table_a)),
            SelectStatementGrouping(select(table_b)),
        ),
        lambda: (
            select(table_a.c.a).scalar_subquery(),
            select(table_a.c.a).where(table_a.c.b == 5).scalar_subquery(),
        ),
        lambda: (
            exists().where(table_a.c.a == 5),
            exists().where(table_a.c.b == 5),
        ),
        lambda: (
            union(select(table_a.c.a), select(table_a.c.b)),
            union(select(table_a.c.a), select(table_a.c.b)).order_by("a"),
            union_all(select(table_a.c.a), select(table_a.c.b)),
            union(select(table_a.c.a)),
            union(
                select(table_a.c.a),
                select(table_a.c.b).where(table_a.c.b > 5),
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
            # note the in_(...) all have different column names because
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
                select(table_a.c.a, a1.c.b, a2.c.b)
                .where(table_a.c.b == a1.c.b)
                .where(a1.c.b == a2.c.b)
                .where(a1.c.a == 5)
            )

            return stmt

        def one_diff():
            a1 = table_b_like_a.alias()
            a2 = table_a.alias()

            stmt = (
                select(table_a.c.a, a1.c.b, a2.c.b)
                .where(table_a.c.b == a1.c.b)
                .where(a1.c.b == a2.c.b)
                .where(a1.c.a == 5)
            )

            return stmt

        def two():
            inner = one().subquery()

            stmt = select(table_b.c.a, inner.c.a, inner.c.b).select_from(
                table_b.join(inner, table_b.c.b == inner.c.b)
            )

            return stmt

        def three():

            a1 = table_a.alias()
            a2 = table_a.alias()
            ex = exists().where(table_b.c.b == a1.c.a)

            stmt = (
                select(a1.c.a, a2.c.a)
                .select_from(a1.join(a2, a1.c.b == a2.c.b))
                .where(ex)
            )
            return stmt

        return [one(), one_diff(), two(), three()]

    fixtures.append(_complex_fixtures)

    def _statements_w_context_options_fixtures():

        return [
            select(table_a)._add_context_option(opt1, True),
            select(table_a)._add_context_option(opt1, 5),
            select(table_a)
            ._add_context_option(opt1, True)
            ._add_context_option(opt2, True),
            select(table_a)
            ._add_context_option(opt1, True)
            ._add_context_option(opt2, 5),
            select(table_a)._add_context_option(opt3, True),
        ]

    fixtures.append(_statements_w_context_options_fixtures)

    def _statements_w_anonymous_col_names():
        def one():
            c = column("q")

            l = c.label(None)

            # new case as of Id810f485c5f7ed971529489b84694e02a3356d6d
            subq = select(l).subquery()

            # this creates a ColumnClause as a proxy to the Label() that has
            # an anoymous name, so the column has one too.
            anon_col = subq.c[0]

            # then when BindParameter is created, it checks the label
            # and doesn't double up on the anonymous name which is uncachable
            return anon_col > 5

        def two():
            c = column("p")

            l = c.label(None)

            # new case as of Id810f485c5f7ed971529489b84694e02a3356d6d
            subq = select(l).subquery()

            # this creates a ColumnClause as a proxy to the Label() that has
            # an anoymous name, so the column has one too.
            anon_col = subq.c[0]

            # then when BindParameter is created, it checks the label
            # and doesn't double up on the anonymous name which is uncachable
            return anon_col > 5

        def three():

            l1, l2 = table_a.c.a.label(None), table_a.c.b.label(None)

            stmt = select(table_a.c.a, table_a.c.b, l1, l2)

            subq = stmt.subquery()
            return select(subq).where(subq.c[2] == 10)

        return (
            one(),
            two(),
            three(),
        )

    fixtures.append(_statements_w_anonymous_col_names)

    def _update_dml_w_dicts():
        return (
            table_b_b.update().values(
                {
                    table_b_b.c.a: 5,
                    table_b_b.c.b: 5,
                    table_b_b.c.c: 5,
                    table_b_b.c.d: 5,
                }
            ),
            # equivalent, but testing dictionary insert ordering as cache key
            # / compare
            table_b_b.update().values(
                {
                    table_b_b.c.a: 5,
                    table_b_b.c.c: 5,
                    table_b_b.c.b: 5,
                    table_b_b.c.d: 5,
                }
            ),
            table_b_b.update().values(
                {table_b_b.c.a: 5, table_b_b.c.b: 5, "c": 5, table_b_b.c.d: 5}
            ),
            table_b_b.update().values(
                {
                    table_b_b.c.a: 5,
                    table_b_b.c.b: 5,
                    table_b_b.c.c: 5,
                    table_b_b.c.d: 5,
                    table_b_b.c.e: 10,
                }
            ),
            table_b_b.update()
            .values(
                {
                    table_b_b.c.a: 5,
                    table_b_b.c.b: 5,
                    table_b_b.c.c: 5,
                    table_b_b.c.d: 5,
                    table_b_b.c.e: 10,
                }
            )
            .where(table_b_b.c.c > 10),
        )

    if util.py37:
        fixtures.append(_update_dml_w_dicts)

    def _lambda_fixtures():
        def one():
            return LambdaElement(
                lambda: table_a.c.a == column("q"), roles.WhereHavingRole
            )

        def two():
            r = random.randint(1, 10)
            q = 408
            return LambdaElement(
                lambda: table_a.c.a + q == r, roles.WhereHavingRole
            )

        some_value = random.randint(20, 30)

        def three(y):
            return LambdaElement(
                lambda: and_(table_a.c.a == some_value, table_a.c.b > y),
                roles.WhereHavingRole,
            )

        def four():
            return LambdaElement(
                lambda: and_(table_a.c.a == Foo.x), roles.WhereHavingRole
            )

        def five():
            return LambdaElement(
                lambda: and_(table_a.c.a == Foo.x, table_a.c.b == Foo.y),
                roles.WhereHavingRole,
            )

        def six():
            d = {"g": random.randint(40, 45)}

            return LambdaElement(
                lambda: and_(table_a.c.b == d["g"]),
                roles.WhereHavingRole,
                opts=LambdaOptions(track_closure_variables=False),
            )

        def seven():
            # lambda statements don't collect bindparameter objects
            # for fixed values, has to be in a variable
            value = random.randint(10, 20)
            return lambda_stmt(lambda: select(table_a)) + (
                lambda s: s.where(table_a.c.a == value)
            )

        from sqlalchemy.sql import lambdas

        def eight():
            q = 5
            return lambdas.DeferredLambdaElement(
                lambda t: t.c.a > q,
                roles.WhereHavingRole,
                lambda_args=(table_a,),
            )

        return [
            one(),
            two(),
            three(random.randint(5, 10)),
            four(),
            five(),
            six(),
            seven(),
            eight(),
        ]

    dont_compare_values_fixtures.append(_lambda_fixtures)

    # like fixture but returns at least two objects that compare equally
    equal_fixtures = [
        lambda: (
            select(table_a.c.a).fetch(3),
            select(table_a.c.a).fetch(2).fetch(3),
            select(table_a.c.a).fetch(3, percent=False, with_ties=False),
            select(table_a.c.a).limit(2).fetch(3),
            select(table_a.c.a).slice(2, 4).fetch(3).offset(None),
        ),
        lambda: (
            select(table_a.c.a).limit(3),
            select(table_a.c.a).fetch(2).limit(3),
            select(table_a.c.a).fetch(2).slice(0, 3).offset(None),
        ),
    ]


class CacheKeyFixture(object):
    def _compare_equal(self, a, b, compare_values):
        a_key = a._generate_cache_key()
        b_key = b._generate_cache_key()

        if a_key is None:
            assert a._annotations.get("nocache")

            assert b_key is None
        else:

            eq_(a_key.key, b_key.key)
            eq_(hash(a_key.key), hash(b_key.key))

            for a_param, b_param in zip(a_key.bindparams, b_key.bindparams):
                assert a_param.compare(b_param, compare_values=compare_values)
        return a_key, b_key

    def _run_cache_key_fixture(self, fixture, compare_values):
        case_a = fixture()
        case_b = fixture()

        for a, b in itertools.combinations_with_replacement(
            range(len(case_a)), 2
        ):
            if a == b:
                a_key, b_key = self._compare_equal(
                    case_a[a], case_b[b], compare_values
                )
                if a_key is None:
                    continue
            else:
                a_key = case_a[a]._generate_cache_key()
                b_key = case_b[b]._generate_cache_key()

                if a_key is None or b_key is None:
                    if a_key is None:
                        assert case_a[a]._annotations.get("nocache")
                    if b_key is None:
                        assert case_b[b]._annotations.get("nocache")
                    continue

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
            # collected all the bound parameters that aren't marked
            # as "literal execute"
            if isinstance(case_a[a], ClauseElement) and isinstance(
                case_b[b], ClauseElement
            ):
                assert_a_params = []
                assert_b_params = []

                for elem in visitors.iterate(case_a[a]):
                    if elem.__visit_name__ == "bindparam":
                        assert_a_params.append(elem)

                for elem in visitors.iterate(case_b[b]):
                    if elem.__visit_name__ == "bindparam":
                        assert_b_params.append(elem)

                # note we're asserting the order of the params as well as
                # if there are dupes or not.  ordering has to be
                # deterministic and matches what a traversal would provide.
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

    def _run_cache_key_equal_fixture(self, fixture, compare_values):
        case_a = fixture()
        case_b = fixture()

        for a, b in itertools.combinations_with_replacement(
            range(len(case_a)), 2
        ):
            self._compare_equal(case_a[a], case_b[b], compare_values)


class CacheKeyTest(CacheKeyFixture, CoreFixtures, fixtures.TestBase):
    # we are slightly breaking the policy of not having external dialect
    # stuff in here, but use pg/mysql as test cases to ensure that these
    # objects don't report an inaccurate cache key, which is dependent
    # on the base insert sending out _post_values_clause and the caching
    # system properly recognizing these constructs as not cacheable

    @testing.combinations(
        postgresql.insert(table_a).on_conflict_do_update(
            index_elements=[table_a.c.a], set_={"name": "foo"}
        ),
        mysql.insert(table_a).on_duplicate_key_update(updated_once=None),
        table_a.insert().values(  # multivalues doesn't cache
            [
                {"name": "some name"},
                {"name": "some other name"},
                {"name": "yet another name"},
            ]
        ),
    )
    def test_dml_not_cached_yet(self, dml_stmt):
        eq_(dml_stmt._generate_cache_key(), None)

    def test_values_doesnt_caches_right_now(self):
        v1 = values(
            column("mykey", Integer),
            column("mytext", String),
            column("myint", Integer),
            name="myvalues",
        ).data([(1, "textA", 99), (2, "textB", 88)])

        is_(v1._generate_cache_key(), None)

        large_v1 = values(
            column("mykey", Integer),
            column("mytext", String),
            column("myint", Integer),
            name="myvalues",
        ).data([(i, "data %s" % i, i * 5) for i in range(500)])

        is_(large_v1._generate_cache_key(), None)

    def test_cache_key(self):
        for fixtures_, compare_values in [
            (self.fixtures, True),
            (self.dont_compare_values_fixtures, False),
        ]:
            for fixture in fixtures_:
                self._run_cache_key_fixture(fixture, compare_values)

    def test_cache_key_equal(self):
        for fixture in self.equal_fixtures:
            self._run_cache_key_equal_fixture(fixture, True)

    def test_literal_binds(self):
        def fixture():
            return (
                bindparam(None, value="x", literal_execute=True),
                bindparam(None, value="y", literal_execute=True),
            )

        self._run_cache_key_fixture(
            fixture,
            True,
        )

    def test_bindparam_subclass_nocache(self):
        # does not implement inherit_cache
        class _literal_bindparam(BindParameter):
            pass

        l1 = _literal_bindparam(None, value="x1")
        is_(l1._generate_cache_key(), None)

    def test_bindparam_subclass_ok_cache(self):
        # implements inherit_cache
        class _literal_bindparam(BindParameter):
            inherit_cache = True

        def fixture():
            return (
                _literal_bindparam(None, value="x1"),
                _literal_bindparam(None, value="x2"),
                _literal_bindparam(None),
            )

        self._run_cache_key_fixture(fixture, True)

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

        s1 = select(column("q"), Foobar2())

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

    def test_generative_cache_key_regen(self):
        t1 = table("t1", column("a"), column("b"))

        s1 = select(t1)

        ck1 = s1._generate_cache_key()

        s2 = s1.where(t1.c.a == 5)

        ck2 = s2._generate_cache_key()

        ne_(ck1, ck2)
        is_not(ck1, None)
        is_not(ck2, None)

    def test_generative_cache_key_regen_w_del(self):
        t1 = table("t1", column("a"), column("b"))

        s1 = select(t1)

        ck1 = s1._generate_cache_key()

        s2 = s1.where(t1.c.a == 5)

        del s1

        # there is now a good chance that id(s3) == id(s1), make sure
        # cache key is regenerated

        s3 = s2.order_by(t1.c.b)

        ck3 = s3._generate_cache_key()

        ne_(ck1, ck3)
        is_not(ck1, None)
        is_not(ck3, None)


class CompareAndCopyTest(CoreFixtures, fixtures.TestBase):
    @classmethod
    def setup_test_class(cls):
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
            if issubclass(cls, (ColumnElement, Selectable, LambdaElement))
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
            select(table_a.c.a, table_b.c.b)
            .where(table_a.c.a == table_b.c.b)
            .alias()
        )
        stmt1_c = (
            select(table_a.c.a, table_b.c.b)
            .where(table_a.c.a == table_b.c.b)
            .alias()
        )

        stmt2 = union(select(table_a), select(table_b))

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

                for idx in range(len(case_a)):
                    assert case_a[idx].compare(
                        case_b[idx], compare_values=compare_values
                    )

                    clone = visitors.replacement_traverse(
                        case_a[idx], {}, lambda elem: None
                    )

                    assert clone.compare(
                        case_b[idx], compare_values=compare_values
                    )

                    assert case_a[idx].compare(
                        case_b[idx], compare_values=compare_values
                    )

                    # copy internals of Select is very different than other
                    # elements and additionally this is extremely well tested
                    # in test_selectable and test_external_traversal, so
                    # skip these
                    if isinstance(case_a[idx], Select):
                        continue

                    for elema, elemb in zip(
                        visitors.iterate(case_a[idx], {}),
                        visitors.iterate(clone, {}),
                    ):
                        if isinstance(elema, ClauseElement) and not isinstance(
                            elema, Immutable
                        ):
                            assert elema is not elemb


class CompareClausesTest(fixtures.TestBase):
    def test_compare_metadata_tables_annotations_one(self):
        # test that cache keys from annotated version of tables refresh
        # properly

        t1 = Table("a", MetaData(), Column("q", Integer), Column("p", Integer))
        t2 = Table("a", MetaData(), Column("q", Integer), Column("p", Integer))

        ne_(t1._generate_cache_key(), t2._generate_cache_key())

        eq_(t1._generate_cache_key().key, (t1,))

        t2 = t1._annotate({"foo": "bar"})
        eq_(
            t2._generate_cache_key().key,
            (t1, "_annotations", (("foo", "bar"),)),
        )
        eq_(
            t2._annotate({"bat": "bar"})._generate_cache_key().key,
            (t1, "_annotations", (("bat", "bar"), ("foo", "bar"))),
        )

    def test_compare_metadata_tables_annotations_two(self):

        t1 = Table("a", MetaData(), Column("q", Integer), Column("p", Integer))
        t2 = Table("a", MetaData(), Column("q", Integer), Column("p", Integer))

        eq_(t2._generate_cache_key().key, (t2,))

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

    def test_cache_key_limit_offset_values(self):
        s1 = select(column("q")).limit(10)
        s2 = select(column("q")).limit(25)
        s3 = select(column("q")).limit(25).offset(5)
        s4 = select(column("q")).limit(25).offset(18)
        s5 = select(column("q")).limit(7).offset(12)
        s6 = select(column("q")).limit(literal_column("q")).offset(12)

        for should_eq_left, should_eq_right in [(s1, s2), (s3, s4), (s3, s5)]:
            eq_(
                should_eq_left._generate_cache_key().key,
                should_eq_right._generate_cache_key().key,
            )

        for shouldnt_eq_left, shouldnt_eq_right in [
            (s1, s3),
            (s5, s6),
            (s2, s3),
        ]:
            ne_(
                shouldnt_eq_left._generate_cache_key().key,
                shouldnt_eq_right._generate_cache_key().key,
            )

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

        s1 = select(t.c.x)._annotate({"foo": True})
        s2 = select(t.c.x)._annotate({"foo": True})

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

        s = select(t).subquery()
        x_p = s.c.x
        is_false(x_a.compare(x_p))
        is_false(t.c.x.compare(x_p))
        x_p_a = x_p._annotate({})
        is_true(x_p_a.compare(x_p))
        is_true(x_p.compare(x_p_a))
        is_false(x_p_a.compare(x_a))


class ExecutableFlagsTest(fixtures.TestBase):
    @testing.combinations(
        (select(column("a")),),
        (table("q", column("a")).insert(),),
        (table("q", column("a")).update(),),
        (table("q", column("a")).delete(),),
        (lambda_stmt(lambda: select(column("a"))),),
    )
    def test_is_select(self, case):
        if isinstance(case, LambdaElement):
            resolved_case = case._resolved
        else:
            resolved_case = case

        if isinstance(resolved_case, Select):
            is_true(case.is_select)
        else:
            is_false(case.is_select)
