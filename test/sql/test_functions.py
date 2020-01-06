from copy import deepcopy
import datetime
import decimal

from sqlalchemy import ARRAY
from sqlalchemy import bindparam
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import extract
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import oracle
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.sql import column
from sqlalchemy.sql import functions
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql import table
from sqlalchemy.sql.compiler import BIND_TEMPLATES
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertions import expect_warnings
from sqlalchemy.testing.engines import all_dialects


table1 = table(
    "mytable",
    column("myid", Integer),
    column("name", String),
    column("description", String),
)


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def setup(self):
        self._registry = deepcopy(functions._registry)
        self._case_sensitive_registry = deepcopy(
            functions._case_sensitive_registry
        )

    def teardown(self):
        functions._registry = self._registry
        functions._case_sensitive_registry = self._case_sensitive_registry

    def test_compile(self):
        for dialect in all_dialects(exclude=("sybase",)):
            bindtemplate = BIND_TEMPLATES[dialect.paramstyle]
            self.assert_compile(
                func.current_timestamp(), "CURRENT_TIMESTAMP", dialect=dialect
            )
            self.assert_compile(func.localtime(), "LOCALTIME", dialect=dialect)
            if dialect.name in ("firebird",):
                self.assert_compile(
                    func.nosuchfunction(), "nosuchfunction", dialect=dialect
                )
            else:
                self.assert_compile(
                    func.nosuchfunction(), "nosuchfunction()", dialect=dialect
                )

            # test generic function compile
            class fake_func(GenericFunction):
                __return_type__ = sqltypes.Integer

                def __init__(self, arg, **kwargs):
                    GenericFunction.__init__(self, arg, **kwargs)

            self.assert_compile(
                fake_func("foo"),
                "fake_func(%s)"
                % bindtemplate
                % {"name": "fake_func_1", "position": 1},
                dialect=dialect,
            )

            functions._registry["_default"].pop("fake_func")
            functions._case_sensitive_registry["_default"].pop("fake_func")

    def test_use_labels(self):
        self.assert_compile(
            select([func.foo()], use_labels=True), "SELECT foo() AS foo_1"
        )

    def test_underscores(self):
        self.assert_compile(func.if_(), "if()")

    def test_underscores_packages(self):
        self.assert_compile(func.foo_.bar_.if_(), "foo.bar.if()")

    def test_uppercase(self):
        # for now, we need to keep case insensitivity
        self.assert_compile(func.UNREGISTERED_FN(), "UNREGISTERED_FN()")

    def test_uppercase_packages(self):
        # for now, we need to keep case insensitivity
        self.assert_compile(func.FOO.BAR.NOW(), "FOO.BAR.NOW()")

    def test_mixed_case(self):
        # for now, we need to keep case insensitivity
        self.assert_compile(func.SomeFunction(), "SomeFunction()")

    def test_mixed_case_packages(self):
        # for now, we need to keep case insensitivity
        self.assert_compile(
            func.Foo.Bar.SomeFunction(), "Foo.Bar.SomeFunction()"
        )

    def test_quote_special_chars(self):
        # however we need to be quoting any other identifiers
        self.assert_compile(
            getattr(func, "im a function")(), '"im a function"()'
        )

    def test_quote_special_chars_packages(self):
        # however we need to be quoting any other identifiers
        self.assert_compile(
            getattr(
                getattr(getattr(func, "im foo package"), "im bar package"),
                "im a function",
            )(),
            '"im foo package"."im bar package"."im a function"()',
        )

    def test_generic_now(self):
        assert isinstance(func.now().type, sqltypes.DateTime)

        for ret, dialect in [
            ("CURRENT_TIMESTAMP", sqlite.dialect()),
            ("now()", postgresql.dialect()),
            ("now()", mysql.dialect()),
            ("CURRENT_TIMESTAMP", oracle.dialect()),
        ]:
            self.assert_compile(func.now(), ret, dialect=dialect)

    def test_generic_random(self):
        assert func.random().type == sqltypes.NULLTYPE
        assert isinstance(func.random(type_=Integer).type, Integer)

        for ret, dialect in [
            ("random()", sqlite.dialect()),
            ("random()", postgresql.dialect()),
            ("rand()", mysql.dialect()),
            ("random()", oracle.dialect()),
        ]:
            self.assert_compile(func.random(), ret, dialect=dialect)

    def test_cube_operators(self):

        t = table(
            "t",
            column("value"),
            column("x"),
            column("y"),
            column("z"),
            column("q"),
        )

        stmt = select([func.sum(t.c.value)])

        self.assert_compile(
            stmt.group_by(func.cube(t.c.x, t.c.y)),
            "SELECT sum(t.value) AS sum_1 FROM t GROUP BY CUBE(t.x, t.y)",
        )

        self.assert_compile(
            stmt.group_by(func.rollup(t.c.x, t.c.y)),
            "SELECT sum(t.value) AS sum_1 FROM t GROUP BY ROLLUP(t.x, t.y)",
        )

        self.assert_compile(
            stmt.group_by(func.grouping_sets(t.c.x, t.c.y)),
            "SELECT sum(t.value) AS sum_1 FROM t "
            "GROUP BY GROUPING SETS(t.x, t.y)",
        )

        self.assert_compile(
            stmt.group_by(
                func.grouping_sets(
                    sql.tuple_(t.c.x, t.c.y), sql.tuple_(t.c.z, t.c.q)
                )
            ),
            "SELECT sum(t.value) AS sum_1 FROM t GROUP BY "
            "GROUPING SETS((t.x, t.y), (t.z, t.q))",
        )

    def test_generic_annotation(self):
        fn = func.coalesce("x", "y")._annotate({"foo": "bar"})
        self.assert_compile(fn, "coalesce(:coalesce_1, :coalesce_2)")

    def test_custom_default_namespace(self):
        class myfunc(GenericFunction):
            pass

        assert isinstance(func.myfunc(), myfunc)
        self.assert_compile(func.myfunc(), "myfunc()")

    def test_custom_type(self):
        class myfunc(GenericFunction):
            type = DateTime

        assert isinstance(func.myfunc().type, DateTime)
        self.assert_compile(func.myfunc(), "myfunc()")

    def test_custom_legacy_type(self):
        # in case someone was using this system
        class myfunc(GenericFunction):
            __return_type__ = DateTime

        assert isinstance(func.myfunc().type, DateTime)

    def test_case_sensitive(self):
        class MYFUNC(GenericFunction):
            type = DateTime

        assert isinstance(func.MYFUNC().type, DateTime)
        assert isinstance(func.MyFunc().type, DateTime)
        assert isinstance(func.mYfUnC().type, DateTime)
        assert isinstance(func.myfunc().type, DateTime)

    def test_replace_function(self):
        class replaceable_func(GenericFunction):
            type = Integer
            identifier = "replaceable_func"

        assert isinstance(func.Replaceable_Func().type, Integer)
        assert isinstance(func.RePlAcEaBlE_fUnC().type, Integer)
        assert isinstance(func.replaceable_func().type, Integer)

        with expect_warnings(
            "The GenericFunction 'replaceable_func' is already registered and "
            "is going to be overriden.",
            regex=False,
        ):

            class replaceable_func_override(GenericFunction):
                type = DateTime
                identifier = "replaceable_func"

        assert isinstance(func.Replaceable_Func().type, DateTime)
        assert isinstance(func.RePlAcEaBlE_fUnC().type, DateTime)
        assert isinstance(func.replaceable_func().type, DateTime)

    def test_custom_w_custom_name(self):
        class myfunc(GenericFunction):
            name = "notmyfunc"

        assert isinstance(func.notmyfunc(), myfunc)
        assert not isinstance(func.myfunc(), myfunc)

    def test_custom_w_quoted_name(self):
        class myfunc(GenericFunction):
            name = quoted_name("NotMyFunc", quote=True)
            identifier = "myfunc"

        self.assert_compile(func.myfunc(), '"NotMyFunc"()')

    def test_custom_w_quoted_name_no_identifier(self):
        class myfunc(GenericFunction):
            name = quoted_name("NotMyFunc", quote=True)

        # note this requires that the quoted name be lower cased for
        # correct lookup
        self.assert_compile(func.notmyfunc(), '"NotMyFunc"()')

    def test_custom_package_namespace(self):
        def cls1(pk_name):
            class myfunc(GenericFunction):
                package = pk_name

            return myfunc

        f1 = cls1("mypackage")
        f2 = cls1("myotherpackage")

        assert isinstance(func.mypackage.myfunc(), f1)
        assert isinstance(func.myotherpackage.myfunc(), f2)

    def test_custom_name(self):
        class MyFunction(GenericFunction):
            name = "my_func"

            def __init__(self, *args):
                args = args + (3,)
                super(MyFunction, self).__init__(*args)

        self.assert_compile(
            func.my_func(1, 2), "my_func(:my_func_1, :my_func_2, :my_func_3)"
        )

    def test_custom_registered_identifier(self):
        class GeoBuffer(GenericFunction):
            type = Integer
            package = "geo"
            name = "BufferOne"
            identifier = "buf1"

        class GeoBuffer2(GenericFunction):
            type = Integer
            name = "BufferTwo"
            identifier = "buf2"

        class BufferThree(GenericFunction):
            type = Integer
            identifier = "buf3"

        class GeoBufferFour(GenericFunction):
            type = Integer
            name = "BufferFour"
            identifier = "Buf4"

        self.assert_compile(func.geo.buf1(), "BufferOne()")
        self.assert_compile(func.buf2(), "BufferTwo()")
        self.assert_compile(func.buf3(), "BufferThree()")
        self.assert_compile(func.Buf4(), "BufferFour()")
        self.assert_compile(func.BuF4(), "BufferFour()")
        self.assert_compile(func.bUf4(), "BufferFour()")
        self.assert_compile(func.bUf4_(), "BufferFour()")
        self.assert_compile(func.buf4(), "BufferFour()")

    def test_custom_args(self):
        class myfunc(GenericFunction):
            pass

        self.assert_compile(
            myfunc(1, 2, 3), "myfunc(:myfunc_1, :myfunc_2, :myfunc_3)"
        )

    def test_namespacing_conflicts(self):
        self.assert_compile(func.text("foo"), "text(:text_1)")

    def test_generic_count(self):
        assert isinstance(func.count().type, sqltypes.Integer)

        self.assert_compile(func.count(), "count(*)")
        self.assert_compile(func.count(1), "count(:count_1)")
        c = column("abc")
        self.assert_compile(func.count(c), "count(abc)")

    def test_ansi_functions_with_args(self):
        ct = func.current_timestamp("somearg")
        self.assert_compile(ct, "CURRENT_TIMESTAMP(:current_timestamp_1)")

    def test_char_length_fixed_args(self):
        assert_raises(TypeError, func.char_length, "a", "b")
        assert_raises(TypeError, func.char_length)

    def test_return_type_detection(self):

        for fn in [func.coalesce, func.max, func.min, func.sum]:
            for args, type_ in [
                (
                    (datetime.date(2007, 10, 5), datetime.date(2005, 10, 15)),
                    sqltypes.Date,
                ),
                ((3, 5), sqltypes.Integer),
                ((decimal.Decimal(3), decimal.Decimal(5)), sqltypes.Numeric),
                (("foo", "bar"), sqltypes.String),
                (
                    (
                        datetime.datetime(2007, 10, 5, 8, 3, 34),
                        datetime.datetime(2005, 10, 15, 14, 45, 33),
                    ),
                    sqltypes.DateTime,
                ),
            ]:
                assert isinstance(fn(*args).type, type_), "%s / %r != %s" % (
                    fn(),
                    fn(*args).type,
                    type_,
                )

        assert isinstance(func.concat("foo", "bar").type, sqltypes.String)

    def test_assorted(self):
        table1 = table("mytable", column("myid", Integer))

        table2 = table("myothertable", column("otherid", Integer))

        # test an expression with a function
        self.assert_compile(
            func.lala(3, 4, literal("five"), table1.c.myid) * table2.c.otherid,
            "lala(:lala_1, :lala_2, :param_1, mytable.myid) * "
            "myothertable.otherid",
        )

        # test it in a SELECT
        self.assert_compile(
            select([func.count(table1.c.myid)]),
            "SELECT count(mytable.myid) AS count_1 FROM mytable",
        )

        # test a "dotted" function name
        self.assert_compile(
            select([func.foo.bar.lala(table1.c.myid)]),
            "SELECT foo.bar.lala(mytable.myid) AS lala_1 FROM mytable",
        )

        # test the bind parameter name with a "dotted" function name is
        # only the name (limits the length of the bind param name)
        self.assert_compile(
            select([func.foo.bar.lala(12)]),
            "SELECT foo.bar.lala(:lala_2) AS lala_1",
        )

        # test a dotted func off the engine itself
        self.assert_compile(func.lala.hoho(7), "lala.hoho(:hoho_1)")

        # test None becomes NULL
        self.assert_compile(
            func.my_func(1, 2, None, 3),
            "my_func(:my_func_1, :my_func_2, NULL, :my_func_3)",
        )

        # test pickling
        self.assert_compile(
            util.pickle.loads(util.pickle.dumps(func.my_func(1, 2, None, 3))),
            "my_func(:my_func_1, :my_func_2, NULL, :my_func_3)",
        )

        # assert func raises AttributeError for __bases__ attribute, since
        # its not a class fixes pydoc
        try:
            func.__bases__
            assert False
        except AttributeError:
            assert True

    def test_functions_with_cols(self):
        users = table(
            "users", column("id"), column("name"), column("fullname")
        )
        calculate = select(
            [column("q"), column("z"), column("r")],
            from_obj=[
                func.calculate(bindparam("x", None), bindparam("y", None))
            ],
        )

        self.assert_compile(
            select([users], users.c.id > calculate.c.z),
            "SELECT users.id, users.name, users.fullname "
            "FROM users, (SELECT q, z, r "
            "FROM calculate(:x, :y)) "
            "WHERE users.id > z",
        )

        s = select(
            [users],
            users.c.id.between(
                calculate.alias("c1").unique_params(x=17, y=45).c.z,
                calculate.alias("c2").unique_params(x=5, y=12).c.z,
            ),
        )

        self.assert_compile(
            s,
            "SELECT users.id, users.name, users.fullname "
            "FROM users, (SELECT q, z, r "
            "FROM calculate(:x_1, :y_1)) AS c1, (SELECT q, z, r "
            "FROM calculate(:x_2, :y_2)) AS c2 "
            "WHERE users.id BETWEEN c1.z AND c2.z",
            checkparams={"y_1": 45, "x_1": 17, "y_2": 12, "x_2": 5},
        )

    def test_non_functions(self):
        expr = func.cast("foo", Integer)
        self.assert_compile(expr, "CAST(:param_1 AS INTEGER)")

        expr = func.extract("year", datetime.date(2010, 12, 5))
        self.assert_compile(expr, "EXTRACT(year FROM :param_1)")

    def test_select_method_one(self):
        expr = func.rows("foo")
        self.assert_compile(expr.select(), "SELECT rows(:rows_2) AS rows_1")

    def test_alias_method_one(self):
        expr = func.rows("foo")
        self.assert_compile(expr.alias(), "rows(:rows_1)")

    def test_select_method_two(self):
        expr = func.rows("foo")
        self.assert_compile(
            select(["*"]).select_from(expr.select()),
            "SELECT * FROM (SELECT rows(:rows_2) AS rows_1)",
        )

    def test_select_method_three(self):
        expr = func.rows("foo")
        self.assert_compile(
            select([column("foo")]).select_from(expr),
            "SELECT foo FROM rows(:rows_1)",
        )

    def test_alias_method_two(self):
        expr = func.rows("foo")
        self.assert_compile(
            select(["*"]).select_from(expr.alias("bar")),
            "SELECT * FROM rows(:rows_1) AS bar",
        )

    def test_alias_method_columns(self):
        expr = func.rows("foo").alias("bar")

        # this isn't very useful but is the old behavior
        # prior to #2974.
        # testing here that the expression exports its column
        # list in a way that at least doesn't break.
        self.assert_compile(
            select([expr]), "SELECT bar.rows_1 FROM rows(:rows_2) AS bar"
        )

    def test_alias_method_columns_two(self):
        expr = func.rows("foo").alias("bar")
        assert len(expr.c)

    def test_funcfilter_empty(self):
        self.assert_compile(func.count(1).filter(), "count(:count_1)")

    def test_funcfilter_criterion(self):
        self.assert_compile(
            func.count(1).filter(table1.c.name != None),  # noqa
            "count(:count_1) FILTER (WHERE mytable.name IS NOT NULL)",
        )

    def test_funcfilter_compound_criterion(self):
        self.assert_compile(
            func.count(1).filter(
                table1.c.name == None, table1.c.myid > 0  # noqa
            ),
            "count(:count_1) FILTER (WHERE mytable.name IS NULL AND "
            "mytable.myid > :myid_1)",
        )

    def test_funcfilter_arrayagg_subscript(self):
        num = column("q")
        self.assert_compile(
            func.array_agg(num).filter(num % 2 == 0)[1],
            "(array_agg(q) FILTER (WHERE q %% %(q_1)s = "
            "%(param_1)s))[%(param_2)s]",
            dialect="postgresql",
        )

    def test_funcfilter_label(self):
        self.assert_compile(
            select(
                [
                    func.count(1)
                    .filter(table1.c.description != None)  # noqa
                    .label("foo")
                ]
            ),
            "SELECT count(:count_1) FILTER (WHERE mytable.description "
            "IS NOT NULL) AS foo FROM mytable",
        )

    def test_funcfilter_fromobj_fromfunc(self):
        # test from_obj generation.
        # from func:
        self.assert_compile(
            select(
                [
                    func.max(table1.c.name).filter(
                        literal_column("description") != None  # noqa
                    )
                ]
            ),
            "SELECT max(mytable.name) FILTER (WHERE description "
            "IS NOT NULL) AS anon_1 FROM mytable",
        )

    def test_funcfilter_fromobj_fromcriterion(self):
        # from criterion:
        self.assert_compile(
            select([func.count(1).filter(table1.c.name == "name")]),
            "SELECT count(:count_1) FILTER (WHERE mytable.name = :name_1) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_chaining(self):
        # test chaining:
        self.assert_compile(
            select(
                [
                    func.count(1)
                    .filter(table1.c.name == "name")
                    .filter(table1.c.description == "description")
                ]
            ),
            "SELECT count(:count_1) FILTER (WHERE "
            "mytable.name = :name_1 AND mytable.description = :description_1) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_windowing_orderby(self):
        # test filtered windowing:
        self.assert_compile(
            select(
                [
                    func.rank()
                    .filter(table1.c.name > "foo")
                    .over(order_by=table1.c.name)
                ]
            ),
            "SELECT rank() FILTER (WHERE mytable.name > :name_1) "
            "OVER (ORDER BY mytable.name) AS anon_1 FROM mytable",
        )

    def test_funcfilter_windowing_orderby_partitionby(self):
        self.assert_compile(
            select(
                [
                    func.rank()
                    .filter(table1.c.name > "foo")
                    .over(order_by=table1.c.name, partition_by=["description"])
                ]
            ),
            "SELECT rank() FILTER (WHERE mytable.name > :name_1) "
            "OVER (PARTITION BY mytable.description ORDER BY mytable.name) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_windowing_range(self):
        self.assert_compile(
            select(
                [
                    func.rank()
                    .filter(table1.c.name > "foo")
                    .over(range_=(1, 5), partition_by=["description"])
                ]
            ),
            "SELECT rank() FILTER (WHERE mytable.name > :name_1) "
            "OVER (PARTITION BY mytable.description RANGE BETWEEN :param_1 "
            "FOLLOWING AND :param_2 FOLLOWING) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_windowing_rows(self):
        self.assert_compile(
            select(
                [
                    func.rank()
                    .filter(table1.c.name > "foo")
                    .over(rows=(1, 5), partition_by=["description"])
                ]
            ),
            "SELECT rank() FILTER (WHERE mytable.name > :name_1) "
            "OVER (PARTITION BY mytable.description ROWS BETWEEN :param_1 "
            "FOLLOWING AND :param_2 FOLLOWING) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_within_group(self):
        stmt = select(
            [
                table1.c.myid,
                func.percentile_cont(0.5).within_group(table1.c.name),
            ]
        )
        self.assert_compile(
            stmt,
            "SELECT mytable.myid, percentile_cont(:percentile_cont_1) "
            "WITHIN GROUP (ORDER BY mytable.name) "
            "AS anon_1 "
            "FROM mytable",
            {"percentile_cont_1": 0.5},
        )

    def test_funcfilter_within_group_multi(self):
        stmt = select(
            [
                table1.c.myid,
                func.percentile_cont(0.5).within_group(
                    table1.c.name, table1.c.description
                ),
            ]
        )
        self.assert_compile(
            stmt,
            "SELECT mytable.myid, percentile_cont(:percentile_cont_1) "
            "WITHIN GROUP (ORDER BY mytable.name, mytable.description) "
            "AS anon_1 "
            "FROM mytable",
            {"percentile_cont_1": 0.5},
        )

    def test_funcfilter_within_group_desc(self):
        stmt = select(
            [
                table1.c.myid,
                func.percentile_cont(0.5).within_group(table1.c.name.desc()),
            ]
        )
        self.assert_compile(
            stmt,
            "SELECT mytable.myid, percentile_cont(:percentile_cont_1) "
            "WITHIN GROUP (ORDER BY mytable.name DESC) "
            "AS anon_1 "
            "FROM mytable",
            {"percentile_cont_1": 0.5},
        )

    def test_funcfilter_within_group_w_over(self):
        stmt = select(
            [
                table1.c.myid,
                func.percentile_cont(0.5)
                .within_group(table1.c.name.desc())
                .over(partition_by=table1.c.description),
            ]
        )
        self.assert_compile(
            stmt,
            "SELECT mytable.myid, percentile_cont(:percentile_cont_1) "
            "WITHIN GROUP (ORDER BY mytable.name DESC) "
            "OVER (PARTITION BY mytable.description) AS anon_1 "
            "FROM mytable",
            {"percentile_cont_1": 0.5},
        )

    def test_incorrect_none_type(self):
        class MissingType(FunctionElement):
            name = "mt"
            type = None

        assert_raises_message(
            TypeError,
            "Object None associated with '.type' attribute is "
            "not a TypeEngine class or object",
            MissingType().compile,
        )

    def test_as_comparison(self):

        fn = func.substring("foo", "foobar").as_comparison(1, 2)
        is_(fn.type._type_affinity, Boolean)

        self.assert_compile(
            fn.left, ":substring_1", checkparams={"substring_1": "foo"}
        )
        self.assert_compile(
            fn.right, ":substring_1", checkparams={"substring_1": "foobar"}
        )

        self.assert_compile(
            fn,
            "substring(:substring_1, :substring_2)",
            checkparams={"substring_1": "foo", "substring_2": "foobar"},
        )

    def test_as_comparison_annotate(self):

        fn = func.foobar("x", "y", "q", "p", "r").as_comparison(2, 5)

        from sqlalchemy.sql import annotation

        fn_annotated = annotation._deep_annotate(fn, {"token": "yes"})

        eq_(fn.left._annotations, {})
        eq_(fn_annotated.left._annotations, {"token": "yes"})

    def test_as_comparison_many_argument(self):

        fn = func.some_comparison("x", "y", "z", "p", "q", "r").as_comparison(
            2, 5
        )
        is_(fn.type._type_affinity, Boolean)

        self.assert_compile(
            fn.left,
            ":some_comparison_1",
            checkparams={"some_comparison_1": "y"},
        )
        self.assert_compile(
            fn.right,
            ":some_comparison_1",
            checkparams={"some_comparison_1": "q"},
        )

        from sqlalchemy.sql import visitors

        fn_2 = visitors.cloned_traverse(fn, {}, {})
        fn_2.right = literal_column("ABC")

        self.assert_compile(
            fn,
            "some_comparison(:some_comparison_1, :some_comparison_2, "
            ":some_comparison_3, "
            ":some_comparison_4, :some_comparison_5, :some_comparison_6)",
            checkparams={
                "some_comparison_1": "x",
                "some_comparison_2": "y",
                "some_comparison_3": "z",
                "some_comparison_4": "p",
                "some_comparison_5": "q",
                "some_comparison_6": "r",
            },
        )

        self.assert_compile(
            fn_2,
            "some_comparison(:some_comparison_1, :some_comparison_2, "
            ":some_comparison_3, "
            ":some_comparison_4, ABC, :some_comparison_5)",
            checkparams={
                "some_comparison_1": "x",
                "some_comparison_2": "y",
                "some_comparison_3": "z",
                "some_comparison_4": "p",
                "some_comparison_5": "r",
            },
        )


class ReturnTypeTest(AssertsCompiledSQL, fixtures.TestBase):
    def test_array_agg(self):
        expr = func.array_agg(column("data", Integer))
        is_(expr.type._type_affinity, ARRAY)
        is_(expr.type.item_type._type_affinity, Integer)

    def test_array_agg_array_datatype(self):
        expr = func.array_agg(column("data", ARRAY(Integer)))
        is_(expr.type._type_affinity, ARRAY)
        is_(expr.type.item_type._type_affinity, Integer)

    def test_array_agg_array_literal_implicit_type(self):
        from sqlalchemy.dialects.postgresql import array, ARRAY as PG_ARRAY

        expr = array([column("data", Integer), column("d2", Integer)])

        assert isinstance(expr.type, PG_ARRAY)

        agg_expr = func.array_agg(expr)
        assert isinstance(agg_expr.type, PG_ARRAY)
        is_(agg_expr.type._type_affinity, ARRAY)
        is_(agg_expr.type.item_type._type_affinity, Integer)

        self.assert_compile(
            agg_expr, "array_agg(ARRAY[data, d2])", dialect="postgresql"
        )

    def test_array_agg_array_literal_explicit_type(self):
        from sqlalchemy.dialects.postgresql import array

        expr = array([column("data", Integer), column("d2", Integer)])

        agg_expr = func.array_agg(expr, type_=ARRAY(Integer))
        is_(agg_expr.type._type_affinity, ARRAY)
        is_(agg_expr.type.item_type._type_affinity, Integer)

        self.assert_compile(
            agg_expr, "array_agg(ARRAY[data, d2])", dialect="postgresql"
        )

    def test_mode(self):
        expr = func.mode(0.5).within_group(column("data", Integer).desc())
        is_(expr.type._type_affinity, Integer)

    def test_percentile_cont(self):
        expr = func.percentile_cont(0.5).within_group(column("data", Integer))
        is_(expr.type._type_affinity, Integer)

    def test_percentile_cont_array(self):
        expr = func.percentile_cont(0.5, 0.7).within_group(
            column("data", Integer)
        )
        is_(expr.type._type_affinity, ARRAY)
        is_(expr.type.item_type._type_affinity, Integer)

    def test_percentile_cont_array_desc(self):
        expr = func.percentile_cont(0.5, 0.7).within_group(
            column("data", Integer).desc()
        )
        is_(expr.type._type_affinity, ARRAY)
        is_(expr.type.item_type._type_affinity, Integer)

    def test_cume_dist(self):
        expr = func.cume_dist(0.5).within_group(column("data", Integer).desc())
        is_(expr.type._type_affinity, Numeric)

    def test_percent_rank(self):
        expr = func.percent_rank(0.5).within_group(column("data", Integer))
        is_(expr.type._type_affinity, Numeric)


class ExecuteTest(fixtures.TestBase):
    __backend__ = True

    @engines.close_first
    def tearDown(self):
        pass

    def test_conn_execute(self):
        from sqlalchemy.sql.expression import FunctionElement
        from sqlalchemy.ext.compiler import compiles

        class myfunc(FunctionElement):
            type = Date()

        @compiles(myfunc)
        def compile_(elem, compiler, **kw):
            return compiler.process(func.current_date())

        conn = testing.db.connect()
        try:
            x = conn.execute(func.current_date()).scalar()
            y = conn.execute(func.current_date().select()).scalar()
            z = conn.scalar(func.current_date())
            q = conn.scalar(myfunc())
        finally:
            conn.close()
        assert (x == y == z == q) is True

    def test_exec_options(self):
        f = func.foo()
        eq_(f._execution_options, {})

        f = f.execution_options(foo="bar")
        eq_(f._execution_options, {"foo": "bar"})
        s = f.select()
        eq_(s._execution_options, {"foo": "bar"})

        ret = testing.db.execute(func.now().execution_options(foo="bar"))
        eq_(ret.context.execution_options, {"foo": "bar"})
        ret.close()

    @engines.close_first
    @testing.provide_metadata
    def test_update(self):
        """
        Tests sending functions and SQL expressions to the VALUES and SET
        clauses of INSERT/UPDATE instances, and that column-level defaults
        get overridden.
        """

        meta = self.metadata
        t = Table(
            "t1",
            meta,
            Column(
                "id",
                Integer,
                Sequence("t1idseq", optional=True),
                primary_key=True,
            ),
            Column("value", Integer),
        )
        t2 = Table(
            "t2",
            meta,
            Column(
                "id",
                Integer,
                Sequence("t2idseq", optional=True),
                primary_key=True,
            ),
            Column("value", Integer, default=7),
            Column("stuff", String(20), onupdate="thisisstuff"),
        )
        meta.create_all()
        t.insert(values=dict(value=func.length("one"))).execute()
        assert t.select().execute().first()["value"] == 3
        t.update(values=dict(value=func.length("asfda"))).execute()
        assert t.select().execute().first()["value"] == 5

        r = t.insert(values=dict(value=func.length("sfsaafsda"))).execute()
        id_ = r.inserted_primary_key[0]
        assert t.select(t.c.id == id_).execute().first()["value"] == 9
        t.update(values={t.c.value: func.length("asdf")}).execute()
        assert t.select().execute().first()["value"] == 4
        t2.insert().execute()
        t2.insert(values=dict(value=func.length("one"))).execute()
        t2.insert(values=dict(value=func.length("asfda") + -19)).execute(
            stuff="hi"
        )

        res = exec_sorted(select([t2.c.value, t2.c.stuff]))
        eq_(res, [(-14, "hi"), (3, None), (7, None)])

        t2.update(values=dict(value=func.length("asdsafasd"))).execute(
            stuff="some stuff"
        )
        assert select([t2.c.value, t2.c.stuff]).execute().fetchall() == [
            (9, "some stuff"),
            (9, "some stuff"),
            (9, "some stuff"),
        ]

        t2.delete().execute()

        t2.insert(values=dict(value=func.length("one") + 8)).execute()
        assert t2.select().execute().first()["value"] == 11

        t2.update(values=dict(value=func.length("asfda"))).execute()
        eq_(
            select([t2.c.value, t2.c.stuff]).execute().first(),
            (5, "thisisstuff"),
        )

        t2.update(
            values={t2.c.value: func.length("asfdaasdf"), t2.c.stuff: "foo"}
        ).execute()

        eq_(select([t2.c.value, t2.c.stuff]).execute().first(), (9, "foo"))

    @testing.fails_on_everything_except("postgresql")
    def test_as_from(self):
        # TODO: shouldn't this work on oracle too ?
        x = func.current_date(bind=testing.db).execute().scalar()
        y = func.current_date(bind=testing.db).select().execute().scalar()
        z = func.current_date(bind=testing.db).scalar()
        w = select(
            ["*"], from_obj=[func.current_date(bind=testing.db)]
        ).scalar()

        assert x == y == z == w

    def test_extract_bind(self):
        """Basic common denominator execution tests for extract()"""

        date = datetime.date(2010, 5, 1)

        def execute(field):
            return testing.db.execute(select([extract(field, date)])).scalar()

        assert execute("year") == 2010
        assert execute("month") == 5
        assert execute("day") == 1

        date = datetime.datetime(2010, 5, 1, 12, 11, 10)

        assert execute("year") == 2010
        assert execute("month") == 5
        assert execute("day") == 1

    def test_extract_expression(self):
        meta = MetaData(testing.db)
        table = Table("test", meta, Column("dt", DateTime), Column("d", Date))
        meta.create_all()
        try:
            table.insert().execute(
                {
                    "dt": datetime.datetime(2010, 5, 1, 12, 11, 10),
                    "d": datetime.date(2010, 5, 1),
                }
            )
            rs = select(
                [extract("year", table.c.dt), extract("month", table.c.d)]
            ).execute()
            row = rs.first()
            assert row[0] == 2010
            assert row[1] == 5
            rs.close()
        finally:
            meta.drop_all()


def exec_sorted(statement, *args, **kw):
    """Executes a statement and returns a sorted list plain tuple rows."""

    return sorted(
        [tuple(row) for row in statement.execute(*args, **kw).fetchall()]
    )


class RegisterTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def setup(self):
        self._registry = deepcopy(functions._registry)

    def teardown(self):
        functions._registry = self._registry

    def test_GenericFunction_is_registered(self):
        assert "GenericFunction" not in functions._registry["_default"]

    def test_register_function(self):

        # test generic function registering
        class registered_func(GenericFunction):
            _register = True

            def __init__(self, *args, **kwargs):
                GenericFunction.__init__(self, *args, **kwargs)

        class registered_func_child(registered_func):
            type = sqltypes.Integer

        assert "registered_func" in functions._registry["_default"]
        assert isinstance(func.registered_func_child().type, Integer)

        class not_registered_func(GenericFunction):
            _register = False

            def __init__(self, *args, **kwargs):
                GenericFunction.__init__(self, *args, **kwargs)

        class not_registered_func_child(not_registered_func):
            type = sqltypes.Integer

        assert "not_registered_func" not in functions._registry["_default"]
        assert isinstance(func.not_registered_func_child().type, Integer)
