from copy import deepcopy
import datetime
import decimal

from sqlalchemy import ARRAY
from sqlalchemy import bindparam
from sqlalchemy import Boolean
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import extract
from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy import true
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import oracle
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.sql import column
from sqlalchemy.sql import functions
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql import table
from sqlalchemy.sql.compiler import BIND_TEMPLATES
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
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

    def setup_test(self):
        self._registry = deepcopy(functions._registry)

    def teardown_test(self):
        functions._registry = self._registry

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

    def test_use_labels(self):
        self.assert_compile(
            select(func.foo()).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT foo() AS foo_1",
        )

    def test_use_labels_function_element(self):
        from sqlalchemy.ext.compiler import compiles

        class max_(FunctionElement):
            name = "max"

        @compiles(max_)
        def visit_max(element, compiler, **kw):
            return "max(%s)" % compiler.process(element.clauses, **kw)

        self.assert_compile(
            select(max_(5, 6)).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT max(:max_2, :max_3) AS max_1",
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

        stmt = select(func.sum(t.c.value))

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
            "is going to be overridden.",
            regex=False,
        ):

            class replaceable_func_override(GenericFunction):
                type = DateTime
                identifier = "replaceable_func"

        assert isinstance(func.Replaceable_Func().type, DateTime)
        assert isinstance(func.RePlAcEaBlE_fUnC().type, DateTime)
        assert isinstance(func.replaceable_func().type, DateTime)

    def test_replace_function_case_insensitive(self):
        class replaceable_func(GenericFunction):
            type = Integer
            identifier = "replaceable_func"

        assert isinstance(func.Replaceable_Func().type, Integer)
        assert isinstance(func.RePlAcEaBlE_fUnC().type, Integer)
        assert isinstance(func.replaceable_func().type, Integer)

        with expect_warnings(
            "The GenericFunction 'replaceable_func' is already registered and "
            "is going to be overridden.",
            regex=False,
        ):

            class replaceable_func_override(GenericFunction):
                type = DateTime
                identifier = "REPLACEABLE_Func"

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
            select(func.count(table1.c.myid)),
            "SELECT count(mytable.myid) AS count_1 FROM mytable",
        )

        # test a "dotted" function name
        self.assert_compile(
            select(func.foo.bar.lala(table1.c.myid)),
            "SELECT foo.bar.lala(mytable.myid) AS lala_1 FROM mytable",
        )

        # test the bind parameter name with a "dotted" function name is
        # only the name (limits the length of the bind param name)
        self.assert_compile(
            select(func.foo.bar.lala(12)),
            "SELECT foo.bar.lala(:lala_2) AS lala_1",
        )

        # test a dotted func off the engine itself
        self.assert_compile(func.lala.hoho(7), "lala.hoho(:hoho_1)")

        # test None becomes NULL
        self.assert_compile(
            func.my_func(1, 2, None, 3),
            "my_func(:my_func_1, :my_func_2, NULL, :my_func_3)",
        )

        f1 = func.my_func(1, 2, None, 3)
        f1._generate_cache_key()

        # test pickling
        self.assert_compile(
            util.pickle.loads(util.pickle.dumps(f1)),
            "my_func(:my_func_1, :my_func_2, NULL, :my_func_3)",
        )

        # assert func raises AttributeError for __bases__ attribute, since
        # its not a class fixes pydoc
        try:
            func.__bases__
            assert False
        except AttributeError:
            assert True

    def test_pickle_over(self):
        # TODO: the test/sql package lacks a comprehensive pickling
        # test suite even though there are __reduce__ methods in several
        # places in sql/elements.py.   likely as part of
        # test/sql/test_compare.py might be a place this can happen but
        # this still relies upon a strategy for table metadata as we have
        # in serializer.

        f1 = func.row_number().over()

        self.assert_compile(
            util.pickle.loads(util.pickle.dumps(f1)),
            "row_number() OVER ()",
        )

    def test_functions_with_cols(self):
        users = table(
            "users", column("id"), column("name"), column("fullname")
        )
        calculate = (
            select(column("q"), column("z"), column("r"))
            .select_from(
                func.calculate(bindparam("x", None), bindparam("y", None))
            )
            .subquery()
        )

        self.assert_compile(
            select(users).where(users.c.id > calculate.c.z),
            "SELECT users.id, users.name, users.fullname "
            "FROM users, (SELECT q, z, r "
            "FROM calculate(:x, :y)) AS anon_1 "
            "WHERE users.id > anon_1.z",
        )

        s = select(users).where(
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
            select("*").select_from(expr.select().subquery()),
            "SELECT * FROM (SELECT rows(:rows_2) AS rows_1) AS anon_1",
        )

    def test_select_method_three(self):
        expr = func.rows("foo")
        self.assert_compile(
            select(column("foo")).select_from(expr),
            "SELECT foo FROM rows(:rows_1)",
        )

    def test_alias_method_two(self):
        expr = func.rows("foo")
        self.assert_compile(
            select("*").select_from(expr.alias("bar")),
            "SELECT * FROM rows(:rows_1) AS bar",
        )

    def test_alias_method_columns(self):
        expr = func.rows("foo").alias("bar")

        # this isn't very useful but is the old behavior
        # prior to #2974.
        # testing here that the expression exports its column
        # list in a way that at least doesn't break.
        self.assert_compile(
            select(expr), "SELECT bar.rows_1 FROM rows(:rows_2) AS bar"
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
                func.count(1)
                .filter(table1.c.description != None)  # noqa
                .label("foo")
            ),
            "SELECT count(:count_1) FILTER (WHERE mytable.description "
            "IS NOT NULL) AS foo FROM mytable",
        )

    def test_funcfilter_fromobj_fromfunc(self):
        # test from_obj generation.
        # from func:
        self.assert_compile(
            select(
                func.max(table1.c.name).filter(
                    literal_column("description") != None  # noqa
                )
            ),
            "SELECT max(mytable.name) FILTER (WHERE description "
            "IS NOT NULL) AS anon_1 FROM mytable",
        )

    def test_funcfilter_fromobj_fromcriterion(self):
        # from criterion:
        self.assert_compile(
            select(func.count(1).filter(table1.c.name == "name")),
            "SELECT count(:count_1) FILTER (WHERE mytable.name = :name_1) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_chaining(self):
        # test chaining:
        self.assert_compile(
            select(
                func.count(1)
                .filter(table1.c.name == "name")
                .filter(table1.c.description == "description")
            ),
            "SELECT count(:count_1) FILTER (WHERE "
            "mytable.name = :name_1 AND mytable.description = :description_1) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_windowing_orderby(self):
        # test filtered windowing:
        self.assert_compile(
            select(
                func.rank()
                .filter(table1.c.name > "foo")
                .over(order_by=table1.c.name)
            ),
            "SELECT rank() FILTER (WHERE mytable.name > :name_1) "
            "OVER (ORDER BY mytable.name) AS anon_1 FROM mytable",
        )

    def test_funcfilter_windowing_orderby_partitionby(self):
        self.assert_compile(
            select(
                func.rank()
                .filter(table1.c.name > "foo")
                .over(order_by=table1.c.name, partition_by=["description"])
            ),
            "SELECT rank() FILTER (WHERE mytable.name > :name_1) "
            "OVER (PARTITION BY mytable.description ORDER BY mytable.name) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_windowing_range(self):
        self.assert_compile(
            select(
                func.rank()
                .filter(table1.c.name > "foo")
                .over(range_=(1, 5), partition_by=["description"])
            ),
            "SELECT rank() FILTER (WHERE mytable.name > :name_1) "
            "OVER (PARTITION BY mytable.description RANGE BETWEEN :param_1 "
            "FOLLOWING AND :param_2 FOLLOWING) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_windowing_rows(self):
        self.assert_compile(
            select(
                func.rank()
                .filter(table1.c.name > "foo")
                .over(rows=(1, 5), partition_by=["description"])
            ),
            "SELECT rank() FILTER (WHERE mytable.name > :name_1) "
            "OVER (PARTITION BY mytable.description ROWS BETWEEN :param_1 "
            "FOLLOWING AND :param_2 FOLLOWING) "
            "AS anon_1 FROM mytable",
        )

    def test_funcfilter_within_group(self):
        stmt = select(
            table1.c.myid,
            func.percentile_cont(0.5).within_group(table1.c.name),
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
            table1.c.myid,
            func.percentile_cont(0.5).within_group(
                table1.c.name, table1.c.description
            ),
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
            table1.c.myid,
            func.percentile_cont(0.5).within_group(table1.c.name.desc()),
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
            table1.c.myid,
            func.percentile_cont(0.5)
            .within_group(table1.c.name.desc())
            .over(partition_by=table1.c.description),
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
        from sqlalchemy.sql.expression import FunctionElement

        class MissingType(FunctionElement):
            name = "mt"
            type = None

        assert_raises_message(
            TypeError,
            "Object None associated with '.type' attribute is "
            "not a TypeEngine class or object",
            lambda: column("x", MissingType()) == 5,
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

    def teardown_test(self):
        pass

    def test_conn_execute(self, connection):
        from sqlalchemy.sql.expression import FunctionElement
        from sqlalchemy.ext.compiler import compiles

        class myfunc(FunctionElement):
            type = Date()

        @compiles(myfunc)
        def compile_(elem, compiler, **kw):
            return compiler.process(func.current_date())

        x = connection.execute(func.current_date()).scalar()
        y = connection.execute(func.current_date().select()).scalar()
        z = connection.scalar(func.current_date())
        q = connection.scalar(myfunc())

        assert (x == y == z == q) is True

    def test_exec_options(self, connection):
        f = func.foo()
        eq_(f._execution_options, {})

        f = f.execution_options(foo="bar")
        eq_(f._execution_options, {"foo": "bar"})
        s = f.select()
        eq_(s._execution_options, {"foo": "bar"})

        ret = connection.execute(func.now().execution_options(foo="bar"))
        eq_(ret.context.execution_options, {"foo": "bar"})
        ret.close()

    @testing.provide_metadata
    def test_update(self, connection):
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
        meta.create_all(connection)
        connection.execute(t.insert().values(value=func.length("one")))
        eq_(connection.execute(t.select()).first().value, 3)
        connection.execute(t.update().values(value=func.length("asfda")))
        eq_(connection.execute(t.select()).first().value, 5)

        r = connection.execute(
            t.insert().values(value=func.length("sfsaafsda"))
        )
        id_ = r.inserted_primary_key[0]
        eq_(
            connection.execute(t.select().where(t.c.id == id_)).first().value,
            9,
        )
        connection.execute(t.update().values({t.c.value: func.length("asdf")}))
        eq_(connection.execute(t.select()).first().value, 4)
        connection.execute(t2.insert())
        connection.execute(t2.insert().values(value=func.length("one")))
        connection.execute(
            t2.insert().values(value=func.length("asfda") + -19),
            dict(stuff="hi"),
        )

        res = sorted(connection.execute(select(t2.c.value, t2.c.stuff)))
        eq_(res, [(-14, "hi"), (3, None), (7, None)])

        connection.execute(
            t2.update().values(value=func.length("asdsafasd")),
            dict(stuff="some stuff"),
        )
        eq_(
            connection.execute(select(t2.c.value, t2.c.stuff)).fetchall(),
            [(9, "some stuff"), (9, "some stuff"), (9, "some stuff")],
        )

        connection.execute(t2.delete())

        connection.execute(t2.insert().values(value=func.length("one") + 8))
        eq_(connection.execute(t2.select()).first().value, 11)

        connection.execute(t2.update().values(value=func.length("asfda")))
        eq_(
            connection.execute(select(t2.c.value, t2.c.stuff)).first(),
            (5, "thisisstuff"),
        )

        connection.execute(
            t2.update().values(
                {t2.c.value: func.length("asfdaasdf"), t2.c.stuff: "foo"}
            )
        )

        eq_(
            connection.execute(select(t2.c.value, t2.c.stuff)).first(),
            (9, "foo"),
        )

    @testing.fails_on_everything_except("postgresql")
    def test_as_from(self, connection):
        # TODO: shouldn't this work on oracle too ?
        x = connection.execute(func.current_date()).scalar()
        y = connection.execute(func.current_date().select()).scalar()
        z = connection.scalar(func.current_date())
        w = connection.scalar(select("*").select_from(func.current_date()))

        assert x == y == z == w

    def test_extract_bind(self, connection):
        """Basic common denominator execution tests for extract()"""

        date = datetime.date(2010, 5, 1)

        def execute(field):
            return connection.execute(select(extract(field, date))).scalar()

        assert execute("year") == 2010
        assert execute("month") == 5
        assert execute("day") == 1

        date = datetime.datetime(2010, 5, 1, 12, 11, 10)

        assert execute("year") == 2010
        assert execute("month") == 5
        assert execute("day") == 1

    @testing.provide_metadata
    def test_extract_expression(self, connection):
        meta = self.metadata
        table = Table("test", meta, Column("dt", DateTime), Column("d", Date))
        meta.create_all(connection)
        connection.execute(
            table.insert(),
            {
                "dt": datetime.datetime(2010, 5, 1, 12, 11, 10),
                "d": datetime.date(2010, 5, 1),
            },
        )
        rs = connection.execute(
            select(extract("year", table.c.dt), extract("month", table.c.d))
        )
        row = rs.first()
        assert row[0] == 2010
        assert row[1] == 5
        rs.close()


class RegisterTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def setup_test(self):
        self._registry = deepcopy(functions._registry)

    def teardown_test(self):
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


class TableValuedCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    """test the full set of functions as FROM developed in [ticket:3566]"""

    __dialect__ = "default_enhanced"

    def test_aggregate_scalar_over_table_valued(self):
        test = table("test", column("id"), column("data", JSON))

        elem = (
            func.json_array_elements_text(test.c.data["key"])
            .table_valued("value")
            .alias("elem")
        )

        maxdepth = select(func.max(cast(elem.c.value, Float))).label(
            "maxdepth"
        )

        stmt = select(test.c.id.label("test_id"), maxdepth).order_by(
            "maxdepth"
        )

        self.assert_compile(
            stmt,
            "SELECT test.id AS test_id, "
            "(SELECT max(CAST(elem.value AS FLOAT)) AS max_1 "
            "FROM json_array_elements_text(test.data[:data_1]) AS elem) "
            "AS maxdepth "
            "FROM test ORDER BY maxdepth",
        )

    def test_scalar_table_valued(self):
        assets_transactions = table(
            "assets_transactions", column("id"), column("contents", JSON)
        )

        stmt = select(
            assets_transactions.c.id,
            func.jsonb_each(
                assets_transactions.c.contents
            ).scalar_table_valued("key"),
            func.jsonb_each(
                assets_transactions.c.contents
            ).scalar_table_valued("value"),
        )
        self.assert_compile(
            stmt,
            "SELECT assets_transactions.id, "
            "(jsonb_each(assets_transactions.contents)).key, "
            "(jsonb_each(assets_transactions.contents)).value "
            "FROM assets_transactions",
        )

    def test_table_valued_one(self):
        assets_transactions = table(
            "assets_transactions", column("id"), column("contents", JSON)
        )

        jb = func.jsonb_each(assets_transactions.c.contents).table_valued(
            "key", "value"
        )

        stmt = select(assets_transactions.c.id, jb.c.key, jb.c.value).join(
            jb, true()
        )

        self.assert_compile(
            stmt,
            "SELECT assets_transactions.id, anon_1.key, anon_1.value "
            "FROM assets_transactions "
            "JOIN jsonb_each(assets_transactions.contents) AS anon_1 ON true",
        )

    def test_table_valued_two(self):
        """
        SELECT vi.id, vv.value
        FROM value_ids() AS vi JOIN values AS vv ON vv.id = vi.id

        """

        values = table(
            "values",
            column(
                "id",
                Integer,
            ),
            column("value", String),
        )
        vi = func.value_ids().table_valued(column("id", Integer)).alias("vi")
        vv = values.alias("vv")

        stmt = select(vi.c.id, vv.c.value).select_from(  # noqa
            vi.join(vv, vv.c.id == vi.c.id)
        )
        self.assert_compile(
            stmt,
            "SELECT vi.id, vv.value FROM value_ids() AS vi "
            "JOIN values AS vv ON vv.id = vi.id",
        )

    def test_table_as_table_valued(self):
        a = table(
            "a",
            column("id"),
            column("x"),
            column("y"),
        )

        stmt = select(func.row_to_json(a.table_valued()))

        self.assert_compile(
            stmt, "SELECT row_to_json(a) AS row_to_json_1 FROM a"
        )

    def test_subquery_as_table_valued(self):
        """
        SELECT row_to_json(anon_1) AS row_to_json_1
        FROM (SELECT a.id AS id, a.x AS x, a.y AS y
        FROM a) AS anon_1

        """

        a = table(
            "a",
            column("id"),
            column("x"),
            column("y"),
        )

        stmt = select(func.row_to_json(a.select().subquery().table_valued()))

        self.assert_compile(
            stmt,
            "SELECT row_to_json(anon_1) AS row_to_json_1 FROM "
            "(SELECT a.id AS id, a.x AS x, a.y AS y FROM a) AS anon_1",
        )

    def test_scalar_subquery(self):

        a = table(
            "a",
            column("id"),
            column("x"),
            column("y"),
        )

        stmt = select(func.row_to_json(a.select().scalar_subquery()))

        self.assert_compile(
            stmt,
            "SELECT row_to_json((SELECT a.id, a.x, a.y FROM a)) "
            "AS row_to_json_1",
        )

    def test_named_with_ordinality(self):
        """
        SELECT a.id AS a_id, a.refs AS a_refs,
        unnested.unnested AS unnested_unnested,
        unnested.ordinality AS unnested_ordinality,
        b.id AS b_id, b.ref AS b_ref
        FROM a LEFT OUTER JOIN unnest(a.refs)
        `WITH ORDINALITY AS unnested(unnested, ordinality) ON true
        LEFT OUTER JOIN b ON unnested.unnested = b.ref

        """  # noqa 501

        a = table("a", column("id"), column("refs"))
        b = table("b", column("id"), column("ref"))

        unnested = (
            func.unnest(a.c.refs)
            .table_valued("unnested", with_ordinality="ordinality")
            .render_derived()
            .alias("unnested")
        )

        stmt = (
            select(
                a.c.id, a.c.refs, unnested.c.unnested, unnested.c.ordinality
            )
            .outerjoin(unnested, true())
            .outerjoin(
                b,
                unnested.c.unnested == b.c.ref,
            )
        )
        self.assert_compile(
            stmt,
            "SELECT a.id, a.refs, unnested.unnested, unnested.ordinality "
            "FROM a "
            "LEFT OUTER JOIN unnest(a.refs) "
            "WITH ORDINALITY AS unnested(unnested, ordinality) ON true "
            "LEFT OUTER JOIN b ON unnested.unnested = b.ref",
        )

    def test_star_with_ordinality(self):
        """
        SELECT * FROM generate_series(4,1,-1) WITH ORDINALITY;
        """

        stmt = select("*").select_from(  # noqa
            func.generate_series(4, 1, -1).table_valued(
                with_ordinality="ordinality"
            )
        )
        self.assert_compile(
            stmt,
            "SELECT * FROM generate_series"
            "(:generate_series_1, :generate_series_2, :generate_series_3) "
            "WITH ORDINALITY AS anon_1",
        )

    def test_json_object_keys_with_ordinality(self):
        """
        SELECT * FROM json_object_keys('{"a1":"1","a2":"2","a3":"3"}')
        WITH ORDINALITY AS t(keys, n);
        """
        stmt = select("*").select_from(
            func.json_object_keys(
                literal({"a1": "1", "a2": "2", "a3": "3"}, type_=JSON)
            )
            .table_valued("keys", with_ordinality="n")
            .render_derived()
            .alias("t")
        )

        self.assert_compile(
            stmt,
            "SELECT * FROM json_object_keys(:param_1) "
            "WITH ORDINALITY AS t(keys, n)",
        )

    def test_alias_column(self):
        """

        ::

            SELECT x, y
            FROM
                generate_series(:generate_series_1, :generate_series_2) AS x,
                generate_series(:generate_series_3, :generate_series_4) AS y

        """

        x = func.generate_series(1, 2).alias("x")
        y = func.generate_series(3, 4).alias("y")
        stmt = select(x.column, y.column)

        self.assert_compile(
            stmt,
            "SELECT x, y FROM "
            "generate_series(:generate_series_1, :generate_series_2) AS x, "
            "generate_series(:generate_series_3, :generate_series_4) AS y",
        )

    def test_column_valued_one(self):
        fn = func.unnest(["one", "two", "three", "four"]).column_valued()

        stmt = select(fn)

        self.assert_compile(
            stmt, "SELECT anon_1 FROM unnest(:unnest_1) AS anon_1"
        )

    def test_column_valued_two(self):
        """

        ::

            SELECT x, y
            FROM
                generate_series(:generate_series_1, :generate_series_2) AS x,
                generate_series(:generate_series_3, :generate_series_4) AS y

        """

        x = func.generate_series(1, 2).column_valued("x")
        y = func.generate_series(3, 4).column_valued("y")
        stmt = select(x, y)

        self.assert_compile(
            stmt,
            "SELECT x, y FROM "
            "generate_series(:generate_series_1, :generate_series_2) AS x, "
            "generate_series(:generate_series_3, :generate_series_4) AS y",
        )

    def test_column_valued_subquery(self):
        x = func.generate_series(1, 2).column_valued("x")
        y = func.generate_series(3, 4).column_valued("y")
        subq = select(x, y).subquery()
        stmt = select(subq).where(subq.c.x > 2)

        self.assert_compile(
            stmt,
            "SELECT anon_1.x, anon_1.y FROM "
            "(SELECT x, y FROM "
            "generate_series(:generate_series_1, :generate_series_2) AS x, "
            "generate_series(:generate_series_3, :generate_series_4) AS y"
            ") AS anon_1 "
            "WHERE anon_1.x > :x_1",
        )

    @testing.combinations((True,), (False,))
    def test_render_derived_with_lateral(self, apply_alias_after_lateral):
        """
        # this is the "record" type

        SELECT
            table1.user_id AS table1_user_id,
            table2.name AS table2_name,
            jsonb_table.name AS jsonb_table_name,
            count(jsonb_table.time) AS count_1
            FROM table1
            JOIN table2 ON table1.user_id = table2.id
            JOIN LATERAL jsonb_to_recordset(table1.jsonb)
            AS jsonb_table(name TEXT, time FLOAT) ON true
            WHERE table2.route_id = %(route_id_1)s
            AND jsonb_table.name IN (%(name_1)s, %(name_2)s, %(name_3)s)
            GROUP BY table1.user_id, table2.name, jsonb_table.name
            ORDER BY table2.name

        """  # noqa

        table1 = table("table1", column("user_id"), column("jsonb"))
        table2 = table(
            "table2", column("id"), column("name"), column("route_id")
        )
        jsonb_table = func.jsonb_to_recordset(table1.c.jsonb).table_valued(
            column("name", Text), column("time", Float)
        )

        # I'm a little concerned about the naming, that lateral() and
        # alias() both make a new name unconditionally.  lateral() already
        # works this way, so try to just make sure .alias() after the
        # fact works too
        if apply_alias_after_lateral:
            jsonb_table = (
                jsonb_table.render_derived(with_types=True)
                .lateral()
                .alias("jsonb_table")
            )
        else:
            jsonb_table = jsonb_table.render_derived(with_types=True).lateral(
                "jsonb_table"
            )

        stmt = (
            select(
                table1.c.user_id,
                table2.c.name,
                jsonb_table.c.name.label("jsonb_table_name"),
                func.count(jsonb_table.c.time),
            )
            .select_from(table1)
            .join(table2, table1.c.user_id == table2.c.id)
            .join(jsonb_table, true())
            .where(table2.c.route_id == 5)
            .where(jsonb_table.c.name.in_(["n1", "n2", "n3"]))
            .group_by(table1.c.user_id, table2.c.name, jsonb_table.c.name)
            .order_by(table2.c.name)
        )

        self.assert_compile(
            stmt,
            "SELECT table1.user_id, table2.name, "
            "jsonb_table.name AS jsonb_table_name, "
            "count(jsonb_table.time) AS count_1 "
            "FROM table1 "
            "JOIN table2 ON table1.user_id = table2.id "
            "JOIN LATERAL jsonb_to_recordset(table1.jsonb) "
            "AS jsonb_table(name TEXT, time FLOAT) ON true "
            "WHERE table2.route_id = 5 "
            "AND jsonb_table.name IN ('n1', 'n2', 'n3') "
            "GROUP BY table1.user_id, table2.name, jsonb_table.name "
            "ORDER BY table2.name",
            literal_binds=True,
            render_postcompile=True,
        )

    def test_function_alias(self):
        """
        ::

            SELECT result_elem -> 'Field' as field
            FROM "check" AS check_, json_array_elements(
            (
                SELECT check_inside.response -> 'Results'
                FROM "check" as check_inside
                WHERE check_inside.id = check_.id
            )
            ) AS result_elem
            WHERE result_elem ->> 'Name' = 'FooBar'

        """
        check = table("check", column("id"), column("response", JSON))

        check_inside = check.alias("check_inside")
        check_outside = check.alias("_check")

        subq = (
            select(check_inside.c.response["Results"])
            .where(check_inside.c.id == check_outside.c.id)
            .scalar_subquery()
        )

        fn = func.json_array_elements(subq, type_=JSON).alias("result_elem")

        stmt = (
            select(fn.column["Field"].label("field"))
            .where(fn.column["Name"] == "FooBar")
            .select_from(check_outside)
        )

        self.assert_compile(
            stmt,
            "SELECT result_elem[:result_elem_1] AS field "
            "FROM json_array_elements("
            "(SELECT check_inside.response[:response_1] AS anon_1 "
            'FROM "check" AS check_inside '
            "WHERE check_inside.id = _check.id)"
            ') AS result_elem, "check" AS _check '
            "WHERE result_elem[:result_elem_2] = :param_1",
        )

    def test_named_table_valued(self):

        fn = (
            func.json_to_recordset(  # noqa
                '[{"a":1,"b":"foo"},{"a":"2","c":"bar"}]'
            )
            .table_valued(column("a", Integer), column("b", String))
            .render_derived(with_types=True)
        )

        stmt = select(fn.c.a, fn.c.b)

        self.assert_compile(
            stmt,
            "SELECT anon_1.a, anon_1.b "
            "FROM json_to_recordset(:json_to_recordset_1) "
            "AS anon_1(a INTEGER, b VARCHAR)",
        )

    def test_named_table_valued_subquery(self):

        fn = (
            func.json_to_recordset(  # noqa
                '[{"a":1,"b":"foo"},{"a":"2","c":"bar"}]'
            )
            .table_valued(column("a", Integer), column("b", String))
            .render_derived(with_types=True)
        )

        stmt = select(fn.c.a, fn.c.b).subquery()

        stmt = select(stmt)

        self.assert_compile(
            stmt,
            "SELECT anon_1.a, anon_1.b FROM "
            "(SELECT anon_2.a AS a, anon_2.b AS b "
            "FROM json_to_recordset(:json_to_recordset_1) "
            "AS anon_2(a INTEGER, b VARCHAR)"
            ") AS anon_1",
        )

    def test_named_table_valued_alias(self):

        """select * from json_to_recordset
        ('[{"a":1,"b":"foo"},{"a":"2","c":"bar"}]') as x(a int, b text);"""

        fn = (
            func.json_to_recordset(  # noqa
                '[{"a":1,"b":"foo"},{"a":"2","c":"bar"}]'
            )
            .table_valued(column("a", Integer), column("b", String))
            .render_derived(with_types=True)
            .alias("jbr")
        )

        stmt = select(fn.c.a, fn.c.b)

        self.assert_compile(
            stmt,
            "SELECT jbr.a, jbr.b "
            "FROM json_to_recordset(:json_to_recordset_1) "
            "AS jbr(a INTEGER, b VARCHAR)",
        )
