from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import TypeDecorator
from sqlalchemy import union
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class _ExprFixture(object):
    def _test_table(self, type_):
        test_table = Table(
            "test_table", MetaData(), Column("x", String), Column("y", type_)
        )
        return test_table

    def _fixture(self):
        class MyString(String):

            # supercedes any processing that might be on
            # String
            def bind_expression(self, bindvalue):
                return func.lower(bindvalue)

            def column_expression(self, col):
                return func.lower(col)

        return self._test_table(MyString)

    def _type_decorator_outside_fixture(self):
        class MyString(TypeDecorator):
            impl = String

            def bind_expression(self, bindvalue):
                return func.outside_bind(bindvalue)

            def column_expression(self, col):
                return func.outside_colexpr(col)

        return self._test_table(MyString)

    def _type_decorator_inside_fixture(self):
        class MyInsideString(String):
            def bind_expression(self, bindvalue):
                return func.inside_bind(bindvalue)

            def column_expression(self, col):
                return func.inside_colexpr(col)

        class MyString(TypeDecorator):
            impl = MyInsideString

        return self._test_table(MyString)

    def _type_decorator_both_fixture(self):
        class MyDialectString(String):
            def bind_expression(self, bindvalue):
                return func.inside_bind(bindvalue)

            def column_expression(self, col):
                return func.inside_colexpr(col)

        class MyString(TypeDecorator):
            impl = String

            # this works because when the compiler calls dialect_impl(),
            # a copy of MyString is created which has just this impl
            # as self.impl
            def load_dialect_impl(self, dialect):
                return MyDialectString()

            # user-defined methods need to invoke explicitly on the impl
            # for now...
            def bind_expression(self, bindvalue):
                return func.outside_bind(self.impl.bind_expression(bindvalue))

            def column_expression(self, col):
                return func.outside_colexpr(self.impl.column_expression(col))

        return self._test_table(MyString)

    def _variant_fixture(self, inner_fixture):
        type_ = inner_fixture.c.y.type

        variant = String().with_variant(type_, "default")
        return self._test_table(variant)

    def _dialect_level_fixture(self):
        class ImplString(String):
            def bind_expression(self, bindvalue):
                return func.dialect_bind(bindvalue)

            def column_expression(self, col):
                return func.dialect_colexpr(col)

        from sqlalchemy.engine import default

        dialect = default.DefaultDialect()
        dialect.colspecs = {String: ImplString}
        return dialect


class SelectTest(_ExprFixture, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_select_cols(self):
        table = self._fixture()

        self.assert_compile(
            select(table),
            "SELECT test_table.x, lower(test_table.y) AS y FROM test_table",
        )

    def test_anonymous_expr(self):
        table = self._fixture()
        self.assert_compile(
            select(cast(table.c.y, String)),
            "SELECT CAST(test_table.y AS VARCHAR) AS y FROM test_table",
        )

    def test_select_cols_use_labels(self):
        table = self._fixture()

        self.assert_compile(
            select(table).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT test_table.x AS test_table_x, "
            "lower(test_table.y) AS test_table_y FROM test_table",
        )

    def test_select_cols_use_labels_result_map_targeting(self):
        table = self._fixture()

        compiled = (
            select(table)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .compile()
        )
        assert table.c.y in compiled._create_result_map()["test_table_y"][1]
        assert table.c.x in compiled._create_result_map()["test_table_x"][1]

        # the lower() function goes into the result_map, we don't really
        # need this but it's fine
        self.assert_compile(
            compiled._create_result_map()["test_table_y"][1][3],
            "lower(test_table.y)",
        )
        # then the original column gets put in there as well.
        # as of 1.1 it's important that it is first as this is
        # taken as significant by the result processor.
        self.assert_compile(
            compiled._create_result_map()["test_table_y"][1][0], "test_table.y"
        )

    def test_insert_binds(self):
        table = self._fixture()

        self.assert_compile(
            table.insert(),
            "INSERT INTO test_table (x, y) VALUES (:x, lower(:y))",
        )

        self.assert_compile(
            table.insert().values(y="hi"),
            "INSERT INTO test_table (y) VALUES (lower(:y))",
        )

    def test_select_binds(self):
        table = self._fixture()
        self.assert_compile(
            select(table).where(table.c.y == "hi"),
            "SELECT test_table.x, lower(test_table.y) AS y FROM "
            "test_table WHERE test_table.y = lower(:y_1)",
        )

    def test_dialect(self):
        table = self._fixture()
        dialect = self._dialect_level_fixture()

        # 'x' is straight String
        self.assert_compile(
            select(table.c.x).where(table.c.x == "hi"),
            "SELECT dialect_colexpr(test_table.x) AS x "
            "FROM test_table WHERE test_table.x = dialect_bind(:x_1)",
            dialect=dialect,
        )

    def test_type_decorator_inner(self):
        table = self._type_decorator_inside_fixture()

        self.assert_compile(
            select(table).where(table.c.y == "hi"),
            "SELECT test_table.x, inside_colexpr(test_table.y) AS y "
            "FROM test_table WHERE test_table.y = inside_bind(:y_1)",
        )

    def test_type_decorator_inner_plus_dialect(self):
        table = self._type_decorator_inside_fixture()
        dialect = self._dialect_level_fixture()

        # for "inner", the MyStringImpl is a subclass of String, #
        # so a dialect-level
        # implementation supersedes that, which is the same as with other
        # processor functions
        self.assert_compile(
            select(table).where(table.c.y == "hi"),
            "SELECT dialect_colexpr(test_table.x) AS x, "
            "dialect_colexpr(test_table.y) AS y FROM test_table "
            "WHERE test_table.y = dialect_bind(:y_1)",
            dialect=dialect,
        )

    def test_type_decorator_outer(self):
        table = self._type_decorator_outside_fixture()

        self.assert_compile(
            select(table).where(table.c.y == "hi"),
            "SELECT test_table.x, outside_colexpr(test_table.y) AS y "
            "FROM test_table WHERE test_table.y = outside_bind(:y_1)",
        )

    def test_type_decorator_outer_plus_dialect(self):
        table = self._type_decorator_outside_fixture()
        dialect = self._dialect_level_fixture()

        # for "outer", the MyString isn't calling the "impl" functions,
        # so we don't get the "impl"
        self.assert_compile(
            select(table).where(table.c.y == "hi"),
            "SELECT dialect_colexpr(test_table.x) AS x, "
            "outside_colexpr(test_table.y) AS y "
            "FROM test_table WHERE test_table.y = outside_bind(:y_1)",
            dialect=dialect,
        )

    def test_type_decorator_both(self):
        table = self._type_decorator_both_fixture()

        self.assert_compile(
            select(table).where(table.c.y == "hi"),
            "SELECT test_table.x, "
            "outside_colexpr(inside_colexpr(test_table.y)) AS y "
            "FROM test_table WHERE "
            "test_table.y = outside_bind(inside_bind(:y_1))",
        )

    def test_type_decorator_both_plus_dialect(self):
        table = self._type_decorator_both_fixture()
        dialect = self._dialect_level_fixture()

        # for "inner", the MyStringImpl is a subclass of String,
        # so a dialect-level
        # implementation supersedes that, which is the same as with other
        # processor functions
        self.assert_compile(
            select(table).where(table.c.y == "hi"),
            "SELECT dialect_colexpr(test_table.x) AS x, "
            "outside_colexpr(dialect_colexpr(test_table.y)) AS y "
            "FROM test_table WHERE "
            "test_table.y = outside_bind(dialect_bind(:y_1))",
            dialect=dialect,
        )

    def test_type_decorator_both_w_variant(self):
        table = self._variant_fixture(self._type_decorator_both_fixture())

        self.assert_compile(
            select(table).where(table.c.y == "hi"),
            "SELECT test_table.x, "
            "outside_colexpr(inside_colexpr(test_table.y)) AS y "
            "FROM test_table WHERE "
            "test_table.y = outside_bind(inside_bind(:y_1))",
        )

    def test_compound_select(self):
        table = self._fixture()

        s1 = select(table).where(table.c.y == "hi")
        s2 = select(table).where(table.c.y == "there")

        self.assert_compile(
            union(s1, s2),
            "SELECT test_table.x, lower(test_table.y) AS y "
            "FROM test_table WHERE test_table.y = lower(:y_1) "
            "UNION SELECT test_table.x, lower(test_table.y) AS y "
            "FROM test_table WHERE test_table.y = lower(:y_2)",
        )

    def test_select_of_compound_select(self):
        table = self._fixture()

        s1 = select(table).where(table.c.y == "hi")
        s2 = select(table).where(table.c.y == "there")

        self.assert_compile(
            union(s1, s2).alias().select(),
            "SELECT anon_1.x, lower(anon_1.y) AS y FROM "
            "(SELECT test_table.x AS x, test_table.y AS y "
            "FROM test_table WHERE test_table.y = lower(:y_1) "
            "UNION SELECT test_table.x AS x, test_table.y AS y "
            "FROM test_table WHERE test_table.y = lower(:y_2)) AS anon_1",
        )


class DerivedTest(_ExprFixture, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_select_from_select(self):
        table = self._fixture()
        self.assert_compile(
            table.select().subquery().select(),
            "SELECT anon_1.x, lower(anon_1.y) AS y FROM "
            "(SELECT test_table.x "
            "AS x, test_table.y AS y FROM test_table) AS anon_1",
        )

    def test_select_from_aliased_join(self):
        table = self._fixture()
        s1 = table.select().alias()
        s2 = table.select().alias()
        j = s1.join(s2, s1.c.x == s2.c.x)
        s3 = j.select()
        self.assert_compile(
            s3,
            "SELECT anon_1.x, lower(anon_1.y) AS y, anon_2.x AS x_1, "
            "lower(anon_2.y) AS y_1 "
            "FROM (SELECT test_table.x AS x, test_table.y AS y "
            "FROM test_table) AS anon_1 JOIN (SELECT "
            "test_table.x AS x, test_table.y AS y "
            "FROM test_table) AS anon_2 ON anon_1.x = anon_2.x",
        )


class RoundTripTestBase(object):
    def test_round_trip(self, connection):
        connection.execute(
            self.tables.test_table.insert(),
            [
                {"x": "X1", "y": "Y1"},
                {"x": "X2", "y": "Y2"},
                {"x": "X3", "y": "Y3"},
            ],
        )

        # test insert coercion alone
        eq_(
            connection.exec_driver_sql(
                "select * from test_table order by y"
            ).fetchall(),
            [("X1", "y1"), ("X2", "y2"), ("X3", "y3")],
        )

        # conversion back to upper
        eq_(
            connection.execute(
                select(self.tables.test_table).order_by(
                    self.tables.test_table.c.y
                )
            ).fetchall(),
            [("X1", "Y1"), ("X2", "Y2"), ("X3", "Y3")],
        )

    def test_targeting_no_labels(self, connection):
        connection.execute(
            self.tables.test_table.insert(), {"x": "X1", "y": "Y1"}
        )
        row = connection.execute(select(self.tables.test_table)).first()
        eq_(row._mapping[self.tables.test_table.c.y], "Y1")

    def test_targeting_by_string(self, connection):
        connection.execute(
            self.tables.test_table.insert(), {"x": "X1", "y": "Y1"}
        )
        row = connection.execute(select(self.tables.test_table)).first()
        eq_(row._mapping["y"], "Y1")

    def test_targeting_apply_labels(self, connection):
        connection.execute(
            self.tables.test_table.insert(), {"x": "X1", "y": "Y1"}
        )
        row = connection.execute(
            select(self.tables.test_table).set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            )
        ).first()
        eq_(row._mapping[self.tables.test_table.c.y], "Y1")

    def test_targeting_individual_labels(self, connection):
        connection.execute(
            self.tables.test_table.insert(), {"x": "X1", "y": "Y1"}
        )
        row = connection.execute(
            select(
                self.tables.test_table.c.x.label("xbar"),
                self.tables.test_table.c.y.label("ybar"),
            )
        ).first()
        eq_(row._mapping[self.tables.test_table.c.y], "Y1")


class StringRoundTripTest(fixtures.TablesTest, RoundTripTestBase):
    @classmethod
    def define_tables(cls, metadata):
        class MyString(String):
            def bind_expression(self, bindvalue):
                return func.lower(bindvalue)

            def column_expression(self, col):
                return func.upper(col)

        Table(
            "test_table",
            metadata,
            Column("x", String(50)),
            Column("y", MyString(50)),
        )


class TypeDecRoundTripTest(fixtures.TablesTest, RoundTripTestBase):
    @classmethod
    def define_tables(cls, metadata):
        class MyString(TypeDecorator):
            impl = String

            def bind_expression(self, bindvalue):
                return func.lower(bindvalue)

            def column_expression(self, col):
                return func.upper(col)

        Table(
            "test_table",
            metadata,
            Column("x", String(50)),
            Column("y", MyString(50)),
        )


class ReturningTest(fixtures.TablesTest):
    __requires__ = ("returning",)

    @classmethod
    def define_tables(cls, metadata):
        class MyString(String):
            def column_expression(self, col):
                return func.lower(col)

        Table(
            "test_table",
            metadata,
            Column("x", String(50)),
            Column("y", MyString(50), server_default="YVALUE"),
        )

    @testing.provide_metadata
    def test_insert_returning(self, connection):
        table = self.tables.test_table
        result = connection.execute(
            table.insert().returning(table.c.y), {"x": "xvalue"}
        )
        eq_(result.first(), ("yvalue",))
