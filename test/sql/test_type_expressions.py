from sqlalchemy import Table, Column, String, func, MetaData, select, TypeDecorator, cast
from sqlalchemy.testing import fixtures, AssertsCompiledSQL
from sqlalchemy import testing
from sqlalchemy.testing import eq_


class _ExprFixture(object):

    def _fixture(self):
        class MyString(String):

            def bind_expression(self, bindvalue):
                return func.lower(bindvalue)

            def column_expression(self, col):
                return func.lower(col)

        test_table = Table(
            'test_table',
            MetaData(), Column('x', String), Column('y', MyString)
        )
        return test_table


class SelectTest(_ExprFixture, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_select_cols(self):
        table = self._fixture()

        self.assert_compile(
            select([table]),
            "SELECT test_table.x, lower(test_table.y) AS y FROM test_table"
        )

    def test_anonymous_expr(self):
        table = self._fixture()
        self.assert_compile(
            select([cast(table.c.y, String)]),
            "SELECT CAST(test_table.y AS VARCHAR) AS anon_1 FROM test_table"
        )

    def test_select_cols_use_labels(self):
        table = self._fixture()

        self.assert_compile(
            select([table]).apply_labels(),
            "SELECT test_table.x AS test_table_x, "
            "lower(test_table.y) AS test_table_y FROM test_table"
        )

    def test_select_cols_use_labels_result_map_targeting(self):
        table = self._fixture()

        compiled = select([table]).apply_labels().compile()
        assert table.c.y in compiled._create_result_map()['test_table_y'][1]
        assert table.c.x in compiled._create_result_map()['test_table_x'][1]

        # the lower() function goes into the result_map, we don't really
        # need this but it's fine
        self.assert_compile(
            compiled._create_result_map()['test_table_y'][1][3],
            "lower(test_table.y)"
        )
        # then the original column gets put in there as well.
        # as of 1.1 it's important that it is first as this is
        # taken as significant by the result processor.
        self.assert_compile(
            compiled._create_result_map()['test_table_y'][1][0],
            "test_table.y"
        )

    def test_insert_binds(self):
        table = self._fixture()

        self.assert_compile(
            table.insert(),
            "INSERT INTO test_table (x, y) VALUES (:x, lower(:y))"
        )

        self.assert_compile(
            table.insert().values(y="hi"),
            "INSERT INTO test_table (y) VALUES (lower(:y))"
        )

    def test_select_binds(self):
        table = self._fixture()
        self.assert_compile(
            select([table]).where(table.c.y == "hi"),
            "SELECT test_table.x, lower(test_table.y) AS y FROM "
            "test_table WHERE test_table.y = lower(:y_1)"
        )


class DerivedTest(_ExprFixture, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_select_from_select(self):
        table = self._fixture()
        self.assert_compile(
            table.select().select(),
            "SELECT x, lower(y) AS y FROM (SELECT test_table.x "
            "AS x, test_table.y AS y FROM test_table)"
        )

    def test_select_from_alias(self):
        table = self._fixture()
        self.assert_compile(
            table.select().alias().select(),
            "SELECT anon_1.x, lower(anon_1.y) AS y FROM (SELECT "
            "test_table.x AS x, test_table.y AS y "
            "FROM test_table) AS anon_1"
        )

    def test_select_from_aliased_join(self):
        table = self._fixture()
        s1 = table.select().alias()
        s2 = table.select().alias()
        j = s1.join(s2, s1.c.x == s2.c.x)
        s3 = j.select()
        self.assert_compile(
            s3, "SELECT anon_1.x, lower(anon_1.y) AS y, anon_2.x, "
            "lower(anon_2.y) AS y "
            "FROM (SELECT test_table.x AS x, test_table.y AS y "
            "FROM test_table) AS anon_1 JOIN (SELECT "
            "test_table.x AS x, test_table.y AS y "
            "FROM test_table) AS anon_2 ON anon_1.x = anon_2.x")


class RoundTripTestBase(object):

    def test_round_trip(self):
        testing.db.execute(
            self.tables.test_table.insert(),
            {"x": "X1", "y": "Y1"},
            {"x": "X2", "y": "Y2"},
            {"x": "X3", "y": "Y3"},
        )

        # test insert coercion alone
        eq_(
            testing.db.execute(
                "select * from test_table order by y").fetchall(),
            [
                ("X1", "y1"),
                ("X2", "y2"),
                ("X3", "y3"),
            ]
        )

        # conversion back to upper
        eq_(
            testing.db.execute(
                select([self.tables.test_table]).
                order_by(self.tables.test_table.c.y)
            ).fetchall(),
            [
                ("X1", "Y1"),
                ("X2", "Y2"),
                ("X3", "Y3"),
            ]
        )

    def test_targeting_no_labels(self):
        testing.db.execute(
            self.tables.test_table.insert(),
            {"x": "X1", "y": "Y1"},
        )
        row = testing.db.execute(select([self.tables.test_table])).first()
        eq_(
            row[self.tables.test_table.c.y],
            "Y1"
        )

    def test_targeting_by_string(self):
        testing.db.execute(
            self.tables.test_table.insert(),
            {"x": "X1", "y": "Y1"},
        )
        row = testing.db.execute(select([self.tables.test_table])).first()
        eq_(
            row["y"],
            "Y1"
        )

    def test_targeting_apply_labels(self):
        testing.db.execute(
            self.tables.test_table.insert(),
            {"x": "X1", "y": "Y1"},
        )
        row = testing.db.execute(select([self.tables.test_table]).
                                 apply_labels()).first()
        eq_(
            row[self.tables.test_table.c.y],
            "Y1"
        )

    def test_targeting_individual_labels(self):
        testing.db.execute(
            self.tables.test_table.insert(),
            {"x": "X1", "y": "Y1"},
        )
        row = testing.db.execute(select([
            self.tables.test_table.c.x.label('xbar'),
            self.tables.test_table.c.y.label('ybar')
        ])).first()
        eq_(
            row[self.tables.test_table.c.y],
            "Y1"
        )


class StringRoundTripTest(fixtures.TablesTest, RoundTripTestBase):

    @classmethod
    def define_tables(cls, metadata):
        class MyString(String):

            def bind_expression(self, bindvalue):
                return func.lower(bindvalue)

            def column_expression(self, col):
                return func.upper(col)

        Table(
            'test_table',
            metadata,
            Column('x', String(50)),
            Column('y', MyString(50)
                   )
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
            'test_table',
            metadata,
            Column('x', String(50)),
            Column('y', MyString(50)
                   )
        )


class ReturningTest(fixtures.TablesTest):
    __requires__ = 'returning',

    @classmethod
    def define_tables(cls, metadata):
        class MyString(String):

            def column_expression(self, col):
                return func.lower(col)

        Table(
            'test_table',
            metadata, Column('x', String(50)),
            Column('y', MyString(50), server_default="YVALUE")
        )

    @testing.provide_metadata
    def test_insert_returning(self):
        table = self.tables.test_table
        result = testing.db.execute(
            table.insert().returning(table.c.y),
            {"x": "xvalue"}
        )
        eq_(
            result.first(),
            ("yvalue",)
        )
