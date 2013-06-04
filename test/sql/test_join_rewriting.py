from sqlalchemy import Table, Column, Integer, MetaData, ForeignKey, select
from sqlalchemy.testing import fixtures, AssertsCompiledSQL
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy import testing


m = MetaData()

a = Table('a', m,
        Column('id', Integer, primary_key=True)
    )

b = Table('b', m,
        Column('id', Integer, primary_key=True),
        Column('a_id', Integer, ForeignKey('a.id'))
    )

c = Table('c', m,
        Column('id', Integer, primary_key=True),
        Column('b_id', Integer, ForeignKey('b.id'))
    )

d = Table('d', m,
        Column('id', Integer, primary_key=True),
        Column('c_id', Integer, ForeignKey('c.id'))
    )

e = Table('e', m,
        Column('id', Integer, primary_key=True)
    )

class _JoinRewriteTestBase(AssertsCompiledSQL):
    def _test(self, s, assert_):
        self.assert_compile(
            s,
            assert_
        )

    def test_a_bc(self):
        j1 = b.join(c)
        j2 = a.join(j1)

        # TODO: if we remove 'b' or 'c', shouldn't we get just
        # the subset of cols from anon_1 ?

        # TODO: do this test also with individual cols, things change
        # lots based on how you go with this

        s = select([a, b, c], use_labels=True).\
            select_from(j2).\
            where(b.c.id == 2).\
            where(c.c.id == 3).order_by(a.c.id, b.c.id, c.c.id)

        self._test(s, self._a_bc)

    def test_a__b_dc(self):
        j1 = c.join(d)
        j2 = b.join(j1)
        j3 = a.join(j2)

        s = select([a, b, c, d], use_labels=True).\
            select_from(j3).\
            where(b.c.id == 2).\
            where(c.c.id == 3).\
            where(d.c.id == 4).\
                order_by(a.c.id, b.c.id, c.c.id, d.c.id)

        self._test(
            s,
            self._a__b_dc
        )

    def test_a_bc_comma_a1_selbc(self):
        # test here we're emulating is
        # test.orm.inheritance.test_polymorphic_rel:PolymorphicJoinsTest.test_multi_join
        j1 = b.join(c)
        j2 = b.join(c).select(use_labels=True).alias()
        j3 = a.join(j1)
        a_a = a.alias()
        j4 = a_a.join(j2)

        s = select([a, a_a, b, c, j2], use_labels=True).\
                select_from(j3).select_from(j4).order_by(j2.c.b_id)

        self._test(
            s,
            self._a_bc_comma_a1_selbc
        )

class JoinRewriteTest(_JoinRewriteTestBase, fixtures.TestBase):
    """test rendering of each join with right-nested rewritten as
    aliased SELECT statements.."""

    @util.classproperty
    def __dialect__(cls):
        dialect = default.DefaultDialect()
        dialect.supports_right_nested_joins = False
        return dialect

    _a__b_dc = (
            "SELECT a.id AS a_id, anon_1.b_id AS b_id, "
            "anon_1.b_a_id AS b_a_id, anon_1.c_id AS c_id, "
            "anon_1.c_b_id AS c_b_id, anon_1.d_id AS d_id, "
            "anon_1.d_c_id AS d_c_id "
            "FROM a JOIN (SELECT b.id AS b_id, b.a_id AS b_a_id, "
            "anon_2.c_id AS c_id, anon_2.c_b_id AS c_b_id, "
            "anon_2.d_id AS d_id, anon_2.d_c_id AS d_c_id "
            "FROM b JOIN (SELECT c.id AS c_id, c.b_id AS c_b_id, "
            "d.id AS d_id, d.c_id AS d_c_id "
            "FROM c JOIN d ON c.id = d.c_id) AS anon_2 "
            "ON b.id = anon_2.c_b_id) AS anon_1 ON a.id = anon_1.b_a_id "
            "WHERE anon_1.b_id = :id_1 AND anon_1.c_id = :id_2 AND "
            "anon_1.d_id = :id_3 "
            "ORDER BY a.id, anon_1.b_id, anon_1.c_id, anon_1.d_id"
            )

    _a_bc = (
            "SELECT a.id AS a_id, anon_1.b_id AS b_id, "
            "anon_1.b_a_id AS b_a_id, anon_1.c_id AS c_id, "
            "anon_1.c_b_id AS c_b_id FROM a JOIN "
            "(SELECT b.id AS b_id, b.a_id AS b_a_id, "
                "c.id AS c_id, c.b_id AS c_b_id "
                "FROM b JOIN c ON b.id = c.b_id) AS anon_1 "
            "ON a.id = anon_1.b_a_id "
            "WHERE anon_1.b_id = :id_1 AND anon_1.c_id = :id_2 "
            "ORDER BY a.id, anon_1.b_id, anon_1.c_id"
            )

    _a_bc_comma_a1_selbc = (
            "SELECT a.id AS a_id, a_1.id AS a_1_id, anon_1.b_id AS b_id, "
            "anon_1.b_a_id AS b_a_id, anon_1.c_id AS c_id, "
            "anon_1.c_b_id AS c_b_id, anon_2.b_id AS anon_2_b_id, "
            "anon_2.b_a_id AS anon_2_b_a_id, anon_2.c_id AS anon_2_c_id, "
            "anon_2.c_b_id AS anon_2_c_b_id FROM a "
            "JOIN (SELECT b.id AS b_id, b.a_id AS b_a_id, c.id AS c_id, "
            "c.b_id AS c_b_id FROM b JOIN c ON b.id = c.b_id) AS anon_1 "
            "ON a.id = anon_1.b_a_id, "
            "a AS a_1 JOIN "
                "(SELECT b.id AS b_id, b.a_id AS b_a_id, "
                "c.id AS c_id, c.b_id AS c_b_id "
                "FROM b JOIN c ON b.id = c.b_id) AS anon_2 "
            "ON a_1.id = anon_2.b_a_id ORDER BY anon_2.b_id"
        )

class JoinPlainTest(_JoinRewriteTestBase, fixtures.TestBase):
    """test rendering of each join with normal nesting."""
    @util.classproperty
    def __dialect__(cls):
        dialect = default.DefaultDialect()
        return dialect

    _a__b_dc = (
            "SELECT a.id AS a_id, b.id AS b_id, "
            "b.a_id AS b_a_id, c.id AS c_id, "
            "c.b_id AS c_b_id, d.id AS d_id, "
            "d.c_id AS d_c_id "
            "FROM a JOIN (b JOIN (c JOIN d ON c.id = d.c_id) "
            "ON b.id = c.b_id) ON a.id = b.a_id "
            "WHERE b.id = :id_1 AND c.id = :id_2 AND "
            "d.id = :id_3 "
            "ORDER BY a.id, b.id, c.id, d.id"
            )


    _a_bc = (
            "SELECT a.id AS a_id, b.id AS b_id, "
            "b.a_id AS b_a_id, c.id AS c_id, "
            "c.b_id AS c_b_id FROM a JOIN "
            "(b JOIN c ON b.id = c.b_id) "
            "ON a.id = b.a_id "
            "WHERE b.id = :id_1 AND c.id = :id_2 "
            "ORDER BY a.id, b.id, c.id"
            )

    _a_bc_comma_a1_selbc = (
            "SELECT a.id AS a_id, a_1.id AS a_1_id, b.id AS b_id, "
            "b.a_id AS b_a_id, c.id AS c_id, "
            "c.b_id AS c_b_id, anon_1.b_id AS anon_1_b_id, "
            "anon_1.b_a_id AS anon_1_b_a_id, anon_1.c_id AS anon_1_c_id, "
            "anon_1.c_b_id AS anon_1_c_b_id FROM a "
            "JOIN (b JOIN c ON b.id = c.b_id) "
            "ON a.id = b.a_id, "
            "a AS a_1 JOIN "
                "(SELECT b.id AS b_id, b.a_id AS b_a_id, "
                "c.id AS c_id, c.b_id AS c_b_id "
                "FROM b JOIN c ON b.id = c.b_id) AS anon_1 "
            "ON a_1.id = anon_1.b_a_id ORDER BY anon_1.b_id"
        )

class JoinNoUseLabelsTest(_JoinRewriteTestBase, fixtures.TestBase):
    @util.classproperty
    def __dialect__(cls):
        dialect = default.DefaultDialect()
        dialect.supports_right_nested_joins = False
        return dialect

    def _test(self, s, assert_):
        s.use_labels = False
        self.assert_compile(
            s,
            assert_
        )

    _a__b_dc = (
            "SELECT a.id, b.id, "
            "b.a_id, c.id, "
            "c.b_id, d.id, "
            "d.c_id "
            "FROM a JOIN (b JOIN (c JOIN d ON c.id = d.c_id) "
            "ON b.id = c.b_id) ON a.id = b.a_id "
            "WHERE b.id = :id_1 AND c.id = :id_2 AND "
            "d.id = :id_3 "
            "ORDER BY a.id, b.id, c.id, d.id"
            )

    _a_bc = (
            "SELECT a.id, b.id, "
            "b.a_id, c.id, "
            "c.b_id FROM a JOIN "
            "(b JOIN c ON b.id = c.b_id) "
            "ON a.id = b.a_id "
            "WHERE b.id = :id_1 AND c.id = :id_2 "
            "ORDER BY a.id, b.id, c.id"
            )

    _a_bc_comma_a1_selbc = (
            "SELECT a.id, a_1.id, b.id, "
            "b.a_id, c.id, "
            "c.b_id, anon_1.b_id, "
            "anon_1.b_a_id, anon_1.c_id, "
            "anon_1.c_b_id FROM a "
            "JOIN (b JOIN c ON b.id = c.b_id) "
            "ON a.id = b.a_id, "
            "a AS a_1 JOIN "
                "(SELECT b.id AS b_id, b.a_id AS b_a_id, "
                "c.id AS c_id, c.b_id AS c_b_id "
                "FROM b JOIN c ON b.id = c.b_id) AS anon_1 "
            "ON a_1.id = anon_1.b_a_id ORDER BY anon_1.b_id"
        )

class JoinExecTest(_JoinRewriteTestBase, fixtures.TestBase):
    """invoke the SQL on the current backend to ensure compatibility"""

    _a_bc = _a_bc_comma_a1_selbc = _a__b_dc = None

    @classmethod
    def setup_class(cls):
        m.create_all(testing.db)

    @classmethod
    def teardown_class(cls):
        m.drop_all(testing.db)

    def _test(self, selectable, assert_):
        testing.db.execute(selectable)


class DialectFlagTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_dialect_flag(self):
        d1 = default.DefaultDialect(supports_right_nested_joins=True)
        d2 = default.DefaultDialect(supports_right_nested_joins=False)

        j1 = b.join(c)
        j2 = a.join(j1)

        s = select([a, b, c], use_labels=True).\
            select_from(j2)

        self.assert_compile(
            s,
            "SELECT a.id AS a_id, b.id AS b_id, b.a_id AS b_a_id, c.id AS c_id, "
            "c.b_id AS c_b_id FROM a JOIN (b JOIN c ON b.id = c.b_id) "
            "ON a.id = b.a_id",
            dialect=d1
        )
        self.assert_compile(
            s,
            "SELECT a.id AS a_id, anon_1.b_id AS b_id, "
            "anon_1.b_a_id AS b_a_id, "
            "anon_1.c_id AS c_id, anon_1.c_b_id AS c_b_id "
            "FROM a JOIN (SELECT b.id AS b_id, b.a_id AS b_a_id, c.id AS c_id, "
            "c.b_id AS c_b_id FROM b JOIN c ON b.id = c.b_id) AS anon_1 "
            "ON a.id = anon_1.b_a_id",
            dialect=d2
        )
