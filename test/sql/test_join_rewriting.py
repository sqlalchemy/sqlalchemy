from sqlalchemy import Table, Column, Integer, MetaData, ForeignKey, select
from sqlalchemy.testing import fixtures, AssertsCompiledSQL
from sqlalchemy import util
from sqlalchemy.engine import default


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

class JoinRewriteTest(fixtures.TestBase, AssertsCompiledSQL):
    @util.classproperty
    def __dialect__(cls):
        dialect = default.DefaultDialect()
        dialect.supports_right_nested_joins = False
        return dialect

    def test_one(self):
        j1 = b.join(c)
        j2 = a.join(j1)
        s = select([a, b, c], use_labels=True).\
            select_from(j2).\
            where(b.c.id == 2).\
            where(c.c.id == 3).order_by(a.c.id, b.c.id, c.c.id)

        self.assert_compile(
            s,
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

    def test_two_froms_overlapping_joins(self):
        j1 = b.join(c)
        j2 = b.join(c).select(use_labels=True).alias()
        j3 = a.join(j1)
        a_a = a.alias()
        j4 = a_a.join(j2)

        s = select([a, a_a, b, c, j2], use_labels=True).\
                select_from(j3).select_from(j4).order_by(j2.c.b_id)

        # this is the non-converted version
        """
        SELECT a.id AS a_id, a_1.id AS a_1_id, b.id AS b_id, b.a_id AS b_a_id,
            c.id AS c_id, c.b_id AS c_b_id, anon_1.b_id AS anon_1_b_id,
            anon_1.b_a_id AS anon_1_b_a_id,
            anon_1.c_id AS anon_1_c_id, anon_1.c_b_id AS anon_1_c_b_id

        FROM
            a JOIN (b JOIN c ON b.id = c.b_id) ON a.id = b.a_id,

            a AS a_1 JOIN (SELECT b.id AS b_id, b.a_id AS b_a_id, c.id AS c_id,
                        c.b_id AS c_b_id
                    FROM b JOIN c ON b.id = c.b_id
            ) AS anon_1 ON a_1.id = anon_1.b_a_id

        ORDER BY anon_1.b_id
        """

        """
        SELECT a.id AS a_id, a_1.id AS a_1_id, anon_1.b_id AS b_id,
        anon_1.b_a_id AS b_a_id, anon_1.c_id AS c_id, anon_1.c_b_id AS c_b_id,
        anon_2.b_id AS anon_2_b_id, anon_2.b_a_id AS anon_2_b_a_id,
        anon_2.c_id AS anon_2_c_id, anon_2.c_b_id AS anon_2_c_b_id

        FROM

            a JOIN (
                    SELECT b.id AS b_id, b.a_id AS b_a_id, c.id AS c_id,
                    c.b_id AS c_b_id
                    FROM b JOIN c ON b.id = c.b_id) AS anon_2 ON a.id = anon_2.b_a_id,

            a AS a_1 JOIN (
                        SELECT anon_2.b_id AS anon_2_b_id, anon_2.b_a_id AS anon_2_b_a_id,
                        anon_2.c_id AS anon_2_c_id, anon_2.c_b_id AS anon_2_c_b_id
                    FROM (
                        SELECT b.id AS b_id, b.a_id AS b_a_id, c.id AS c_id,
                        c.b_id AS c_b_id FROM b JOIN c ON b.id = c.b_id)
                        AS anon_2 JOIN (
                            SELECT b.id AS b_id, b.a_id AS b_a_id, c.id AS c_id, c.b_id AS c_b_id
                            FROM b JOIN c ON b.id = c.b_id) AS anon_2
                        ON anon_2.b_id = anon_2.c_b_id) AS anon_1 ON a_1.id = anon_1.b_a_id

            ORDER BY anon_1.b_id

        """

        self.assert_compile(
            s,
            ""
        )
