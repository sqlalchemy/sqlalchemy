from sqlalchemy import extract
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy.dialects import sybase
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import fixtures


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = sybase.dialect()

    def test_extract(self):
        t = sql.table("t", sql.column("col1"))

        mapping = {
            "day": "day",
            "doy": "dayofyear",
            "dow": "weekday",
            "milliseconds": "millisecond",
            "millisecond": "millisecond",
            "year": "year",
        }

        for field, subst in list(mapping.items()):
            self.assert_compile(
                select(extract(field, t.c.col1)),
                'SELECT DATEPART("%s", t.col1) AS anon_1 FROM t' % subst,
            )

    def test_limit_offset(self):
        stmt = select(1).limit(5).offset(6)
        assert stmt.compile().params == {"param_1": 5, "param_2": 6}
        self.assert_compile(
            stmt, "SELECT 1 ROWS LIMIT :param_1 OFFSET :param_2"
        )

    def test_offset(self):
        stmt = select(1).offset(10)
        assert stmt.compile().params == {"param_1": 10}
        self.assert_compile(stmt, "SELECT 1 ROWS OFFSET :param_1")

    def test_limit(self):
        stmt = select(1).limit(5)
        assert stmt.compile().params == {"param_1": 5}
        self.assert_compile(stmt, "SELECT 1 ROWS LIMIT :param_1")

    def test_delete_extra_froms(self):
        t1 = sql.table("t1", sql.column("c1"))
        t2 = sql.table("t2", sql.column("c1"))
        q = sql.delete(t1).where(t1.c.c1 == t2.c.c1)
        self.assert_compile(
            q, "DELETE FROM t1 FROM t1, t2 WHERE t1.c1 = t2.c1"
        )

    def test_delete_extra_froms_alias(self):
        a1 = sql.table("t1", sql.column("c1")).alias("a1")
        t2 = sql.table("t2", sql.column("c1"))
        q = sql.delete(a1).where(a1.c.c1 == t2.c.c1)
        self.assert_compile(
            q, "DELETE FROM a1 FROM t1 AS a1, t2 WHERE a1.c1 = t2.c1"
        )
        self.assert_compile(sql.delete(a1), "DELETE FROM t1 AS a1")
