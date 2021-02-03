# coding: utf-8
from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import Computed
from sqlalchemy import exc
from sqlalchemy import except_
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import outerjoin
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import type_coerce
from sqlalchemy import TypeDecorator
from sqlalchemy import union
from sqlalchemy.dialects.oracle import base as oracle
from sqlalchemy.dialects.oracle import cx_oracle
from sqlalchemy.engine import default
from sqlalchemy.sql import column
from sqlalchemy.sql import ddl
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql import table
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "oracle"

    def test_true_false(self):
        self.assert_compile(sql.false(), "0")
        self.assert_compile(sql.true(), "1")

    def test_owner(self):
        meta = MetaData()
        parent = Table(
            "parent",
            meta,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            schema="ed",
        )
        child = Table(
            "child",
            meta,
            Column("id", Integer, primary_key=True),
            Column("parent_id", Integer, ForeignKey("ed.parent.id")),
            schema="ed",
        )
        self.assert_compile(
            parent.join(child),
            "ed.parent JOIN ed.child ON ed.parent.id = " "ed.child.parent_id",
        )

    def test_subquery(self):
        t = table("sometable", column("col1"), column("col2"))
        s = select(t).subquery()
        s = select(s.c.col1, s.c.col2)

        self.assert_compile(
            s,
            "SELECT anon_1.col1, anon_1.col2 FROM (SELECT "
            "sometable.col1 AS col1, sometable.col2 "
            "AS col2 FROM sometable) anon_1",
        )

    def test_bindparam_quote(self):
        """test that bound parameters take on quoting for reserved words,
        column names quote flag enabled."""
        # note: this is only in cx_oracle at the moment.  not sure
        # what other hypothetical oracle dialects might need

        self.assert_compile(bindparam("option"), ':"option"')
        self.assert_compile(bindparam("plain"), ":plain")
        t = Table("s", MetaData(), Column("plain", Integer, quote=True))
        self.assert_compile(
            t.insert().values(plain=5),
            'INSERT INTO s ("plain") VALUES (:"plain")',
        )
        self.assert_compile(
            t.update().values(plain=5), 'UPDATE s SET "plain"=:"plain"'
        )

    def test_bindparam_quote_works_on_expanding(self):
        self.assert_compile(
            bindparam("uid", expanding=True),
            "([POSTCOMPILE_uid])",
            dialect=cx_oracle.dialect(),
        )

    def test_cte(self):
        part = table(
            "part", column("part"), column("sub_part"), column("quantity")
        )

        included_parts = (
            select(part.c.sub_part, part.c.part, part.c.quantity)
            .where(part.c.part == "p1")
            .cte(name="included_parts", recursive=True)
            .suffix_with(
                "search depth first by part set ord1",
                "cycle part set y_cycle to 1 default 0",
                dialect="oracle",
            )
        )

        incl_alias = included_parts.alias("pr1")
        parts_alias = part.alias("p")
        included_parts = included_parts.union_all(
            select(
                parts_alias.c.sub_part,
                parts_alias.c.part,
                parts_alias.c.quantity,
            ).where(parts_alias.c.part == incl_alias.c.sub_part)
        )

        q = select(
            included_parts.c.sub_part,
            func.sum(included_parts.c.quantity).label("total_quantity"),
        ).group_by(included_parts.c.sub_part)

        self.assert_compile(
            q,
            "WITH included_parts(sub_part, part, quantity) AS "
            "(SELECT part.sub_part AS sub_part, part.part AS part, "
            "part.quantity AS quantity FROM part WHERE part.part = :part_1 "
            "UNION ALL SELECT p.sub_part AS sub_part, p.part AS part, "
            "p.quantity AS quantity FROM part p, included_parts pr1 "
            "WHERE p.part = pr1.sub_part) "
            "search depth first by part set ord1 cycle part set "
            "y_cycle to 1 default 0  "
            "SELECT included_parts.sub_part, sum(included_parts.quantity) "
            "AS total_quantity FROM included_parts "
            "GROUP BY included_parts.sub_part",
        )

    def test_limit_one(self):
        t = table("sometable", column("col1"), column("col2"))
        s = select(t)
        c = s.compile(dialect=oracle.OracleDialect())
        assert t.c.col1 in set(c._create_result_map()["col1"][1])
        s = select(t).limit(10).offset(20)
        self.assert_compile(
            s,
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT anon_2.col1 AS col1, "
            "anon_2.col2 AS col2, ROWNUM AS ora_rn FROM (SELECT "
            "sometable.col1 AS col1, sometable.col2 AS "
            "col2 FROM sometable) anon_2 WHERE ROWNUM <= "
            "[POSTCOMPILE_param_1]) anon_1 WHERE ora_rn > "
            "[POSTCOMPILE_param_2]",
            checkparams={"param_1": 30, "param_2": 20},
        )

        c = s.compile(dialect=oracle.OracleDialect())
        eq_(len(c._result_columns), 2)
        assert t.c.col1 in set(c._create_result_map()["col1"][1])

    def test_limit_one_firstrows(self):
        t = table("sometable", column("col1"), column("col2"))
        s = select(t)
        s = select(t).limit(10).offset(20)
        self.assert_compile(
            s,
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT /*+ FIRST_ROWS([POSTCOMPILE_ora_frow_1]) */ "
            "anon_2.col1 AS col1, "
            "anon_2.col2 AS col2, ROWNUM AS ora_rn FROM (SELECT "
            "sometable.col1 AS col1, sometable.col2 AS "
            "col2 FROM sometable) anon_2 WHERE ROWNUM <= "
            "[POSTCOMPILE_param_1]) anon_1 WHERE ora_rn > "
            "[POSTCOMPILE_param_2]",
            checkparams={"ora_frow_1": 10, "param_1": 30, "param_2": 20},
            dialect=oracle.OracleDialect(optimize_limits=True),
        )

    def test_limit_two(self):
        t = table("sometable", column("col1"), column("col2"))
        s = select(t).limit(10).offset(20).subquery()

        s2 = select(s.c.col1, s.c.col2)
        self.assert_compile(
            s2,
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT anon_2.col1 AS col1, "
            "anon_2.col2 AS col2 "
            "FROM (SELECT anon_3.col1 AS col1, anon_3.col2 AS col2, "
            "ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, "
            "sometable.col2 AS col2 FROM sometable) anon_3 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]) anon_2 "
            "WHERE ora_rn > [POSTCOMPILE_param_2]) anon_1",
            checkparams={"param_1": 30, "param_2": 20},
        )

        self.assert_compile(
            s2,
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT anon_2.col1 AS col1, "
            "anon_2.col2 AS col2 "
            "FROM (SELECT anon_3.col1 AS col1, anon_3.col2 AS col2, "
            "ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, "
            "sometable.col2 AS col2 FROM sometable) anon_3 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]) anon_2 "
            "WHERE ora_rn > [POSTCOMPILE_param_2]) anon_1",
        )
        c = s2.compile(dialect=oracle.OracleDialect())
        eq_(len(c._result_columns), 2)
        assert s.c.col1 in set(c._create_result_map()["col1"][1])

    def test_limit_three(self):
        t = table("sometable", column("col1"), column("col2"))

        s = select(t).limit(10).offset(20).order_by(t.c.col2)
        self.assert_compile(
            s,
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT anon_2.col1 AS col1, "
            "anon_2.col2 AS col2, ROWNUM AS ora_rn FROM (SELECT "
            "sometable.col1 AS col1, sometable.col2 AS "
            "col2 FROM sometable ORDER BY "
            "sometable.col2) anon_2 WHERE ROWNUM <= "
            "[POSTCOMPILE_param_1]) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_2]",
            checkparams={"param_1": 30, "param_2": 20},
        )
        c = s.compile(dialect=oracle.OracleDialect())
        eq_(len(c._result_columns), 2)
        assert t.c.col1 in set(c._create_result_map()["col1"][1])

    def test_limit_four(self):
        t = table("sometable", column("col1"), column("col2"))

        s = select(t).with_for_update().limit(10).order_by(t.c.col2)
        self.assert_compile(
            s,
            "SELECT anon_1.col1, anon_1.col2 FROM (SELECT "
            "sometable.col1 AS col1, sometable.col2 AS "
            "col2 FROM sometable ORDER BY "
            "sometable.col2) anon_1 WHERE ROWNUM <= [POSTCOMPILE_param_1] "
            "FOR UPDATE",
            checkparams={"param_1": 10},
        )

    def test_limit_four_firstrows(self):
        t = table("sometable", column("col1"), column("col2"))

        s = select(t).with_for_update().limit(10).order_by(t.c.col2)
        self.assert_compile(
            s,
            "SELECT /*+ FIRST_ROWS([POSTCOMPILE_ora_frow_1]) */ "
            "anon_1.col1, anon_1.col2 FROM (SELECT "
            "sometable.col1 AS col1, sometable.col2 AS "
            "col2 FROM sometable ORDER BY "
            "sometable.col2) anon_1 WHERE ROWNUM <= [POSTCOMPILE_param_1] "
            "FOR UPDATE",
            checkparams={"param_1": 10, "ora_frow_1": 10},
            dialect=oracle.OracleDialect(optimize_limits=True),
        )

    def test_limit_five(self):
        t = table("sometable", column("col1"), column("col2"))

        s = select(t).with_for_update().limit(10).offset(20).order_by(t.c.col2)
        self.assert_compile(
            s,
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT anon_2.col1 AS col1, "
            "anon_2.col2 AS col2, ROWNUM AS ora_rn FROM (SELECT "
            "sometable.col1 AS col1, sometable.col2 AS "
            "col2 FROM sometable ORDER BY "
            "sometable.col2) anon_2 WHERE ROWNUM <= "
            "[POSTCOMPILE_param_1]) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_2] FOR "
            "UPDATE",
            checkparams={"param_1": 30, "param_2": 20},
        )

    def test_limit_six(self):
        t = table("sometable", column("col1"), column("col2"))

        s = (
            select(t)
            .limit(10)
            .offset(literal(10) + literal(20))
            .order_by(t.c.col2)
        )
        self.assert_compile(
            s,
            "SELECT anon_1.col1, anon_1.col2 FROM (SELECT anon_2.col1 AS "
            "col1, anon_2.col2 AS col2, ROWNUM AS ora_rn FROM "
            "(SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable ORDER BY sometable.col2) anon_2 WHERE "
            "ROWNUM <= :param_1 + :param_2 + :param_3) anon_1 "
            "WHERE ora_rn > :param_2 + :param_3",
            checkparams={"param_1": 10, "param_2": 10, "param_3": 20},
        )

    def test_limit_special_quoting(self):
        """Oracle-specific test for #4730.

        Even though this issue is generic, test the originally reported Oracle
        use case.

        """

        col = literal_column("SUM(ABC)").label("SUM(ABC)")
        tbl = table("my_table")
        query = select(col).select_from(tbl).order_by(col).limit(100)

        self.assert_compile(
            query,
            'SELECT anon_1."SUM(ABC)" FROM '
            '(SELECT SUM(ABC) AS "SUM(ABC)" '
            "FROM my_table ORDER BY SUM(ABC)) anon_1 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]",
        )

        col = literal_column("SUM(ABC)").label(quoted_name("SUM(ABC)", True))
        tbl = table("my_table")
        query = select(col).select_from(tbl).order_by(col).limit(100)

        self.assert_compile(
            query,
            'SELECT anon_1."SUM(ABC)" FROM '
            '(SELECT SUM(ABC) AS "SUM(ABC)" '
            "FROM my_table ORDER BY SUM(ABC)) anon_1 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]",
        )

        col = literal_column("SUM(ABC)").label("SUM(ABC)_")
        tbl = table("my_table")
        query = select(col).select_from(tbl).order_by(col).limit(100)

        self.assert_compile(
            query,
            'SELECT anon_1."SUM(ABC)_" FROM '
            '(SELECT SUM(ABC) AS "SUM(ABC)_" '
            "FROM my_table ORDER BY SUM(ABC)) anon_1 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]",
        )

        col = literal_column("SUM(ABC)").label(quoted_name("SUM(ABC)_", True))
        tbl = table("my_table")
        query = select(col).select_from(tbl).order_by(col).limit(100)

        self.assert_compile(
            query,
            'SELECT anon_1."SUM(ABC)_" FROM '
            '(SELECT SUM(ABC) AS "SUM(ABC)_" '
            "FROM my_table ORDER BY SUM(ABC)) anon_1 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]",
        )

    def test_for_update(self):
        table1 = table(
            "mytable", column("myid"), column("name"), column("description")
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                of=table1.c.myid
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 "
            "FOR UPDATE OF mytable.myid",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(nowait=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                nowait=True, of=table1.c.myid
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 "
            "FOR UPDATE OF mytable.myid NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                nowait=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE OF "
            "mytable.myid, mytable.name NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                skip_locked=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE OF "
            "mytable.myid, mytable.name SKIP LOCKED",
        )

        # key_share has no effect
        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(key_share=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE",
        )

        # read has no effect
        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, key_share=True
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE",
        )

        ta = table1.alias()
        self.assert_compile(
            ta.select(ta.c.myid == 7).with_for_update(
                of=[ta.c.myid, ta.c.name]
            ),
            "SELECT mytable_1.myid, mytable_1.name, mytable_1.description "
            "FROM mytable mytable_1 "
            "WHERE mytable_1.myid = :myid_1 FOR UPDATE OF "
            "mytable_1.myid, mytable_1.name",
        )

        # ensure of=text() for of works
        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, of=text("table1")
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE OF table1",
        )

        # ensure of=literal_column() for of works
        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, of=literal_column("table1")
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE OF table1",
        )

    def test_for_update_of_w_limit_adaption_col_present(self):
        table1 = table("mytable", column("myid"), column("name"))

        self.assert_compile(
            select(table1.c.myid, table1.c.name)
            .where(table1.c.myid == 7)
            .with_for_update(nowait=True, of=table1.c.name)
            .limit(10),
            "SELECT anon_1.myid, anon_1.name FROM "
            "(SELECT mytable.myid AS myid, mytable.name AS name "
            "FROM mytable WHERE mytable.myid = :myid_1) anon_1 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1] "
            "FOR UPDATE OF anon_1.name NOWAIT",
            checkparams={"param_1": 10, "myid_1": 7},
        )

    def test_for_update_of_w_limit_adaption_col_unpresent(self):
        table1 = table("mytable", column("myid"), column("name"))

        self.assert_compile(
            select(table1.c.myid)
            .where(table1.c.myid == 7)
            .with_for_update(nowait=True, of=table1.c.name)
            .limit(10),
            "SELECT anon_1.myid FROM "
            "(SELECT mytable.myid AS myid, mytable.name AS name "
            "FROM mytable WHERE mytable.myid = :myid_1) anon_1 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1] "
            "FOR UPDATE OF anon_1.name NOWAIT",
        )

    def test_for_update_of_w_limit_offset_adaption_col_present(self):
        table1 = table("mytable", column("myid"), column("name"))

        self.assert_compile(
            select(table1.c.myid, table1.c.name)
            .where(table1.c.myid == 7)
            .with_for_update(nowait=True, of=table1.c.name)
            .limit(10)
            .offset(50),
            "SELECT anon_1.myid, anon_1.name FROM "
            "(SELECT anon_2.myid AS myid, anon_2.name AS name, "
            "ROWNUM AS ora_rn "
            "FROM (SELECT mytable.myid AS myid, mytable.name AS name "
            "FROM mytable WHERE mytable.myid = :myid_1) anon_2 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_2] "
            "FOR UPDATE OF anon_1.name NOWAIT",
            checkparams={"param_1": 60, "param_2": 50, "myid_1": 7},
        )

    def test_for_update_of_w_limit_offset_adaption_col_unpresent(self):
        table1 = table("mytable", column("myid"), column("name"))

        self.assert_compile(
            select(table1.c.myid)
            .where(table1.c.myid == 7)
            .with_for_update(nowait=True, of=table1.c.name)
            .limit(10)
            .offset(50),
            "SELECT anon_1.myid FROM (SELECT anon_2.myid AS myid, "
            "ROWNUM AS ora_rn, anon_2.name AS name "
            "FROM (SELECT mytable.myid AS myid, mytable.name AS name "
            "FROM mytable WHERE mytable.myid = :myid_1) anon_2 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_2] "
            "FOR UPDATE OF anon_1.name NOWAIT",
            checkparams={"param_1": 60, "param_2": 50, "myid_1": 7},
        )

    def test_for_update_of_w_limit_offset_adaption_partial_col_unpresent(self):
        table1 = table("mytable", column("myid"), column("foo"), column("bar"))

        self.assert_compile(
            select(table1.c.myid, table1.c.bar)
            .where(table1.c.myid == 7)
            .with_for_update(nowait=True, of=[table1.c.foo, table1.c.bar])
            .limit(10)
            .offset(50),
            "SELECT anon_1.myid, anon_1.bar FROM (SELECT anon_2.myid AS myid, "
            "anon_2.bar AS bar, ROWNUM AS ora_rn, "
            "anon_2.foo AS foo FROM (SELECT mytable.myid AS myid, "
            "mytable.bar AS bar, "
            "mytable.foo AS foo FROM mytable "
            "WHERE mytable.myid = :myid_1) anon_2 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_2] "
            "FOR UPDATE OF anon_1.foo, anon_1.bar NOWAIT",
            checkparams={"param_1": 60, "param_2": 50, "myid_1": 7},
        )

    def test_limit_preserves_typing_information(self):
        class MyType(TypeDecorator):
            impl = Integer

        stmt = select(type_coerce(column("x"), MyType).label("foo")).limit(1)
        dialect = oracle.dialect()
        compiled = stmt.compile(dialect=dialect)
        assert isinstance(compiled._create_result_map()["foo"][-2], MyType)

    def test_use_binds_for_limits_disabled_one(self):
        t = table("sometable", column("col1"), column("col2"))
        with testing.expect_deprecated(
            "The ``use_binds_for_limits`` Oracle dialect parameter is "
            "deprecated."
        ):
            dialect = oracle.OracleDialect(use_binds_for_limits=False)

        self.assert_compile(
            select(t).limit(10),
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT sometable.col1 AS col1, "
            "sometable.col2 AS col2 FROM sometable) anon_1 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]",
            dialect=dialect,
        )

    def test_use_binds_for_limits_disabled_two(self):
        t = table("sometable", column("col1"), column("col2"))
        with testing.expect_deprecated(
            "The ``use_binds_for_limits`` Oracle dialect parameter is "
            "deprecated."
        ):
            dialect = oracle.OracleDialect(use_binds_for_limits=False)

        self.assert_compile(
            select(t).offset(10),
            "SELECT anon_1.col1, anon_1.col2 FROM (SELECT "
            "anon_2.col1 AS col1, anon_2.col2 AS col2, ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable) anon_2) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_1]",
            dialect=dialect,
        )

    def test_use_binds_for_limits_disabled_three(self):
        t = table("sometable", column("col1"), column("col2"))
        with testing.expect_deprecated(
            "The ``use_binds_for_limits`` Oracle dialect parameter is "
            "deprecated."
        ):
            dialect = oracle.OracleDialect(use_binds_for_limits=False)

        self.assert_compile(
            select(t).limit(10).offset(10),
            "SELECT anon_1.col1, anon_1.col2 FROM (SELECT "
            "anon_2.col1 AS col1, anon_2.col2 AS col2, ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable) anon_2 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_2]",
            dialect=dialect,
        )

    def test_use_binds_for_limits_enabled_one(self):
        t = table("sometable", column("col1"), column("col2"))
        with testing.expect_deprecated(
            "The ``use_binds_for_limits`` Oracle dialect parameter is "
            "deprecated."
        ):
            dialect = oracle.OracleDialect(use_binds_for_limits=True)

        self.assert_compile(
            select(t).limit(10),
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT sometable.col1 AS col1, "
            "sometable.col2 AS col2 FROM sometable) anon_1 WHERE ROWNUM "
            "<= [POSTCOMPILE_param_1]",
            dialect=dialect,
        )

    def test_use_binds_for_limits_enabled_two(self):
        t = table("sometable", column("col1"), column("col2"))
        with testing.expect_deprecated(
            "The ``use_binds_for_limits`` Oracle dialect parameter is "
            "deprecated."
        ):
            dialect = oracle.OracleDialect(use_binds_for_limits=True)

        self.assert_compile(
            select(t).offset(10),
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT anon_2.col1 AS col1, anon_2.col2 AS col2, "
            "ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable) anon_2) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_1]",
            dialect=dialect,
        )

    def test_use_binds_for_limits_enabled_three(self):
        t = table("sometable", column("col1"), column("col2"))
        with testing.expect_deprecated(
            "The ``use_binds_for_limits`` Oracle dialect parameter is "
            "deprecated."
        ):
            dialect = oracle.OracleDialect(use_binds_for_limits=True)

        self.assert_compile(
            select(t).limit(10).offset(10),
            "SELECT anon_1.col1, anon_1.col2 FROM "
            "(SELECT anon_2.col1 AS col1, anon_2.col2 AS col2, "
            "ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable) anon_2 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_2]",
            dialect=dialect,
            checkparams={"param_1": 20, "param_2": 10},
        )

    def test_long_labels_legacy_ident_length(self):
        dialect = default.DefaultDialect()
        dialect.max_identifier_length = 30

        ora_dialect = oracle.dialect(max_identifier_length=30)

        m = MetaData()
        a_table = Table(
            "thirty_characters_table_xxxxxx",
            m,
            Column("id", Integer, primary_key=True),
        )

        other_table = Table(
            "other_thirty_characters_table_",
            m,
            Column("id", Integer, primary_key=True),
            Column(
                "thirty_characters_table_id",
                Integer,
                ForeignKey("thirty_characters_table_xxxxxx.id"),
                primary_key=True,
            ),
        )

        anon = a_table.alias()
        self.assert_compile(
            select(other_table, anon)
            .select_from(other_table.outerjoin(anon))
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT other_thirty_characters_table_.id "
            "AS other_thirty_characters__1, "
            "other_thirty_characters_table_.thirty_char"
            "acters_table_id AS other_thirty_characters"
            "__2, thirty_characters_table__1.id AS "
            "thirty_characters_table__3 FROM "
            "other_thirty_characters_table_ LEFT OUTER "
            "JOIN thirty_characters_table_xxxxxx AS "
            "thirty_characters_table__1 ON "
            "thirty_characters_table__1.id = "
            "other_thirty_characters_table_.thirty_char"
            "acters_table_id",
            dialect=dialect,
        )
        self.assert_compile(
            select(other_table, anon)
            .select_from(other_table.outerjoin(anon))
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT other_thirty_characters_table_.id "
            "AS other_thirty_characters__1, "
            "other_thirty_characters_table_.thirty_char"
            "acters_table_id AS other_thirty_characters"
            "__2, thirty_characters_table__1.id AS "
            "thirty_characters_table__3 FROM "
            "other_thirty_characters_table_ LEFT OUTER "
            "JOIN thirty_characters_table_xxxxxx "
            "thirty_characters_table__1 ON "
            "thirty_characters_table__1.id = "
            "other_thirty_characters_table_.thirty_char"
            "acters_table_id",
            dialect=ora_dialect,
        )

    def _test_outer_join_fixture(self):
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        table2 = table(
            "myothertable",
            column("otherid", Integer),
            column("othername", String),
        )

        table3 = table(
            "thirdtable",
            column("userid", Integer),
            column("otherstuff", String),
        )
        return table1, table2, table3

    def test_outer_join_one(self):
        table1, table2, table3 = self._test_outer_join_fixture()

        query = (
            select(table1, table2)
            .where(
                or_(
                    table1.c.name == "fred",
                    table1.c.myid == 10,
                    table2.c.othername != "jack",
                    text("EXISTS (select yay from foo where boo = lar)"),
                )
            )
            .select_from(
                outerjoin(table1, table2, table1.c.myid == table2.c.otherid)
            )
        )
        self.assert_compile(
            query,
            "SELECT mytable.myid, mytable.name, "
            "mytable.description, myothertable.otherid,"
            " myothertable.othername FROM mytable, "
            "myothertable WHERE (mytable.name = "
            ":name_1 OR mytable.myid = :myid_1 OR "
            "myothertable.othername != :othername_1 OR "
            "EXISTS (select yay from foo where boo = "
            "lar)) AND mytable.myid = "
            "myothertable.otherid(+)",
            dialect=oracle.OracleDialect(use_ansi=False),
        )

    def test_outer_join_two(self):
        table1, table2, table3 = self._test_outer_join_fixture()

        query = table1.outerjoin(
            table2, table1.c.myid == table2.c.otherid
        ).outerjoin(table3, table3.c.userid == table2.c.otherid)
        self.assert_compile(
            query.select(),
            "SELECT mytable.myid, mytable.name, "
            "mytable.description, myothertable.otherid,"
            " myothertable.othername, "
            "thirdtable.userid, thirdtable.otherstuff "
            "FROM mytable LEFT OUTER JOIN myothertable "
            "ON mytable.myid = myothertable.otherid "
            "LEFT OUTER JOIN thirdtable ON "
            "thirdtable.userid = myothertable.otherid",
        )

    def test_outer_join_three(self):
        table1, table2, table3 = self._test_outer_join_fixture()

        query = table1.outerjoin(
            table2, table1.c.myid == table2.c.otherid
        ).outerjoin(table3, table3.c.userid == table2.c.otherid)

        self.assert_compile(
            query.select(),
            "SELECT mytable.myid, mytable.name, "
            "mytable.description, myothertable.otherid,"
            " myothertable.othername, "
            "thirdtable.userid, thirdtable.otherstuff "
            "FROM mytable, myothertable, thirdtable "
            "WHERE thirdtable.userid(+) = "
            "myothertable.otherid AND mytable.myid = "
            "myothertable.otherid(+)",
            dialect=oracle.dialect(use_ansi=False),
        )

    def test_outer_join_four(self):
        table1, table2, table3 = self._test_outer_join_fixture()

        query = table1.join(table2, table1.c.myid == table2.c.otherid).join(
            table3, table3.c.userid == table2.c.otherid
        )
        self.assert_compile(
            query.select(),
            "SELECT mytable.myid, mytable.name, "
            "mytable.description, myothertable.otherid,"
            " myothertable.othername, "
            "thirdtable.userid, thirdtable.otherstuff "
            "FROM mytable, myothertable, thirdtable "
            "WHERE thirdtable.userid = "
            "myothertable.otherid AND mytable.myid = "
            "myothertable.otherid",
            dialect=oracle.dialect(use_ansi=False),
        )

    def test_outer_join_five(self):
        table1, table2, table3 = self._test_outer_join_fixture()

        query = table1.join(
            table2, table1.c.myid == table2.c.otherid
        ).outerjoin(table3, table3.c.userid == table2.c.otherid)
        self.assert_compile(
            query.select().order_by(table1.c.name).limit(10).offset(5),
            "SELECT anon_1.myid, anon_1.name, anon_1.description, "
            "anon_1.otherid, "
            "anon_1.othername, anon_1.userid, anon_1.otherstuff FROM "
            "(SELECT anon_2.myid AS myid, anon_2.name AS name, "
            "anon_2.description AS description, anon_2.otherid AS otherid, "
            "anon_2.othername AS othername, anon_2.userid AS userid, "
            "anon_2.otherstuff AS otherstuff, ROWNUM AS "
            "ora_rn FROM (SELECT mytable.myid AS myid, "
            "mytable.name AS name, mytable.description "
            "AS description, myothertable.otherid AS "
            "otherid, myothertable.othername AS "
            "othername, thirdtable.userid AS userid, "
            "thirdtable.otherstuff AS otherstuff FROM "
            "mytable, myothertable, thirdtable WHERE "
            "thirdtable.userid(+) = "
            "myothertable.otherid AND mytable.myid = "
            "myothertable.otherid ORDER BY mytable.name) anon_2 "
            "WHERE ROWNUM <= [POSTCOMPILE_param_1]) anon_1 "
            "WHERE ora_rn > [POSTCOMPILE_param_2]",
            checkparams={"param_1": 15, "param_2": 5},
            dialect=oracle.dialect(use_ansi=False),
        )

    def test_outer_join_six(self):
        table1, table2, table3 = self._test_outer_join_fixture()

        subq = (
            select(table1)
            .select_from(
                table1.outerjoin(table2, table1.c.myid == table2.c.otherid)
            )
            .alias()
        )
        q = select(table3).select_from(
            table3.outerjoin(subq, table3.c.userid == subq.c.myid)
        )

        self.assert_compile(
            q,
            "SELECT thirdtable.userid, "
            "thirdtable.otherstuff FROM thirdtable "
            "LEFT OUTER JOIN (SELECT mytable.myid AS "
            "myid, mytable.name AS name, "
            "mytable.description AS description FROM "
            "mytable LEFT OUTER JOIN myothertable ON "
            "mytable.myid = myothertable.otherid) "
            "anon_1 ON thirdtable.userid = anon_1.myid",
            dialect=oracle.dialect(use_ansi=True),
        )

        self.assert_compile(
            q,
            "SELECT thirdtable.userid, "
            "thirdtable.otherstuff FROM thirdtable, "
            "(SELECT mytable.myid AS myid, "
            "mytable.name AS name, mytable.description "
            "AS description FROM mytable, myothertable "
            "WHERE mytable.myid = myothertable.otherid("
            "+)) anon_1 WHERE thirdtable.userid = "
            "anon_1.myid(+)",
            dialect=oracle.dialect(use_ansi=False),
        )

    def test_outer_join_seven(self):
        table1, table2, table3 = self._test_outer_join_fixture()

        q = select(table1.c.name).where(table1.c.name == "foo")
        self.assert_compile(
            q,
            "SELECT mytable.name FROM mytable WHERE " "mytable.name = :name_1",
            dialect=oracle.dialect(use_ansi=False),
        )

    def test_outer_join_eight(self):
        table1, table2, table3 = self._test_outer_join_fixture()
        subq = (
            select(table3.c.otherstuff)
            .where(table3.c.otherstuff == table1.c.name)
            .label("bar")
        )
        q = select(table1.c.name, subq)
        self.assert_compile(
            q,
            "SELECT mytable.name, (SELECT "
            "thirdtable.otherstuff FROM thirdtable "
            "WHERE thirdtable.otherstuff = "
            "mytable.name) AS bar FROM mytable",
            dialect=oracle.dialect(use_ansi=False),
        )

    def test_nonansi_plusses_everthing_in_the_condition(self):
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        table2 = table(
            "myothertable",
            column("otherid", Integer),
            column("othername", String),
        )

        stmt = select(table1).select_from(
            table1.outerjoin(
                table2,
                and_(
                    table1.c.myid == table2.c.otherid,
                    table2.c.othername > 5,
                    table1.c.name == "foo",
                ),
            )
        )
        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable, myothertable WHERE mytable.myid = "
            "myothertable.otherid(+) AND myothertable.othername(+) > "
            ":othername_1 AND mytable.name = :name_1",
            dialect=oracle.dialect(use_ansi=False),
        )

        stmt = select(table1).select_from(
            table1.outerjoin(
                table2,
                and_(
                    table1.c.myid == table2.c.otherid,
                    table2.c.othername == None,
                    table1.c.name == None,
                ),
            )
        )
        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable, myothertable WHERE mytable.myid = "
            "myothertable.otherid(+) AND myothertable.othername(+) IS NULL "
            "AND mytable.name IS NULL",
            dialect=oracle.dialect(use_ansi=False),
        )

    def test_nonansi_nested_right_join(self):
        a = table("a", column("a"))
        b = table("b", column("b"))
        c = table("c", column("c"))

        j = a.join(b.join(c, b.c.b == c.c.c), a.c.a == b.c.b)

        self.assert_compile(
            select(j),
            "SELECT a.a, b.b, c.c FROM a, b, c "
            "WHERE a.a = b.b AND b.b = c.c",
            dialect=oracle.OracleDialect(use_ansi=False),
        )

        j = a.outerjoin(b.join(c, b.c.b == c.c.c), a.c.a == b.c.b)

        self.assert_compile(
            select(j),
            "SELECT a.a, b.b, c.c FROM a, b, c "
            "WHERE a.a = b.b(+) AND b.b = c.c",
            dialect=oracle.OracleDialect(use_ansi=False),
        )

        j = a.join(b.outerjoin(c, b.c.b == c.c.c), a.c.a == b.c.b)

        self.assert_compile(
            select(j),
            "SELECT a.a, b.b, c.c FROM a, b, c "
            "WHERE a.a = b.b AND b.b = c.c(+)",
            dialect=oracle.OracleDialect(use_ansi=False),
        )

    def test_alias_outer_join(self):
        address_types = table("address_types", column("id"), column("name"))
        addresses = table(
            "addresses",
            column("id"),
            column("user_id"),
            column("address_type_id"),
            column("email_address"),
        )
        at_alias = address_types.alias()
        s = (
            select(at_alias, addresses)
            .select_from(
                addresses.outerjoin(
                    at_alias, addresses.c.address_type_id == at_alias.c.id
                )
            )
            .where(addresses.c.user_id == 7)
            .order_by(addresses.c.id, address_types.c.id)
        )
        self.assert_compile(
            s,
            "SELECT address_types_1.id, "
            "address_types_1.name, addresses.id AS id_1, "
            "addresses.user_id, addresses.address_type_"
            "id, addresses.email_address FROM "
            "addresses LEFT OUTER JOIN address_types "
            "address_types_1 ON addresses.address_type_"
            "id = address_types_1.id WHERE "
            "addresses.user_id = :user_id_1 ORDER BY "
            "addresses.id, address_types.id",
        )

    def test_returning_insert(self):
        t1 = table("t1", column("c1"), column("c2"), column("c3"))
        self.assert_compile(
            t1.insert().values(c1=1).returning(t1.c.c2, t1.c.c3),
            "INSERT INTO t1 (c1) VALUES (:c1) RETURNING "
            "t1.c2, t1.c3 INTO :ret_0, :ret_1",
        )

    def test_returning_insert_functional(self):
        t1 = table(
            "t1", column("c1"), column("c2", String()), column("c3", String())
        )
        fn = func.lower(t1.c.c2, type_=String())
        stmt = t1.insert().values(c1=1).returning(fn, t1.c.c3)
        compiled = stmt.compile(dialect=oracle.dialect())
        eq_(
            compiled._create_result_map(),
            {
                "c3": ("c3", (t1.c.c3, "c3", "c3"), t1.c.c3.type, 1),
                "lower": ("lower", (fn, "lower", None), fn.type, 0),
            },
        )

        self.assert_compile(
            stmt,
            "INSERT INTO t1 (c1) VALUES (:c1) RETURNING "
            "lower(t1.c2), t1.c3 INTO :ret_0, :ret_1",
        )

    def test_returning_insert_labeled(self):
        t1 = table("t1", column("c1"), column("c2"), column("c3"))
        self.assert_compile(
            t1.insert()
            .values(c1=1)
            .returning(t1.c.c2.label("c2_l"), t1.c.c3.label("c3_l")),
            "INSERT INTO t1 (c1) VALUES (:c1) RETURNING "
            "t1.c2, t1.c3 INTO :ret_0, :ret_1",
        )

    def test_returning_insert_computed(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            Column("bar", Integer, Computed("foo + 42")),
        )

        self.assert_compile(
            t1.insert().values(id=1, foo=5).returning(t1.c.bar),
            "INSERT INTO t1 (id, foo) VALUES (:id, :foo) "
            "RETURNING t1.bar INTO :ret_0",
        )

    def test_returning_update_computed_warning(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            Column("bar", Integer, Computed("foo + 42")),
        )

        with testing.expect_warnings(
            "Computed columns don't work with Oracle UPDATE"
        ):
            self.assert_compile(
                t1.update().values(id=1, foo=5).returning(t1.c.bar),
                "UPDATE t1 SET id=:id, foo=:foo RETURNING t1.bar INTO :ret_0",
            )

    def test_compound(self):
        t1 = table("t1", column("c1"), column("c2"), column("c3"))
        t2 = table("t2", column("c1"), column("c2"), column("c3"))
        self.assert_compile(
            union(t1.select(), t2.select()),
            "SELECT t1.c1, t1.c2, t1.c3 FROM t1 UNION "
            "SELECT t2.c1, t2.c2, t2.c3 FROM t2",
        )
        self.assert_compile(
            except_(t1.select(), t2.select()),
            "SELECT t1.c1, t1.c2, t1.c3 FROM t1 MINUS "
            "SELECT t2.c1, t2.c2, t2.c3 FROM t2",
        )

    def test_no_paren_fns(self):
        for fn, expected in [
            (func.uid(), "uid"),
            (func.UID(), "UID"),
            (func.sysdate(), "sysdate"),
            (func.row_number(), "row_number()"),
            (func.rank(), "rank()"),
            (func.now(), "CURRENT_TIMESTAMP"),
            (func.current_timestamp(), "CURRENT_TIMESTAMP"),
            (func.user(), "USER"),
        ]:
            self.assert_compile(fn, expected)

    def test_create_index_alt_schema(self):
        m = MetaData()
        t1 = Table("foo", m, Column("x", Integer), schema="alt_schema")
        self.assert_compile(
            schema.CreateIndex(Index("bar", t1.c.x)),
            "CREATE INDEX alt_schema.bar ON alt_schema.foo (x)",
        )

    def test_create_index_expr(self):
        m = MetaData()
        t1 = Table("foo", m, Column("x", Integer))
        self.assert_compile(
            schema.CreateIndex(Index("bar", t1.c.x > 5)),
            "CREATE INDEX bar ON foo (x > 5)",
        )

    def test_table_options(self):
        m = MetaData()

        t = Table(
            "foo",
            m,
            Column("x", Integer),
            prefixes=["GLOBAL TEMPORARY"],
            oracle_on_commit="PRESERVE ROWS",
        )

        self.assert_compile(
            schema.CreateTable(t),
            "CREATE GLOBAL TEMPORARY TABLE "
            "foo (x INTEGER) ON COMMIT PRESERVE ROWS",
        )

    def test_create_table_compress(self):
        m = MetaData()
        tbl1 = Table(
            "testtbl1", m, Column("data", Integer), oracle_compress=True
        )
        tbl2 = Table(
            "testtbl2", m, Column("data", Integer), oracle_compress="OLTP"
        )

        self.assert_compile(
            schema.CreateTable(tbl1),
            "CREATE TABLE testtbl1 (data INTEGER) COMPRESS",
        )
        self.assert_compile(
            schema.CreateTable(tbl2),
            "CREATE TABLE testtbl2 (data INTEGER) " "COMPRESS FOR OLTP",
        )

    def test_create_index_bitmap_compress(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))
        idx1 = Index("idx1", tbl.c.data, oracle_compress=True)
        idx2 = Index("idx2", tbl.c.data, oracle_compress=1)
        idx3 = Index("idx3", tbl.c.data, oracle_bitmap=True)

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX idx1 ON testtbl (data) COMPRESS",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX idx2 ON testtbl (data) COMPRESS 1",
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE BITMAP INDEX idx3 ON testtbl (data)",
        )

    @testing.combinations(
        ("no_persisted", "", "ignore"),
        ("persisted_none", "", None),
        ("persisted_false", " VIRTUAL", False),
        id_="iaa",
    )
    def test_column_computed(self, text, persisted):
        m = MetaData()
        kwargs = {"persisted": persisted} if persisted != "ignore" else {}
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, Computed("x + 2", **kwargs)),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (x INTEGER, y INTEGER GENERATED "
            "ALWAYS AS (x + 2)%s)" % text,
        )

    def test_column_computed_persisted_true(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, Computed("x + 2", persisted=True)),
        )
        assert_raises_message(
            exc.CompileError,
            r".*Oracle computed columns do not support 'stored' ",
            schema.CreateTable(t).compile,
            dialect=oracle.dialect(),
        )

    def test_column_identity(self):
        # all other tests are in test_identity_column.py
        m = MetaData()
        t = Table(
            "t",
            m,
            Column(
                "y",
                Integer,
                Identity(
                    always=True,
                    start=4,
                    increment=7,
                    nominvalue=True,
                    nomaxvalue=True,
                    cycle=False,
                    order=False,
                ),
            ),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (y INTEGER GENERATED ALWAYS AS IDENTITY "
            "(INCREMENT BY 7 START WITH 4 NOMINVALUE NOMAXVALUE "
            "NOORDER NOCYCLE))",
        )

    def test_column_identity_no_generated(self):
        m = MetaData()
        t = Table("t", m, Column("y", Integer, Identity(always=None)))
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (y INTEGER GENERATED  AS IDENTITY)",
        )

    @testing.combinations(
        (True, True, "ALWAYS ON NULL"),  # this would error when executed
        (False, None, "BY DEFAULT"),
        (False, False, "BY DEFAULT"),
        (False, True, "BY DEFAULT ON NULL"),
    )
    def test_column_identity_on_null(self, always, on_null, text):
        m = MetaData()
        t = Table(
            "t", m, Column("y", Integer, Identity(always, on_null=on_null))
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (y INTEGER GENERATED %s AS IDENTITY)" % text,
        )


class SequenceTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_basic(self):
        seq = Sequence("my_seq_no_schema")
        dialect = oracle.OracleDialect()
        assert (
            dialect.identifier_preparer.format_sequence(seq)
            == "my_seq_no_schema"
        )
        seq = Sequence("my_seq", schema="some_schema")
        assert (
            dialect.identifier_preparer.format_sequence(seq)
            == "some_schema.my_seq"
        )
        seq = Sequence("My_Seq", schema="Some_Schema")
        assert (
            dialect.identifier_preparer.format_sequence(seq)
            == '"Some_Schema"."My_Seq"'
        )

    def test_compile(self):
        self.assert_compile(
            ddl.CreateSequence(
                Sequence("my_seq", nomaxvalue=True, nominvalue=True)
            ),
            "CREATE SEQUENCE my_seq START WITH 1 NOMINVALUE NOMAXVALUE",
            dialect=oracle.OracleDialect(),
        )


class RegexpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "oracle"

    def setup_test(self):
        self.table = table(
            "mytable", column("myid", Integer), column("name", String)
        )

    def test_regexp_match(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern"),
            "REGEXP_LIKE(mytable.myid, :myid_1)",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_match_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_match(self.table.c.name),
            "REGEXP_LIKE(mytable.myid, mytable.name)",
            checkparams={},
        )

    def test_regexp_match_str(self):
        self.assert_compile(
            literal("string").regexp_match(self.table.c.name),
            "REGEXP_LIKE(:param_1, mytable.name)",
            checkparams={"param_1": "string"},
        )

    def test_regexp_match_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="ig"),
            "REGEXP_LIKE(mytable.myid, :myid_1, :myid_2)",
            checkparams={"myid_1": "pattern", "myid_2": "ig"},
        )

    def test_regexp_match_flags_col(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags=self.table.c.name),
            "REGEXP_LIKE(mytable.myid, :myid_1, mytable.name)",
            checkparams={"myid_1": "pattern"},
        )

    def test_not_regexp_match(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern"),
            "NOT REGEXP_LIKE(mytable.myid, :myid_1)",
            checkparams={"myid_1": "pattern"},
        )

    def test_not_regexp_match_column(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match(self.table.c.name),
            "NOT REGEXP_LIKE(mytable.myid, mytable.name)",
            checkparams={},
        )

    def test_not_regexp_match_str(self):
        self.assert_compile(
            ~literal("string").regexp_match(self.table.c.name),
            "NOT REGEXP_LIKE(:param_1, mytable.name)",
            checkparams={"param_1": "string"},
        )

    def test_not_regexp_match_flags_col(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match(
                "pattern", flags=self.table.c.name
            ),
            "NOT REGEXP_LIKE(mytable.myid, :myid_1, mytable.name)",
            checkparams={"myid_1": "pattern"},
        )

    def test_not_regexp_match_flags(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="ig"),
            "NOT REGEXP_LIKE(mytable.myid, :myid_1, :myid_2)",
            checkparams={"myid_1": "pattern", "myid_2": "ig"},
        )

    def test_regexp_replace(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace("pattern", "replacement"),
            "REGEXP_REPLACE(mytable.myid, :myid_1, :myid_2)",
            checkparams={"myid_1": "pattern", "myid_2": "replacement"},
        )

    def test_regexp_replace_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace("pattern", self.table.c.name),
            "REGEXP_REPLACE(mytable.myid, :myid_1, mytable.name)",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_replace_column2(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(self.table.c.name, "replacement"),
            "REGEXP_REPLACE(mytable.myid, mytable.name, :myid_1)",
            checkparams={"myid_1": "replacement"},
        )

    def test_regexp_replace_string(self):
        self.assert_compile(
            literal("string").regexp_replace("pattern", self.table.c.name),
            "REGEXP_REPLACE(:param_1, :param_2, mytable.name)",
            checkparams={"param_2": "pattern", "param_1": "string"},
        )

    def test_regexp_replace_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags="ig"
            ),
            "REGEXP_REPLACE(mytable.myid, :myid_1, :myid_3, :myid_2)",
            checkparams={
                "myid_1": "pattern",
                "myid_3": "replacement",
                "myid_2": "ig",
            },
        )

    def test_regexp_replace_flags_col(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags=self.table.c.name
            ),
            "REGEXP_REPLACE(mytable.myid, :myid_1, :myid_2, mytable.name)",
            checkparams={"myid_1": "pattern", "myid_2": "replacement"},
        )


class TableValuedFunctionTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "oracle"

    def test_scalar_alias_column(self):
        fn = func.scalar_strings(5)
        stmt = select(fn.alias().column)
        self.assert_compile(
            stmt,
            "SELECT COLUMN_VALUE anon_1 "
            "FROM TABLE (scalar_strings(:scalar_strings_1)) anon_1",
        )

    def test_column_valued(self):
        fn = func.scalar_strings(5)
        stmt = select(fn.column_valued())
        self.assert_compile(
            stmt,
            "SELECT COLUMN_VALUE anon_1 "
            "FROM TABLE (scalar_strings(:scalar_strings_1)) anon_1",
        )

    def test_table_valued(self):
        fn = func.three_pairs().table_valued("string1", "string2")
        stmt = select(fn.c.string1, fn.c.string2)
        self.assert_compile(
            stmt,
            "SELECT anon_1.string1, anon_1.string2 "
            "FROM TABLE (three_pairs()) anon_1",
        )
