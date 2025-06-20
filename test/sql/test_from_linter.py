from sqlalchemy import column
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import true
from sqlalchemy import update
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


def find_unmatching_froms(query, start=None):
    compiled = query.compile(linting=sql.COLLECT_CARTESIAN_PRODUCTS)

    return compiled.from_linter.lint(start)


class TestFindUnmatchingFroms(fixtures.TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("table_a", metadata, Column("col_a", Integer, primary_key=True))
        Table("table_b", metadata, Column("col_b", Integer, primary_key=True))
        Table("table_c", metadata, Column("col_c", Integer, primary_key=True))
        Table("table_d", metadata, Column("col_d", Integer, primary_key=True))

    def setup_test(self):
        self.a = self.tables.table_a
        self.b = self.tables.table_b
        self.c = self.tables.table_c
        self.d = self.tables.table_d

    @testing.variation(
        "what_to_clone", ["nothing", "fromclause", "whereclause", "both"]
    )
    def test_cloned_aliases(self, what_to_clone):
        a1 = self.a.alias()
        b1 = self.b.alias()
        c = self.c

        j1 = a1.join(b1, a1.c.col_a == b1.c.col_b)
        j1_from = j1
        b1_where = b1

        if what_to_clone.fromclause or what_to_clone.both:
            a1c = a1._clone()
            b1c = b1._clone()
            j1_from = a1c.join(b1c, a1c.c.col_a == b1c.c.col_b)

        if what_to_clone.whereclause or what_to_clone.both:
            b1_where = b1_where._clone()

        query = (
            select(c)
            .select_from(c, j1_from)
            .where(b1_where.c.col_b == c.c.col_c)
        )
        for start in None, c:
            froms, start = find_unmatching_froms(query, start)
            assert not froms

    def test_everything_is_connected(self):
        query = (
            select(self.a)
            .select_from(self.a.join(self.b, self.a.c.col_a == self.b.c.col_b))
            .select_from(self.c)
            .select_from(self.d)
            .where(self.d.c.col_d == self.b.c.col_b)
            .where(self.c.c.col_c == self.d.c.col_d)
            .where(self.c.c.col_c == 5)
        )
        froms, start = find_unmatching_froms(query)
        assert not froms

        for start in self.a, self.b, self.c, self.d:
            froms, start = find_unmatching_froms(query, start)
            assert not froms

    def test_plain_cartesian(self):
        query = select(self.a).where(self.b.c.col_b == 5)
        froms, start = find_unmatching_froms(query, self.a)
        assert start == self.a
        assert froms == {self.b}

        froms, start = find_unmatching_froms(query, self.b)
        assert start == self.b
        assert froms == {self.a}

    @testing.combinations(("lateral",), ("cartesian",), ("join",))
    def test_lateral_subqueries(self, control):
        """
        .. sourcecode:: sql

            test=> create table a (id integer);
            CREATE TABLE
            test=> create table b (id integer);
            CREATE TABLE
            test=> insert into a(id) values (1), (2), (3);
            INSERT 0 3
            test=> insert into b(id) values (1), (2), (3);
            INSERT 0 3

            test=> select * from (select id from a) as a1,
            lateral (select id from b where id=a1.id) as b1;
            id | id
            ----+----
            1 |  1
            2 |  2
            3 |  3
            (3 rows)

        """
        p1 = select(self.a).subquery()

        p2 = select(self.b).where(self.b.c.col_b == p1.c.col_a).subquery()

        if control == "lateral":
            p2 = p2.lateral()

        query = select(p1, p2)

        if control == "join":
            query = query.join_from(p1, p2, p1.c.col_a == p2.c.col_b)

        froms, start = find_unmatching_froms(query, p1)

        if control == "cartesian":
            assert start is p1
            assert froms == {p2}
        else:
            assert start is None
            assert froms is None

        froms, start = find_unmatching_froms(query, p2)

        if control == "cartesian":
            assert start is p2
            assert froms == {p1}
        else:
            assert start is None
            assert froms is None

    def test_lateral_subqueries_w_joins(self):
        p1 = select(self.a).subquery()
        p2 = (
            select(self.b)
            .where(self.b.c.col_b == p1.c.col_a)
            .subquery()
            .lateral()
        )
        p3 = (
            select(self.c)
            .where(self.c.c.col_c == p1.c.col_a)
            .subquery()
            .lateral()
        )

        query = select(p1, p2, p3).join_from(p1, p2, true()).join(p3, true())

        for p in (p1, p2, p3):
            froms, start = find_unmatching_froms(query, p)
            assert start is None
            assert froms is None

    def test_lateral_subqueries_ok_do_we_still_find_cartesians(self):
        p1 = select(self.a).subquery()

        p3 = select(self.a).subquery()

        p2 = select(self.b).where(self.b.c.col_b == p3.c.col_a).subquery()

        p2 = p2.lateral()

        query = select(p1, p2, p3)

        froms, start = find_unmatching_froms(query, p1)

        assert start is p1
        assert froms == {p2, p3}

        froms, start = find_unmatching_froms(query, p2)

        assert start is p2
        assert froms == {p1}

        froms, start = find_unmatching_froms(query, p3)

        assert start is p3
        assert froms == {p1}

    @testing.variation("additional_transformation", ["alias", "none"])
    @testing.variation("joins_implicitly", [True, False])
    @testing.variation(
        "type_", ["table_valued", "table_valued_derived", "column_valued"]
    )
    def test_fn_valued(
        self, joins_implicitly, additional_transformation, type_
    ):
        """test #7845, #9009"""

        my_table = table(
            "tbl",
            column("id", Integer),
            column("data", JSON()),
        )

        sub_dict = my_table.c.data["d"]

        if type_.table_valued or type_.table_valued_derived:
            tv = func.json_each(sub_dict)

            tv = tv.table_valued("key", joins_implicitly=joins_implicitly)

            if type_.table_valued_derived:
                tv = tv.render_derived(name="tv", with_types=True)

            if additional_transformation.alias:
                tv = tv.alias()

            has_key = tv.c.key == "f"
            stmt = select(my_table.c.id).where(has_key)
        elif type_.column_valued:
            tv = func.json_array_elements(sub_dict)

            if additional_transformation.alias:
                tv = tv.alias(joins_implicitly=joins_implicitly).column
            else:
                tv = tv.column_valued("key", joins_implicitly=joins_implicitly)

            stmt = select(my_table.c.id, tv)
        else:
            type_.fail()

        froms, start = find_unmatching_froms(stmt, my_table)

        if joins_implicitly:
            is_(start, None)
            is_(froms, None)
        elif type_.column_valued:
            assert start == my_table
            assert froms == {tv.scalar_alias}

        elif type_.table_valued or type_.table_valued_derived:
            assert start == my_table
            assert froms == {tv}
        else:
            type_.fail()

    def test_count_non_eq_comparison_operators(self):
        query = select(self.a).where(self.a.c.col_a > self.b.c.col_b)
        froms, start = find_unmatching_froms(query, self.a)
        is_(start, None)
        is_(froms, None)

    def test_dont_count_non_comparison_operators(self):
        query = select(self.a).where(self.a.c.col_a + self.b.c.col_b == 5)
        froms, start = find_unmatching_froms(query, self.a)
        assert start == self.a
        assert froms == {self.b}

    def test_disconnect_between_ab_cd(self):
        query = (
            select(self.a)
            .select_from(self.a.join(self.b, self.a.c.col_a == self.b.c.col_b))
            .select_from(self.c)
            .select_from(self.d)
            .where(self.c.c.col_c == self.d.c.col_d)
            .where(self.c.c.col_c == 5)
        )
        for start in self.a, self.b:
            froms, start = find_unmatching_froms(query, start)
            assert start == start
            assert froms == {self.c, self.d}
        for start in self.c, self.d:
            froms, start = find_unmatching_froms(query, start)
            assert start == start
            assert froms == {self.a, self.b}

    def test_c_and_d_both_disconnected(self):
        query = (
            select(self.a)
            .select_from(self.a.join(self.b, self.a.c.col_a == self.b.c.col_b))
            .where(self.c.c.col_c == 5)
            .where(self.d.c.col_d == 10)
        )
        for start in self.a, self.b:
            froms, start = find_unmatching_froms(query, start)
            assert start == start
            assert froms == {self.c, self.d}

        froms, start = find_unmatching_froms(query, self.c)
        assert start == self.c
        assert froms == {self.a, self.b, self.d}

        froms, start = find_unmatching_froms(query, self.d)
        assert start == self.d
        assert froms == {self.a, self.b, self.c}

    def test_now_connected(self):
        query = (
            select(self.a)
            .select_from(self.a.join(self.b, self.a.c.col_a == self.b.c.col_b))
            .select_from(self.c.join(self.d, self.c.c.col_c == self.d.c.col_d))
            .where(self.c.c.col_c == self.b.c.col_b)
            .where(self.c.c.col_c == 5)
            .where(self.d.c.col_d == 10)
        )
        froms, start = find_unmatching_froms(query)
        assert not froms

        for start in self.a, self.b, self.c, self.d:
            froms, start = find_unmatching_froms(query, start)
            assert not froms

    def test_disconnected_subquery(self):
        subq = (
            select(self.a).where(self.a.c.col_a == self.b.c.col_b).subquery()
        )
        stmt = select(self.c).select_from(subq)

        froms, start = find_unmatching_froms(stmt, self.c)
        assert start == self.c
        assert froms == {subq}

        froms, start = find_unmatching_froms(stmt, subq)
        assert start == subq
        assert froms == {self.c}

    def test_now_connect_it(self):
        subq = (
            select(self.a).where(self.a.c.col_a == self.b.c.col_b).subquery()
        )
        stmt = (
            select(self.c)
            .select_from(subq)
            .where(self.c.c.col_c == subq.c.col_a)
        )

        froms, start = find_unmatching_froms(stmt)
        assert not froms

        for start in self.c, subq:
            froms, start = find_unmatching_froms(stmt, start)
            assert not froms

    def test_right_nested_join_without_issue(self):
        query = select(self.a).select_from(
            self.a.join(
                self.b.join(self.c, self.b.c.col_b == self.c.c.col_c),
                self.a.c.col_a == self.b.c.col_b,
            )
        )
        froms, start = find_unmatching_froms(query)
        assert not froms

        for start in self.a, self.b, self.c:
            froms, start = find_unmatching_froms(query, start)
            assert not froms

    def test_join_on_true(self):
        # test that a join(a, b) counts a->b as an edge even if there isn't
        # actually a join condition.  this essentially allows a cartesian
        # product to be added explicitly.

        query = select(self.a).select_from(self.a.join(self.b, true()))
        froms, start = find_unmatching_froms(query)
        assert not froms

    def test_join_on_true_muti_levels(self):
        """test #6886"""
        # test that a join(a, b).join(c) counts b->c as an edge even if there
        # isn't actually a join condition.  this essentially allows a cartesian
        # product to be added explicitly.

        query = select(self.a, self.b, self.c).select_from(
            self.a.join(self.b, true()).join(self.c, true())
        )
        froms, start = find_unmatching_froms(query)
        assert not froms

    def test_right_nested_join_with_an_issue(self):
        query = (
            select(self.a)
            .select_from(
                self.a.join(
                    self.b.join(self.c, self.b.c.col_b == self.c.c.col_c),
                    self.a.c.col_a == self.b.c.col_b,
                )
            )
            .where(self.d.c.col_d == 5)
        )

        for start in self.a, self.b, self.c:
            froms, start = find_unmatching_froms(query, start)
            assert start == start
            assert froms == {self.d}

        froms, start = find_unmatching_froms(query, self.d)
        assert start == self.d
        assert froms == {self.a, self.b, self.c}

    def test_no_froms(self):
        query = select(1)

        froms, start = find_unmatching_froms(query)
        assert not froms

    @testing.variation("dml", ["update", "delete"])
    @testing.combinations(
        (False, False), (True, False), (True, True), argnames="twotable,error"
    )
    def test_dml(self, dml, twotable, error):
        if dml.update:
            stmt = update(self.a)
        elif dml.delete:
            stmt = delete(self.a)
        else:
            dml.fail()

        stmt = stmt.where(self.a.c.col_a == "a1")
        if twotable:
            stmt = stmt.where(self.b.c.col_b == "a1")

            if not error:
                stmt = stmt.where(self.b.c.col_b == self.a.c.col_a)

        froms, _ = find_unmatching_froms(stmt)
        if error:
            assert froms
        else:
            assert not froms


class TestLinterRoundTrip(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "table_a",
            metadata,
            Column("col_a", Integer, primary_key=True, autoincrement=False),
        )
        Table(
            "table_b",
            metadata,
            Column("col_b", Integer, primary_key=True, autoincrement=False),
        )

    @classmethod
    def setup_bind(cls):
        # from linting is enabled by default
        return config.db

    @testing.only_on("sqlite")
    def test_noop_for_unhandled_objects(self):
        with self.bind.connect() as conn:
            conn.exec_driver_sql("SELECT 1;").fetchone()

    def test_does_not_modify_query(self):
        with self.bind.connect() as conn:
            [result] = conn.execute(select(1)).fetchone()
            assert result == 1

    def test_warn_simple(self):
        a, b = self.tables("table_a", "table_b")
        query = select(a.c.col_a).where(b.c.col_b == 5)

        with expect_warnings(
            r"SELECT statement has a cartesian product between FROM "
            r'element\(s\) "table_[ab]" '
            r'and FROM element "table_[ba]"'
        ):
            with self.bind.connect() as conn:
                conn.execute(query)

    def test_warn_anon_alias(self):
        a, b = self.tables("table_a", "table_b")

        b_alias = b.alias()
        query = select(a.c.col_a).where(b_alias.c.col_b == 5)

        with expect_warnings(
            r"SELECT statement has a cartesian product between FROM "
            r'element\(s\) "table_(?:a|b_1)" '
            r'and FROM element "table_(?:a|b_1)"'
        ):
            with self.bind.connect() as conn:
                conn.execute(query)

    @testing.requires.ctes
    def test_warn_anon_cte(self):
        a, b = self.tables("table_a", "table_b")

        b_cte = select(b).cte()
        query = select(a.c.col_a).where(b_cte.c.col_b == 5)

        with expect_warnings(
            r"SELECT statement has a cartesian product between "
            r"FROM element\(s\) "
            r'"(?:anon_1|table_a)" '
            r'and FROM element "(?:anon_1|table_a)"'
        ):
            with self.bind.connect() as conn:
                conn.execute(query)

    @testing.variation(
        "dml",
        [
            ("update", testing.requires.update_from),
            ("delete", testing.requires.delete_using),
        ],
    )
    @testing.combinations(
        (False, False), (True, False), (True, True), argnames="twotable,error"
    )
    def test_warn_dml(self, dml, twotable, error):
        a, b = self.tables("table_a", "table_b")

        if dml.update:
            stmt = update(a).values(col_a=5)
        elif dml.delete:
            stmt = delete(a)
        else:
            dml.fail()

        stmt = stmt.where(a.c.col_a == 1)
        if twotable:
            stmt = stmt.where(b.c.col_b == 1)

            if not error:
                stmt = stmt.where(b.c.col_b == a.c.col_a)

        stmt_type = "UPDATE" if dml.update else "DELETE"

        with self.bind.connect() as conn:
            if error:
                with expect_warnings(
                    rf"{stmt_type} statement has a cartesian product between "
                    rf'FROM element\(s\) "table_[ab]" and FROM '
                    rf'element "table_[ab]"'
                ):
                    with self.bind.connect() as conn:
                        conn.execute(stmt)
            else:
                conn.execute(stmt)

    def test_no_linting(self, metadata, connection):
        eng = engines.testing_engine(
            options={"enable_from_linting": False, "use_reaper": False}
        )
        eng.pool = self.bind.pool  # needed for SQLite
        a, b = self.tables("table_a", "table_b")
        query = select(a.c.col_a).where(b.c.col_b == 5)

        with eng.connect() as conn:
            conn.execute(query)
