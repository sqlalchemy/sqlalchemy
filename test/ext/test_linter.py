from sqlalchemy import select, Integer, event, testing
from sqlalchemy.ext import linter
from sqlalchemy.ext.linter import find_unmatching_froms
from sqlalchemy.testing import fixtures, expect_warnings
from sqlalchemy.testing.schema import Table, Column


class TestFinder(fixtures.TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("table_a", metadata, Column("col_a", Integer, primary_key=True))
        Table("table_b", metadata, Column("col_b", Integer, primary_key=True))
        Table("table_c", metadata, Column("col_c", Integer, primary_key=True))
        Table("table_d", metadata, Column("col_d", Integer, primary_key=True))

    def setup(self):
        self.a = self.tables.table_a
        self.b = self.tables.table_b
        self.c = self.tables.table_c
        self.d = self.tables.table_d

    def test_everything_is_connected(self):
        query = (
            select([self.a])
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
        query = (
            select([self.a])
            .where(self.b.c.col_b == 5)
        )
        froms, start = find_unmatching_froms(query, self.a)
        assert start == self.a
        assert froms == {self.b}

        froms, start = find_unmatching_froms(query, self.b)
        assert start == self.b
        assert froms == {self.a}

    def test_disconnect_between_ab_cd(self):
        query = (
            select([self.a])
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
            select([self.a])
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
            select([self.a])
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
        subq = select([self.a]).where(self.a.c.col_a == self.b.c.col_b).subquery()
        stmt = select([self.c]).select_from(subq)

        froms, start = find_unmatching_froms(stmt, self.c)
        assert start == self.c
        assert froms == {subq}

        froms, start = find_unmatching_froms(stmt, subq)
        assert start == subq
        assert froms == {self.c}

    def test_now_connect_it(self):
        subq = select([self.a]).where(self.a.c.col_a == self.b.c.col_b).subquery()
        stmt = select([self.c]).select_from(subq).where(self.c.c.col_c == subq.c.col_a)

        froms, start = find_unmatching_froms(stmt)
        assert not froms

        for start in self.c, subq:
            froms, start = find_unmatching_froms(stmt, start)
            assert not froms

    def test_right_nested_join_without_issue(self):
        query = (
            select([self.a])
            .select_from(
                self.a.join(self.b.join(self.c, self.b.c.col_b == self.c.c.col_c), self.a.c.col_a == self.b.c.col_b)
            )
        )
        froms, start = find_unmatching_froms(query)
        assert not froms

        for start in self.a, self.b, self.c:
            froms, start = find_unmatching_froms(query, start)
            assert not froms

    def test_right_nested_join_with_an_issue(self):
        query = (
            select([self.a])
            .select_from(
                self.a.join(
                    self.b.join(self.c, self.b.c.col_b == self.c.c.col_c),
                    self.a.c.col_a == self.b.c.col_b,
                ),
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


class TestLinter(fixtures.TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("table_a", metadata, Column("col_a", Integer, primary_key=True))
        Table("table_b", metadata, Column("col_b", Integer, primary_key=True))
        Table("table_c", metadata, Column("col_c", Integer, primary_key=True))
        Table("table_d", metadata, Column("col_d", Integer, primary_key=True))

    def setup(self):
        self.a = self.tables.table_a
        self.b = self.tables.table_b
        self.c = self.tables.table_c
        self.d = self.tables.table_d
        event.listen(testing.db, 'before_execute', linter.before_execute_hook)

    def test_integration(self):
        query = (
            select([self.a])
            .where(self.b.c.col_b == 5)
        )
        # TODO:
        #  - make it a unit by mocking or spying "find_unmatching_froms"
        #  - Make error string proper
        with expect_warnings(
            r"for stmt .* FROM elements .*table_.*col_.* "
            r"are not joined up to FROM element .*table_.*col_.*"
        ):
            with testing.db.connect() as conn:
                conn.execute(query)

    def teardown(self):
        event.remove(testing.db, 'before_execute', linter.before_execute_hook)
