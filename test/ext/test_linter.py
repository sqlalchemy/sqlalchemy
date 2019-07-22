from sqlalchemy import select, Integer, event, testing
from sqlalchemy.ext import linter
from sqlalchemy.testing import fixtures, expect_warnings
from sqlalchemy.testing.schema import Table, Column


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

    def teardown(self):
        event.remove(testing.db, 'before_execute', linter.before_execute_hook)

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
        with testing.db.connect() as conn:
            conn.execute(query)

    def test_plain_cartesian(self):
        query = (
            select([self.a])
            .where(self.b.c.col_b == 5)
        )
        with expect_warnings(
            r"for stmt .* FROM elements .*table_b.*col_b.* "
            r"are not joined up to FROM element .*table_a.*col_a.*"
        ):
            with testing.db.connect() as conn:
                conn.execute(query)

    def test_disconnect_between_ab_cd(self):
        query = (
            select([self.a])
            .select_from(self.a.join(self.b, self.a.c.col_a == self.b.c.col_b))
            .select_from(self.c)
            .select_from(self.d)
            .where(self.c.c.col_c == self.d.c.col_d)
            .where(self.c.c.col_c == 5)
        )
        with expect_warnings(
            # TODO: Fix FROM element parts being undeterministic (impl uses set, no order is guaranteed)
            r"for stmt .* FROM elements .* "
            r"are not joined up to FROM element .*"
        ):
            with testing.db.connect() as conn:
                conn.execute(query)

    def test_c_and_d_both_disconnected(self):
        query = (
            select([self.a])
            .select_from(self.a.join(self.b, self.a.c.col_a == self.b.c.col_b))
            .where(self.c.c.col_c == 5)
            .where(self.d.c.col_d == 10)
        )
        with expect_warnings(
            # TODO: Fix FROM element parts being undeterministic (impl uses set, no order is guaranteed)
            r"for stmt .* FROM elements .* "
            r"are not joined up to FROM element .*"
        ):
            with testing.db.connect() as conn:
                conn.execute(query)

    def test_now_connected(self):
        query = (
            select([self.a])
            .select_from(self.a.join(self.b, self.a.c.col_a == self.b.c.col_b))
            .select_from(self.c.join(self.d, self.c.c.col_c == self.d.c.col_d))
            .where(self.c.c.col_c == self.b.c.col_b)
            .where(self.c.c.col_c == 5)
            .where(self.d.c.col_d == 10)
        )
        with testing.db.connect() as conn:
            conn.execute(query)

    def test_disconnected_subquery(self):
        subq = select([self.a]).where(self.a.c.col_a == self.b.c.col_b).subquery()
        stmt = select([self.c]).select_from(subq)
        with expect_warnings(
            # TODO: Fix subquery not being displayed properly
            r"for stmt .* FROM elements .* "
            r"are not joined up to FROM element .*table_c.*col_c.*"
        ):
            with testing.db.connect() as conn:
                conn.execute(stmt)

    def test_now_connect_it(self):
        subq = select([self.a]).where(self.a.c.col_a == self.b.c.col_b).subquery()
        stmt = select([self.c]).select_from(subq).where(self.c.c.col_c == subq.c.col_a)
        with testing.db.connect() as conn:
            conn.execute(stmt)

    def test_right_nested_join_without_issue(self):
        query = (
            select([self.a])
            .select_from(
                self.a.join(self.b.join(self.c, self.b.c.col_b == self.c.c.col_c), self.a.c.col_a == self.b.c.col_b)
            )
        )
        with testing.db.connect() as conn:
            conn.execute(query)

    def test_right_nested_join_with_an_issue(self):
        query = (
            select([self.a])
            .select_from(self.a.join(self.b.join(self.c, self.b.c.col_b == self.c.c.col_c), self.a.c.col_a == self.b.c.col_b))
            .where(self.d.c.col_d == 5)
        )
        with expect_warnings(
                # TODO: Fix FROM element parts being undeterministic (impl uses set, no order is guaranteed)
            r"for stmt .* FROM elements .* "
            r"are not joined up to FROM element .*"
        ):
            with testing.db.connect() as conn:
                conn.execute(query)