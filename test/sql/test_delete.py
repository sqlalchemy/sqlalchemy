#! coding:utf-8

from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import exc
from sqlalchemy import exists
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import default
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class _DeleteTestBase(object):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "mytable",
            metadata,
            Column("myid", Integer),
            Column("name", String(30)),
            Column("description", String(50)),
        )
        Table(
            "myothertable",
            metadata,
            Column("otherid", Integer),
            Column("othername", String(30)),
        )


class DeleteTest(_DeleteTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_delete_literal_binds(self):
        table1 = self.tables.mytable

        stmt = table1.delete().where(table1.c.name == "jill")

        self.assert_compile(
            stmt,
            "DELETE FROM mytable WHERE mytable.name = 'jill'",
            literal_binds=True,
        )

    def test_delete(self):
        table1 = self.tables.mytable

        self.assert_compile(
            delete(table1).where(table1.c.myid == 7),
            "DELETE FROM mytable WHERE mytable.myid = :myid_1",
        )

        self.assert_compile(
            table1.delete().where(table1.c.myid == 7),
            "DELETE FROM mytable WHERE mytable.myid = :myid_1",
        )

        self.assert_compile(
            table1.delete()
            .where(table1.c.myid == 7)
            .where(table1.c.name == "somename"),
            "DELETE FROM mytable "
            "WHERE mytable.myid = :myid_1 "
            "AND mytable.name = :name_1",
        )

    def test_where_empty(self):
        table1 = self.tables.mytable

        with expect_deprecated():
            self.assert_compile(
                table1.delete().where(and_()), "DELETE FROM mytable"
            )
        with expect_deprecated():
            self.assert_compile(
                table1.delete().where(or_()), "DELETE FROM mytable"
            )

    def test_prefix_with(self):
        table1 = self.tables.mytable

        stmt = (
            table1.delete()
            .prefix_with("A", "B", dialect="mysql")
            .prefix_with("C", "D")
        )

        self.assert_compile(stmt, "DELETE C D FROM mytable")

        self.assert_compile(
            stmt, "DELETE A B C D FROM mytable", dialect=mysql.dialect()
        )

    def test_alias(self):
        table1 = self.tables.mytable

        talias1 = table1.alias("t1")
        stmt = delete(talias1).where(talias1.c.myid == 7)

        self.assert_compile(
            stmt, "DELETE FROM mytable AS t1 WHERE t1.myid = :myid_1"
        )

    def test_non_correlated_select(self):
        table1, table2 = self.tables.mytable, self.tables.myothertable

        # test a non-correlated WHERE clause
        s = select(table2.c.othername).where(table2.c.otherid == 7)
        self.assert_compile(
            delete(table1).where(table1.c.name == s.scalar_subquery()),
            "DELETE FROM mytable "
            "WHERE mytable.name = ("
            "SELECT myothertable.othername "
            "FROM myothertable "
            "WHERE myothertable.otherid = :otherid_1"
            ")",
        )

    def test_correlated_select(self):
        table1, table2 = self.tables.mytable, self.tables.myothertable

        # test one that is actually correlated...
        s = select(table2.c.othername).where(table2.c.otherid == table1.c.myid)
        self.assert_compile(
            table1.delete().where(table1.c.name == s.scalar_subquery()),
            "DELETE FROM mytable "
            "WHERE mytable.name = ("
            "SELECT myothertable.othername "
            "FROM myothertable "
            "WHERE myothertable.otherid = mytable.myid"
            ")",
        )


class DeleteFromCompileTest(
    _DeleteTestBase, fixtures.TablesTest, AssertsCompiledSQL
):
    # DELETE FROM is also tested by individual dialects since there is no
    # consistent syntax.  here we use the StrSQLcompiler which has a fake
    # syntax.

    __dialect__ = "default_enhanced"

    def test_delete_extra_froms(self):
        table1, table2 = self.tables.mytable, self.tables.myothertable

        stmt = table1.delete().where(table1.c.myid == table2.c.otherid)
        self.assert_compile(
            stmt,
            "DELETE FROM mytable , myothertable "
            "WHERE mytable.myid = myothertable.otherid",
        )

    def test_correlation_to_extra(self):
        table1, table2 = self.tables.mytable, self.tables.myothertable

        stmt = (
            table1.delete()
            .where(table1.c.myid == table2.c.otherid)
            .where(
                ~exists()
                .where(table2.c.otherid == table1.c.myid)
                .where(table2.c.othername == "x")
                .correlate(table2)
            )
        )

        self.assert_compile(
            stmt,
            "DELETE FROM mytable , myothertable WHERE mytable.myid = "
            "myothertable.otherid AND NOT (EXISTS "
            "(SELECT * FROM mytable WHERE myothertable.otherid = "
            "mytable.myid AND myothertable.othername = :othername_1))",
        )

    def test_dont_correlate_to_extra(self):
        table1, table2 = self.tables.mytable, self.tables.myothertable

        stmt = (
            table1.delete()
            .where(table1.c.myid == table2.c.otherid)
            .where(
                ~exists()
                .where(table2.c.otherid == table1.c.myid)
                .where(table2.c.othername == "x")
                .correlate()
            )
        )

        self.assert_compile(
            stmt,
            "DELETE FROM mytable , myothertable WHERE mytable.myid = "
            "myothertable.otherid AND NOT (EXISTS "
            "(SELECT * FROM myothertable, mytable "
            "WHERE myothertable.otherid = "
            "mytable.myid AND myothertable.othername = :othername_1))",
        )

    def test_autocorrelate_error(self):
        table1, table2 = self.tables.mytable, self.tables.myothertable

        stmt = (
            table1.delete()
            .where(table1.c.myid == table2.c.otherid)
            .where(
                ~exists()
                .where(table2.c.otherid == table1.c.myid)
                .where(table2.c.othername == "x")
            )
        )

        assert_raises_message(
            exc.InvalidRequestError,
            ".*returned no FROM clauses due to auto-correlation.*",
            stmt.compile,
            dialect=default.StrCompileDialect(),
        )


class DeleteFromRoundTripTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "mytable",
            metadata,
            Column("myid", Integer),
            Column("name", String(30)),
            Column("description", String(50)),
        )
        Table(
            "myothertable",
            metadata,
            Column("otherid", Integer),
            Column("othername", String(30)),
        )
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
        )
        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("name", String(30), nullable=False),
            Column("email_address", String(50), nullable=False),
        )
        Table(
            "dingalings",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("address_id", None, ForeignKey("addresses.id")),
            Column("data", String(30)),
        )
        Table(
            "update_w_default",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
            Column("ycol", Integer, key="y"),
            Column("data", String(30), onupdate=lambda: "hi"),
        )

    @classmethod
    def fixtures(cls):
        return dict(
            users=(
                ("id", "name"),
                (7, "jack"),
                (8, "ed"),
                (9, "fred"),
                (10, "chuck"),
            ),
            addresses=(
                ("id", "user_id", "name", "email_address"),
                (1, 7, "x", "jack@bean.com"),
                (2, 8, "x", "ed@wood.com"),
                (3, 8, "x", "ed@bettyboop.com"),
                (4, 8, "x", "ed@lala.com"),
                (5, 9, "x", "fred@fred.com"),
            ),
            dingalings=(
                ("id", "address_id", "data"),
                (1, 2, "ding 1/2"),
                (2, 5, "ding 2/5"),
            ),
        )

    @testing.requires.delete_from
    def test_exec_two_table(self, connection):
        users, addresses = self.tables.users, self.tables.addresses
        dingalings = self.tables.dingalings

        connection.execute(dingalings.delete())  # fk violation otherwise

        connection.execute(
            addresses.delete()
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
        )

        expected = [
            (1, 7, "x", "jack@bean.com"),
            (5, 9, "x", "fred@fred.com"),
        ]
        self._assert_table(connection, addresses, expected)

    @testing.requires.delete_from
    def test_exec_three_table(self, connection):
        users = self.tables.users
        addresses = self.tables.addresses
        dingalings = self.tables.dingalings

        connection.execute(
            dingalings.delete()
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
            .where(addresses.c.id == dingalings.c.address_id)
        )

        expected = [(2, 5, "ding 2/5")]
        self._assert_table(connection, dingalings, expected)

    @testing.requires.delete_from
    def test_exec_two_table_plus_alias(self, connection):
        users, addresses = self.tables.users, self.tables.addresses
        dingalings = self.tables.dingalings

        connection.execute(dingalings.delete())  # fk violation otherwise
        a1 = addresses.alias()
        connection.execute(
            addresses.delete()
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
            .where(a1.c.id == addresses.c.id)
        )

        expected = [(1, 7, "x", "jack@bean.com"), (5, 9, "x", "fred@fred.com")]
        self._assert_table(connection, addresses, expected)

    @testing.requires.delete_from
    def test_exec_alias_plus_table(self, connection):
        users, addresses = self.tables.users, self.tables.addresses
        dingalings = self.tables.dingalings

        d1 = dingalings.alias()

        connection.execute(
            delete(d1)
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
            .where(addresses.c.id == d1.c.address_id)
        )

        expected = [(2, 5, "ding 2/5")]
        self._assert_table(connection, dingalings, expected)

    def _assert_table(self, connection, table, expected):
        stmt = table.select().order_by(table.c.id)
        eq_(connection.execute(stmt).fetchall(), expected)
