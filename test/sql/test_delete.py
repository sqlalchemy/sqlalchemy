#! coding:utf-8

from sqlalchemy import Integer, String, ForeignKey, delete, select, and_, \
    or_, exists
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import default
from sqlalchemy import testing
from sqlalchemy import exc
from sqlalchemy.testing import AssertsCompiledSQL, fixtures, eq_, \
    assert_raises_message
from sqlalchemy.testing.schema import Table, Column


class _DeleteTestBase(object):

    @classmethod
    def define_tables(cls, metadata):
        Table('mytable', metadata,
              Column('myid', Integer),
              Column('name', String(30)),
              Column('description', String(50)))
        Table('myothertable', metadata,
              Column('otherid', Integer),
              Column('othername', String(30)))


class DeleteTest(_DeleteTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_delete(self):
        table1 = self.tables.mytable

        self.assert_compile(
            delete(table1, table1.c.myid == 7),
            'DELETE FROM mytable WHERE mytable.myid = :myid_1')

        self.assert_compile(
            table1.delete().where(table1.c.myid == 7),
            'DELETE FROM mytable WHERE mytable.myid = :myid_1')

        self.assert_compile(
            table1.delete().
            where(table1.c.myid == 7).
            where(table1.c.name == 'somename'),
            'DELETE FROM mytable '
            'WHERE mytable.myid = :myid_1 '
            'AND mytable.name = :name_1')

    def test_where_empty(self):
        table1 = self.tables.mytable

        self.assert_compile(
            table1.delete().where(and_()),
            "DELETE FROM mytable"
        )
        self.assert_compile(
            table1.delete().where(or_()),
            "DELETE FROM mytable"
        )

    def test_prefix_with(self):
        table1 = self.tables.mytable

        stmt = table1.delete().\
            prefix_with('A', 'B', dialect='mysql').\
            prefix_with('C', 'D')

        self.assert_compile(stmt,
                            'DELETE C D FROM mytable')

        self.assert_compile(stmt,
                            'DELETE A B C D FROM mytable',
                            dialect=mysql.dialect())

    def test_alias(self):
        table1 = self.tables.mytable

        talias1 = table1.alias('t1')
        stmt = delete(talias1).where(talias1.c.myid == 7)

        self.assert_compile(
            stmt,
            'DELETE FROM mytable AS t1 WHERE t1.myid = :myid_1')

    def test_correlated(self):
        table1, table2 = self.tables.mytable, self.tables.myothertable

        # test a non-correlated WHERE clause
        s = select([table2.c.othername], table2.c.otherid == 7)
        self.assert_compile(delete(table1, table1.c.name == s),
                            'DELETE FROM mytable '
                            'WHERE mytable.name = ('
                            'SELECT myothertable.othername '
                            'FROM myothertable '
                            'WHERE myothertable.otherid = :otherid_1'
                            ')')

        # test one that is actually correlated...
        s = select([table2.c.othername], table2.c.otherid == table1.c.myid)
        self.assert_compile(table1.delete(table1.c.name == s),
                            'DELETE FROM mytable '
                            'WHERE mytable.name = ('
                            'SELECT myothertable.othername '
                            'FROM myothertable '
                            'WHERE myothertable.otherid = mytable.myid'
                            ')')


class DeleteFromCompileTest(
        _DeleteTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    # DELETE FROM is also tested by individual dialects since there is no
    # consistent syntax.  here we use the StrSQLcompiler which has a fake
    # syntax.

    __dialect__ = 'default_enhanced'

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

        stmt = table1.delete().where(
            table1.c.myid == table2.c.otherid).where(
                ~exists().where(table2.c.otherid == table1.c.myid).
                where(table2.c.othername == 'x').correlate(table2)
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

        stmt = table1.delete().where(
            table1.c.myid == table2.c.otherid).where(
                ~exists().where(table2.c.otherid == table1.c.myid).
                where(table2.c.othername == 'x').correlate()
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

        stmt = table1.delete().where(
            table1.c.myid == table2.c.otherid).where(
                ~exists().where(table2.c.otherid == table1.c.myid).
                where(table2.c.othername == 'x')
        )

        assert_raises_message(
            exc.InvalidRequestError,
            ".*returned no FROM clauses due to auto-correlation.*",
            stmt.compile, dialect=default.StrCompileDialect()
        )


class DeleteFromRoundTripTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('mytable', metadata,
              Column('myid', Integer),
              Column('name', String(30)),
              Column('description', String(50)))
        Table('myothertable', metadata,
              Column('otherid', Integer),
              Column('othername', String(30)))
        Table('users', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(30), nullable=False))
        Table('addresses', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('name', String(30), nullable=False),
              Column('email_address', String(50), nullable=False))
        Table('dingalings', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('address_id', None, ForeignKey('addresses.id')),
              Column('data', String(30)))
        Table('update_w_default', metadata,
              Column('id', Integer, primary_key=True),
              Column('x', Integer),
              Column('ycol', Integer, key='y'),
              Column('data', String(30), onupdate=lambda: "hi"))

    @classmethod
    def fixtures(cls):
        return dict(
            users=(
                ('id', 'name'),
                (7, 'jack'),
                (8, 'ed'),
                (9, 'fred'),
                (10, 'chuck')
            ),
            addresses=(
                ('id', 'user_id', 'name', 'email_address'),
                (1, 7, 'x', 'jack@bean.com'),
                (2, 8, 'x', 'ed@wood.com'),
                (3, 8, 'x', 'ed@bettyboop.com'),
                (4, 8, 'x', 'ed@lala.com'),
                (5, 9, 'x', 'fred@fred.com')
            ),
            dingalings=(
                ('id', 'address_id', 'data'),
                (1, 2, 'ding 1/2'),
                (2, 5, 'ding 2/5')
            ),
        )

    @testing.requires.delete_from
    def test_exec_two_table(self):
        users, addresses = self.tables.users, self.tables.addresses
        dingalings = self.tables.dingalings

        with testing.db.connect() as conn:
            conn.execute(dingalings.delete())  # fk violation otherwise

            conn.execute(
                addresses.delete().
                where(users.c.id == addresses.c.user_id).
                where(users.c.name == 'ed')
            )

            expected = [
                (1, 7, 'x', 'jack@bean.com'),
                (5, 9, 'x', 'fred@fred.com')
            ]
        self._assert_table(addresses, expected)

    @testing.requires.delete_from
    def test_exec_three_table(self):
        users = self.tables.users
        addresses = self.tables.addresses
        dingalings = self.tables.dingalings

        testing.db.execute(
            dingalings.delete().
            where(users.c.id == addresses.c.user_id).
            where(users.c.name == 'ed').
            where(addresses.c.id == dingalings.c.address_id))

        expected = [
            (2, 5, 'ding 2/5')
        ]
        self._assert_table(dingalings, expected)

    @testing.requires.delete_from
    def test_exec_two_table_plus_alias(self):
        users, addresses = self.tables.users, self.tables.addresses
        dingalings = self.tables.dingalings

        with testing.db.connect() as conn:
            conn.execute(dingalings.delete())  # fk violation otherwise
            a1 = addresses.alias()
            conn.execute(
                addresses.delete().
                where(users.c.id == addresses.c.user_id).
                where(users.c.name == 'ed').
                where(a1.c.id == addresses.c.id)
            )

        expected = [
            (1, 7, 'x', 'jack@bean.com'),
            (5, 9, 'x', 'fred@fred.com')
        ]
        self._assert_table(addresses, expected)

    @testing.requires.delete_from
    def test_exec_alias_plus_table(self):
        users, addresses = self.tables.users, self.tables.addresses
        dingalings = self.tables.dingalings

        d1 = dingalings.alias()

        testing.db.execute(
            delete(d1).
            where(users.c.id == addresses.c.user_id).
            where(users.c.name == 'ed').
            where(addresses.c.id == d1.c.address_id))

        expected = [
            (2, 5, 'ding 2/5')
        ]
        self._assert_table(dingalings, expected)

    def _assert_table(self, table, expected):
        stmt = table.select().order_by(table.c.id)
        eq_(testing.db.execute(stmt).fetchall(), expected)
